"""
Live Data Fetcher
Queries AOM-Dev SQL Server for real-time system context
Replaces hardcoded values across all agents
"""

import pyodbc
import os
from typing import Dict, List, Optional
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()


class LiveDataFetcher:
    """
    Fetches live system context from AOM-Dev database
    Used by orchestrator and agents to get real telemetry
    """

    def __init__(self, connection_string: Optional[str] = None):

        if connection_string is None:
            driver   = os.getenv('AZURE_DRIVER', os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'))
            server   = os.getenv('AZURE_SQL_SERVER', os.getenv('DB_SERVER', r'localhost\SQLEXPRESS'))
            database = os.getenv('AZURE_SQL_DATABASE', os.getenv('DB_NAME', 'AOM-Dev'))
            user     = os.getenv('AZURE_SQL_USER', os.getenv('DB_USER'))
            password = os.getenv('AZURE_SQL_PWD', os.getenv('DB_PASSWORD'))
            if user and password:
                if ';' in password or '{' in password or '}' in password:
                    password = '{' + password + '}'
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={user};"
                    f"PWD={password};"
                    f"TrustServerCertificate=yes;"
                )
            else:
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Trusted_Connection=yes;"
                )
        self.connection_string = connection_string

    def _get_connection(self):
        return pyodbc.connect(self.connection_string)

    def get_current_context(self) -> Dict:
        """
        Build full system context from latest DB records.
        This is the main input to orchestrator.analyze_and_propose()

        Returns:
            Context dict matching orchestrator expected format
        """

        try:
            conn = self._get_connection()

            facility     = self._fetch_latest(conn, "FacilityPower")
            weather      = self._fetch_latest(conn, "WeatherConditions")
            system_perf  = self._fetch_latest(conn, "SystemPerformanceMetrics")
            chillers     = self._fetch_chillers_online(conn)
            chiller_health = self._fetch_chiller_health(conn)
            maintenance  = self._fetch_maintenance_status(conn)

            conn.close()

            context = {
                # Power & load
                'total_facility_power_kw': facility.get('TotalFacilityPowerKW'),
                'it_load_kw':              facility.get('ITLoadPowerKW'),
                'cooling_system_power_kw': facility.get('CoolingSystemPowerKW'),
                'current_pue':             facility.get('PUE'),

                # Cooling system
                'cooling_load_kw':         system_perf.get('TotalCoolingLoadKW'),
                'cooling_load_tons':        system_perf.get('TotalCoolingLoadTons'),
                'plant_efficiency_kw_per_ton': system_perf.get('PlantEfficiencyKWPerTon'),
                'plant_cop':               system_perf.get('PlantCOP'),
                'current_wue':             system_perf.get('WUE'),
                'carbon_emissions_kg':     system_perf.get('TotalCarbonEmissionsKgCO2'),
                'economizer_enabled':      bool(system_perf.get('EconomizerMode', 0)),
                'redundancy_level':        system_perf.get('RedundancyLevel'),
                'chillers_online':         chillers,
                'chillers_online_count':   len(chillers),

                # Weather
                'wet_bulb_temp':           weather.get('WetBulbTempCelsius'),
                'dry_bulb_temp':           weather.get('OutdoorTempCelsius'),
                'humidity_percent':        weather.get('RelativeHumidityPercent'),
                'dew_point_celsius':       weather.get('DewPointCelsius'),

                # Equipment health (for maintenance agent)
                'chiller_health':          chiller_health,

                # Maintenance (for maintenance agent)
                'maintenance_status':      maintenance,

                'timestamp': datetime.now().isoformat()
            }

            return context

        except Exception as e:
            print(f"  Warning: LiveDataFetcher error: {e}")
            return self._fallback_context()

    def get_current_metrics(self) -> Dict:
        """
        Flat metrics dict for base_agent.get_current_metrics()
        Returns latest values from SystemPerformanceMetrics + FacilityPower
        """

        try:
            conn = self._get_connection()
            facility    = self._fetch_latest(conn, "FacilityPower")
            system_perf = self._fetch_latest(conn, "SystemPerformanceMetrics")
            conn.close()

            return {
                'total_facility_power_kw':     facility.get('TotalFacilityPowerKW'),
                'it_load_kw':                  facility.get('ITLoadPowerKW'),
                'cooling_system_power_kw':     facility.get('CoolingSystemPowerKW'),
                'current_pue':                 facility.get('PUE'),
                'cooling_load_kw':             system_perf.get('TotalCoolingLoadKW'),
                'cooling_load_tons':            system_perf.get('TotalCoolingLoadTons'),
                'plant_efficiency_kw_per_ton': system_perf.get('PlantEfficiencyKWPerTon'),
                'wue':                         system_perf.get('WUE'),
                'carbon_emissions_kg':         system_perf.get('TotalCarbonEmissionsKgCO2'),
                'timestamp':                   datetime.now().isoformat()
            }

        except Exception as e:
            print(f"  Warning: get_current_metrics error: {e}")
            return {}

    def get_chiller_telemetry(self, chiller_id: Optional[str] = None) -> List[Dict]:
        """Latest telemetry for all or specific chiller"""

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if chiller_id:
                cursor.execute("""
                    SELECT TOP 1 * FROM ChillerTelemetry
                    WHERE ChillerID = ?
                    ORDER BY Timestamp DESC
                """, (chiller_id,))
            else:
                cursor.execute("""
                    SELECT t.* FROM ChillerTelemetry t
                    INNER JOIN (
                        SELECT ChillerID, MAX(Timestamp) AS MaxTS
                        FROM ChillerTelemetry
                        GROUP BY ChillerID
                    ) latest ON t.ChillerID = latest.ChillerID AND t.Timestamp = latest.MaxTS
                    ORDER BY t.ChillerID
                """)

            cols = [c[0] for c in cursor.description]
            rows = [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
            conn.close()
            return rows

        except Exception as e:
            print(f"  Warning: get_chiller_telemetry error: {e}")
            return []

    def get_pump_telemetry(self, pump_id: Optional[str] = None) -> List[Dict]:
        """Latest telemetry for all pumps or a specific pump"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if pump_id:
                cursor.execute("""
                    SELECT TOP 1 * FROM PumpTelemetry
                    WHERE PumpID = ?
                    ORDER BY Timestamp DESC
                """, (pump_id,))
            else:
                cursor.execute("""
                    SELECT t.* FROM PumpTelemetry t
                    INNER JOIN (
                        SELECT PumpID, MAX(Timestamp) AS MaxTS
                        FROM PumpTelemetry
                        GROUP BY PumpID
                    ) latest ON t.PumpID = latest.PumpID AND t.Timestamp = latest.MaxTS
                    ORDER BY t.PumpID
                """)
            cols = [c[0] for c in cursor.description]
            rows = [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            print(f"  Warning: get_pump_telemetry error: {e}")
            return []

    def get_tower_telemetry(self, tower_id: Optional[str] = None) -> List[Dict]:
        """Latest telemetry for all cooling towers or a specific tower"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if tower_id:
                cursor.execute("""
                    SELECT TOP 1 * FROM CoolingTowerTelemetry
                    WHERE TowerID = ?
                    ORDER BY Timestamp DESC
                """, (tower_id,))
            else:
                cursor.execute("""
                    SELECT t.* FROM CoolingTowerTelemetry t
                    INNER JOIN (
                        SELECT TowerID, MAX(Timestamp) AS MaxTS
                        FROM CoolingTowerTelemetry
                        GROUP BY TowerID
                    ) latest ON t.TowerID = latest.TowerID AND t.Timestamp = latest.MaxTS
                    ORDER BY t.TowerID
                """)
            cols = [c[0] for c in cursor.description]
            rows = [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            print(f"  Warning: get_tower_telemetry error: {e}")
            return []

    def get_it_load_by_hour(self, days: int = 30) -> Dict[int, float]:
        """
        Get average IT load (kW) per hour of day from the last N days.
        Used by DemandConditionsAgent to replace hardcoded hourly patterns.
        Returns a dict {hour(0-23): avg_kw} — empty dict on failure.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DATEPART(hour, Timestamp) AS Hour,
                       AVG(CAST(ITLoadPowerKW AS float)) AS AvgLoad,
                       COUNT(*) AS SampleCount
                FROM FacilityPower
                WHERE Timestamp >= DATEADD(DAY, ?, GETDATE())
                  AND ITLoadPowerKW IS NOT NULL
                  AND ITLoadPowerKW > 0
                GROUP BY DATEPART(hour, Timestamp)
                ORDER BY Hour
            """, (-days,))
            result = {}
            for row in cursor.fetchall():
                hour, avg_load, sample_count = row
                if avg_load is not None and sample_count >= 3:
                    result[int(hour)] = float(avg_load)
            conn.close()
            return result
        except Exception as e:
            print(f"  Warning: get_it_load_by_hour error: {e}")
            return {}

    def get_similar_conditions(
        self,
        cooling_load_kw: float,
        wet_bulb_temp: float,
        days: int = 30,
        tolerance_pct: float = 15.0
    ) -> List[Dict]:
        """
        Find historical records with similar operating conditions
        Used by medium_term_memory.get_similar_operating_conditions()
        """

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            load_min = cooling_load_kw * (1 - tolerance_pct / 100)
            load_max = cooling_load_kw * (1 + tolerance_pct / 100)
            wb_min   = wet_bulb_temp - 2.0
            wb_max   = wet_bulb_temp + 2.0

            cursor.execute("""
                SELECT TOP 50
                    s.Timestamp,
                    s.TotalCoolingLoadKW,
                    s.PUE,
                    s.PlantEfficiencyKWPerTon,
                    s.PlantCOP,
                    s.WUE,
                    w.WetBulbTempCelsius,
                    w.OutdoorTempCelsius
                FROM SystemPerformanceMetrics s
                INNER JOIN WeatherConditions w
                    ON ABS(DATEDIFF(MINUTE, s.Timestamp, w.Timestamp)) < 30
                WHERE s.Timestamp >= DATEADD(DAY, ?, GETDATE())
                  AND s.TotalCoolingLoadKW BETWEEN ? AND ?
                  AND w.WetBulbTempCelsius BETWEEN ? AND ?
                ORDER BY s.Timestamp DESC
            """, (-days, load_min, load_max, wb_min, wb_max))

            cols = [c[0] for c in cursor.description]
            rows = [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
            conn.close()
            return rows

        except Exception as e:
            print(f"  Warning: get_similar_conditions error: {e}")
            return []

    # ── private helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _clean(value):
        """Convert Decimal→float and datetime→ISO string for JSON safety"""
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _clean_row(self, row: Dict) -> Dict:
        """Apply _clean to every value in a dict"""
        return {k: self._clean(v) for k, v in row.items()}

    def _fetch_latest(self, conn, table: str) -> Dict:
        """Fetch most recent row from a table"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT TOP 1 * FROM {table} ORDER BY Timestamp DESC")
            cols = [c[0] for c in cursor.description]
            row  = cursor.fetchone()
            return self._clean_row(dict(zip(cols, row))) if row else {}
        except Exception as e:
            print(f"  Warning: _fetch_latest({table}) error: {e}")
            return {}

    def _fetch_chillers_online(self, conn) -> List[str]:
        """Get list of currently running chiller IDs"""
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.ChillerID FROM ChillerTelemetry t
                INNER JOIN (
                    SELECT ChillerID, MAX(Timestamp) AS MaxTS
                    FROM ChillerTelemetry
                    GROUP BY ChillerID
                ) latest ON t.ChillerID = latest.ChillerID AND t.Timestamp = latest.MaxTS
                WHERE t.RunningStatus IN (1, '1', 'ON', 'Running', 'TRUE', 'true', 'on')
            """)
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"  Warning: _fetch_chillers_online error: {e}")
            return []

    def _fetch_chiller_health(self, conn) -> List[Dict]:
        """Get latest health indicators per chiller"""
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.ChillerID,
                       t.VibrationMmS,
                       t.BearingTempCelsius,
                       t.OilPressureBar,
                       t.OilTempCelsius,
                       t.RuntimeHoursSinceService,
                       t.RuntimeHoursTotal,
                       t.ActiveAlarms,
                       t.RunningStatus
                FROM ChillerTelemetry t
                INNER JOIN (
                    SELECT ChillerID, MAX(Timestamp) AS MaxTS
                    FROM ChillerTelemetry
                    GROUP BY ChillerID
                ) latest ON t.ChillerID = latest.ChillerID AND t.Timestamp = latest.MaxTS
                ORDER BY t.ChillerID
            """)
            cols = [c[0] for c in cursor.description]
            return [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
        except Exception as e:
            print(f"  Warning: _fetch_chiller_health error: {e}")
            return []

    def _fetch_maintenance_status(self, conn) -> List[Dict]:
        """Get upcoming/overdue maintenance from EquipmentRegistry"""
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EquipmentID, EquipmentType, LastServiceDate,
                       NextServiceDueDate, ServiceIntervalHours, Status
                FROM EquipmentRegistry
                WHERE NextServiceDueDate IS NOT NULL
                ORDER BY NextServiceDueDate ASC
            """)
            cols = [c[0] for c in cursor.description]
            return [self._clean_row(dict(zip(cols, row))) for row in cursor.fetchall()]
        except Exception as e:
            print(f"  Warning: _fetch_maintenance_status error: {e}")
            return []

    def _fallback_context(self) -> Dict:
        """Fallback hardcoded context if DB unavailable"""
        return {
            'cooling_load_kw': 2800,
            'it_load_kw': 9500,
            'total_facility_power_kw': 11800,
            'wet_bulb_temp': 25.5,
            'dry_bulb_temp': 31.0,
            'humidity_percent': 78,
            'chillers_online': ['Chiller-1', 'Chiller-2'],
            'current_pue': 1.24,
            'economizer_enabled': False,
            'timestamp': datetime.now().isoformat()
        }


# Singleton
live_data = LiveDataFetcher()
