"""
Long-Term Memory (SQL Server)
Stores 1+ years of historical data, seasonal baselines, equipment lifecycle trends
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from typing import Dict, List, Optional

load_dotenv()

class LongTermMemory:
    """
    Long-term memory: 1+ years of historical patterns
    
    Used by agents for:
    - Seasonal baselines (summer vs winter patterns)
    - Equipment degradation trends
    - Long-term efficiency trends
    - Maintenance history and patterns
    - Compliance audit history
    """
    
    def __init__(self):
        self.conn_str = (
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={{{os.getenv('DB_PASSWORD')}}};"
            f"TrustServerCertificate=yes;"
        )
        
        print(f"✅ Long-term memory connected to {os.getenv('DB_NAME')}")
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.conn_str)
    
    def get_seasonal_baseline(
        self,
        month: int,
        metric: str = 'PUE'
    ) -> Dict:
        """
        Get seasonal baseline for a specific month
        
        Args:
            month: Month number (1-12)
            metric: Metric to retrieve (PUE, SystemEfficiencyKWPerTon, etc.)
        
        Returns:
            Baseline statistics for that month
        """
        
        conn = self.get_connection()
        
        query = f"""
            SELECT 
                AVG({metric}) AS Baseline,
                STDEV({metric}) AS StdDev,
                MIN({metric}) AS Best,
                MAX({metric}) AS Worst,
                COUNT(*) AS SampleSize
            FROM SystemPerformanceMetrics
            WHERE MONTH(Timestamp) = ?
        """
        
        df = pd.read_sql(query, conn, params=(month,))
        conn.close()
        
        if df.empty:
            return {}
        
        return df.iloc[0].to_dict()
    
    def get_equipment_lifecycle_data(
        self,
        equipment_id: str
    ) -> Dict:
        """
        Get complete equipment lifecycle data
        
        Args:
            equipment_id: Equipment identifier
        
        Returns:
            Installation date, total runtime, maintenance history
        """
        
        conn = self.get_connection()
        
        # Get equipment summary from operational tables
        if 'Chiller' in equipment_id:
            query_master = """
                SELECT
                    MIN(Timestamp)          AS FirstSeen,
                    MAX(Timestamp)          AS LastSeen,
                    AVG(PartLoadEfficiencyKWPerTon) AS AvgEfficiency,
                    AVG(LoadPercentage)             AS AvgLoadPercent,
                    MAX(RefrigerantType)            AS RefrigerantType,
                    COUNT(*)                AS TotalRecords
                FROM ChillerPerformanceMonitoring
                WHERE ChillerID = ?
            """
        elif 'Pump' in equipment_id:
            query_master = """
                SELECT
                    MIN(Timestamp)          AS FirstSeen,
                    MAX(Timestamp)          AS LastSeen,
                    AVG(PowerConsumptionKW) AS AvgPowerKW,
                    COUNT(*)                AS TotalRecords
                FROM PumpTelemetry
                WHERE PumpID = ?
            """
        else:
            conn.close()
            return {}

        df_master = pd.read_sql(query_master, conn, params=(equipment_id,))

        if df_master.empty:
            conn.close()
            return {}

        lifecycle = df_master.iloc[0].to_dict()
        
        # Get maintenance history
        query_maint = """
            SELECT 
                Timestamp,
                ServiceType,
                HoursAtService,
                CostSGD
            FROM MaintenanceLogs
            WHERE EquipmentID = ?
            ORDER BY Timestamp DESC
        """
        
        df_maint = pd.read_sql(query_maint, conn, params=(equipment_id,))
        lifecycle['maintenance_history'] = df_maint.to_dict('records')
        lifecycle['total_maintenance_events'] = len(df_maint)
        
        if not df_maint.empty:
            lifecycle['total_maintenance_cost'] = df_maint['CostSGD'].sum()
        
        conn.close()
        
        return lifecycle
    
    def get_efficiency_degradation_trend(
        self,
        equipment_id: str,
        months: int = 12
    ) -> pd.DataFrame:
        """
        Get efficiency degradation trend over time
        
        Args:
            equipment_id: Equipment identifier
            months: Months to analyze
        
        Returns:
            Monthly average efficiency over time
        """
        
        conn = self.get_connection()
        
        if 'Chiller' in equipment_id:
            query = """
                SELECT 
                    YEAR(Timestamp) AS Year,
                    MONTH(Timestamp) AS Month,
                    AVG(KWPerTon) AS AvgEfficiency,
                    AVG(LoadPercentage) AS AvgLoad,
                    COUNT(*) AS SampleSize
                FROM ChillerOperatingPoints
                WHERE ChillerID = ?
                  AND Timestamp >= DATEADD(MONTH, ?, (SELECT MAX(Timestamp) FROM ChillerOperatingPoints))
                GROUP BY YEAR(Timestamp), MONTH(Timestamp)
                ORDER BY Year, Month
            """
            
            df = pd.read_sql(query, conn, params=(equipment_id, -months))
        else:
            df = pd.DataFrame()
        
        conn.close()
        
        return df
    
    def get_historical_pue_trend(
        self,
        months: int = 12
    ) -> pd.DataFrame:
        """
        Get PUE trend over time
        
        Args:
            months: Months to retrieve
        
        Returns:
            Monthly PUE statistics
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT 
                YEAR(Timestamp) AS Year,
                MONTH(Timestamp) AS Month,
                AVG(PUE) AS AvgPUE,
                MIN(PUE) AS MinPUE,
                MAX(PUE) AS MaxPUE,
                AVG(WeatherNormalizedPUE) AS AvgWeatherNormalizedPUE
            FROM SystemPerformanceMetrics
            WHERE Timestamp >= DATEADD(MONTH, ?, (SELECT MAX(Timestamp) FROM SystemPerformanceMetrics))
            GROUP BY YEAR(Timestamp), MONTH(Timestamp)
            ORDER BY Year, Month
        """
        
        df = pd.read_sql(query, conn, params=(-months,))
        conn.close()
        
        return df
    
    def get_incident_history(
        self,
        equipment_type: Optional[str] = None,
        months: int = 12
    ) -> pd.DataFrame:
        """
        Get historical alarm/incident data
        
        Args:
            equipment_type: Filter by equipment type
            months: Months to retrieve
        
        Returns:
            Incident history
        """
        
        conn = self.get_connection()
        
        if equipment_type:
            query = """
                SELECT 
                    Timestamp,
                    EquipmentID,
                    EquipmentType,
                    AlarmCode,
                    AlarmDescription,
                    AlarmSeverity,
                    DATEDIFF(MINUTE, Timestamp, ClearedTime) AS DurationMinutes
                FROM EquipmentAlarms
                WHERE EquipmentType = ?
                  AND Timestamp >= DATEADD(MONTH, ?, (SELECT MAX(Timestamp) FROM EquipmentAlarms))
                ORDER BY Timestamp DESC
            """
            df = pd.read_sql(query, conn, params=(equipment_type, -months))
        else:
            query = """
                SELECT 
                    Timestamp,
                    EquipmentID,
                    EquipmentType,
                    AlarmCode,
                    AlarmDescription,
                    AlarmSeverity,
                    DATEDIFF(MINUTE, Timestamp, ClearedTime) AS DurationMinutes
                FROM EquipmentAlarms
                WHERE Timestamp >= DATEADD(MONTH, ?, (SELECT MAX(Timestamp) FROM EquipmentAlarms))
                ORDER BY Timestamp DESC
            """
            df = pd.read_sql(query, conn, params=(-months,))
        
        conn.close()
        
        return df
    
    def get_compliance_history(self) -> Dict:
        """
        Get compliance audit history
        
        Returns:
            Summary of compliance status
        """
        
        # In production, this would query a compliance audit table
        # For now, return placeholder
        
        return {
            'last_audit_date': 'N/A',
            'compliance_status': 'COMPLIANT',
            'areas_of_concern': []
        }
    
    def calculate_baseline_comparison(
        self,
        current_value: float,
        metric: str,
        month: int
    ) -> Dict:
        """
        Compare current value to seasonal baseline
        
        Args:
            current_value: Current metric value
            metric: Metric name
            month: Current month
        
        Returns:
            Comparison results
        """
        
        baseline = self.get_seasonal_baseline(month, metric)
        
        if not baseline:
            return {
                'comparison': 'NO_BASELINE',
                'deviation': None
            }
        
        baseline_value = baseline.get('Baseline', current_value)
        std_dev = baseline.get('StdDev', 0)
        
        deviation = current_value - baseline_value
        
        if std_dev > 0:
            z_score = deviation / std_dev
        else:
            z_score = 0
        
        if abs(z_score) < 1:
            status = 'NORMAL'
        elif abs(z_score) < 2:
            status = 'CAUTION'
        else:
            status = 'ABNORMAL'
        
        return {
            'current_value': current_value,
            'baseline': baseline_value,
            'deviation': deviation,
            'z_score': z_score,
            'status': status,
            'comparison': f"{'Above' if deviation > 0 else 'Below'} baseline by {abs(deviation):.3f}"
        }


# Singleton instance
long_term_memory = LongTermMemory()

# Test
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING LONG-TERM MEMORY")
    print("="*70)
    
    # Test 1: Seasonal baseline
    print("\n[TEST 1] Seasonal Baseline (current month)...")
    current_month = 10  # Data is from October 2025
    baseline = long_term_memory.get_seasonal_baseline(current_month, 'PUE')

    if baseline:
        print(f"  Baseline PUE: {baseline.get('Baseline') or 0:.3f}")
        print(f"  Best: {baseline.get('Best') or 0:.3f}")
        print(f"  Worst: {baseline.get('Worst') or 0:.3f}")
    
    # Test 2: Equipment lifecycle
    print("\n[TEST 2] Equipment Lifecycle Data...")
    lifecycle = long_term_memory.get_equipment_lifecycle_data('Chiller-1')
    
    if lifecycle:
        print(f"  Install Date: {lifecycle.get('InstallDate')}")
        print(f"  Maintenance Events: {lifecycle.get('total_maintenance_events', 0)}")
    
    # Test 3: PUE trend
    print("\n[TEST 3] PUE Trend (last 6 months)...")
    pue_trend = long_term_memory.get_historical_pue_trend(months=6)
    
    if not pue_trend.empty:
        print(f"  Data points: {len(pue_trend)}")
        print(pue_trend[['Year', 'Month', 'AvgPUE']].head())
    
    print("\n✅ Long-term memory tests complete!")