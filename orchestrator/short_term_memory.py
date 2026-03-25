"""
Short-Term Memory (SQL Server)
Stores last 24 hours of operational state, recent decisions, active constraints
Fast queries for real-time agent decision-making
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from typing import Dict, List, Optional, Any
import json

load_dotenv()

class ShortTermMemory:
    """
    Short-term memory: Last 24 hours of operational data
    
    Used by agents for:
    - Current system state
    - Recent decisions (last few hours)
    - Active alarms and constraints
    - Latest equipment performance
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
        
        print(f"✅ Short-term memory connected to {os.getenv('DB_NAME')}")
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.conn_str)
    
    def get_latest_system_state(self) -> Dict:
        """
        Get the most recent complete system state
        
        Returns:
            Dictionary with latest metrics from all subsystems
        """
        
        conn = self.get_connection()
        
        # Get latest system performance metrics
        query_system = """
            SELECT TOP 1
                Timestamp,
                TotalChillerPowerKW,
                TotalCoolingLoadTons,
                TotalCoolingLoadKW,
                SystemEfficiencyKWPerTon,
                SystemCOP,
                ITLoadPowerKW,
                PUE,
                ChillersOnline,
                RedundancyLevel,
                OutdoorWetBulbC
            FROM SystemPerformanceMetrics
            ORDER BY Timestamp DESC
        """
        
        df_system = pd.read_sql(query_system, conn)
        
        if df_system.empty:
            conn.close()
            return {}
        
        system_state = df_system.iloc[0].to_dict()
        
        # Get active chillers status
        query_chillers = """
            SELECT TOP 3
                ChillerID,
                LoadPercentage,
                KWPerTon,
                COP,
                CHWSTHeaderTempC,
                CWSTHeaderTempC,
                RunningStatus
            FROM ChillerOperatingPoints
            WHERE Timestamp = (SELECT MAX(Timestamp) FROM ChillerOperatingPoints)
            ORDER BY ChillerID
        """
        
        df_chillers = pd.read_sql(query_chillers, conn)
        system_state['active_chillers'] = df_chillers.to_dict('records')
        
        # Get latest weather
        query_weather = """
            SELECT TOP 1
                WetBulbTempCelsius,
                OutdoorTempCelsius,
                RelativeHumidityPercent
            FROM WeatherConditions
            ORDER BY Timestamp DESC
        """
        
        df_weather = pd.read_sql(query_weather, conn)
        if not df_weather.empty:
            system_state['weather'] = df_weather.iloc[0].to_dict()
        
        conn.close()
        
        return system_state
    
    def get_active_alarms(self, severity: Optional[str] = None) -> List[Dict]:
        """
        Get currently active equipment alarms
        
        Args:
            severity: Filter by severity (CRITICAL, WARNING)
        
        Returns:
            List of active alarms
        """
        
        conn = self.get_connection()
        
        if severity:
            query = """
                SELECT 
                    ID,
                    Timestamp,
                    EquipmentID,
                    EquipmentType,
                    AlarmCode,
                    AlarmDescription,
                    AlarmSeverity,
                    TriggeredValue,
                    ThresholdValue,
                    Unit
                FROM EquipmentAlarms
                WHERE AlarmStatus = 'ACTIVE'
                  AND AlarmSeverity = ?
                ORDER BY Timestamp DESC
            """
            df = pd.read_sql(query, conn, params=(severity,))
        else:
            query = """
                SELECT 
                    ID,
                    Timestamp,
                    EquipmentID,
                    EquipmentType,
                    AlarmCode,
                    AlarmDescription,
                    AlarmSeverity,
                    TriggeredValue,
                    ThresholdValue,
                    Unit
                FROM EquipmentAlarms
                WHERE AlarmStatus = 'ACTIVE'
                ORDER BY AlarmSeverity DESC, Timestamp DESC
            """
            df = pd.read_sql(query, conn)
        
        conn.close()
        
        return df.to_dict('records')
    
    def get_recent_decisions(self, hours: int = 4) -> List[Dict]:
        """
        Get agent decisions from last N hours
        
        Args:
            hours: Hours to look back
        
        Returns:
            Recent decisions with outcomes
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT 
                DecisionID,
                Timestamp,
                ProposedByAgent,
                DecisionType,
                Proposal,
                AgentsVotes,
                Approved,
                Executed,
                PredictedEnergySavingsKW,
                ActualEnergySavingsKW
            FROM AgentDecisions
            WHERE Timestamp >= DATEADD(HOUR, ?, (SELECT MAX(Timestamp) FROM AgentDecisions))
            ORDER BY Timestamp DESC
        """

        df = pd.read_sql(query, conn, params=(-hours,))
        conn.close()
        
        # Parse JSON columns
        if not df.empty:
            df['Proposal'] = df['Proposal'].apply(lambda x: json.loads(x) if x else {})
            df['AgentsVotes'] = df['AgentsVotes'].apply(lambda x: json.loads(x) if x else {})
        
        return df.to_dict('records')
    
    def get_recent_chiller_performance(
        self,
        chiller_id: str,
        hours: int = 1
    ) -> pd.DataFrame:
        """
        Get recent chiller performance (last hour by default)
        
        Args:
            chiller_id: Chiller identifier
            hours: Hours to retrieve
        
        Returns:
            Recent performance data
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT 
                Timestamp,
                LoadPercentage,
                KWPerTon,
                COP,
                ChillerPowerKW,
                CoolingLoadKW,
                CHWSTHeaderTempC,
                CHWRTHeaderTempC,
                CWSTHeaderTempC
            FROM ChillerOperatingPoints
            WHERE ChillerID = ?
              AND Timestamp >= DATEADD(HOUR, ?, GETDATE())
            ORDER BY Timestamp DESC
        """
        
        df = pd.read_sql(query, conn, params=(chiller_id, -hours))
        conn.close()
        
        return df
    
    def get_equipment_runtime_status(self) -> Dict:
        """
        Get current runtime status for all equipment
        
        Returns:
            Dictionary of equipment runtime info
        """
        
        conn = self.get_connection()
        
        # Get chiller runtime
        query_chillers = """
            SELECT TOP 1
                ChillerID,
                RuntimeHoursTotal,
                RuntimeHoursSinceService,
                StartsToday
            FROM ChillerTelemetry
            WHERE Timestamp = (SELECT MAX(Timestamp) FROM ChillerTelemetry)
        """
        
        df_chillers = pd.read_sql(query_chillers, conn)
        
        runtime_status = {
            'chillers': df_chillers.to_dict('records') if not df_chillers.empty else []
        }
        
        conn.close()
        
        return runtime_status
    
    def store_proposal(self, proposal: Dict):
        """
        Store agent proposal in short-term memory
        (This would go to a proposals cache table in production)
        
        Args:
            proposal: Proposal dictionary
        """
        
        # For now, proposals are stored in AgentDecisions table
        # In production, you might have a separate ProposalsCache table
        # for proposals that haven't been decided yet
        
        print(f"📝 Stored proposal from {proposal.get('agent')}")
    
    def get_current_equipment_constraints(self) -> Dict:
        """
        Get current equipment constraints
        (e.g., equipment under maintenance, operating limits)
        
        Returns:
            Active constraints
        """
        
        conn = self.get_connection()
        
        # Check for recent maintenance
        query_maintenance = """
            SELECT 
                EquipmentID,
                ServiceType,
                NextServiceDueHours,
                HoursAtService
            FROM MaintenanceLogs
            WHERE Timestamp >= DATEADD(DAY, -30, GETDATE())
            ORDER BY Timestamp DESC
        """
        
        df_maint = pd.read_sql(query_maintenance, conn)
        
        constraints = {
            'recent_maintenance': df_maint.to_dict('records') if not df_maint.empty else []
        }
        
        conn.close()
        
        return constraints
    
    def get_last_24hr_summary(self) -> Dict:
        """
        Get 24-hour performance summary
        
        Returns:
            Summary statistics for last 24 hours
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT 
                AVG(PUE) AS AvgPUE,
                MIN(PUE) AS MinPUE,
                MAX(PUE) AS MaxPUE,
                AVG(SystemEfficiencyKWPerTon) AS AvgEfficiency,
                AVG(ITLoadPowerKW) AS AvgITLoad,
                AVG(TotalCoolingSystemPowerKW) AS AvgCoolingPower
            FROM SystemPerformanceMetrics
            WHERE Timestamp >= DATEADD(HOUR, -24, (SELECT MAX(Timestamp) FROM SystemPerformanceMetrics))
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            return {}
        
        return df.iloc[0].to_dict()


# Singleton instance
short_term_memory = ShortTermMemory()

# Test
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING SHORT-TERM MEMORY")
    print("="*70)
    
    # Test 1: Get current state
    print("\n[TEST 1] Latest System State...")
    state = short_term_memory.get_latest_system_state()
    
    if state:
        print(f"  Timestamp: {state.get('Timestamp')}")
        print(f"  PUE: {state.get('PUE')}")
        print(f"  Chillers Online: {state.get('ChillersOnline')}")
        print(f"  Active Chillers: {len(state.get('active_chillers', []))}")
    
    # Test 2: Active alarms
    print("\n[TEST 2] Active Alarms...")
    alarms = short_term_memory.get_active_alarms()
    print(f"  Active alarms: {len(alarms)}")
    
    # Test 3: Recent decisions
    print("\n[TEST 3] Recent Decisions...")
    decisions = short_term_memory.get_recent_decisions(hours=24)
    print(f"  Decisions (last 24hr): {len(decisions)}")
    
    # Test 4: 24hr summary
    print("\n[TEST 4] 24-Hour Summary...")
    summary = short_term_memory.get_last_24hr_summary()
    
    if summary:
        avg_pue = summary.get('AvgPUE') or 0
        avg_eff = summary.get('AvgEfficiency') or 0
        print(f"  Avg PUE: {avg_pue:.3f}")
        print(f"  Avg Efficiency: {avg_eff:.3f} kW/ton")
    
    print("\n✅ Short-term memory tests complete!")