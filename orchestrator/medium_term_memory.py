"""
Medium-Term Memory (SQL Server)
Stores 7-90 day patterns, decision outcomes, equipment performance
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from typing import Dict, List, Optional

load_dotenv()

class MediumTermMemory:
    """
    Medium-term memory: 7-90 days of operational patterns
    Used by agents to find historical precedents
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
        
        print(f"✅ Medium-term memory connected to {os.getenv('DB_NAME')}")
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.conn_str)
    
    def get_recent_chiller_performance(
        self,
        chiller_id: str,
        days: int = 200
    ) -> pd.DataFrame:
        """
        Get recent chiller performance data
        
        Args:
            chiller_id: Chiller identifier
            days: Number of days to look back
        
        Returns:
            DataFrame with recent performance metrics
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT
                Timestamp,
                LoadPercentage,
                KWPerTon,
                COP,
                CHWSTHeaderTempC,
                CWSTHeaderTempC
            FROM ChillerOperatingPoints
            WHERE ChillerID = ?
              AND Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM ChillerOperatingPoints))
            ORDER BY Timestamp DESC
        """
        
        df = pd.read_sql(query, conn, params=(chiller_id, -days))
        conn.close()
        
        return df
    
    def get_similar_operating_conditions(
        self,
        cooling_load_kw: float,
        wet_bulb_temp: float,
        tolerance_load: float = 100,
        tolerance_temp: float = 1.0,
        days: int = 90
    ) -> pd.DataFrame:
        """
        Find historical precedents with similar operating conditions
        
        Args:
            cooling_load_kw: Target cooling load
            wet_bulb_temp: Target wet-bulb temp
            tolerance_load: Load tolerance (±kW)
            tolerance_temp: Temperature tolerance (±°C)
            days: How far back to search
        
        Returns:
            DataFrame with similar historical conditions and outcomes
        """
        
        conn = self.get_connection()
        
        query = """
            SELECT TOP 20
                c.Timestamp,
                c.ChillerID,
                c.LoadPercentage,
                c.KWPerTon,
                c.COP,
                c.ChillerPowerKW,
                c.CoolingLoadKW,
                w.WetBulbTempCelsius
            FROM ChillerOperatingPoints c
            JOIN WeatherConditions w ON c.Timestamp = w.Timestamp
            WHERE c.CoolingLoadKW BETWEEN ? AND ?
              AND w.WetBulbTempCelsius BETWEEN ? AND ?
              AND c.Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM ChillerOperatingPoints))
            ORDER BY c.Timestamp DESC
        """
        
        df = pd.read_sql(
            query, 
            conn, 
            params=(
                cooling_load_kw - tolerance_load,
                cooling_load_kw + tolerance_load,
                wet_bulb_temp - tolerance_temp,
                wet_bulb_temp + tolerance_temp,
                -days
            )
        )
        
        conn.close()
        
        return df
    
    def get_decision_outcomes(
        self,
        decision_type: Optional[str] = None,
        days: int = 30
    ) -> pd.DataFrame:
        """
        Get recent agent decisions and their outcomes
        
        Args:
            decision_type: Filter by decision type (e.g., "CHILLER_STAGING")
            days: Days to look back
        
        Returns:
            DataFrame with decisions and accuracy metrics
        """
        
        conn = self.get_connection()
        
        if decision_type:
            query = """
                SELECT 
                    Timestamp,
                    ProposedByAgent,
                    DecisionType,
                    Approved,
                    Executed,
                    PredictedEnergySavingsKW,
                    ActualEnergySavingsKW,
                    EnergyPredictionErrorPct
                FROM AgentDecisions
                WHERE DecisionType = ?
                  AND Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM AgentDecisions))
                ORDER BY Timestamp DESC
            """
            df = pd.read_sql(query, conn, params=(decision_type, -days))
        else:
            query = """
                SELECT 
                    Timestamp,
                    ProposedByAgent,
                    DecisionType,
                    Approved,
                    Executed,
                    PredictedEnergySavingsKW,
                    ActualEnergySavingsKW,
                    EnergyPredictionErrorPct
                FROM AgentDecisions
                WHERE Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM AgentDecisions))
                ORDER BY Timestamp DESC
            """
            df = pd.read_sql(query, conn, params=(-days,))
        
        conn.close()
        
        return df
    
    def get_equipment_health_trends(
        self,
        equipment_id: str,
        days: int = 100
    ) -> Dict:
        """
        Get equipment health trends (vibration, efficiency degradation)
        
        Args:
            equipment_id: Equipment identifier
            days: Days to analyze
        
        Returns:
            Dictionary with health metrics
        """
        
        conn = self.get_connection()
        
        # Different query based on equipment type
        if 'Chiller' in equipment_id:
            query = """
                SELECT 
                    AVG(VibrationMmS) AS AvgVibration,
                    MAX(VibrationMmS) AS MaxVibration,
                    AVG(BearingTempCelsius) AS AvgBearingTemp,
                    AVG(EfficiencyKwPerTon) AS AvgEfficiency
                FROM ChillerTelemetry
                WHERE ChillerID = ?
                  AND Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM ChillerTelemetry))
            """
        elif 'Pump' in equipment_id:
            query = """
                SELECT 
                    AVG(VibrationMmS) AS AvgVibration,
                    MAX(VibrationMmS) AS MaxVibration,
                    AVG(PowerConsumptionKW) AS AvgPowerKW
                FROM PumpTelemetry
                WHERE PumpID = ?
                  AND Timestamp >= DATEADD(DAY, ?, (SELECT MAX(Timestamp) FROM PumpTelemetry))
            """
        else:
            return {}
        
        df = pd.read_sql(query, conn, params=(equipment_id, -days))
        conn.close()
        
        if not df.empty:
            return df.iloc[0].to_dict()
        return {}


# Singleton instance
medium_term_memory = MediumTermMemory()

# Example usage
if __name__ == "__main__":
    
    # Test 1: Get recent performance
    print("\n[TEST 1] Recent Chiller Performance...")
    df = medium_term_memory.get_recent_chiller_performance('Chiller-1', days=7)
    print(f"Found {len(df)} records")
    if not df.empty:
        print(df.head())
    
    # Test 2: Find similar conditions
    print("\n[TEST 2] Similar Operating Conditions...")
    df = medium_term_memory.get_similar_operating_conditions(
        cooling_load_kw=2560,
        wet_bulb_temp=25.0,
        tolerance_load=300,
        days=30
    )
    print(f"Found {len(df)} similar conditions")
    if not df.empty:
        print(df.head())
    
    # Test 3: Equipment health
    print("\n[TEST 3] Equipment Health Trends...")
    health = medium_term_memory.get_equipment_health_trends('Chiller-1', days=30)
    print(health)