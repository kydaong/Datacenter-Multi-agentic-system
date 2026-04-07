"""
Medium-Term Memory
Stores operational patterns, performance trends (last 30-90 days)
Enables pattern recognition and seasonal adjustments
"""

import pyodbc
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class MediumTermMemory:
    """
    Medium-term memory for MAGS
    
    Stores:
    - Performance trends (30-90 days)
    - Operational patterns (time of day, day of week)
    - Decision outcomes and effectiveness
    - Agent performance metrics
    - System efficiency trends
    
    Purpose: Pattern recognition, learning from recent history
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize medium-term memory
        
        Args:
            connection_string: SQL Server connection string
        """
        
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
        self.retention_days = 90  # Keep data for 90 days
        
        print("  Initializing Medium-Term Memory...")
        self._create_tables_if_not_exist()
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _create_tables_if_not_exist(self):
        """Create medium-term memory tables"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Table: Performance Trends
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mtm_performance_trends')
            CREATE TABLE mtm_performance_trends (
                trend_id INT IDENTITY(1,1) PRIMARY KEY,
                record_date DATE NOT NULL,
                record_hour INT,
                avg_pue FLOAT,
                avg_cooling_load_kw FLOAT,
                avg_efficiency_kw_per_ton FLOAT,
                total_energy_kwh FLOAT,
                total_cost_sgd FLOAT,
                trends_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Operational Patterns
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mtm_operational_patterns')
            CREATE TABLE mtm_operational_patterns (
                pattern_id INT IDENTITY(1,1) PRIMARY KEY,
                pattern_type VARCHAR(50),
                pattern_name VARCHAR(100),
                frequency INT,
                success_rate FLOAT,
                avg_savings_kw FLOAT,
                pattern_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Decision Outcomes
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mtm_decision_outcomes')
            CREATE TABLE mtm_decision_outcomes (
                outcome_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50),
                decision_time DATETIME NOT NULL,
                decision_type VARCHAR(50),
                predicted_savings_kw FLOAT,
                actual_savings_kw FLOAT,
                accuracy_percent FLOAT,
                outcome_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Agent Performance
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'mtm_agent_performance')
            CREATE TABLE mtm_agent_performance (
                performance_id INT IDENTITY(1,1) PRIMARY KEY,
                agent_name VARCHAR(100) NOT NULL,
                evaluation_date DATE NOT NULL,
                proposals_made INT,
                proposals_accepted INT,
                avg_confidence FLOAT,
                avg_accuracy FLOAT,
                performance_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: MTM table creation error: {e}")
    
    def store_daily_performance(self, date: datetime, metrics: Dict):
        """
        Store daily performance summary
        
        Args:
            date: Date of performance
            metrics: Performance metrics
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO mtm_performance_trends (
                record_date, avg_pue, avg_cooling_load_kw,
                avg_efficiency_kw_per_ton, total_energy_kwh,
                total_cost_sgd, trends_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                date.date(),
                metrics.get('avg_pue'),
                metrics.get('avg_cooling_load_kw'),
                metrics.get('avg_efficiency_kw_per_ton'),
                metrics.get('total_energy_kwh'),
                metrics.get('total_cost_sgd'),
                json.dumps(metrics)
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: MTM performance storage error: {e}")
    
    def store_decision_outcome(
        self,
        session_id: str,
        decision_type: str,
        predicted_savings: float,
        actual_savings: float
    ):
        """
        Store decision outcome for learning
        
        Args:
            session_id: Decision session ID
            decision_type: Type of decision
            predicted_savings: Predicted energy savings (kW)
            actual_savings: Actual measured savings (kW)
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Calculate accuracy
            if predicted_savings > 0:
                accuracy = (actual_savings / predicted_savings) * 100
            else:
                accuracy = 100 if actual_savings == 0 else 0
            
            cursor.execute("""
            INSERT INTO mtm_decision_outcomes (
                session_id, decision_time, decision_type,
                predicted_savings_kw, actual_savings_kw, accuracy_percent
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                datetime.now(),
                decision_type,
                predicted_savings,
                actual_savings,
                accuracy
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: MTM outcome storage error: {e}")
    
    def get_performance_trend(
        self,
        metric: str,
        days: int = 30
    ) -> List[Dict]:
        """
        Get performance trend over time
        
        Args:
            metric: Metric name (e.g., 'avg_pue', 'total_cost_sgd')
            days: Number of days to retrieve
        
        Returns:
            List of daily values
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            query = f"""
            SELECT record_date, {metric}
            FROM mtm_performance_trends
            WHERE record_date >= ?
            ORDER BY record_date
            """
            
            cursor.execute(query, (cutoff_date.date(),))
            
            rows = cursor.fetchall()
            conn.close()
            
            trend = []
            for row in rows:
                trend.append({
                    'date': row[0],
                    'value': row[1]
                })
            
            return trend
            
        except Exception as e:
            print(f"  Warning: MTM trend retrieval error: {e}")
            return []
    
    def get_decision_accuracy(
        self,
        decision_type: Optional[str] = None,
        days: int = 30
    ) -> Dict:
        """
        Get decision accuracy metrics
        
        Args:
            decision_type: Filter by decision type
            days: Look back period
        
        Returns:
            Accuracy statistics
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            if decision_type:
                cursor.execute("""
                SELECT 
                    AVG(accuracy_percent) as avg_accuracy,
                    MIN(accuracy_percent) as min_accuracy,
                    MAX(accuracy_percent) as max_accuracy,
                    COUNT(*) as sample_count
                FROM mtm_decision_outcomes
                WHERE decision_type = ? AND decision_time >= ?
                """, (decision_type, cutoff_date))
            else:
                cursor.execute("""
                SELECT 
                    AVG(accuracy_percent) as avg_accuracy,
                    MIN(accuracy_percent) as min_accuracy,
                    MAX(accuracy_percent) as max_accuracy,
                    COUNT(*) as sample_count
                FROM mtm_decision_outcomes
                WHERE decision_time >= ?
                """, (cutoff_date,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'avg_accuracy': row[0] if row[0] else 0,
                    'min_accuracy': row[1] if row[1] else 0,
                    'max_accuracy': row[2] if row[2] else 0,
                    'sample_count': row[3] if row[3] else 0
                }
            
            return {
                'avg_accuracy': 0,
                'min_accuracy': 0,
                'max_accuracy': 0,
                'sample_count': 0
            }
            
        except Exception as e:
            print(f"  Warning: MTM accuracy retrieval error: {e}")
            return {}
    
    def get_similar_operating_conditions(
        self,
        cooling_load_kw: float,
        wet_bulb_temp: float,
        days: int = 30,
        tolerance_percent: float = 15.0
    ) -> pd.DataFrame:
        """
        Find historical records with similar operating conditions

        Args:
            cooling_load_kw: Current cooling load to match
            wet_bulb_temp: Current wet-bulb temperature (used as metadata filter)
            days: How many days back to search
            tolerance_percent: Acceptable % deviation in cooling load

        Returns:
            DataFrame of matching records (empty if none found)
        """

        try:
            conn = self._get_connection()

            cutoff_date = datetime.now() - timedelta(days=days)
            load_min = cooling_load_kw * (1 - tolerance_percent / 100)
            load_max = cooling_load_kw * (1 + tolerance_percent / 100)

            query = """
            SELECT
                record_date, record_hour,
                avg_pue, avg_cooling_load_kw,
                avg_efficiency_kw_per_ton,
                total_energy_kwh, total_cost_sgd,
                trends_json
            FROM mtm_performance_trends
            WHERE record_date >= ?
              AND avg_cooling_load_kw BETWEEN ? AND ?
            ORDER BY record_date DESC
            """

            cursor = conn.cursor()
            cursor.execute(query, (cutoff_date.date(), load_min, load_max))
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            conn.close()
            df = pd.DataFrame(
                [list(row) for row in rows],
                columns=cols
            )

            # If MAGS table is empty, pull from live AOM-Dev data instead
            if df.empty:
                try:
                    from live_data import live_data as _live_data
                except ImportError:
                    try:
                        from orchestrator.live_data import live_data as _live_data
                    except ImportError:
                        _live_data = None

                if _live_data is not None:
                    rows = _live_data.get_similar_conditions(
                        cooling_load_kw=cooling_load_kw,
                        wet_bulb_temp=wet_bulb_temp,
                        days=days,
                        tolerance_pct=tolerance_percent
                    )
                    if rows:
                        df = pd.DataFrame(rows)

            return df

        except Exception as e:
            print(f"  Warning: MTM similar conditions retrieval error: {e}")
            return pd.DataFrame()

    def identify_patterns(self, days: int = 30) -> List[Dict]:
        """
        Identify operational patterns
        
        Args:
            days: Analysis period
        
        Returns:
            List of identified patterns
        """
        
        # Simplified pattern identification
        # In production, this would use ML algorithms
        
        patterns = []
        
        # Pattern 1: Time-of-day load patterns
        trend = self.get_performance_trend('avg_cooling_load_kw', days)
        
        if trend:
            loads = [t['value'] for t in trend if t['value']]
            if loads:
                avg_load = np.mean(loads)
                patterns.append({
                    'pattern_type': 'LOAD_PATTERN',
                    'pattern_name': 'Average Daily Load',
                    'value': avg_load,
                    'confidence': 0.85
                })
        
        return patterns
# Singleton instance
medium_term_memory = MediumTermMemory()
