"""
Long-Term Memory
Stores historical knowledge, learned patterns, system evolution (1+ years)
Enables strategic learning and continuous improvement
"""

import pyodbc
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import numpy as np


class LongTermMemory:
    """
    Long-term memory for MAGS
    
    Stores:
    - Historical performance baselines
    - Seasonal patterns (summer vs winter)
    - Equipment degradation curves
    - Proven optimization strategies
    - Long-term efficiency trends
    - Major events and anomalies
    
    Purpose: Strategic learning, seasonal adjustments, equipment life-cycle management
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize long-term memory
        
        Args:
            connection_string: SQL Server connection string
        """
        
        if connection_string is None:
            connection_string = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost\\SQLEXPRESS;"
                "DATABASE=AOM-Dev;"
                "Trusted_Connection=yes;"
            )
        
        self.connection_string = connection_string
        
        print("  Initializing Long-Term Memory...")
        self._create_tables_if_not_exist()
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _create_tables_if_not_exist(self):
        """Create long-term memory tables"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Table: Historical Baselines
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_baselines')
            CREATE TABLE ltm_baselines (
                baseline_id INT IDENTITY(1,1) PRIMARY KEY,
                baseline_type VARCHAR(50) NOT NULL,
                baseline_name VARCHAR(100),
                baseline_value FLOAT,
                established_date DATE,
                last_updated DATETIME,
                baseline_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Seasonal Patterns
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_seasonal_patterns')
            CREATE TABLE ltm_seasonal_patterns (
                pattern_id INT IDENTITY(1,1) PRIMARY KEY,
                season VARCHAR(20),
                month INT,
                pattern_type VARCHAR(50),
                avg_cooling_load_kw FLOAT,
                avg_wet_bulb_temp FLOAT,
                avg_pue FLOAT,
                pattern_json NVARCHAR(MAX),
                years_data INT DEFAULT 1,
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Equipment Degradation
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_equipment_degradation')
            CREATE TABLE ltm_equipment_degradation (
                degradation_id INT IDENTITY(1,1) PRIMARY KEY,
                equipment_id VARCHAR(50) NOT NULL,
                equipment_type VARCHAR(50),
                install_date DATE,
                current_age_years FLOAT,
                baseline_efficiency FLOAT,
                current_efficiency FLOAT,
                degradation_rate_percent_per_year FLOAT,
                degradation_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Proven Strategies
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_proven_strategies')
            CREATE TABLE ltm_proven_strategies (
                strategy_id INT IDENTITY(1,1) PRIMARY KEY,
                strategy_name VARCHAR(100) NOT NULL,
                strategy_type VARCHAR(50),
                success_count INT DEFAULT 0,
                total_attempts INT DEFAULT 0,
                success_rate FLOAT,
                avg_savings_kw FLOAT,
                avg_roi_months FLOAT,
                applicable_conditions NVARCHAR(MAX),
                strategy_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Major Events
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_major_events')
            CREATE TABLE ltm_major_events (
                event_id INT IDENTITY(1,1) PRIMARY KEY,
                event_date DATETIME NOT NULL,
                event_type VARCHAR(50),
                event_description NVARCHAR(500),
                impact_description NVARCHAR(MAX),
                lessons_learned NVARCHAR(MAX),
                event_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Knowledge Base
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ltm_knowledge_base')
            CREATE TABLE ltm_knowledge_base (
                knowledge_id INT IDENTITY(1,1) PRIMARY KEY,
                knowledge_category VARCHAR(50),
                knowledge_title VARCHAR(200),
                knowledge_content NVARCHAR(MAX),
                confidence_score FLOAT,
                source VARCHAR(100),
                knowledge_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM table creation error: {e}")
    
    def establish_baseline(
        self,
        baseline_type: str,
        baseline_name: str,
        baseline_value: float,
        metadata: Optional[Dict] = None
    ):
        """
        Establish a performance baseline
        
        Args:
            baseline_type: Type (e.g., 'PUE', 'EFFICIENCY', 'COST')
            baseline_name: Descriptive name
            baseline_value: Baseline value
            metadata: Additional metadata
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if baseline exists
            cursor.execute("""
            SELECT baseline_id FROM ltm_baselines
            WHERE baseline_type = ? AND baseline_name = ?
            """, (baseline_type, baseline_name))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                cursor.execute("""
                UPDATE ltm_baselines
                SET baseline_value = ?, last_updated = ?, baseline_json = ?
                WHERE baseline_type = ? AND baseline_name = ?
                """, (
                    baseline_value,
                    datetime.now(),
                    json.dumps(metadata or {}),
                    baseline_type,
                    baseline_name
                ))
            else:
                # Insert new
                cursor.execute("""
                INSERT INTO ltm_baselines (
                    baseline_type, baseline_name, baseline_value,
                    established_date, last_updated, baseline_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    baseline_type,
                    baseline_name,
                    baseline_value,
                    datetime.now().date(),
                    datetime.now(),
                    json.dumps(metadata or {})
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM baseline establishment error: {e}")
    
    def get_baseline(
        self,
        baseline_type: str,
        baseline_name: str
    ) -> Optional[Dict]:
        """
        Get established baseline
        
        Args:
            baseline_type: Baseline type
            baseline_name: Baseline name
        
        Returns:
            Baseline data or None
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT baseline_value, established_date, last_updated, baseline_json
            FROM ltm_baselines
            WHERE baseline_type = ? AND baseline_name = ?
            """, (baseline_type, baseline_name))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'baseline_value': row[0],
                    'established_date': row[1],
                    'last_updated': row[2],
                    'metadata': json.loads(row[3]) if row[3] else {}
                }
            
            return None
            
        except Exception as e:
            print(f"  Warning: LTM baseline retrieval error: {e}")
            return None
    
    def store_seasonal_pattern(
        self,
        season: str,
        month: int,
        pattern_data: Dict
    ):
        """
        Store seasonal pattern
        
        Args:
            season: Season name (WINTER, SPRING, SUMMER, FALL)
            month: Month number (1-12)
            pattern_data: Pattern metrics
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if pattern exists
            cursor.execute("""
            SELECT pattern_id, years_data FROM ltm_seasonal_patterns
            WHERE season = ? AND month = ?
            """, (season, month))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing (rolling average)
                pattern_id = existing[0]
                years_data = existing[1] + 1
                
                cursor.execute("""
                UPDATE ltm_seasonal_patterns
                SET 
                    avg_cooling_load_kw = (avg_cooling_load_kw * years_data + ?) / (years_data + 1),
                    avg_wet_bulb_temp = (avg_wet_bulb_temp * years_data + ?) / (years_data + 1),
                    avg_pue = (avg_pue * years_data + ?) / (years_data + 1),
                    pattern_json = ?,
                    years_data = ?,
                    updated_at = ?
                WHERE pattern_id = ?
                """, (
                    pattern_data.get('avg_cooling_load_kw', 0),
                    pattern_data.get('avg_wet_bulb_temp', 0),
                    pattern_data.get('avg_pue', 0),
                    json.dumps(pattern_data),
                    years_data,
                    datetime.now(),
                    pattern_id
                ))
            else:
                # Insert new
                cursor.execute("""
                INSERT INTO ltm_seasonal_patterns (
                    season, month, pattern_type,
                    avg_cooling_load_kw, avg_wet_bulb_temp, avg_pue,
                    pattern_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    season,
                    month,
                    'SEASONAL',
                    pattern_data.get('avg_cooling_load_kw'),
                    pattern_data.get('avg_wet_bulb_temp'),
                    pattern_data.get('avg_pue'),
                    json.dumps(pattern_data)
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM seasonal pattern storage error: {e}")
    
    def get_seasonal_pattern(
        self,
        month: int
    ) -> Optional[Dict]:
        """
        Get seasonal pattern for month
        
        Args:
            month: Month number (1-12)
        
        Returns:
            Seasonal pattern data
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT season, avg_cooling_load_kw, avg_wet_bulb_temp,
                   avg_pue, pattern_json, years_data
            FROM ltm_seasonal_patterns
            WHERE month = ?
            """, (month,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'season': row[0],
                    'avg_cooling_load_kw': row[1],
                    'avg_wet_bulb_temp': row[2],
                    'avg_pue': row[3],
                    'pattern': json.loads(row[4]) if row[4] else {},
                    'years_data': row[5]
                }
            
            return None
            
        except Exception as e:
            print(f"  Warning: LTM seasonal pattern retrieval error: {e}")
            return None
    
    def track_equipment_degradation(
        self,
        equipment_id: str,
        equipment_type: str,
        current_efficiency: float,
        baseline_efficiency: float,
        install_date: datetime
    ):
        """
        Track equipment degradation over time
        
        Args:
            equipment_id: Equipment identifier
            equipment_type: Type (CHILLER, PUMP, TOWER)
            current_efficiency: Current efficiency
            baseline_efficiency: Original efficiency
            install_date: Installation date
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Calculate age and degradation rate
            age_years = (datetime.now() - install_date).days / 365.25
            
            if baseline_efficiency > 0:
                degradation_rate = ((baseline_efficiency - current_efficiency) / baseline_efficiency * 100) / age_years if age_years > 0 else 0
            else:
                degradation_rate = 0
            
            # Check if record exists
            cursor.execute("""
            SELECT degradation_id FROM ltm_equipment_degradation
            WHERE equipment_id = ?
            """, (equipment_id,))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update
                cursor.execute("""
                UPDATE ltm_equipment_degradation
                SET 
                    current_age_years = ?,
                    current_efficiency = ?,
                    degradation_rate_percent_per_year = ?,
                    updated_at = ?
                WHERE equipment_id = ?
                """, (
                    age_years,
                    current_efficiency,
                    degradation_rate,
                    datetime.now(),
                    equipment_id
                ))
            else:
                # Insert
                cursor.execute("""
                INSERT INTO ltm_equipment_degradation (
                    equipment_id, equipment_type, install_date,
                    current_age_years, baseline_efficiency,
                    current_efficiency, degradation_rate_percent_per_year
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    equipment_id,
                    equipment_type,
                    install_date.date(),
                    age_years,
                    baseline_efficiency,
                    current_efficiency,
                    degradation_rate
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM degradation tracking error: {e}")
    
    def get_equipment_degradation(
        self,
        equipment_id: str
    ) -> Optional[Dict]:
        """
        Get equipment degradation data
        
        Args:
            equipment_id: Equipment identifier
        
        Returns:
            Degradation data
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT equipment_type, install_date, current_age_years,
                   baseline_efficiency, current_efficiency,
                   degradation_rate_percent_per_year
            FROM ltm_equipment_degradation
            WHERE equipment_id = ?
            """, (equipment_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'equipment_type': row[0],
                    'install_date': row[1],
                    'current_age_years': row[2],
                    'baseline_efficiency': row[3],
                    'current_efficiency': row[4],
                    'degradation_rate_percent_per_year': row[5]
                }
            
            return None
            
        except Exception as e:
            print(f"  Warning: LTM degradation retrieval error: {e}")
            return None
    
    def record_proven_strategy(
        self,
        strategy_name: str,
        strategy_type: str,
        success: bool,
        savings_kw: float,
        conditions: Dict
    ):
        """
        Record outcome of optimization strategy
        
        Args:
            strategy_name: Strategy name
            strategy_type: Strategy type
            success: Was it successful?
            savings_kw: Energy savings achieved
            conditions: Conditions under which strategy was applied
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if strategy exists
            cursor.execute("""
            SELECT strategy_id, success_count, total_attempts,
                   avg_savings_kw
            FROM ltm_proven_strategies
            WHERE strategy_name = ?
            """, (strategy_name,))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update
                strategy_id = existing[0]
                success_count = existing[1] + (1 if success else 0)
                total_attempts = existing[2] + 1
                old_avg_savings = existing[3] or 0
                
                # Rolling average for savings
                new_avg_savings = ((old_avg_savings * existing[2]) + savings_kw) / total_attempts
                success_rate = (success_count / total_attempts) * 100
                
                cursor.execute("""
                UPDATE ltm_proven_strategies
                SET 
                    success_count = ?,
                    total_attempts = ?,
                    success_rate = ?,
                    avg_savings_kw = ?,
                    applicable_conditions = ?,
                    updated_at = ?
                WHERE strategy_id = ?
                """, (
                    success_count,
                    total_attempts,
                    success_rate,
                    new_avg_savings,
                    json.dumps(conditions),
                    datetime.now(),
                    strategy_id
                ))
            else:
                # Insert
                cursor.execute("""
                INSERT INTO ltm_proven_strategies (
                    strategy_name, strategy_type,
                    success_count, total_attempts,
                    success_rate, avg_savings_kw,
                    applicable_conditions
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    strategy_name,
                    strategy_type,
                    1 if success else 0,
                    1,
                    100.0 if success else 0.0,
                    savings_kw,
                    json.dumps(conditions)
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM strategy recording error: {e}")
    
    def get_proven_strategies(
        self,
        min_success_rate: float = 70.0,
        min_attempts: int = 3
    ) -> List[Dict]:
        """
        Get proven optimization strategies
        
        Args:
            min_success_rate: Minimum success rate (%)
            min_attempts: Minimum number of attempts
        
        Returns:
            List of proven strategies
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT strategy_name, strategy_type, success_rate,
                   avg_savings_kw, total_attempts, applicable_conditions
            FROM ltm_proven_strategies
            WHERE success_rate >= ? AND total_attempts >= ?
            ORDER BY success_rate DESC, avg_savings_kw DESC
            """, (min_success_rate, min_attempts))
            
            rows = cursor.fetchall()
            conn.close()
            
            strategies = []
            for row in rows:
                strategies.append({
                    'strategy_name': row[0],
                    'strategy_type': row[1],
                    'success_rate': row[2],
                    'avg_savings_kw': row[3],
                    'total_attempts': row[4],
                    'applicable_conditions': json.loads(row[5]) if row[5] else {}
                })
            
            return strategies
            
        except Exception as e:
            print(f"  Warning: LTM strategy retrieval error: {e}")
            return []
    
    def record_major_event(
        self,
        event_type: str,
        description: str,
        impact: str,
        lessons_learned: str
    ):
        """
        Record major event for future reference
        
        Args:
            event_type: Event type (FAILURE, UPGRADE, ANOMALY, etc.)
            description: Event description
            impact: Impact description
            lessons_learned: Lessons learned
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO ltm_major_events (
                event_date, event_type, event_description,
                impact_description, lessons_learned
            )
            VALUES (?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                event_type,
                description,
                impact,
                lessons_learned
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: LTM event recording error: {e}")
# Singleton instance
long_term_memory = LongTermMemory()
