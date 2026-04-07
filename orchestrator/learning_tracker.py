"""
Learning Tracker
Tracks system learning progress and improvement over time
Measures how the MAGS is getting better at optimization
"""

import pyodbc
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
import numpy as np
from dotenv import load_dotenv

load_dotenv()


class LearningTracker:
    """
    Tracks learning progress for MAGS
    
    Metrics tracked:
    - Decision accuracy improvement over time
    - Agent confidence calibration
    - Strategy success rates
    - PUE improvement trajectory
    - Cost savings accumulation
    
    Purpose: Measure continuous improvement, identify areas needing attention
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize learning tracker
        
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

        print("  Initializing Learning Tracker...")
        self._create_tables_if_not_exist()
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _create_tables_if_not_exist(self):
        """Create learning tracker tables"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Table: Learning Metrics
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'learning_metrics')
            CREATE TABLE learning_metrics (
                metric_id INT IDENTITY(1,1) PRIMARY KEY,
                metric_date DATE NOT NULL,
                metric_type VARCHAR(50),
                metric_name VARCHAR(100),
                metric_value FLOAT,
                baseline_value FLOAT,
                improvement_percent FLOAT,
                metric_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Agent Learning Progress
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'agent_learning_progress')
            CREATE TABLE agent_learning_progress (
                progress_id INT IDENTITY(1,1) PRIMARY KEY,
                agent_name VARCHAR(100) NOT NULL,
                progress_date DATE NOT NULL,
                accuracy_score FLOAT,
                confidence_calibration FLOAT,
                proposal_acceptance_rate FLOAT,
                learning_rate FLOAT,
                progress_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Improvement Milestones
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'improvement_milestones')
            CREATE TABLE improvement_milestones (
                milestone_id INT IDENTITY(1,1) PRIMARY KEY,
                milestone_date DATETIME NOT NULL,
                milestone_type VARCHAR(50),
                milestone_description NVARCHAR(500),
                metric_improved VARCHAR(100),
                old_value FLOAT,
                new_value FLOAT,
                improvement_percent FLOAT,
                milestone_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: Learning tracker table creation error: {e}")
    
    def track_daily_metrics(
        self,
        date: datetime,
        metrics: Dict
    ):
        """
        Track daily learning metrics
        
        Args:
            date: Metric date
            metrics: Dictionary of metrics
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for metric_name, metric_value in metrics.items():
                # Get baseline if exists
                baseline = self._get_baseline_value(metric_name)
                
                # Calculate improvement
                if baseline and baseline > 0:
                    improvement = ((metric_value - baseline) / baseline) * 100
                else:
                    improvement = 0
                
                cursor.execute("""
                INSERT INTO learning_metrics (
                    metric_date, metric_type, metric_name,
                    metric_value, baseline_value, improvement_percent
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    date.date(),
                    'SYSTEM',
                    metric_name,
                    metric_value,
                    baseline,
                    improvement
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: Metric tracking error: {e}")
    
    def track_agent_progress(
        self,
        agent_name: str,
        date: datetime,
        accuracy: float,
        confidence_calibration: float,
        acceptance_rate: float
    ):
        """
        Track individual agent learning progress
        
        Args:
            agent_name: Agent name
            date: Progress date
            accuracy: Prediction accuracy
            confidence_calibration: How well-calibrated confidence is
            acceptance_rate: Proposal acceptance rate
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Calculate learning rate (improvement from previous week)
            cursor.execute("""
            SELECT TOP 1 accuracy_score
            FROM agent_learning_progress
            WHERE agent_name = ? AND progress_date < ?
            ORDER BY progress_date DESC
            """, (agent_name, date.date()))
            
            prev_row = cursor.fetchone()
            
            if prev_row and prev_row[0]:
                learning_rate = accuracy - prev_row[0]
            else:
                learning_rate = 0.0
            
            # Insert progress
            cursor.execute("""
            INSERT INTO agent_learning_progress (
                agent_name, progress_date, accuracy_score,
                confidence_calibration, proposal_acceptance_rate,
                learning_rate
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                agent_name,
                date.date(),
                accuracy,
                confidence_calibration,
                acceptance_rate,
                learning_rate
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: Agent progress tracking error: {e}")
    
    def record_milestone(
        self,
        milestone_type: str,
        description: str,
        metric_improved: str,
        old_value: float,
        new_value: float
    ):
        """
        Record improvement milestone
        
        Args:
            milestone_type: Milestone type
            description: Description
            metric_improved: Which metric improved
            old_value: Previous value
            new_value: New value
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            improvement = ((new_value - old_value) / old_value * 100) if old_value > 0 else 0
            
            cursor.execute("""
            INSERT INTO improvement_milestones (
                milestone_date, milestone_type, milestone_description,
                metric_improved, old_value, new_value, improvement_percent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                milestone_type,
                description,
                metric_improved,
                old_value,
                new_value,
                improvement
            ))
            
            conn.commit()
            conn.close()
            
            print(f"  🎯 Milestone: {description}")
            print(f"     {metric_improved}: {old_value:.2f} → {new_value:.2f} ({improvement:+.1f}%)")
            
        except Exception as e:
            print(f"  Warning: Milestone recording error: {e}")
    
    def get_learning_summary(
        self,
        days: int = 30
    ) -> Dict:
        """
        Get learning progress summary
        
        Args:
            days: Analysis period
        
        Returns:
            Learning summary
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Get overall improvement
            cursor.execute("""
            SELECT 
                metric_name,
                AVG(improvement_percent) as avg_improvement,
                MAX(metric_value) as best_value,
                MIN(metric_value) as worst_value
            FROM learning_metrics
            WHERE metric_date >= ?
            GROUP BY metric_name
            """, (cutoff_date.date(),))
            
            metrics = {}
            for row in cursor.fetchall():
                metrics[row[0]] = {
                    'avg_improvement': row[1],
                    'best_value': row[2],
                    'worst_value': row[3]
                }
            
            # Get agent performance
            cursor.execute("""
            SELECT 
                agent_name,
                AVG(accuracy_score) as avg_accuracy,
                AVG(learning_rate) as avg_learning_rate
            FROM agent_learning_progress
            WHERE progress_date >= ?
            GROUP BY agent_name
            """, (cutoff_date.date(),))
            
            agents = {}
            for row in cursor.fetchall():
                agents[row[0]] = {
                    'avg_accuracy': row[1],
                    'avg_learning_rate': row[2]
                }
            
            # Get recent milestones
            cursor.execute("""
            SELECT TOP 5
                milestone_date, milestone_description,
                metric_improved, improvement_percent
            FROM improvement_milestones
            ORDER BY milestone_date DESC
            """)
            
            milestones = []
            for row in cursor.fetchall():
                milestones.append({
                    'date': row[0],
                    'description': row[1],
                    'metric': row[2],
                    'improvement': row[3]
                })
            
            conn.close()
            
            return {
                'period_days': days,
                'metrics': metrics,
                'agents': agents,
                'recent_milestones': milestones
            }
            
        except Exception as e:
            print(f"  Warning: Learning summary error: {e}")
            return {}
    
    def _get_baseline_value(self, metric_name: str) -> Optional[float]:
        """Get baseline value for metric"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get earliest value as baseline
            cursor.execute("""
            SELECT TOP 1 metric_value
            FROM learning_metrics
            WHERE metric_name = ?
            ORDER BY metric_date ASC
            """, (metric_name,))
            
            row = cursor.fetchone()
            conn.close()
            
            return row[0] if row else None
            
        except:
            return None
# Singleton instance
learning_tracker = LearningTracker()
