"""
Short-Term Memory
Stores recent decisions, system state, and agent interactions (last 24 hours)
Uses SQL Server for persistence (could be Redis in production for faster access)
"""

import pyodbc
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json


class ShortTermMemory:
    """
    Short-term memory for MAGS
    
    Stores:
    - Recent agent proposals (last 24 hours)
    - Recent decisions and outcomes
    - Active system state
    - Recent performance metrics
    - Pending actions
    
    Purpose: Fast retrieval for real-time decision making
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize short-term memory
        
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
        self.retention_hours = 24  # Keep data for 24 hours
        
        print("  Initializing Short-Term Memory...")
        self._create_tables_if_not_exist()
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _create_tables_if_not_exist(self):
        """Create short-term memory tables"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Table: Recent Proposals
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stm_recent_proposals')
            CREATE TABLE stm_recent_proposals (
                proposal_id INT IDENTITY(1,1) PRIMARY KEY,
                agent_name VARCHAR(100) NOT NULL,
                proposal_time DATETIME NOT NULL,
                action_type VARCHAR(50),
                proposal_json NVARCHAR(MAX),
                context_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Recent Decisions
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stm_recent_decisions')
            CREATE TABLE stm_recent_decisions (
                decision_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50),
                decision_time DATETIME NOT NULL,
                decision_type VARCHAR(50),
                executed BIT DEFAULT 0,
                execution_result VARCHAR(20),
                decision_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: System State Snapshots
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stm_system_state')
            CREATE TABLE stm_system_state (
                state_id INT IDENTITY(1,1) PRIMARY KEY,
                snapshot_time DATETIME NOT NULL,
                cooling_load_kw FLOAT,
                it_load_kw FLOAT,
                total_facility_power_kw FLOAT,
                current_pue FLOAT,
                chillers_online VARCHAR(100),
                state_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table: Pending Actions
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stm_pending_actions')
            CREATE TABLE stm_pending_actions (
                action_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50),
                action_type VARCHAR(50),
                scheduled_time DATETIME,
                status VARCHAR(20) DEFAULT 'PENDING',
                action_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: STM table creation error: {e}")
    
    def store_proposal(
        self,
        agent_name: str,
        proposal: Dict,
        context: Dict
    ):
        """
        Store agent proposal in short-term memory
        
        Args:
            agent_name: Name of proposing agent
            proposal: Proposal data
            context: System context at time of proposal
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO stm_recent_proposals (
                agent_name, proposal_time, action_type,
                proposal_json, context_json
            )
            VALUES (?, ?, ?, ?, ?)
            """, (
                agent_name,
                datetime.now(),
                proposal.get('action_type'),
                json.dumps(proposal),
                json.dumps(context)
            ))
            
            conn.commit()
            conn.close()
            
            # Clean old proposals (>24 hours)
            self._clean_old_proposals()
            
        except Exception as e:
            print(f"  Warning: STM proposal storage error: {e}")
    
    def store_decision(
        self,
        session_id: str,
        decision: Dict
    ):
        """
        Store decision in short-term memory
        
        Args:
            session_id: Session ID
            decision: Decision data
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO stm_recent_decisions (
                session_id, decision_time, decision_type, decision_json
            )
            VALUES (?, ?, ?, ?)
            """, (
                session_id,
                datetime.now(),
                decision.get('action_type'),
                json.dumps(decision)
            ))
            
            conn.commit()
            conn.close()
            
            self._clean_old_decisions()
            
        except Exception as e:
            print(f"  Warning: STM decision storage error: {e}")
    
    def store_system_state(self, state: Dict):
        """
        Store system state snapshot
        
        Args:
            state: Current system state
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO stm_system_state (
                snapshot_time, cooling_load_kw, it_load_kw,
                total_facility_power_kw, current_pue,
                chillers_online, state_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                state.get('cooling_load_kw'),
                state.get('it_load_kw'),
                state.get('total_facility_power_kw'),
                state.get('current_pue'),
                ','.join(state.get('chillers_online', [])),
                json.dumps(state)
            ))
            
            conn.commit()
            conn.close()
            
            self._clean_old_states()
            
        except Exception as e:
            print(f"  Warning: STM state storage error: {e}")
    
    def get_recent_proposals(
        self,
        agent_name: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Get recent proposals
        
        Args:
            agent_name: Filter by agent (optional)
            hours: Look back hours
        
        Returns:
            List of recent proposals
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            if agent_name:
                cursor.execute("""
                SELECT agent_name, proposal_time, action_type, proposal_json
                FROM stm_recent_proposals
                WHERE agent_name = ? AND proposal_time >= ?
                ORDER BY proposal_time DESC
                """, (agent_name, cutoff_time))
            else:
                cursor.execute("""
                SELECT agent_name, proposal_time, action_type, proposal_json
                FROM stm_recent_proposals
                WHERE proposal_time >= ?
                ORDER BY proposal_time DESC
                """, (cutoff_time,))
            
            rows = cursor.fetchall()
            conn.close()
            
            proposals = []
            for row in rows:
                proposals.append({
                    'agent_name': row[0],
                    'proposal_time': row[1],
                    'action_type': row[2],
                    'proposal': json.loads(row[3]) if row[3] else {}
                })
            
            return proposals
            
        except Exception as e:
            print(f"  Warning: STM proposal retrieval error: {e}")
            return []
    
    def get_recent_decisions(self, hours: int = 24) -> List[Dict]:
        """
        Get recent decisions
        
        Args:
            hours: Look back hours
        
        Returns:
            List of recent decisions
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            cursor.execute("""
            SELECT session_id, decision_time, decision_type,
                   executed, execution_result, decision_json
            FROM stm_recent_decisions
            WHERE decision_time >= ?
            ORDER BY decision_time DESC
            """, (cutoff_time,))
            
            rows = cursor.fetchall()
            conn.close()
            
            decisions = []
            for row in rows:
                decisions.append({
                    'session_id': row[0],
                    'decision_time': row[1],
                    'decision_type': row[2],
                    'executed': bool(row[3]),
                    'execution_result': row[4],
                    'decision': json.loads(row[5]) if row[5] else {}
                })
            
            return decisions
            
        except Exception as e:
            print(f"  Warning: STM decision retrieval error: {e}")
            return []
    
    def get_current_state(self) -> Optional[Dict]:
        """
        Get most recent system state
        
        Returns:
            Current system state
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT TOP 1 snapshot_time, state_json
            FROM stm_system_state
            ORDER BY snapshot_time DESC
            """)
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'snapshot_time': row[0],
                    'state': json.loads(row[1]) if row[1] else {}
                }
            
            return None
            
        except Exception as e:
            print(f"  Warning: STM state retrieval error: {e}")
            return None
    
    def _clean_old_proposals(self):
        """Remove proposals older than retention period"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
            
            cursor.execute("""
            DELETE FROM stm_recent_proposals
            WHERE proposal_time < ?
            """, (cutoff_time,))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            pass
    
    def _clean_old_decisions(self):
        """Remove decisions older than retention period"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
            
            cursor.execute("""
            DELETE FROM stm_recent_decisions
            WHERE decision_time < ?
            """, (cutoff_time,))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            pass
    
    def _clean_old_states(self):
        """Remove state snapshots older than retention period"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
            
            cursor.execute("""
            DELETE FROM stm_system_state
            WHERE snapshot_time < ?
            """, (cutoff_time,))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            pass
# Singleton instance
short_term_memory = ShortTermMemory()
