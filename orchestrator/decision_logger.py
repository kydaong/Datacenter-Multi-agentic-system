"""
Decision Logger
Logs complete debate sessions, decisions, and execution results to SQL Server
"""

import pyodbc
import os
from typing import Dict, List, Optional
from datetime import datetime
import json
from dotenv import load_dotenv

load_dotenv()


def _parse_dt(value) -> Optional[datetime]:
    """Parse ISO string or datetime to datetime object for SQL Server"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


class DecisionLogger:
    """
    Logs all orchestrator activities to SQL Server
    
    Tables:
    - debate_sessions: Complete debate transcripts
    - decisions: Final decisions with consensus data
    - executions: Execution results
    - conversation_log: Message-by-message log
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize decision logger
        
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

        print("  Initializing Decision Logger...")
        self._create_tables_if_not_exist()
    
    def _get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def _create_tables_if_not_exist(self):
        """Create logging tables if they don't exist"""
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Table 1: Debate Sessions
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'debate_sessions')
            CREATE TABLE debate_sessions (
                session_id VARCHAR(50) PRIMARY KEY,
                start_time DATETIME NOT NULL,
                end_time DATETIME,
                context_json NVARCHAR(MAX),
                human_input NVARCHAR(MAX),
                rounds_json NVARCHAR(MAX),
                conversation_log_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
            """)
            
            # Table 2: Decisions
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'decisions')
            CREATE TABLE decisions (
                decision_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50) NOT NULL,
                decision_type VARCHAR(50),
                description NVARCHAR(MAX),
                consensus_type VARCHAR(20),
                confidence FLOAT,
                support_percentage FLOAT,
                has_veto BIT DEFAULT 0,
                decision_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                FOREIGN KEY (session_id) REFERENCES debate_sessions(session_id)
            )
            """)
            
            # Table 3: Executions
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'executions')
            CREATE TABLE executions (
                execution_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50) NOT NULL,
                executed_at DATETIME NOT NULL,
                approval_notes NVARCHAR(MAX),
                execution_status VARCHAR(20),
                execution_result_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                FOREIGN KEY (session_id) REFERENCES debate_sessions(session_id)
            )
            """)
            
            # Table 4: Conversation Log
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'conversation_log')
            CREATE TABLE conversation_log (
                log_id INT IDENTITY(1,1) PRIMARY KEY,
                session_id VARCHAR(50) NOT NULL,
                timestamp DATETIME NOT NULL,
                speaker VARCHAR(100),
                message NVARCHAR(MAX),
                message_type VARCHAR(50),
                metadata_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                FOREIGN KEY (session_id) REFERENCES debate_sessions(session_id)
            )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"  Warning: Could not create tables: {e}")
    
    def log_session(
        self,
        session_id: str,
        context: Dict,
        human_input: Optional[str],
        debate_result: Dict,
        consensus_result: Dict
    ):
        """
        Log complete debate session
        
        Args:
            session_id: Unique session identifier
            context: System context
            human_input: Human query/input
            debate_result: Complete debate result
            consensus_result: Consensus building result
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Log debate session
            cursor.execute("""
            INSERT INTO debate_sessions (
                session_id, start_time, end_time, context_json, 
                human_input, rounds_json, conversation_log_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                _parse_dt(debate_result.get('start_time')),
                _parse_dt(debate_result.get('end_time')),
                json.dumps(context),
                human_input,
                json.dumps(debate_result.get('rounds', [])),
                json.dumps(debate_result.get('conversation_log', []))
            ))
            
            # Log decision
            decision = consensus_result['decision']
            cursor.execute("""
            INSERT INTO decisions (
                session_id, decision_type, description, consensus_type,
                confidence, support_percentage, has_veto, decision_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                decision.get('action_type'),
                decision.get('description'),
                consensus_result.get('consensus_type'),
                consensus_result.get('confidence'),
                consensus_result.get('support_percentage'),
                1 if consensus_result.get('vetoes') else 0,
                json.dumps(consensus_result)
            ))
            
            # Log conversation messages
            for log_entry in debate_result.get('conversation_log', []):
                cursor.execute("""
                INSERT INTO conversation_log (
                    session_id, timestamp, speaker, message, 
                    message_type, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    session_id,
                    _parse_dt(log_entry.get('timestamp')),
                    log_entry.get('speaker'),
                    log_entry.get('message'),
                    log_entry.get('message_type', 'GENERAL'),
                    json.dumps(log_entry.get('metadata', {}))
                ))
            
            conn.commit()
            conn.close()
            
            print(f"  ✅ Session logged: {session_id}")
            
        except Exception as e:
            print(f"  ⚠️  Logging error: {e}")
    
    def log_execution(
        self,
        session_id: str,
        approval_notes: Optional[str],
        execution_result: Dict
    ):
        """
        Log execution of approved decision
        
        Args:
            session_id: Session ID
            approval_notes: Human approval notes
            execution_result: Result of execution
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO executions (
                session_id, executed_at, approval_notes, 
                execution_status, execution_result_json
            )
            VALUES (?, ?, ?, ?, ?)
            """, (
                session_id,
                _parse_dt(execution_result.get('executed_at')),
                approval_notes,
                execution_result.get('status'),
                json.dumps(execution_result)
            ))
            
            conn.commit()
            conn.close()
            
            print(f"  ✅ Execution logged: {session_id}")
            
        except Exception as e:
            print(f"  ⚠️  Execution logging error: {e}")
    
    def load_session(self, session_id: str) -> Optional[Dict]:
        """
        Load session from database
        
        Args:
            session_id: Session ID to load
        
        Returns:
            Complete session data or None
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT 
                ds.session_id, ds.start_time, ds.end_time,
                ds.context_json, ds.human_input, ds.rounds_json,
                ds.conversation_log_json,
                d.decision_json
            FROM debate_sessions ds
            LEFT JOIN decisions d ON ds.session_id = d.session_id
            WHERE ds.session_id = ?
            """, (session_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'session_id': row[0],
                    'start_time': row[1],
                    'end_time': row[2],
                    'context': json.loads(row[3]) if row[3] else {},
                    'human_input': row[4],
                    'rounds': json.loads(row[5]) if row[5] else [],
                    'conversation_log': json.loads(row[6]) if row[6] else [],
                    'consensus_result': json.loads(row[7]) if row[7] else {}
                }
            
            return None
            
        except Exception as e:
            print(f"  ⚠️  Session load error: {e}")
            return None
    
    def get_recent_sessions(self, limit: int = 10) -> List[Dict]:
        """
        Get recent decision sessions
        
        Args:
            limit: Number of sessions to retrieve
        
        Returns:
            List of recent sessions
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT TOP (?)
                ds.session_id, ds.start_time, ds.human_input,
                d.decision_type, d.consensus_type, d.confidence
            FROM debate_sessions ds
            LEFT JOIN decisions d ON ds.session_id = d.session_id
            ORDER BY ds.start_time DESC
            """, (limit,))
            
            rows = cursor.fetchall()
            conn.close()
            
            sessions = []
            for row in rows:
                sessions.append({
                    'session_id': row[0],
                    'start_time': row[1],
                    'human_input': row[2],
                    'decision_type': row[3],
                    'consensus_type': row[4],
                    'confidence': row[5]
                })
            
            return sessions
            
        except Exception as e:
            print(f"    Recent sessions error: {e}")
            return []