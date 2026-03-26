"""
Data Ingestion Server
Real-time streaming data from sensors/BMS to database
Handles high-frequency telemetry data ingestion
"""

import pyodbc
from typing import Dict, List, Optional
from datetime import datetime
import json
from collections import deque
import threading
import time
from dotenv import load_dotenv
import os

load_dotenv()


class DataBuffer:
    """
    Thread-safe buffer for incoming data
    Batches data before database insert
    """
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize buffer
        
        Args:
            max_size: Maximum buffer size before auto-flush
        """
        self.max_size = max_size
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
    
    def add(self, data: Dict):
        """Add data to buffer"""
        with self.lock:
            self.buffer.append(data)
    
    def get_batch(self, batch_size: int = 100) -> List[Dict]:
        """Get batch of data"""
        with self.lock:
            batch = []
            for _ in range(min(batch_size, len(self.buffer))):
                if self.buffer:
                    batch.append(self.buffer.popleft())
            return batch
    
    def size(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return len(self.buffer)
    
    def clear(self):
        """Clear buffer"""
        with self.lock:
            self.buffer.clear()


class DataIngestionServer:
    """
    MCP Server for real-time data ingestion
    Receives telemetry from BMS and stores in database
    """
    
    def __init__(self):
        """Initialize data ingestion server"""
        
        self.conn_str = (
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={{{os.getenv('DB_PASSWORD')}}};"
            f"TrustServerCertificate=yes;"
        )
        
        # Buffers for different data types
        self.buffers = {
            'chiller_telemetry': DataBuffer(max_size=1000),
            'pump_telemetry': DataBuffer(max_size=1000),
            'tower_telemetry': DataBuffer(max_size=1000),
            'weather': DataBuffer(max_size=500),
            'system_metrics': DataBuffer(max_size=500)
        }
        
        # Ingestion statistics
        self.stats = {
            'total_records_ingested': 0,
            'total_batches_processed': 0,
            'errors': 0,
            'last_flush_time': None
        }
        
        # Auto-flush thread
        self.flush_interval = 60  # seconds
        self.running = False
        
        print("Data Ingestion Server initialized")
    
    def start_auto_flush(self):
        """Start background thread for periodic buffer flush"""
        
        if self.running:
            print("  Auto-flush already running")
            return
        
        self.running = True
        
        def flush_worker():
            while self.running:
                time.sleep(self.flush_interval)
                self.flush_all_buffers()
        
        thread = threading.Thread(target=flush_worker, daemon=True)
        thread.start()
        
        print(f" Auto-flush started (interval: {self.flush_interval}s)")
    
    def stop_auto_flush(self):
        """Stop auto-flush thread"""
        self.running = False
        print(" Auto-flush stopped")
    
    def ingest_chiller_telemetry(self, data: Dict) -> Dict:
        """
        Ingest chiller telemetry data
        
        Args:
            data: Chiller telemetry dictionary
        
        Returns:
            Ingestion result
        """
        
        # Validate required fields
        required_fields = ['ChillerID', 'Timestamp', 'RunningStatus', 'PowerConsumptionKW']
        
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing required field: {field}'
                }
        
        # Add to buffer
        self.buffers['chiller_telemetry'].add(data)
        
        # Check if buffer is full (trigger flush)
        if self.buffers['chiller_telemetry'].size() >= 100:
            self._flush_buffer('chiller_telemetry')
        
        return {
            'success': True,
            'buffer_size': self.buffers['chiller_telemetry'].size()
        }
    
    def ingest_pump_telemetry(self, data: Dict) -> Dict:
        """
        Ingest pump telemetry data
        
        Args:
            data: Pump telemetry dictionary
        
        Returns:
            Ingestion result
        """
        
        required_fields = ['PumpID', 'Timestamp', 'RunningStatus', 'VFDSpeedPercent']
        
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing required field: {field}'
                }
        
        self.buffers['pump_telemetry'].add(data)
        
        if self.buffers['pump_telemetry'].size() >= 100:
            self._flush_buffer('pump_telemetry')
        
        return {
            'success': True,
            'buffer_size': self.buffers['pump_telemetry'].size()
        }
    
    def ingest_tower_telemetry(self, data: Dict) -> Dict:
        """
        Ingest cooling tower telemetry data
        
        Args:
            data: Tower telemetry dictionary
        
        Returns:
            Ingestion result
        """
        
        required_fields = ['TowerID', 'Timestamp', 'Fan1Status', 'TotalFanPowerKW']
        
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing required field: {field}'
                }
        
        self.buffers['tower_telemetry'].add(data)
        
        if self.buffers['tower_telemetry'].size() >= 100:
            self._flush_buffer('tower_telemetry')
        
        return {
            'success': True,
            'buffer_size': self.buffers['tower_telemetry'].size()
        }
    
    def ingest_weather_data(self, data: Dict) -> Dict:
        """
        Ingest weather data
        
        Args:
            data: Weather data dictionary
        
        Returns:
            Ingestion result
        """
        
        required_fields = ['Timestamp', 'OutdoorTempCelsius', 'WetBulbTempCelsius', 'RelativeHumidityPercent']
        
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing required field: {field}'
                }
        
        self.buffers['weather'].add(data)
        
        if self.buffers['weather'].size() >= 50:
            self._flush_buffer('weather')
        
        return {
            'success': True,
            'buffer_size': self.buffers['weather'].size()
        }
    
    def ingest_system_metrics(self, data: Dict) -> Dict:
        """
        Ingest system-level performance metrics
        
        Args:
            data: System metrics dictionary
        
        Returns:
            Ingestion result
        """
        
        required_fields = ['Timestamp', 'TotalChillerPowerKW', 'TotalCoolingLoadKW', 'PUE']
        
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing required field: {field}'
                }
        
        self.buffers['system_metrics'].add(data)
        
        if self.buffers['system_metrics'].size() >= 50:
            self._flush_buffer('system_metrics')
        
        return {
            'success': True,
            'buffer_size': self.buffers['system_metrics'].size()
        }
    
    def ingest_batch(self, data_type: str, batch: List[Dict]) -> Dict:
        """
        Ingest batch of data at once
        
        Args:
            data_type: Type of data (chiller_telemetry, pump_telemetry, etc.)
            batch: List of data dictionaries
        
        Returns:
            Ingestion result
        """
        
        if data_type not in self.buffers:
            return {
                'success': False,
                'error': f'Unknown data type: {data_type}'
            }
        
        # Add all to buffer
        for data in batch:
            self.buffers[data_type].add(data)
        
        # Flush immediately
        result = self._flush_buffer(data_type)
        
        return {
            'success': result['success'],
            'records_ingested': len(batch),
            'buffer_size': self.buffers[data_type].size()
        }
    
    def _flush_buffer(self, buffer_name: str) -> Dict:
        """
        Flush buffer to database
        
        Args:
            buffer_name: Buffer to flush
        
        Returns:
            Flush result
        """
        
        buffer = self.buffers.get(buffer_name)
        
        if not buffer:
            return {'success': False, 'error': f'Unknown buffer: {buffer_name}'}
        
        # Get batch
        batch = buffer.get_batch(batch_size=1000)
        
        if not batch:
            return {'success': True, 'records_flushed': 0}
        
        # Insert to database
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            # Choose insert query based on buffer type
            if buffer_name == 'chiller_telemetry':
                result = self._insert_chiller_telemetry(cursor, batch)
            
            elif buffer_name == 'pump_telemetry':
                result = self._insert_pump_telemetry(cursor, batch)
            
            elif buffer_name == 'tower_telemetry':
                result = self._insert_tower_telemetry(cursor, batch)
            
            elif buffer_name == 'weather':
                result = self._insert_weather(cursor, batch)
            
            elif buffer_name == 'system_metrics':
                result = self._insert_system_metrics(cursor, batch)
            
            else:
                result = {'success': False, 'error': 'Unknown buffer type'}
            
            conn.commit()
            conn.close()
            
            # Update stats
            if result['success']:
                self.stats['total_records_ingested'] += len(batch)
                self.stats['total_batches_processed'] += 1
                self.stats['last_flush_time'] = datetime.now()
            else:
                self.stats['errors'] += 1
            
            return result
            
        except Exception as e:
            self.stats['errors'] += 1
            return {
                'success': False,
                'error': str(e)
            }
    
    def _insert_chiller_telemetry(self, cursor, batch: List[Dict]) -> Dict:
        """Insert chiller telemetry batch"""
        
        insert_sql = """
            INSERT INTO ChillerTelemetry
            (Timestamp, ChillerID, RunningStatus, CapacityPercent, PowerConsumptionKW,
             EfficiencyKwPerTon, CHWSupplyTempCelsius, CHWReturnTempCelsius, CHWFlowRateLPM)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        rows = []
        for record in batch:
            rows.append((
                record.get('Timestamp'),
                record.get('ChillerID'),
                record.get('RunningStatus'),
                record.get('CapacityPercent'),
                record.get('PowerConsumptionKW'),
                record.get('EfficiencyKwPerTon'),
                record.get('CHWSupplyTempCelsius'),
                record.get('CHWReturnTempCelsius'),
                record.get('CHWFlowRateLPM')
            ))
        
        cursor.executemany(insert_sql, rows)
        
        return {
            'success': True,
            'records_flushed': len(batch)
        }
    
    def _insert_pump_telemetry(self, cursor, batch: List[Dict]) -> Dict:
        """Insert pump telemetry batch"""
        
        insert_sql = """
            INSERT INTO PumpTelemetry
            (Timestamp, PumpID, RunningStatus, VFDSpeedPercent, PowerConsumptionKW,
             FlowRateLPM, DifferentialPressureBar)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        rows = []
        for record in batch:
            rows.append((
                record.get('Timestamp'),
                record.get('PumpID'),
                record.get('RunningStatus'),
                record.get('VFDSpeedPercent'),
                record.get('PowerConsumptionKW'),
                record.get('FlowRateLPM'),
                record.get('DifferentialPressureBar')
            ))
        
        cursor.executemany(insert_sql, rows)
        
        return {
            'success': True,
            'records_flushed': len(batch)
        }
    
    def _insert_tower_telemetry(self, cursor, batch: List[Dict]) -> Dict:
        """Insert cooling tower telemetry batch"""
        
        insert_sql = """
            INSERT INTO CoolingTowerTelemetry
            (Timestamp, TowerID, Fan1Status, Fan1VFDSpeedPercent, Fan2Status, 
             Fan2VFDSpeedPercent, TotalFanPowerKW, BasinTempCelsius)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        rows = []
        for record in batch:
            rows.append((
                record.get('Timestamp'),
                record.get('TowerID'),
                record.get('Fan1Status'),
                record.get('Fan1VFDSpeedPercent'),
                record.get('Fan2Status'),
                record.get('Fan2VFDSpeedPercent'),
                record.get('TotalFanPowerKW'),
                record.get('BasinTempCelsius')
            ))
        
        cursor.executemany(insert_sql, rows)
        
        return {
            'success': True,
            'records_flushed': len(batch)
        }
    
    def _insert_weather(self, cursor, batch: List[Dict]) -> Dict:
        """Insert weather data batch"""
        
        insert_sql = """
            INSERT INTO WeatherConditions
            (Timestamp, OutdoorTempCelsius, WetBulbTempCelsius, RelativeHumidityPercent,
             DewPointCelsius, BarometricPressureMbar)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        rows = []
        for record in batch:
            rows.append((
                record.get('Timestamp'),
                record.get('OutdoorTempCelsius'),
                record.get('WetBulbTempCelsius'),
                record.get('RelativeHumidityPercent'),
                record.get('DewPointCelsius'),
                record.get('BarometricPressureMbar')
            ))
        
        cursor.executemany(insert_sql, rows)
        
        return {
            'success': True,
            'records_flushed': len(batch)
        }
    
    def _insert_system_metrics(self, cursor, batch: List[Dict]) -> Dict:
        """Insert system metrics batch"""
        
        insert_sql = """
            INSERT INTO SystemPerformanceMetrics
            (Timestamp, TotalChillerPowerKW, TotalCoolingLoadKW, PlantEfficiencyKWPerTon,
             PlantCOP, PUE, ChillersOnline)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        rows = []
        for record in batch:
            rows.append((
                record.get('Timestamp'),
                record.get('TotalChillerPowerKW'),
                record.get('TotalCoolingLoadKW'),
                record.get('PlantEfficiencyKWPerTon'),
                record.get('PlantCOP'),
                record.get('PUE'),
                record.get('ChillersOnline')
            ))
        
        cursor.executemany(insert_sql, rows)
        
        return {
            'success': True,
            'records_flushed': len(batch)
        }
    
    def flush_all_buffers(self) -> Dict:
        """Flush all buffers to database"""
        
        results = {}
        
        for buffer_name in self.buffers.keys():
            result = self._flush_buffer(buffer_name)
            results[buffer_name] = result
        
        return results
    
    def get_stats(self) -> Dict:
        """Get ingestion statistics"""
        
        return {
            'total_records_ingested': self.stats['total_records_ingested'],
            'total_batches_processed': self.stats['total_batches_processed'],
            'errors': self.stats['errors'],
            'last_flush_time': self.stats['last_flush_time'].isoformat() if self.stats['last_flush_time'] else None,
            'buffer_sizes': {
                name: buffer.size()
                for name, buffer in self.buffers.items()
            }
        }
    
    def reset_stats(self):
        """Reset statistics"""
        
        self.stats = {
            'total_records_ingested': 0,
            'total_batches_processed': 0,
            'errors': 0,
            'last_flush_time': None
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING DATA INGESTION SERVER")
    print("="*70)
    
    # Initialize server
    ingestor = DataIngestionServer()
    
    # Test 1: Ingest single chiller record
    print("\n[TEST 1] Ingest Chiller Telemetry:")
    result = ingestor.ingest_chiller_telemetry({
        'Timestamp': datetime.now(),
        'ChillerID': 'Chiller-1',
        'RunningStatus': 'ON',
        'CapacityPercent': 75.0,
        'PowerConsumptionKW': 450.0,
        'EfficiencyKwPerTon': 0.52,
        'CHWSupplyTempCelsius': 6.8,
        'CHWReturnTempCelsius': 12.2,
        'CHWFlowRateLPM': 7500
    })
    print(f"  Success: {result['success']}")
    print(f"  Buffer size: {result['buffer_size']}")
    
    # Test 2: Ingest batch
    print("\n[TEST 2] Ingest Batch:")
    batch = []
    for i in range(5):
        batch.append({
            'Timestamp': datetime.now(),
            'PumpID': f'PCHWP-{i+1}',
            'RunningStatus': 'ON',
            'VFDSpeedPercent': 70.0 + i,
            'PowerConsumptionKW': 30.0 + i,
            'FlowRateLPM': 6000,
            'DifferentialPressureBar': 2.5
        })
    
    result = ingestor.ingest_batch('pump_telemetry', batch)
    print(f"  Success: {result['success']}")
    print(f"  Records ingested: {result['records_ingested']}")
    
    # Test 3: Get statss
    print("\n[TEST 3] Ingestion Statistics:")
    stats = ingestor.get_stats()
    print(f"  Total records: {stats['total_records_ingested']}")
    print(f"  Total batches: {stats['total_batches_processed']}")
    print(f"  Errors: {stats['errors']}")
    print(f"  Buffer sizes: {stats['buffer_sizes']}")
    
    print("\n Data Ingestion Server tests complete!")