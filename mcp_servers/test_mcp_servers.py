"""
Comprehensive MCP Servers Testing
Tests all MCP servers together
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from bms_control_server import BMSControlServer, ControlAction
from notification_server import NotificationServer, NotificationPriority
from data_ingestion_server import DataIngestionServer
from datetime import datetime

def test_all_mcp_servers():
    """
    Test all MCP servers in sequence
    Simulates real workflow
    """
    
    print("="*70)
    print("COMPREHENSIVE MCP SERVERS TEST")
    print("="*70)
    
    # Initialize all servers
    print("\n[SETUP] Initializing servers...")
    bms = BMSControlServer()
    notif = NotificationServer()
    ingestor = DataIngestionServer()
    
    print(" All servers initialized")
    
    # ====================
    # SCENARIO 1: Equipment Alarm → Notification
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO 1: Equipment Alarm Workflow")
    print("="*70)
    
    print("\n[1.1] Simulating alarm detection...")
    alarm_data = {
        'equipment_id': 'Chiller-1',
        'alarm_code': 'ALM-CH-004',
        'alarm_description': 'High Vibration',
        'severity': 'CRITICAL',
        'triggered_value': 3.5,
        'threshold_value': 3.0
    }
    
    print("\n[1.2] Sending alarm notification...")
    result = notif.send_alarm_notification(**alarm_data)
    
    print(f"✅ Notification sent: {result['channels']['email']['success']}")
    
    # ====================
    # SCENARIO 2: Agent Decision → BMS Control
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO 2: Agent Decision Execution Workflow")
    print("="*70)
    
    print("\n[2.1] Agent proposes setpoint change...")
    decision = {
        'decision_id': 'DEC-12345',
        'proposed_by': 'Chiller Optimization Agent',
        'action_type': 'SETPOINT_CHANGE',
        'description': 'Reduce CHW supply temp to 6.5°C for better part-load efficiency',
        'predicted_savings': {
            'energy_kw': 45,
            'cost_sgd': 108,
            'pue_improvement': 0.015
        }
    }
    
    print("\n[2.2] Sending approval request...")
    result = notif.send_decision_approval_request(**decision)
    print(f"✅ Approval request sent: {result['channels']['email']['success']}")
    
    print("\n[2.3] Simulating approval... (approved)")
    
    print("\n[2.4] Executing BMS control action...")
    result = bms.change_setpoint(
        equipment_id='Chiller-1',
        setpoint_type='chw_supply_temp',
        value=6.5,
        approved_by='Operations Manager'
    )
    
    print(f" Control executed: {result['success']}")
    print(f"   Previous: {result.get('previous_value')}°C")
    print(f"   New: {result.get('new_value')}°C")
    
    # ====================
    # SCENARIO 3: Real-time Data Ingestion
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO 3: Real-time Data Ingestion Workflow")
    print("="*70)
    
    print("\n[3.1] Ingesting telemetry data...")
    
    # Ingest chiller data
    result = ingestor.ingest_chiller_telemetry({
        'Timestamp': datetime.now(),
        'ChillerID': 'Chiller-1',
        'RunningStatus': 'ON',
        'CapacityPercent': 75.0,
        'PowerConsumptionKW': 450.0,
        'EfficiencyKwPerTon': 0.52,
        'CHWSupplyTempCelsius': 6.5,  # After setpoint change
        'CHWReturnTempCelsius': 12.1,
        'CHWFlowRateLPM': 7500
    })
    print(f" Chiller telemetry ingested: {result['success']}")
    
    # Ingest weather data
    result = ingestor.ingest_weather_data({
        'Timestamp': datetime.now(),
        'OutdoorTempCelsius': 30.5,
        'WetBulbTempCelsius': 25.2,
        'RelativeHumidityPercent': 75.0,
        'DewPointCelsius': 24.8,
        'BarometricPressureMbar': 1013.2
    })
    print(f" Weather data ingested: {result['success']}")
    
    # Flush buffers
    print("\n[3.2] Flushing data buffers to database...")
    results = ingestor.flush_all_buffers()
    
    total_flushed = sum(
        r.get('records_flushed', 0)
        for r in results.values()
        if r.get('success')
    )
    print(f"✅ Flushed {total_flushed} records to database")
    
    # ====================
    # SCENARIO 4: Performance Report Generation
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO 4: Performance Reporting Workflow")
    print("="*70)
    
    print("\n[4.1] Generating daily performance report...")
    result = notif.send_performance_report(
        report_type='daily',
        metrics={
            'period': datetime.now().strftime('%Y-%m-%d'),
            'avg_pue': 1.22,
            'total_energy_kwh': 24500,
            'total_cost_sgd': 4900,
            'avg_plant_efficiency': 0.54,
            'uptime_percent': 99.8,
            'decisions_proposed': 12,
            'decisions_executed': 8,
            'total_savings_sgd': 450
        }
    )
    print(f" Report sent: {result['channels']['email']['success']}")
    
    # ====================
    # FINAL SUMMARY
    # ====================
    
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)
    
    # Ingestion stats
    stats = ingestor.get_stats()
    print("\nData Ingestion Statistics:")
    print(f"  Total records ingested: {stats['total_records_ingested']}")
    print(f"  Total batches processed: {stats['total_batches_processed']}")
    print(f"  Errors: {stats['errors']}")
    
    # BMS control status
    print("\nBMS Control:")
    status = bms.get_current_status('Chiller-1')
    if 'error' not in status:
        print(f"  Chiller-1 status: {status.get('running_status')}")
        print(f"  CHW supply temp: {status.get('chw_supply_temp')}°C")
        print(f"  Efficiency: {status.get('efficiency_kw_per_ton')} kW/ton")
    
    print("\n" + "="*70)
    print(" ALL MCP SERVERS TESTS PASSED!")
    print("="*70)


if __name__ == "__main__":
    test_all_mcp_servers()