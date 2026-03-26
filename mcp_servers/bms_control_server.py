"""
BMS Control Server
Interface to Building Management System for equipment control
Enables agents to execute approved setpoint changes
"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()


class EquipmentType(Enum):
    """Equipment types in BMS"""
    CHILLER = "chiller"
    PUMP = "pump"
    COOLING_TOWER = "tower"
    AHU = "ahu"


class ControlAction(Enum):
    """Available control actions"""
    START = "start"
    STOP = "stop"
    SETPOINT_CHANGE = "setpoint_change"
    VFD_SPEED = "vfd_speed"
    STAGING = "staging"


class BMSControlServer:
    """
    MCP Server for BMS control operations
    
    In production, this would connect to actual BMS (e.g., Niagara, Tridium, Schneider)
    For development, we simulate control via database
    """
    
    def __init__(self):
        """Initialize BMS control server"""
        
        self.conn_str = (
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={{{os.getenv('DB_PASSWORD')}}};"
            f"TrustServerCertificate=yes;"
        )
        
        # Control limits (safety boundaries)
        self.control_limits = {
            'chiller': {
                'chw_supply_temp': {'min': 5.0, 'max': 8.0},  # °C
                'chw_delta_t': {'min': 4.0, 'max': 7.0},  # °C
                'max_load_rate_change': 10  # % per minute
            },
            'pump': {
                'vfd_speed': {'min': 50, 'max': 100},  # %
                'min_flow': 0.3  # Fraction of rated flow
            },
            'tower': {
                'fan_speed': {'min': 40, 'max': 100},  # %
                'approach_temp': {'min': 2.0, 'max': 6.0}  # °C
            }
        }
        
        print("BMS Control Server initialized")
    
    def execute_control_action(
        self,
        equipment_id: str,
        action: ControlAction,
        parameters: Dict[str, Any],
        approved_by: str,
        decision_id: Optional[str] = None
    ) -> Dict:
        """
        Execute control action on equipment
        
        Args:
            equipment_id: Equipment identifier (e.g., "Chiller-1")
            action: Control action type
            parameters: Action parameters (e.g., {'setpoint': 6.5})
            approved_by: Who approved this action
            decision_id: Related agent decision ID
        
        Returns:
            Execution result
        """
        
        # Validate action
        validation = self._validate_action(equipment_id, action, parameters)
        
        if not validation['valid']:
            return {
                'success': False,
                'equipment_id': equipment_id,
                'action': action.value,
                'error': validation['reason'],
                'timestamp': datetime.now().isoformat()
            }
        
        # Execute action (in production, send to BMS API)
        result = self._execute_action_internal(
            equipment_id,
            action,
            parameters
        )
        
        # Log to database
        self._log_control_action(
            equipment_id=equipment_id,
            action=action.value,
            parameters=parameters,
            result=result,
            approved_by=approved_by,
            decision_id=decision_id
        )
        
        return result
    
    def start_equipment(
        self,
        equipment_id: str,
        approved_by: str
    ) -> Dict:
        """
        Start equipment
        
        Args:
            equipment_id: Equipment to start
            approved_by: Approver
        
        Returns:
            Start result
        """
        
        return self.execute_control_action(
            equipment_id=equipment_id,
            action=ControlAction.START,
            parameters={},
            approved_by=approved_by
        )
    
    def stop_equipment(
        self,
        equipment_id: str,
        approved_by: str
    ) -> Dict:
        """
        Stop equipment
        
        Args:
            equipment_id: Equipment to stop
            approved_by: Approver
        
        Returns:
            Stop result
        """
        
        return self.execute_control_action(
            equipment_id=equipment_id,
            action=ControlAction.STOP,
            parameters={},
            approved_by=approved_by
        )
    
    def change_setpoint(
        self,
        equipment_id: str,
        setpoint_type: str,
        value: float,
        approved_by: str
    ) -> Dict:
        """
        Change equipment setpoint
        
        Args:
            equipment_id: Equipment identifier
            setpoint_type: Type (e.g., 'chw_supply_temp')
            value: New setpoint value
            approved_by: Approver
        
        Returns:
            Change result
        """
        
        return self.execute_control_action(
            equipment_id=equipment_id,
            action=ControlAction.SETPOINT_CHANGE,
            parameters={
                'setpoint_type': setpoint_type,
                'value': value
            },
            approved_by=approved_by
        )
    
    def adjust_vfd_speed(
        self,
        equipment_id: str,
        speed_percent: float,
        approved_by: str
    ) -> Dict:
        """
        Adjust VFD speed for pump or tower fan
        
        Args:
            equipment_id: Pump or tower ID
            speed_percent: Target speed (%)
            approved_by: Approver
        
        Returns:
            Adjustment result
        """
        
        return self.execute_control_action(
            equipment_id=equipment_id,
            action=ControlAction.VFD_SPEED,
            parameters={'speed_percent': speed_percent},
            approved_by=approved_by
        )
    
    def stage_chillers(
        self,
        active_chillers: List[str],
        approved_by: str
    ) -> Dict:
        """
        Stage chillers (turn on/off to match load)
        
        Args:
            active_chillers: List of chillers to run (e.g., ['Chiller-1', 'Chiller-2'])
            approved_by: Approver
        
        Returns:
            Staging result
        """
        
        return self.execute_control_action(
            equipment_id='PLANT',
            action=ControlAction.STAGING,
            parameters={'active_chillers': active_chillers},
            approved_by=approved_by
        )
    
    def _validate_action(
        self,
        equipment_id: str,
        action: ControlAction,
        parameters: Dict
    ) -> Dict:
        """
        Validate control action against safety limits
        
        Returns:
            {'valid': bool, 'reason': str}
        """
        
        # Extract equipment type
        equipment_type = self._get_equipment_type(equipment_id)
        
        if not equipment_type:
            return {
                'valid': False,
                'reason': f'Unknown equipment: {equipment_id}'
            }
        
        # Validate based on action type
        if action == ControlAction.SETPOINT_CHANGE:
            setpoint_type = parameters.get('setpoint_type')
            value = parameters.get('value')
            
            if equipment_type == 'chiller' and setpoint_type == 'chw_supply_temp':
                limits = self.control_limits['chiller']['chw_supply_temp']
                
                if value < limits['min'] or value > limits['max']:
                    return {
                        'valid': False,
                        'reason': f'CHW setpoint {value}°C outside limits [{limits["min"]}, {limits["max"]}]'
                    }
        
        elif action == ControlAction.VFD_SPEED:
            speed = parameters.get('speed_percent')
            
            if equipment_type in ['pump', 'tower']:
                limits = self.control_limits.get(equipment_type, {}).get('vfd_speed', {})
                
                if speed < limits.get('min', 0) or speed > limits.get('max', 100):
                    return {
                        'valid': False,
                        'reason': f'VFD speed {speed}% outside limits'
                    }
        
        return {'valid': True, 'reason': ''}
    
    def _execute_action_internal(
        self,
        equipment_id: str,
        action: ControlAction,
        parameters: Dict
    ) -> Dict:
        """
        Internal execution (simulated for development)
        In production, this would call actual BMS API
        """
        
        # Simulate successful execution
        result = {
            'success': True,
            'equipment_id': equipment_id,
            'action': action.value,
            'parameters': parameters,
            'timestamp': datetime.now().isoformat(),
            'estimated_execution_time': '30 seconds'
        }
        
        # Add action-specific details
        if action == ControlAction.SETPOINT_CHANGE:
            result['previous_value'] = 6.8  # Simulated
            result['new_value'] = parameters.get('value')
        
        elif action == ControlAction.START:
            result['status'] = 'STARTING'
            result['expected_online_time'] = '2 minutes'
        
        elif action == ControlAction.STOP:
            result['status'] = 'STOPPING'
            result['expected_offline_time'] = '1 minute'
        
        return result
    
    def _log_control_action(
        self,
        equipment_id: str,
        action: str,
        parameters: Dict,
        result: Dict,
        approved_by: str,
        decision_id: Optional[str]
    ):
        """
        Log control action to database for audit trail
        """
        
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            # Insert into control log table (would need to create this table)
            # For now, just print
            print(f"[BMS LOG] {equipment_id} - {action} - {approved_by} - {result['success']}")
            
            conn.close()
            
        except Exception as e:
            print(f"⚠️  Failed to log control action: {e}")
    
    def _get_equipment_type(self, equipment_id: str) -> Optional[str]:
        """Extract equipment type from ID"""
        
        if 'Chiller' in equipment_id:
            return 'chiller'
        elif 'Pump' in equipment_id or 'PCHWP' in equipment_id or 'SCHWP' in equipment_id or 'CWP' in equipment_id:
            return 'pump'
        elif 'CT' in equipment_id or 'Tower' in equipment_id:
            return 'tower'
        elif 'AHU' in equipment_id:
            return 'ahu'
        else:
            return None
    
    def get_current_status(self, equipment_id: str) -> Dict:
        """
        Get current equipment status from BMS
        
        Args:
            equipment_id: Equipment identifier
        
        Returns:
            Current status dictionary
        """
        
        # In production, query BMS API
        # For development, query database
        
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            equipment_type = self._get_equipment_type(equipment_id)
            
            if equipment_type == 'chiller':
                query = """
                    SELECT TOP 1
                        RunningStatus,
                        CapacityPercent,
                        PowerConsumptionKW,
                        CHWSupplyTempCelsius,
                        EfficiencyKwPerTon
                    FROM ChillerTelemetry
                    WHERE ChillerID = ?
                    ORDER BY Timestamp DESC
                """
                
                cursor.execute(query, (equipment_id,))
                row = cursor.fetchone()
                
                if row:
                    return {
                        'equipment_id': equipment_id,
                        'running_status': row[0],
                        'capacity_percent': row[1],
                        'power_kw': row[2],
                        'chw_supply_temp': row[3],
                        'efficiency_kw_per_ton': row[4],
                        'timestamp': datetime.now().isoformat()
                    }
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error getting status: {e}")
        
        return {
            'equipment_id': equipment_id,
            'error': 'Status not available'
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING BMS CONTROL SERVER")
    print("="*70)
    
    # Initialize server
    bms = BMSControlServer()
    
    # Test 1: Change setpoint
    print("\n[TEST 1] Change Chiller Setpoint:")
    result = bms.change_setpoint(
        equipment_id='Chiller-1',
        setpoint_type='chw_supply_temp',
        value=6.5,
        approved_by='Orchestrator'
    )
    print(f"  Success: {result['success']}")
    print(f"  Previous: {result.get('previous_value')}°C")
    print(f"  New: {result.get('new_value')}°C")
    
    # Test 2: Adjust VFD speed
    print("\n[TEST 2] Adjust Pump VFD:")
    result = bms.adjust_vfd_speed(
        equipment_id='PCHWP-1',
        speed_percent=75.0,
        approved_by='Building Systems Agent'
    )
    print(f"  Success: {result['success']}")
    
    # Test 3: Stage chillers
    print("\n[TEST 3] Stage Chillers:")
    result = bms.stage_chillers(
        active_chillers=['Chiller-1', 'Chiller-2'],
        approved_by='Chiller Optimization Agent'
    )
    print(f"  Success: {result['success']}")
    
    # Test 4: Safety limit violation
    print("\n[TEST 4] Test Safety Limits:")
    result = bms.change_setpoint(
        equipment_id='Chiller-1',
        setpoint_type='chw_supply_temp',
        value=4.0,  # Below minimum
        approved_by='Test'
    )
    print(f"  Success: {result['success']}")
    print(f"  Error: {result.get('error')}")
    
    print("\nBMS Control Server tests complete!")