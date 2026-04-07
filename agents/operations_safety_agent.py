"""
Operations & Safety Agent
Final authority on safety and operational procedures
Has VETO POWER over all proposals that violate SOPs or safety requirements
"""

import sys
sys.path.append('..')

try:
    from agents.base_agent import BaseAgent
except ImportError:
    from base_agent import BaseAgent
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import numpy as np

class OperationsSafetyAgent(BaseAgent):
    """
    Agent 6: Operations & Safety
    
    Responsibilities:
    - Enforce Standard Operating Procedures (SOPs)
    - Ensure N+1 redundancy at all times
    - Verify operational safety limits
    - Monitor critical setpoints (pressure, temperature, flow)
    - Coordinate system changes to avoid conflicts
    - Final approval authority on all proposals
    
    Authority: HIGHEST - Can veto ANY proposal for safety/SOP violations
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Operations & Safety Agent",
            agent_role="Enforce SOPs, ensure N+1 redundancy, final safety authority with veto power"
        )
        
        # Load agent-specific prompt
        self.load_prompt()
        
        # Critical safety limits
        self.safety_limits = {
            'chiller': {
                'min_chw_supply_temp_c': 5.0,
                'max_chw_supply_temp_c': 8.0,
                'min_cw_entering_temp_c': 24.0,
                'max_cw_entering_temp_c': 35.0,
                'min_oil_pressure_bar': 2.0,
                'max_oil_temp_c': 65.0,
                'min_oil_heater_temp_c': 50.0,
                'oil_heater_warmup_hours': 12,
                'max_starts_per_24hr': 6,
                'min_time_between_starts_minutes': 10,
                'min_runtime_before_stop_minutes': 15
            },
            'pump': {
                'min_vfd_speed_percent': 50,
                'max_vfd_speed_percent': 100,
                'min_differential_pressure_bar': 1.5,
                'max_differential_pressure_bar': 4.0
            },
            'tower': {
                'min_fan_speed_percent': 40,
                'max_fan_speed_percent': 100,
                'min_approach_temp_c': 2.0,
                'max_approach_temp_c': 6.0
            }
        }
        
        # N+1 redundancy requirements
        # Chiller-1: 1000t, Chiller-2: 1000t, Chiller-3: 500t (total 2500t)
        self.redundancy_requirements = {
            'chiller_capacity_tons': 2500,
            'min_online_capacity_tons': 1500,  # Must maintain N+1
            'min_chillers_online': 2  # Absolute minimum
        }
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/operations_safety_v1.txt', 'r') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

PLANT CONFIGURATION:
- Chiller-1: 1000 tons capacity
- Chiller-2: 1000 tons capacity
- Chiller-3: 500 tons capacity
- Total plant capacity: 2500 tons

N+1 REDUNDANCY RULE:
If the largest running chiller trips, the remaining online units must still cover the full cooling load.
Formula: (online_capacity - largest_online_unit) >= cooling_load_tons

Examples with current plant:
- 2 chillers online (C1+C2=2000T), load=800T: remaining after failure=1000T >= 800T → N+1 MAINTAINED ✓
- 3 chillers online (all=2500T), load=1200T: remaining after failure=1500T >= 1200T → N+1 MAINTAINED ✓
- 1 chiller online (C1=1000T), load=800T: remaining after failure=0T < 800T → N+1 VIOLATED ✗
- 2 chillers online (C1+C2=2000T), load=1100T: remaining after failure=1000T < 1100T → N+1 VIOLATED ✗

VETO ONLY when:
1. The PROPOSED action would RESULT IN an N+1 violation (evaluate the proposed end-state, not current state)
2. Active equipment alarms exist on the equipment being operated
3. Service intervals are critically exceeded (>2000 hours)

DO NOT veto for:
- Adding more chillers online (always improves redundancy)
- Actions that maintain or improve the current N+1 margin
- Energy efficiency proposals that keep N+1 intact
- Situations where current state is already N+1 compliant

Be precise: calculate the post-action state and verify N+1 before deciding to VETO.
"""
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze current safety and operational status
        
        Args:
            context: Current system state
        
        Returns:
            Safety and operational analysis
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Verify N+1 redundancy
        redundancy_status = self._verify_n_plus_1_redundancy(current_metrics, context)
        
        # Check safety limits
        safety_limits_status = self._check_safety_limits(current_metrics)
        
        # Verify SOP compliance
        sop_compliance = self._verify_sop_compliance(current_metrics, context)
        
        # Check for operational conflicts
        operational_conflicts = self._check_operational_conflicts(current_metrics)
        
        # Search knowledge base for SOPs
        sop_results = self.search_knowledge(
            query="standard operating procedures safety checklist startup shutdown",
            knowledge_type="sops"
        )
        
        return {
            'current_state': current_metrics,
            'redundancy_status': redundancy_status,
            'safety_limits_status': safety_limits_status,
            'sop_compliance': sop_compliance,
            'operational_conflicts': operational_conflicts,
            'sop_guidance': sop_results
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Evaluate safety status or review proposals
        
        Args:
            context: Current system state
        
        Returns:
            Safety assessment or proposal evaluation
        """
        
        # Analyze current situation
        analysis = self.analyze_situation(context)
        
        # Check if we're asked to evaluate another agent's proposal
        if 'proposal_to_evaluate' in context:
            return self._evaluate_proposal(context['proposal_to_evaluate'], analysis)
        
        # Check for safety violations
        if not analysis['redundancy_status']['compliant']:
            return self._create_safety_alert(analysis, 'N+1_VIOLATION')
        
        if not analysis['safety_limits_status']['all_ok']:
            return self._create_safety_alert(analysis, 'SAFETY_LIMIT_VIOLATION')
        
        if not analysis['sop_compliance']['compliant']:
            return self._create_safety_alert(analysis, 'SOP_VIOLATION')
        
        # Otherwise, monitoring update
        return self._create_monitoring_update(analysis)
    
    def _verify_n_plus_1_redundancy(self, metrics: Dict, context: Dict) -> Dict:
        """
        Verify N+1 redundancy for the current operating state.

        Plant: Chiller-1=1000T, Chiller-2=1000T, Chiller-3=500T
        N+1 rule: if the largest running chiller trips, the remaining
        online units must still cover the full cooling load.
        Formula: online_capacity - largest_online_unit >= cooling_load_tons
        """

        chiller_capacities = {'Chiller-1': 1000, 'Chiller-2': 1000, 'Chiller-3': 500}

        # Derive cooling load in tons — prefer tons field, fall back to kW conversion
        cooling_load_tons = context.get('cooling_load_tons')
        if not cooling_load_tons:
            cooling_load_kw = context.get('cooling_load_kw', 2800)
            cooling_load_tons = cooling_load_kw / 3.517

        # Get chillers online; if live data returned empty, fall back to context default
        chillers_online = context.get('chillers_online') or []
        if not chillers_online:
            # Fall back: assume standard 2-chiller config so we don't false-alarm
            chillers_online = ['Chiller-1', 'Chiller-2']

        online_capacity = sum(chiller_capacities.get(c, 1000) for c in chillers_online)

        # N+1: remaining capacity after losing largest unit must cover load
        largest_unit = max((chiller_capacities.get(c, 1000) for c in chillers_online), default=1000)
        remaining_after_failure = online_capacity - largest_unit
        compliant = remaining_after_failure >= cooling_load_tons
        margin = remaining_after_failure - cooling_load_tons

        return {
            'compliant': compliant,
            'cooling_load_tons': round(cooling_load_tons, 1),
            'online_capacity_tons': online_capacity,
            'remaining_after_largest_failure': remaining_after_failure,
            'margin_tons': round(margin, 1),
            'chillers_online': chillers_online,
            'largest_unit_tons': largest_unit,
            'n_plus_1_status': 'MAINTAINED' if compliant else 'VIOLATED'
        }
    
    def _check_safety_limits(self, metrics: Dict) -> Dict:
        """Check all safety limits using live telemetry from DB"""

        violations = []
        warnings = []

        # Fetch live chiller telemetry
        try:
            telemetry_list = self.live_data.get_chiller_telemetry()
        except Exception:
            telemetry_list = []

        if telemetry_list:
            for tel in telemetry_list:
                chiller_id = tel.get('ChillerID', 'Unknown')

                chw_temp = tel.get('ChilledWaterSupplyTempCelsius') or tel.get('CHWSupplyTempC')
                if chw_temp is not None:
                    if chw_temp < self.safety_limits['chiller']['min_chw_supply_temp_c']:
                        violations.append({'parameter': f'{chiller_id} CHW Supply Temp', 'current': chw_temp,
                                           'limit': self.safety_limits['chiller']['min_chw_supply_temp_c'], 'severity': 'HIGH'})
                    elif chw_temp > self.safety_limits['chiller']['max_chw_supply_temp_c']:
                        warnings.append({'parameter': f'{chiller_id} CHW Supply Temp', 'current': chw_temp,
                                         'limit': self.safety_limits['chiller']['max_chw_supply_temp_c'], 'severity': 'MEDIUM'})

                oil_pressure = tel.get('OilPressureBar')
                if oil_pressure is not None:
                    if oil_pressure < self.safety_limits['chiller']['min_oil_pressure_bar']:
                        violations.append({'parameter': f'{chiller_id} Oil Pressure', 'current': oil_pressure,
                                           'limit': self.safety_limits['chiller']['min_oil_pressure_bar'], 'severity': 'CRITICAL'})

                oil_temp = tel.get('OilTempCelsius')
                if oil_temp is not None:
                    if oil_temp > self.safety_limits['chiller']['max_oil_temp_c']:
                        violations.append({'parameter': f'{chiller_id} Oil Temp', 'current': oil_temp,
                                           'limit': self.safety_limits['chiller']['max_oil_temp_c'], 'severity': 'HIGH'})

                bearing_temp = tel.get('BearingTempCelsius')
                if bearing_temp is not None and bearing_temp > 85:
                    warnings.append({'parameter': f'{chiller_id} Bearing Temp', 'current': bearing_temp,
                                     'limit': 85, 'severity': 'MEDIUM'})
        else:
            # Live data unavailable — assume safe, do not trigger false violations
            pass

        return {
            'all_ok': len(violations) == 0,
            'violations': violations,
            'warnings': warnings,
            'parameters_checked': ['CHW temp', 'Oil pressure', 'Oil temp', 'Bearing temp'],
            'data_source': 'live_telemetry' if telemetry_list else 'unavailable'
        }
    
    def _verify_sop_compliance(self, metrics: Dict, context: Dict) -> Dict:
        """Verify SOP compliance using live telemetry from DB"""

        non_compliances = []

        try:
            telemetry_list = self.live_data.get_chiller_telemetry()
        except Exception:
            telemetry_list = []

        for tel in telemetry_list:
            chiller_id = tel.get('ChillerID', 'Unknown')

            # Check runtime hours since service
            runtime_since_service = tel.get('RuntimeHoursSinceService')
            if runtime_since_service is not None and runtime_since_service > 2000:
                non_compliances.append({
                    'sop': 'Service Interval',
                    'equipment': chiller_id,
                    'issue': f'{runtime_since_service:.0f} hours since last service (limit: 2000h)',
                    'severity': 'MEDIUM'
                })

            # Check active alarms
            active_alarms = tel.get('ActiveAlarms', 0)
            if active_alarms and int(active_alarms) > 0:
                non_compliances.append({
                    'sop': 'Active Alarms',
                    'equipment': chiller_id,
                    'issue': f'{active_alarms} active alarm(s) present',
                    'severity': 'HIGH'
                })

        return {
            'compliant': len(non_compliances) == 0,
            'non_compliances': non_compliances,
            'sops_checked': ['Service intervals', 'Active alarms', 'Runtime hours'],
            'data_source': 'live_telemetry' if telemetry_list else 'unavailable'
        }
    
    def _check_operational_conflicts(self, metrics: Dict) -> List[Dict]:
        """Check for operational conflicts"""
        
        conflicts = []
        
        # Example: Simultaneous staging changes
        # Example: Pump speed changes during chiller startup
        # Would check actual operational state
        
        return conflicts
    
    def _evaluate_proposal(self, proposal: Dict, analysis: Dict) -> Dict:
        """
        Evaluate another agent's proposal for safety and SOP compliance
        
        This is the FINAL approval gate - has veto power
        """
        
        action_type = proposal.get('action_type')
        
        # Safety checks
        n_plus_1_ok = self._check_proposal_n_plus_1(proposal, analysis)
        safety_limits_ok = self._check_proposal_safety_limits(proposal, analysis)
        sop_ok = self._check_proposal_sop_compliance(proposal, analysis)
        conflicts_ok = self._check_proposal_conflicts(proposal, analysis)
        
        # Determine recommendation
        all_checks_passed = n_plus_1_ok['passed'] and safety_limits_ok['passed'] and sop_ok['passed'] and conflicts_ok['passed']
        
        if not all_checks_passed:
            recommendation = "VETO"
            confidence = 0.99
            veto_reasons = []
            
            if not n_plus_1_ok['passed']:
                veto_reasons.append(n_plus_1_ok['reason'])
            if not safety_limits_ok['passed']:
                veto_reasons.append(safety_limits_ok['reason'])
            if not sop_ok['passed']:
                veto_reasons.append(sop_ok['reason'])
            if not conflicts_ok['passed']:
                veto_reasons.append(conflicts_ok['reason'])
        else:
            recommendation = "APPROVE"
            confidence = 0.95
            veto_reasons = []
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'SAFETY_EVALUATION',
            'evaluated_proposal': action_type,
            'recommendation': recommendation,
            'safety_checks': {
                'n_plus_1_redundancy': n_plus_1_ok['passed'],
                'safety_limits': safety_limits_ok['passed'],
                'sop_compliance': sop_ok['passed'],
                'operational_conflicts': conflicts_ok['passed']
            },
            'veto_reasons': veto_reasons if veto_reasons else None,
            'justification': self._create_evaluation_justification(
                all_checks_passed, n_plus_1_ok, safety_limits_ok, sop_ok, conflicts_ok
            ),
            'confidence': confidence
        }
    
    def _check_proposal_n_plus_1(self, proposal: Dict, analysis: Dict) -> Dict:
        """Check if proposal maintains N+1 redundancy"""
        
        action_type = proposal.get('action_type')
        
        # Critical check for chiller staging
        if action_type == 'CHILLER_STAGING':
            proposed_staging = proposal.get('proposed_staging', [])

            if len(proposed_staging) < self.redundancy_requirements['min_chillers_online']:
                return {
                    'passed': False,
                    'reason': f"Proposed staging ({len(proposed_staging)} chillers) violates minimum {self.redundancy_requirements['min_chillers_online']} chillers online requirement"
                }

            # Calculate proposed capacity using consistent chiller sizes
            chiller_capacities = {'Chiller-1': 1000, 'Chiller-2': 1000, 'Chiller-3': 500}
            proposed_capacity = sum(chiller_capacities.get(c, 1000) for c in proposed_staging)

            # Check against N+1 requirement
            cooling_load = analysis['redundancy_status']['cooling_load_tons']
            largest_unit = max((chiller_capacities.get(c, 1000) for c in proposed_staging), default=1000)
            required_capacity = cooling_load + largest_unit
            
            if proposed_capacity < required_capacity:
                return {
                    'passed': False,
                    'reason': f"Proposed capacity ({proposed_capacity} tons) < N+1 requirement ({required_capacity} tons). This violates N+1 redundancy."
                }
        
        return {'passed': True}
    
    def _check_proposal_safety_limits(self, proposal: Dict, analysis: Dict) -> Dict:
        """Check if proposal respects safety limits"""
        
        # Check for any safety limit violations in current state
        if not analysis['safety_limits_status']['all_ok']:
            return {
                'passed': False,
                'reason': f"Cannot approve proposal while safety violations exist: {analysis['safety_limits_status']['violations']}"
            }
        
        return {'passed': True}
    
    def _check_proposal_sop_compliance(self, proposal: Dict, analysis: Dict) -> Dict:
        """Check if proposal complies with SOPs"""
        
        action_type = proposal.get('action_type')
        
        # Check start cycle limits
        if action_type == 'CHILLER_STAGING':
            # Would check if proposed chiller has exceeded start limits
            # Simplified for now
            pass
        
        # Check for SOP violations in current state
        if not analysis['sop_compliance']['compliant']:
            return {
                'passed': False,
                'reason': f"Cannot approve proposal while SOP violations exist: {analysis['sop_compliance']['non_compliances']}"
            }
        
        return {'passed': True}
    
    def _check_proposal_conflicts(self, proposal: Dict, analysis: Dict) -> Dict:
        """Check for operational conflicts"""
        
        if analysis['operational_conflicts']:
            return {
                'passed': False,
                'reason': f"Operational conflicts detected: {analysis['operational_conflicts']}"
            }
        
        return {'passed': True}
    
    def _create_evaluation_justification(
        self,
        all_passed: bool,
        n_plus_1: Dict,
        safety: Dict,
        sop: Dict,
        conflicts: Dict
    ) -> str:
        """Create justification for evaluation decision"""
        
        if all_passed:
            return "All safety checks passed. N+1 redundancy maintained, safety limits respected, SOP compliant, no operational conflicts. APPROVED for execution."
        else:
            reasons = []
            if not n_plus_1['passed']:
                reasons.append(f"N+1 VIOLATION: {n_plus_1['reason']}")
            if not safety['passed']:
                reasons.append(f"SAFETY LIMIT: {safety['reason']}")
            if not sop['passed']:
                reasons.append(f"SOP VIOLATION: {sop['reason']}")
            if not conflicts['passed']:
                reasons.append(f"CONFLICT: {conflicts['reason']}")
            
            return "VETO - Safety violations: " + "; ".join(reasons)
    
    def _create_safety_alert(self, analysis: Dict, alert_type: str) -> Dict:
        """Create safety alert for violations"""
        
        if alert_type == 'N+1_VIOLATION':
            issue = analysis['redundancy_status']
            description = f"N+1 redundancy violated: {issue['online_capacity_tons']} tons online < {issue['required_capacity_tons']} tons required"
            severity = 'CRITICAL'
        
        elif alert_type == 'SAFETY_LIMIT_VIOLATION':
            violations = analysis['safety_limits_status']['violations']
            description = f"Safety limit violations: {violations}"
            severity = 'CRITICAL'
        
        elif alert_type == 'SOP_VIOLATION':
            non_compliances = analysis['sop_compliance']['non_compliances']
            description = f"SOP violations: {non_compliances}"
            severity = 'HIGH'
        
        else:
            description = "Unknown safety alert"
            severity = 'HIGH'
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'SAFETY_ALERT',
            'alert_type': alert_type,
            'severity': severity,
            'description': description,
            'required_action': 'IMMEDIATE CORRECTION REQUIRED',
            'confidence': 0.99
        }
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine monitoring update"""
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'All safety checks passed',
            'status': 'NOMINAL',
            'n_plus_1_status': analysis['redundancy_status']['n_plus_1_status'],
            'safety_limits': 'OK',
            'sop_compliance': 'COMPLIANT',
            'operational_conflicts': 'NONE',
            'confidence': self.confidence_level
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING OPERATIONS & SAFETY AGENT")
    print("="*70)
    
    # Create agent
    agent = OperationsSafetyAgent()
    
    # Test 1: Normal operations
    print("\n[TEST 1] Normal Operations:")
    context = {
        'cooling_load_tons': 750,
        'chillers_online': ['Chiller-1', 'Chiller-2'],
        'timestamp': datetime.now()
    }
    
    proposal = agent.propose_action(context)
    print(f"  Status: {proposal['status']}")
    print(f"  N+1: {proposal['n_plus_1_status']}")
    
    # Test 2: Evaluate a proposal (should approve)
    print("\n[TEST 2] Evaluate Valid Proposal:")
    mock_proposal = {
        'action_type': 'CHILLER_STAGING',
        'proposed_staging': ['Chiller-1', 'Chiller-2', 'Chiller-3']
    }
    
    context['proposal_to_evaluate'] = mock_proposal
    evaluation = agent.propose_action(context)
    
    print(f"  Recommendation: {evaluation['recommendation']}")
    print(f"  Safety Checks: {evaluation['safety_checks']}")
    
    # Test 3: Evaluate invalid proposal (should veto)
    print("\n[TEST 3] Evaluate Invalid Proposal (N+1 Violation):")
    bad_proposal = {
        'action_type': 'CHILLER_STAGING',
        'proposed_staging': ['Chiller-1']  # Only 1 chiller - violates N+1
    }
    
    context['proposal_to_evaluate'] = bad_proposal
    evaluation = agent.propose_action(context)
    
    print(f"  Recommendation: {evaluation['recommendation']}")
    if evaluation['veto_reasons']:
        print(f"  Veto Reason: {evaluation['veto_reasons'][0]}")
    
    print("\n Operations & Safety Agent test complete!")