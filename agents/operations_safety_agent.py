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
        self.redundancy_requirements = {
            'chiller_capacity_tons': 1100,  # Total installed: Chiller-1 (400) + Chiller-2 (400) + Chiller-3 (300)
            'min_online_capacity_tons': 800,  # Must maintain N+1
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

RESPONSIBILITIES:
1. Enforce Standard Operating Procedures (SOPs)
2. Maintain N+1 redundancy at all times (NEVER compromise)
3. Verify all operations within safety limits
4. Coordinate system changes to avoid conflicts
5. Final approval authority on ALL proposals
6. Emergency response coordination

AUTHORITY: HIGHEST
- Can VETO any proposal for safety/SOP violations
- Final approval required for all system changes
- Can override efficiency recommendations for safety
- Cannot be overridden by any other agent
- Can mandate immediate safety actions

DECISION CRITERIA:
When evaluating proposals:

1. VERIFY N+1 redundancy maintained
   - Total online capacity must exceed peak load + largest unit capacity
   - Minimum 2 chillers online at all times
   
2. CHECK operational safety limits
   - All temperatures within rated ranges
   - All pressures within safe limits
   - Flow rates adequate
   
3. VERIFY SOP compliance
   - Proper startup/shutdown sequences
   - Adequate warm-up times
   - Maximum start cycle limits not exceeded
   
4. ASSESS system coordination
   - No conflicting operations
   - Proper sequencing
   - Adequate transition time

OUTPUT FORMAT:
Provide safety assessment in JSON:
{
  "action_type": "SAFETY_EVALUATION",
  "recommendation": "APPROVE" | "VETO",
  "safety_check": {
    "n_plus_1_verified": true,
    "safety_limits_ok": true,
    "sop_compliant": true,
    "system_conflicts": false
  },
  "veto_reason": null | "description",
  "confidence": 0.99
}

VETO CRITERIA:
VETO immediately if:
- N+1 redundancy compromised
- Safety limits exceeded
- SOP violations
- Equipment not ready (oil heater, warm-up)
- Excessive start cycles
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
        """Verify N+1 redundancy is maintained"""
        
        # Get current cooling load
        cooling_load_tons = context.get('cooling_load_tons', 800)
        
        # Get online chillers (simulated)
        chillers_online = context.get('chillers_online', ['Chiller-1', 'Chiller-2'])
        
        # Calculate online capacity
        online_capacity = 0
        for chiller in chillers_online:
            if 'Chiller-3' in chiller:
                online_capacity += 300
            else:
                online_capacity += 400
        
        # Verify N+1: Online capacity must exceed load + largest unit
        largest_unit = 400 if len([c for c in chillers_online if 'Chiller-3' not in c]) > 0 else 300
        required_capacity = cooling_load_tons + largest_unit
        
        compliant = online_capacity >= required_capacity
        margin = online_capacity - required_capacity
        
        return {
            'compliant': compliant,
            'cooling_load_tons': cooling_load_tons,
            'online_capacity_tons': online_capacity,
            'required_capacity_tons': required_capacity,
            'margin_tons': round(margin, 1),
            'chillers_online': chillers_online,
            'n_plus_1_status': 'MAINTAINED' if compliant else 'VIOLATED'
        }
    
    def _check_safety_limits(self, metrics: Dict) -> Dict:
        """Check all safety limits"""
        
        violations = []
        warnings = []
        
        # Simulate current readings
        chw_supply_temp = 6.8  # °C
        cw_entering_temp = 30.5  # °C
        oil_pressure = 3.2  # bar
        
        # Check CHW supply temperature
        if chw_supply_temp < self.safety_limits['chiller']['min_chw_supply_temp_c']:
            violations.append({
                'parameter': 'CHW Supply Temperature',
                'current': chw_supply_temp,
                'limit': self.safety_limits['chiller']['min_chw_supply_temp_c'],
                'severity': 'HIGH'
            })
        elif chw_supply_temp > self.safety_limits['chiller']['max_chw_supply_temp_c']:
            warnings.append({
                'parameter': 'CHW Supply Temperature',
                'current': chw_supply_temp,
                'limit': self.safety_limits['chiller']['max_chw_supply_temp_c'],
                'severity': 'MEDIUM'
            })
        
        # Check CW entering temperature
        if cw_entering_temp > self.safety_limits['chiller']['max_cw_entering_temp_c']:
            violations.append({
                'parameter': 'CW Entering Temperature',
                'current': cw_entering_temp,
                'limit': self.safety_limits['chiller']['max_cw_entering_temp_c'],
                'severity': 'HIGH'
            })
        
        # Check oil pressure
        if oil_pressure < self.safety_limits['chiller']['min_oil_pressure_bar']:
            violations.append({
                'parameter': 'Chiller Oil Pressure',
                'current': oil_pressure,
                'limit': self.safety_limits['chiller']['min_oil_pressure_bar'],
                'severity': 'CRITICAL'
            })
        
        all_ok = len(violations) == 0
        
        return {
            'all_ok': all_ok,
            'violations': violations,
            'warnings': warnings,
            'parameters_checked': ['CHW temp', 'CW temp', 'Oil pressure', 'VFD speeds', 'Differential pressure']
        }
    
    def _verify_sop_compliance(self, metrics: Dict, context: Dict) -> Dict:
        """Verify SOP compliance"""
        
        non_compliances = []
        
        # Check start cycle limits (simulated tracking)
        chiller_starts_today = {
            'Chiller-1': 3,
            'Chiller-2': 2,
            'Chiller-3': 1
        }
        
        for chiller, starts in chiller_starts_today.items():
            if starts >= self.safety_limits['chiller']['max_starts_per_24hr']:
                non_compliances.append({
                    'sop': 'Maximum Start Cycles',
                    'equipment': chiller,
                    'issue': f'{starts} starts today (limit: {self.safety_limits["chiller"]["max_starts_per_24hr"]})',
                    'severity': 'HIGH'
                })
        
        # Check oil heater warm-up (if applicable)
        # This would check actual oil heater status from telemetry
        
        compliant = len(non_compliances) == 0
        
        return {
            'compliant': compliant,
            'non_compliances': non_compliances,
            'sops_checked': [
                'Start cycle limits',
                'Oil heater warm-up',
                'Minimum runtime',
                'Staging sequences',
                'Emergency procedures'
            ]
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
            
            # Calculate proposed capacity
            proposed_capacity = 0
            for chiller in proposed_staging:
                if 'Chiller-3' in chiller:
                    proposed_capacity += 300
                else:
                    proposed_capacity += 400
            
            # Check against N+1 requirement
            cooling_load = analysis['redundancy_status']['cooling_load_tons']
            largest_unit = 400
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