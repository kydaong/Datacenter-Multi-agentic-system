"""
Maintenance & Compliance Agent
Ensures equipment health, preventive maintenance, and regulatory compliance
Safety-focused agent that prevents failures and ensures compliance
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

class MaintenanceComplianceAgent(BaseAgent):
    """
    Agent 5: Maintenance & Compliance
    
    Responsibilities:
    - Monitor equipment health (vibration, bearing temps, efficiency degradation)
    - Schedule preventive maintenance
    - Ensure regulatory compliance (NEA, ASHRAE, ISO)
    - Track equipment runtime and service intervals
    - Flag equipment due for maintenance
    - Prevent operations that accelerate wear
    
    Authority: Can veto proposals that risk equipment health or compliance violations
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Maintenance & Compliance Agent",
            agent_role="Ensure equipment health, preventive maintenance, and regulatory compliance"
        )
        
        # Load agent-specific prompt
        self.load_prompt()
        
        # Maintenance intervals (hours)
        self.maintenance_intervals = {
            'chiller': {
                'oil_change': 1200,
                'refrigerant_top_up': 2400,
                'compressor_inspection': 4000,
                'tube_cleaning_evaporator': 8760,
                'tube_cleaning_condenser': 8760,
                'annual_service': 8760
            },
            'pump': {
                'bearing_replacement': 8760,
                'seal_replacement': 4380,
                'vfd_calibration': 4380,
                'motor_inspection': 8760
            },
            'tower': {
                'fill_media_cleaning': 4380,
                'fan_motor_service': 8760,
                'basin_cleaning': 2190,
                'water_treatment': 720
            }
        }
        
        # Health thresholds
        self.health_thresholds = {
            'vibration_warning_mm_s': 4.5,
            'vibration_critical_mm_s': 6,
            'bearing_temp_warning_c': 65,
            'bearing_temp_critical_c': 75,
            'efficiency_degradation_warning_percent': 10,
            'efficiency_degradation_critical_percent': 20
        }
        
        # Compliance requirements (NEA Singapore)
        self.compliance_requirements = {
            'pue_reporting_frequency_months': 3,
            'pue_limit_new_dc': 1.30,
            'pue_limit_existing_dc': 1.40,
            'free_cooling_mandatory_wb_threshold_c': 22.0,
            'min_chiller_iplv_kw_per_ton': 0.50,
            'data_logging_interval_minutes': 15,
            'measurement_accuracy_percent': 2.0
        }
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/maintenance_compliance_v1.txt', 'r') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

RESPONSIBILITIES:
1. Monitor equipment health continuously
2. Track maintenance schedules and service intervals
3. Flag equipment due for maintenance
4. Ensure regulatory compliance (NEA Singapore)
5. Prevent operations that accelerate equipment wear
6. Review proposals for equipment health impact

AUTHORITY:
- Can veto proposals that risk equipment damage
- Can veto proposals that violate compliance requirements
- Can flag equipment for immediate maintenance
- Can recommend preventive maintenance timing

DECISION CRITERIA:
Equipment health assessment:
- Vibration monitoring (>3.0 mm/s = warning, >4.0 = critical)
- Bearing temperature (<65°C normal, >75°C critical)
- Efficiency degradation (>10% = investigation needed)
- Runtime since last service
- Start/stop cycles (excessive cycling damages equipment)

Compliance verification:
- NEA PUE limits (≤1.30 new, ≤1.40 existing)
- Free cooling requirement (wet-bulb <22°C)
- Quarterly reporting requirements
- Data logging and accuracy requirements

OUTPUT FORMAT:
Provide maintenance/compliance status in JSON with:
- Equipment health scores
- Maintenance due dates
- Compliance status
- Risk flags
- Recommended actions
"""
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze equipment health and compliance status
        
        Args:
            context: Current system state
        
        Returns:
            Health and compliance analysis
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Assess equipment health
        equipment_health = self._assess_equipment_health(current_metrics)
        
        # Check maintenance schedules
        maintenance_status = self._check_maintenance_schedules()
        
        # Verify compliance
        compliance_status = self._verify_compliance(current_metrics, context)
        
        # Identify maintenance opportunities/requirements
        maintenance_actions = self._identify_maintenance_actions(
            equipment_health,
            maintenance_status,
            compliance_status
        )
        
        # Search knowledge base
        sop_results = self.search_knowledge(
            query="preventive maintenance equipment health monitoring",
            knowledge_type="sops",
            filters={'equipment_type': 'chiller'}
        )
        
        regulations = self.search_knowledge(
            query="NEA energy efficiency requirements Singapore datacenters",
            knowledge_type="regulations"
        )
        
        return {
            'current_state': current_metrics,
            'equipment_health': equipment_health,
            'maintenance_status': maintenance_status,
            'compliance_status': compliance_status,
            'maintenance_actions': maintenance_actions,
            'sop_guidance': sop_results,
            'regulations': regulations
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose maintenance or compliance actions
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action or status update
        """
        
        # Analyze current situation
        analysis = self.analyze_situation(context)
        
        # Check if we're asked to evaluate another agent's proposal
        if 'proposal_to_evaluate' in context:
            return self._evaluate_proposal(context['proposal_to_evaluate'], analysis)
        
        # Check for critical health issues
        critical_issues = [
            action for action in analysis['maintenance_actions']
            if action['priority'] == 'CRITICAL'
        ]
        
        if critical_issues:
            return self._propose_urgent_maintenance(critical_issues[0], analysis)
        
        # Check for upcoming maintenance
        due_soon = [
            action for action in analysis['maintenance_actions']
            if action['priority'] == 'HIGH' and action['type'] == 'PREVENTIVE'
        ]
        
        if due_soon:
            return self._propose_preventive_maintenance(due_soon[0], analysis)
        
        # Check compliance issues
        if not analysis['compliance_status']['compliant']:
            return self._propose_compliance_action(analysis)
        
        # Otherwise, monitoring update
        return self._create_monitoring_update(analysis)
    
    def _assess_equipment_health(self, metrics: Dict) -> Dict:
        """Assess health of all equipment"""
        
        health_scores = {}
        
        # Chiller health (simulated - would query actual telemetry)
        for chiller_id in ['Chiller-1', 'Chiller-2', 'Chiller-3']:
            vibration = 2.5 + np.random.normal(0, 0.3)
            bearing_temp = 55 + np.random.normal(0, 3)
            efficiency_degradation = 5 + np.random.normal(0, 2)
            
            # Calculate health score (0-100)
            health_score = 100
            
            if vibration > self.health_thresholds['vibration_warning_mm_s']:
                health_score -= 20
            if vibration > self.health_thresholds['vibration_critical_mm_s']:
                health_score -= 30
            
            if bearing_temp > self.health_thresholds['bearing_temp_warning_c']:
                health_score -= 15
            if bearing_temp > self.health_thresholds['bearing_temp_critical_c']:
                health_score -= 35
            
            if efficiency_degradation > self.health_thresholds['efficiency_degradation_warning_percent']:
                health_score -= 10
            
            health_score = max(0, health_score)
            
            # Determine status
            if health_score >= 90:
                status = "EXCELLENT"
            elif health_score >= 75:
                status = "GOOD"
            elif health_score >= 60:
                status = "FAIR"
            elif health_score >= 40:
                status = "POOR"
            else:
                status = "CRITICAL"
            
            health_scores[chiller_id] = {
                'health_score': round(health_score, 1),
                'status': status,
                'vibration_mm_s': round(vibration, 2),
                'bearing_temp_c': round(bearing_temp, 1),
                'efficiency_degradation_percent': round(efficiency_degradation, 1)
            }
        
        return health_scores
    
    def _check_maintenance_schedules(self) -> Dict:
        """Check maintenance schedules for all equipment"""
        
        # Simulated maintenance tracking (would query maintenance log database)
        current_time = datetime.now()
        
        maintenance_schedule = {
            'Chiller-1': {
                'runtime_hours': 8350,
                'last_service': (current_time - timedelta(days=320)).isoformat(),
                'next_service_due': 'Oil change (1200hr interval)',
                'hours_until_service': 50,
                'urgency': 'MEDIUM'
            },
            'Chiller-2': {
                'runtime_hours': 8200,
                'last_service': (current_time - timedelta(days=310)).isoformat(),
                'next_service_due': 'Oil change (1200hr interval)',
                'hours_until_service': 200,
                'urgency': 'LOW'
            },
            'Chiller-3': {
                'runtime_hours': 6800,
                'last_service': (current_time - timedelta(days=250)).isoformat(),
                'next_service_due': 'Oil change (1200hr interval)',
                'hours_until_service': 600,
                'urgency': 'LOW'
            }
        }
        
        return maintenance_schedule
    
    def _verify_compliance(self, metrics: Dict, context: Dict) -> Dict:
        """Verify regulatory compliance"""
        
        compliance_checks = {}
        all_compliant = True
        
        # Check 1: PUE compliance
        current_pue = context.get('current_pue', 1.25)
        pue_limit = self.compliance_requirements['pue_limit_existing_dc']
        
        pue_compliant = current_pue <= pue_limit
        compliance_checks['pue'] = {
            'compliant': pue_compliant,
            'current_value': current_pue,
            'limit': pue_limit,
            'requirement': f'PUE ≤ {pue_limit} (NEA requirement for existing datacenters)'
        }
        
        if not pue_compliant:
            all_compliant = False
        
        # Check 2: Free cooling requirement
        wet_bulb = context.get('wet_bulb_temp', 25.0)
        economizer_enabled = context.get('economizer_enabled', False)
        free_cooling_threshold = self.compliance_requirements['free_cooling_mandatory_wb_threshold_c']
        
        if wet_bulb < free_cooling_threshold:
            free_cooling_compliant = economizer_enabled
            compliance_checks['free_cooling'] = {
                'compliant': free_cooling_compliant,
                'wet_bulb_temp': wet_bulb,
                'threshold': free_cooling_threshold,
                'economizer_enabled': economizer_enabled,
                'requirement': f'Mandatory free cooling when wet-bulb < {free_cooling_threshold}°C'
            }
            
            if not free_cooling_compliant:
                all_compliant = False
        else:
            compliance_checks['free_cooling'] = {
                'compliant': True,
                'requirement': 'Not applicable (wet-bulb above threshold)'
            }
        
        # Check 3: Data logging
        data_logging_compliant = True  # Assume compliant (would check actual logging)
        compliance_checks['data_logging'] = {
            'compliant': data_logging_compliant,
            'requirement': f'Log data every ≤{self.compliance_requirements["data_logging_interval_minutes"]} minutes with ±{self.compliance_requirements["measurement_accuracy_percent"]}% accuracy'
        }
        
        return {
            'compliant': all_compliant,
            'checks': compliance_checks,
            'last_audit': (datetime.now() - timedelta(days=45)).isoformat(),
            'next_reporting_due': (datetime.now() + timedelta(days=45)).isoformat()
        }
    
    def _identify_maintenance_actions(
        self,
        equipment_health: Dict,
        maintenance_status: Dict,
        compliance_status: Dict
    ) -> List[Dict]:
        """Identify required maintenance actions"""
        
        actions = []
        
        # Check equipment health
        for equipment_id, health in equipment_health.items():
            if health['status'] in ['POOR', 'CRITICAL']:
                actions.append({
                    'type': 'CORRECTIVE',
                    'priority': 'CRITICAL' if health['status'] == 'CRITICAL' else 'HIGH',
                    'equipment_id': equipment_id,
                    'issue': f"Health score: {health['health_score']} ({health['status']})",
                    'recommended_action': 'Immediate inspection and corrective maintenance'
                })
        
        # Check maintenance schedules
        for equipment_id, schedule in maintenance_status.items():
            if schedule['hours_until_service'] < 100:
                actions.append({
                    'type': 'PREVENTIVE',
                    'priority': 'HIGH' if schedule['hours_until_service'] < 50 else 'MEDIUM',
                    'equipment_id': equipment_id,
                    'issue': f"{schedule['next_service_due']} due in {schedule['hours_until_service']} hours",
                    'recommended_action': 'Schedule preventive maintenance'
                })
        
        # Check compliance
        if not compliance_status['compliant']:
            for check_name, check_data in compliance_status['checks'].items():
                if not check_data['compliant']:
                    actions.append({
                        'type': 'COMPLIANCE',
                        'priority': 'HIGH',
                        'equipment_id': 'FACILITY',
                        'issue': f"{check_name} non-compliant: {check_data['requirement']}",
                        'recommended_action': 'Immediate compliance action required'
                    })
        
        return actions
    
    def _evaluate_proposal(self, proposal: Dict, analysis: Dict) -> Dict:
        """
        Evaluate another agent's proposal for equipment health and compliance impact
        """
        
        equipment_impact = self._assess_proposal_equipment_impact(proposal, analysis)
        compliance_impact = self._assess_proposal_compliance_impact(proposal, analysis)
        
        # Determine recommendation
        if equipment_impact['risk'] == 'HIGH' or compliance_impact['violation']:
            recommendation = "VETO"
            confidence = 0.95
        elif equipment_impact['risk'] == 'MEDIUM':
            recommendation = "APPROVE_WITH_CONDITIONS"
            confidence = 0.75
        else:
            recommendation = "APPROVE"
            confidence = 0.85
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'PROPOSAL_EVALUATION',
            'evaluated_proposal': proposal['action_type'],
            'recommendation': recommendation,
            'equipment_impact': equipment_impact,
            'compliance_impact': compliance_impact,
            'conditions': equipment_impact.get('conditions', []),
            'confidence': confidence
        }
    
    def _assess_proposal_equipment_impact(self, proposal: Dict, analysis: Dict) -> Dict:
        """Assess proposal's impact on equipment health"""
        
        action_type = proposal.get('action_type')
        
        # Check for excessive cycling
        if action_type == 'CHILLER_STAGING':
            # Check if equipment due for maintenance
            equipment_id = proposal.get('description', '')
            
            for equip_id, health in analysis['equipment_health'].items():
                if equip_id in equipment_id and health['status'] in ['POOR', 'CRITICAL']:
                    return {
                        'risk': 'HIGH',
                        'issue': f'{equip_id} health status: {health["status"]}',
                        'recommendation': f'Complete maintenance on {equip_id} before staging changes'
                    }
            
            # Check maintenance schedule
            for equip_id, schedule in analysis['maintenance_status'].items():
                if equip_id in equipment_id and schedule['hours_until_service'] < 50:
                    return {
                        'risk': 'MEDIUM',
                        'issue': f'{equip_id} due for maintenance in {schedule["hours_until_service"]} hours',
                        'recommendation': 'Schedule maintenance soon',
                        'conditions': [f'Monitor {equip_id} closely', 'Schedule maintenance within 48 hours']
                    }
        
        return {
            'risk': 'LOW',
            'issue': None
        }
    
    def _assess_proposal_compliance_impact(self, proposal: Dict, analysis: Dict) -> Dict:
        """Assess proposal's compliance impact"""
        
        # Check if proposal would violate PUE limits
        predicted_pue = proposal.get('predicted_savings', {}).get('pue_improvement', 0)
        current_pue = analysis['compliance_status']['checks']['pue']['current_value']
        new_pue = current_pue - predicted_pue
        
        pue_limit = self.compliance_requirements['pue_limit_existing_dc']
        
        if new_pue > pue_limit:
            return {
                'violation': True,
                'regulation': 'NEA PUE Limit',
                'issue': f'Proposed PUE {new_pue:.3f} exceeds limit {pue_limit}',
                'severity': 'HIGH'
            }
        
        return {
            'violation': False
        }
    
    def _propose_urgent_maintenance(self, critical_issue: Dict, analysis: Dict) -> Dict:
        """Propose urgent maintenance action"""
        
        confidence = self.calculate_confidence(
            historical_matches=3,
            data_quality=0.95,
            risk_level='HIGH'
        )
        
        proposal = self.format_proposal(
            action_type='URGENT_MAINTENANCE',
            description=f"Urgent: {critical_issue['issue']} on {critical_issue['equipment_id']}",
            justification=f"Equipment health critical. {critical_issue['recommended_action']} required immediately to prevent failure.",
            predicted_savings={
                'avoided_failure_cost_sgd': 50000,
                'avoided_downtime_hours': 24
            },
            evidence=[
                self.cite_evidence(
                    'TELEMETRY',
                    'Equipment Health Monitoring',
                    analysis['equipment_health'][critical_issue['equipment_id']]
                ),
                self.cite_evidence(
                    'SOP',
                    'Maintenance Procedures',
                    analysis['sop_guidance'][:1]
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_preventive_maintenance(self, maintenance_action: Dict, analysis: Dict) -> Dict:
        """Propose preventive maintenance"""
        
        confidence = self.calculate_confidence(
            historical_matches=5,
            data_quality=0.90,
            risk_level='MEDIUM'
        )
        
        proposal = self.format_proposal(
            action_type='PREVENTIVE_MAINTENANCE',
            description=f"Schedule: {maintenance_action['issue']} on {maintenance_action['equipment_id']}",
            justification=f"Equipment approaching maintenance interval. {maintenance_action['recommended_action']} to prevent unplanned downtime.",
            predicted_savings={
                'avoided_failure_cost_sgd': 25000,
                'extended_equipment_life_years': 2
            },
            evidence=[
                self.cite_evidence(
                    'MAINTENANCE_LOG',
                    'Service History',
                    analysis['maintenance_status'][maintenance_action['equipment_id']]
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_compliance_action(self, analysis: Dict) -> Dict:
        """Propose compliance correction action"""
        
        non_compliant = [
            check for check_name, check in analysis['compliance_status']['checks'].items()
            if not check['compliant']
        ][0]
        
        confidence = self.calculate_confidence(
            historical_matches=2,
            data_quality=0.95,
            risk_level='HIGH'
        )
        
        proposal = self.format_proposal(
            action_type='COMPLIANCE_CORRECTION',
            description=f"Compliance violation: {non_compliant['requirement']}",
            justification="NEA compliance violation detected. Immediate corrective action required to avoid penalties (SGD 50K-100K).",
            predicted_savings={
                'avoided_penalty_sgd': 50000
            },
            evidence=[
                self.cite_evidence(
                    'REGULATION',
                    'NEA Requirements',
                    analysis['regulations'][:1]
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine monitoring update"""
        
        avg_health = np.mean([
            h['health_score'] for h in analysis['equipment_health'].values()
        ])
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'All equipment healthy, compliant',
            'status': 'NOMINAL',
            'average_health_score': round(avg_health, 1),
            'compliance_status': 'COMPLIANT',
            'next_maintenance': self._get_next_maintenance_date(analysis),
            'confidence': self.confidence_level
        }
    
    def _get_next_maintenance_date(self, analysis: Dict) -> str:
        """Get next scheduled maintenance date"""
        
        min_hours = float('inf')
        next_equipment = None
        
        for equipment_id, schedule in analysis['maintenance_status'].items():
            if schedule['hours_until_service'] < min_hours:
                min_hours = schedule['hours_until_service']
                next_equipment = equipment_id
        
        days = int(min_hours / 24)
        
        return f"{next_equipment} in {days} days"


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING MAINTENANCE & COMPLIANCE AGENT")
    print("="*70)
    
    # Create agent
    agent = MaintenanceComplianceAgent()
    
    # Test context
    context = {
        'current_pue': 1.28,
        'wet_bulb_temp': 25.5,
        'economizer_enabled': False,
        'timestamp': datetime.now()
    }
    
    # Test
    print("\n[TEST] Analyze & Propose...")
    proposal = agent.propose_action(context)
    
    print(f"\nAgent: {proposal['agent']}")
    print(f"Action: {proposal['action_type']}")
    print(f"Description: {proposal.get('description', proposal.get('status'))}")
    
    if 'average_health_score' in proposal:
        print(f"Average Health Score: {proposal['average_health_score']}")
        print(f"Compliance: {proposal['compliance_status']}")
    
    print("\n Maintenance & Compliance Agent test complete!")