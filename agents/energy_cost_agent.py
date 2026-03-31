"""
Energy & Cost Optimization Agent
Multi-objective optimization: PUE, WUE, carbon footprint, total cost
Business-focused agent that evaluates all proposals from cost/sustainability perspective
"""

import sys
sys.path.append('..')

from base_agent import BaseAgent
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import numpy as np

class EnergyCostAgent(BaseAgent):
    """
    Agent 4: Energy & Cost Optimization
    
    Responsibilities:
    - Optimize for total cost (energy + demand charges)
    - Minimize PUE (Power Usage Effectiveness)
    - Minimize WUE (Water Usage Effectiveness)
    - Reduce carbon footprint
    - Evaluate cost-benefit of all proposals
    - Manage demand charge peaks
    
    Authority: Advisory + veto on proposals with negative ROI
    """


    def __init__(self):  
        super().__init__(  # inherit attributes from BaseAgent 
            agent_name="Energy & Cost Optimization Agent",
            agent_role="Optimize for business outcomes: total cost, PUE, WUE, carbon, demand charges"
        )
        
        # Load agent-specific prompt (system prompt for this) 
        self.load_prompt()   
        
        # Cost parameters (Singapore) - Change here to reflect latest costings 
        self.cost_params = {
            'energy_rate_sgd_per_kwh': 0.20,  # Base rate
            'demand_charge_sgd_per_kw': 15.00,  # Monthly demand charge
            'water_rate_sgd_per_m3': 2.50,
            'carbon_price_sgd_per_ton_co2': 25.00,  # Singapore carbon tax
            'grid_carbon_intensity_g_per_kwh': 420  # Singapore grid
        }
        
        # Performance targets - change here to reflect latest performance targets 
        self.targets = {
            'pue': 1.25,  # Target PUE
            'wue': 1.8,   # Target WUE (L/kWh IT)
            'monthly_savings_target_sgd': 5000
        }
        
        # Current month demand peak tracking
        self.current_month_peak_kw = 0
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/energy_cost_v1.txt', 'r') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

RESPONSIBILITIES:
1. Evaluate all proposals from cost-benefit perspective
2. Calculate total cost of operation (energy + demand + water + carbon)
3. Monitor and optimize PUE (target: ≤1.25)
4. Monitor and optimize WUE (target: ≤1.8 L/kWh)
5. Manage demand charge peaks (avoid new monthly peaks)
6. Evaluate carbon footprint reduction opportunities
7. Calculate payback periods for efficiency investments

AUTHORITY:
- Advisory role on all proposals
- Can veto proposals with negative ROI or PUE degradation
- Can propose demand response actions
- Can propose load-shifting for cost optimization

DECISION CRITERIA:
When evaluating proposals:
1. Calculate total cost impact (energy + demand + water + carbon)
2. Calculate PUE impact
3. Calculate simple payback period
4. Consider demand charge implications
5. Evaluate carbon footprint
6. Assess risk vs reward

PUE Calculation:
PUE = Total Facility Power / IT Equipment Power
Target: ≤1.25 (world-class: ≤1.20)

WUE Calculation:
WUE = Annual Water Usage (L) / IT Equipment Energy (kWh)
Target: ≤1.8 L/kWh

OUTPUT FORMAT:
Provide cost-benefit analysis in JSON:
{
  "analysis_type": "PROPOSAL_EVALUATION" | "COST_OPTIMIZATION",
  "total_cost_impact_sgd": -125.50,  // Negative = savings
  "cost_breakdown": {
    "energy_cost_sgd": -100,
    "demand_charge_impact_sgd": 0,
    "water_cost_sgd": -15,
    "carbon_cost_sgd": -10.50
  },
  "pue_impact": -0.02,  // Negative = improvement
  "wue_impact": -0.1,
  "payback_period_months": 3.2,
  "recommendation": "APPROVE" | "REJECT" | "MODIFY",
  "confidence": 0.87
}

CONSTRAINTS:
- Cannot approve proposals that degrade PUE by >0.05
- Cannot approve proposals with payback >24 months (without justification)
- Must flag demand charge risks (new monthly peaks)
"""
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze current cost and energy performance
        
        Args:
            context: Current system state
        
        Returns:
            Cost and performance analysis
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Calculate current costs
        current_costs = self._calculate_current_costs(current_metrics, context)
        
        # Calculate current PUE
        current_pue = self._calculate_pue(current_metrics, context)
        
        # Calculate current WUE
        current_wue = self._calculate_wue(current_metrics, context)
        
        # Calculate carbon footprint
        carbon_footprint = self._calculate_carbon_footprint(current_metrics, context)
        
        # Identify cost optimization opportunities
        opportunities = self._identify_cost_opportunities(current_metrics, context)
        
        # Get historical cost performance
        precedents = self.get_historical_precedents(
            cooling_load_kw=context.get('cooling_load_kw', 2800),
            wet_bulb_temp=context.get('wet_bulb_temp', 25.0),
            days=30
        )
        
        return {
            'current_state': current_metrics,
            'current_costs': current_costs,
            'current_pue': current_pue,
            'current_wue': current_wue,
            'carbon_footprint': carbon_footprint,
            'opportunities': opportunities,
            'historical_precedents': precedents,
            'targets': self.targets
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose cost optimization actions
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action or cost analysis
        """
        
        # Analyze current situation
        analysis = self.analyze_situation(context)
        
        # Check if we're asked to evaluate another agent's proposal
        if 'proposal_to_evaluate' in context:
            return self._evaluate_proposal(context['proposal_to_evaluate'], analysis)
        
        # Otherwise, look for cost optimization opportunities
        opportunities = analysis['opportunities']
        
        if not opportunities:
            return self._create_monitoring_update(analysis)
        
        # Propose highest-value opportunity
        best_opportunity = max(opportunities, key=lambda x: x.get('savings_sgd', 0))
        
        return self._create_cost_optimization_proposal(best_opportunity, analysis)
    
    def _calculate_current_costs(self, metrics: Dict, context: Dict) -> Dict:
        """Calculate current operating costs"""
        
        # Total facility power
        total_power_kw = context.get('total_facility_power_kw', 
                                     metrics.get('total_facility_power_kw', 11000))
        
        # Energy cost (per hour)
        energy_cost_hr = total_power_kw * self.cost_params['energy_rate_sgd_per_kwh']
        
        # Demand charge (monthly, prorated to hourly)
        # Demand charge is based on monthly peak
        peak_power_kw = max(total_power_kw, self.current_month_peak_kw)
        demand_charge_monthly = peak_power_kw * self.cost_params['demand_charge_sgd_per_kw']
        demand_charge_hr = demand_charge_monthly / (30 * 24)  # Prorated
        
        # Water cost (cooling tower makeup)
        cooling_load_kw = context.get('cooling_load_kw', 2800)
        water_consumption_lph = cooling_load_kw * 0.5  # ~0.5 L/kW-hr typical
        water_cost_hr = (water_consumption_lph / 1000) * self.cost_params['water_rate_sgd_per_m3']
        
        # Carbon cost
        carbon_kg_co2 = (total_power_kw * self.cost_params['grid_carbon_intensity_g_per_kwh']) / 1000
        carbon_cost_hr = (carbon_kg_co2 / 1000) * self.cost_params['carbon_price_sgd_per_ton_co2']
        
        total_cost_hr = energy_cost_hr + demand_charge_hr + water_cost_hr + carbon_cost_hr
        
        return {
            'total_cost_sgd_per_hour': round(total_cost_hr, 2),
            'energy_cost_sgd_per_hour': round(energy_cost_hr, 2),
            'demand_charge_sgd_per_hour': round(demand_charge_hr, 2),
            'water_cost_sgd_per_hour': round(water_cost_hr, 2),
            'carbon_cost_sgd_per_hour': round(carbon_cost_hr, 2),
            'total_cost_sgd_per_day': round(total_cost_hr * 24, 2),
            'total_cost_sgd_per_month': round(total_cost_hr * 24 * 30, 2)
        }
    
    def _calculate_pue(self, metrics: Dict, context: Dict) -> Dict:
        """Calculate Power Usage Effectiveness"""
        
        total_facility_power = context.get('total_facility_power_kw', 11000)
        it_load_power = context.get('it_load_kw', 9500)
        
        pue = total_facility_power / it_load_power if it_load_power > 0 else 1.30
        
        # Compare to target
        target_pue = self.targets['pue']
        delta_from_target = pue - target_pue
        
        # Performance rating
        if pue <= 1.20:
            rating = "WORLD_CLASS"
        elif pue <= 1.25:
            rating = "EXCELLENT"
        elif pue <= 1.30:
            rating = "GOOD"
        elif pue <= 1.40:
            rating = "ACCEPTABLE"
        else:
            rating = "NEEDS_IMPROVEMENT"
        
        return {
            'current_pue': round(pue, 3),
            'target_pue': target_pue,
            'delta_from_target': round(delta_from_target, 3),
            'rating': rating,
            'it_load_kw': it_load_power,
            'total_facility_kw': total_facility_power,
            'cooling_overhead_kw': total_facility_power - it_load_power
        }
    
    def _calculate_wue(self, metrics: Dict, context: Dict) -> Dict:
        """Calculate Water Usage Effectiveness"""
        
        cooling_load_kw = context.get('cooling_load_kw', 2800)
        it_load_kw = context.get('it_load_kw', 9500)
        
        # Estimate water consumption (L/hr)
        water_lph = cooling_load_kw * 0.5
        
        # WUE = L per kWh of IT load
        wue = water_lph / it_load_kw if it_load_kw > 0 else 2.0
        
        target_wue = self.targets['wue']
        delta_from_target = wue - target_wue
        
        return {
            'current_wue': round(wue, 2),
            'target_wue': target_wue,
            'delta_from_target': round(delta_from_target, 2),
            'water_consumption_lph': round(water_lph, 1),
            'water_consumption_m3_per_day': round(water_lph * 24 / 1000, 2)
        }
    
    def _calculate_carbon_footprint(self, metrics: Dict, context: Dict) -> Dict:
        """Calculate carbon emissions"""
        
        total_power_kw = context.get('total_facility_power_kw', 11000)
        
        # Carbon emissions (kg CO2 per hour)
        carbon_kg_hr = (total_power_kw * self.cost_params['grid_carbon_intensity_g_per_kwh']) / 1000
        
        return {
            'carbon_kg_co2_per_hour': round(carbon_kg_hr, 2),
            'carbon_tons_co2_per_day': round(carbon_kg_hr * 24 / 1000, 3),
            'carbon_tons_co2_per_year': round(carbon_kg_hr * 24 * 365 / 1000, 1),
            'grid_carbon_intensity': self.cost_params['grid_carbon_intensity_g_per_kwh']
        }
    
    def _identify_cost_opportunities(self, metrics: Dict, context: Dict) -> List[Dict]:
        """Identify cost optimization opportunities"""
        
        opportunities = []
        
        current_pue = self._calculate_pue(metrics, context)
        
        # Opportunity 1: PUE above target
        if current_pue['delta_from_target'] > 0.02:
            pue_reduction_potential = current_pue['delta_from_target']
            it_load = current_pue['it_load_kw']
            
            # Savings from PUE improvement
            current_overhead = current_pue['cooling_overhead_kw']
            target_overhead = it_load * (self.targets['pue'] - 1.0)
            savings_kw = current_overhead - target_overhead
            savings_sgd_hr = savings_kw * self.cost_params['energy_rate_sgd_per_kwh']
            
            opportunities.append({
                'type': 'PUE_IMPROVEMENT',
                'description': f'Reduce PUE from {current_pue["current_pue"]:.3f} to {self.targets["pue"]:.3f}',
                'savings_kw': round(savings_kw, 1),
                'savings_sgd': round(savings_sgd_hr * 24 * 30, 2),  # Monthly
                'priority': 'HIGH'
            })
        
        # Opportunity 2: Demand charge management
        current_power = context.get('total_facility_power_kw', 11000)
        if current_power > self.current_month_peak_kw * 0.95:
            # Approaching new peak
            opportunities.append({
                'type': 'DEMAND_CHARGE_AVOIDANCE',
                'description': 'Current power approaching monthly peak - consider load reduction',
                'current_power_kw': current_power,
                'current_peak_kw': self.current_month_peak_kw,
                'potential_charge_increase_sgd': (current_power - self.current_month_peak_kw) * 15,
                'priority': 'MEDIUM'
            })
        
        return opportunities
    
    def _evaluate_proposal(self, proposal: Dict, analysis: Dict) -> Dict:
        """
        Evaluate another agent's proposal from cost perspective
        
        Returns approval/rejection with cost-benefit analysis
        """
        
        predicted_savings = proposal.get('predicted_savings', {})
        energy_savings_kw = predicted_savings.get('energy_kw', 0)
        
        # Calculate cost impact
        energy_cost_savings = energy_savings_kw * self.cost_params['energy_rate_sgd_per_kwh']
        pue_improvement = predicted_savings.get('pue_improvement', 0)
        
        # Calculate ROI
        # (Need implementation cost - for now assume minimal)
        annual_savings = energy_cost_savings * 24 * 365
        
        # Recommendation
        if energy_savings_kw > 10 and pue_improvement >= 0:
            recommendation = "APPROVE"
            confidence = 0.85
        elif energy_savings_kw > 0:
            recommendation = "APPROVE"
            confidence = 0.70
        else:
            recommendation = "REJECT"
            confidence = 0.90
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'PROPOSAL_EVALUATION',
            'evaluated_proposal': proposal['action_type'],
            'recommendation': recommendation,
            'cost_benefit_analysis': {
                'energy_savings_kw': energy_savings_kw,
                'cost_savings_sgd_per_hour': round(energy_cost_savings, 2),
                'cost_savings_sgd_per_month': round(energy_cost_savings * 24 * 30, 2),
                'pue_impact': pue_improvement,
                'annual_savings_sgd': round(annual_savings, 2)
            },
            'confidence': confidence
        }
    
    def _create_cost_optimization_proposal(self, opportunity: Dict, analysis: Dict) -> Dict:
        """Create cost optimization proposal"""
        
        confidence = self.calculate_confidence(
            historical_matches=len(analysis['historical_precedents']),
            data_quality=0.88,
            risk_level='MEDIUM'
        )
        
        proposal = self.format_proposal(
            action_type='COST_OPTIMIZATION',
            description=opportunity['description'],
            justification=f"Current PUE: {analysis['current_pue']['current_pue']:.3f}, Target: {self.targets['pue']:.3f}. Opportunity to reduce overhead power and improve cost efficiency.",
            predicted_savings={
                'energy_kw': opportunity.get('savings_kw', 0),
                'cost_sgd': opportunity.get('savings_sgd', 0),
                'pue_improvement': 0.02
            },
            evidence=[
                self.cite_evidence(
                    'ANALYTICAL',
                    'Cost Model',
                    analysis['current_costs']
                ),
                self.cite_evidence(
                    'PERFORMANCE',
                    'PUE Analysis',
                    analysis['current_pue']
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine cost monitoring update"""
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'Cost performance within targets',
            'status': 'NOMINAL',
            'current_pue': analysis['current_pue']['current_pue'],
            'current_wue': analysis['current_wue']['current_wue'],
            'monthly_cost_sgd': analysis['current_costs']['total_cost_sgd_per_month'],
            'confidence': self.confidence_level
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING ENERGY & COST OPTIMIZATION AGENT")
    print("="*70)
    
    # Create agent
    agent = EnergyCostAgent()
    
    # Test context
    context = {
        'cooling_load_kw': 2800,
        'it_load_kw': 9500,
        'total_facility_power_kw': 11800,
        'wet_bulb_temp': 25.5,
        'timestamp': datetime.now()
    }
    
    # Test 1: Cost analysis
    print("\n[TEST 1] Cost Analysis:")
    proposal = agent.propose_action(context)
    
    print(f"  Action: {proposal['action_type']}")
    if 'monthly_cost_sgd' in proposal:
        print(f"  Monthly Cost: SGD {proposal['monthly_cost_sgd']:.2f}")
        print(f"  Current PUE: {proposal['current_pue']:.3f}")
        print(f"  Current WUE: {proposal['current_wue']:.2f}")
    
    # Test 2: Evaluate another agent's proposal
    print("\n[TEST 2] Evaluate Proposal:")
    mock_proposal = {
        'action_type': 'CHILLER_STAGING',
        'predicted_savings': {
            'energy_kw': 45,
            'pue_improvement': 0.02
        }
    }
    
    context['proposal_to_evaluate'] = mock_proposal
    evaluation = agent.propose_action(context)
    
    print(f"  Recommendation: {evaluation['recommendation']}")
    print(f"  Annual Savings: SGD {evaluation['cost_benefit_analysis']['annual_savings_sgd']:.2f}")
    print(f"  Confidence: {evaluation['confidence']:.2f}")
    
    print("\n  Energy & Cost Agent test complete!")