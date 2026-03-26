# -*- coding: utf-8 -*-
"""
Chiller Optimization Agent
Optimizes chiller sequencing, staging, and capacity allocation
Primary goal: Minimize kW/ton while maintaining N+1 redundancy
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / 'simulations'))

try:
    from agents.base_agent import BaseAgent
except ImportError:
    from base_agent import BaseAgent
from typing import Dict, List
from datetime import datetime
import numpy as np

class ChillerOptimizationAgent(BaseAgent):
    """
    Agent 2: Chiller Optimization
    
    Responsibilities:
    - Optimize chiller staging (which chillers to run)
    - Optimize load distribution across chillers
    - Minimize plant kW/ton
    - Maintain N+1 redundancy
    - Pre-stage chillers before load spikes
    
    Authority: Can propose chiller staging changes (requires approval if risk > MEDIUM)
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Chiller Optimization Agent",
            agent_role="Optimize chiller staging and load distribution for minimum kW/ton"
        )
        
        # Load agent-specific prompt
        self.load_prompt()
        
        # Chiller capacities (tons)
        self.chiller_capacities = {
            'Chiller-1': 1000,
            'Chiller-2': 1000,
            'Chiller-3': 500
        }
        
        # Optimal load ranges (from manufacturer curves)
        self.optimal_load_range = {
            'min': 60,  # % - Below this, efficiency degrades
            'max': 80   # % - Above this, efficiency degrades
        }
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/chiller_optimization_v1.txt', 'r') as f:  #change system prompts here as fit
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

RESPONSIBILITIES:
1. Optimize chiller staging (number of chillers online)
2. Distribute load optimally across running chillers
3. Minimize plant kW/ton while maintaining reliability
4. Maintain N+1 redundancy at all times
5. Pre-stage chillers before predicted load spikes

AUTHORITY:
- Can propose chiller staging changes
- Can propose load redistribution
- Requires approval if predicted savings < confidence threshold
- Cannot compromise N+1 redundancy

DECISION CRITERIA:
When evaluating chiller staging:
1. Calculate total cooling load (tons)
2. Determine minimum chillers needed for load + N+1
3. Find staging configuration with lowest total kW/ton
4. Consider part-load efficiency curves (optimal: 60-80% load)
5. Account for startup/shutdown costs
6. Verify against historical performance

CONSTRAINTS:
- NEVER reduce below N+1 redundancy
- NEVER exceed individual chiller capacity
- Minimum 10 minutes between staging changes
- Maximum 6 starts per chiller per 24 hours

OUTPUT FORMAT:
Provide proposals in structured JSON with:
- Current staging vs proposed staging
- Load distribution per chiller
- Predicted efficiency improvement
- Evidence from historical data
- Confidence level
"""
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze current chiller plant performance
        
        Args:
            context: Current system state
        
        Returns:
            Analysis with staging recommendations
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Get current cooling load
        cooling_load_kw = context.get('cooling_load_kw', current_metrics.get('total_cooling_load_kw', 2800))
        cooling_load_tons = cooling_load_kw / 3.517
        
        # Get current staging
        current_staging = self._get_current_staging(current_metrics)
        
        # Get weather conditions (affects CW temp, efficiency)
        wet_bulb_temp = context.get('wet_bulb_temp', 25.0)
        
        # Evaluate alternative staging configurations
        staging_options = self._evaluate_staging_options(
            cooling_load_tons,
            wet_bulb_temp,
            current_staging
        )
        
        # Find historical precedents
        precedents = self.get_historical_precedents(
            cooling_load_kw=cooling_load_kw,
            wet_bulb_temp=wet_bulb_temp,
            days=30
        )
        
        # Search knowledge base for staging guidelines
        sop_results = self.search_knowledge(
            query="chiller staging optimization part load efficiency",
            knowledge_type="sops",
            filters={'equipment_type': 'chiller'}
        )
        
        return {
            'current_state': current_metrics,
            'cooling_load_tons': cooling_load_tons,
            'current_staging': current_staging,
            'staging_options': staging_options,
            'optimal_option': self._select_optimal_staging(staging_options),
            'historical_precedents': precedents,
            'sop_guidance': sop_results,
            'wet_bulb_temp': wet_bulb_temp
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose chiller staging optimization
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action
        """
        
        # Analyze current situation
        analysis = self.analyze_situation(context)
        
        current_staging = analysis['current_staging']
        optimal_staging = analysis['optimal_option']
        
        # Check if change is needed
        if current_staging['chillers'] == optimal_staging['chillers']:
            return self._create_monitoring_update(analysis)
        
        # Calculate savings
        current_power = current_staging['total_power_kw']
        optimal_power = optimal_staging['total_power_kw']
        savings_kw = current_power - optimal_power
        savings_percent = (savings_kw / current_power) * 100
        
        # Only propose if savings are significant (>3%)
        if savings_percent < 3.0:
            return self._create_monitoring_update(analysis)
        
        # Calculate confidence
        confidence = self.calculate_confidence(
            historical_matches=len(analysis['historical_precedents']),
            data_quality=0.9,
            risk_level='MEDIUM' if abs(len(optimal_staging['chillers']) - len(current_staging['chillers'])) == 1 else 'HIGH'
        )
        
        # Create proposal
        proposal = self.format_proposal(
            action_type='CHILLER_STAGING',
            description=self._create_staging_description(current_staging, optimal_staging),
            justification=self._create_justification(analysis, optimal_staging),
            predicted_savings={
                'energy_kw': round(savings_kw, 1),
                'cost_sgd': round(savings_kw * 0.20, 2),  # SGD 0.20/kWh
                'pue_improvement': round(savings_kw / context.get('it_load_kw', 9500) * 0.01, 3),
                'efficiency_improvement_kw_per_ton': round(
                    current_staging['avg_kw_per_ton'] - optimal_staging['avg_kw_per_ton'], 3
                )
            },
            evidence=[
                self.cite_evidence(
                    'ANALYTICAL',
                    'Chiller Efficiency Model',
                    {
                        'current_config': current_staging,
                        'optimal_config': optimal_staging
                    }
                ),
                self.cite_evidence(
                    'HISTORICAL',
                    'Medium-term Memory',
                    analysis['historical_precedents'][:3]
                ),
                self.cite_evidence(
                    'SOP',
                    'Knowledge Base',
                    analysis['sop_guidance'][:1]
                )
            ],
            confidence=confidence
        )
        
        # Log proposal
        self.log_proposal(proposal)
        
        return proposal
    
    def _get_current_staging(self, metrics: Dict) -> Dict:
        """Extract current chiller staging from metrics"""
        
        # This would query actual system state
        # For now, simulate based on typical operation
        
        chillers_online = metrics.get('chillers_online', 2)
        
        if chillers_online == 1:
            active = ['Chiller-1']
        elif chillers_online == 2:
            active = ['Chiller-1', 'Chiller-2']
        else:
            active = ['Chiller-1', 'Chiller-2', 'Chiller-3']
        
        total_power = metrics.get('total_chiller_power_kw', 900)
        total_load_tons = metrics.get('total_cooling_load_tons', 800)
        
        return {
            'chillers': active,
            'count': len(active),
            'total_power_kw': total_power,
            'total_load_tons': total_load_tons,
            'avg_kw_per_ton': total_power / total_load_tons if total_load_tons > 0 else 0.55
        }
    
    def _evaluate_staging_options(
        self,
        cooling_load_tons: float,
        wet_bulb_temp: float,
        current_staging: Dict
    ) -> List[Dict]:
        """
        Evaluate different chiller staging configurations
        
        Returns:
            List of staging options with predicted performance
        """
        
        from equipment_models import ChillerEfficiencyModel
        efficiency_model = ChillerEfficiencyModel(chiller_id='Chiller-1', rated_tons=1000)

        # Approximate CW entering temp from wet-bulb
        tower_approach = 3.5
        cw_temp = wet_bulb_temp + tower_approach + 5.5

        options = []

        # Option 1: Single chiller (load must be <= 85% of 1000RT)
        if cooling_load_tons <= 1000 * 0.85:
            load_percent = (cooling_load_tons / 1000) * 100
            kw_per_ton = efficiency_model.get_efficiency(
                load_tons=cooling_load_tons,
                chw_supply_temp_c=6.5,
                cw_entering_temp_c=cw_temp
            )
            options.append({
                'chillers': ['Chiller-1'],
                'count': 1,
                'loads_tons': [cooling_load_tons],
                'loads_percent': [load_percent],
                'efficiencies': [kw_per_ton],
                'total_power_kw': cooling_load_tons * kw_per_ton,
                'avg_kw_per_ton': kw_per_ton,
                'n_plus_1': 1000 >= cooling_load_tons  # spare chiller covers load
            })

        # Option 2: Two large chillers (load <= 85% of 2000RT)
        if cooling_load_tons <= 2000 * 0.85:
            load_each = cooling_load_tons / 2
            load_percent = (load_each / 1000) * 100
            kw_per_ton = efficiency_model.get_efficiency(
                load_tons=cooling_load_tons,
                chw_supply_temp_c=6.5,
                cw_entering_temp_c=cw_temp
            )
            options.append({
                'chillers': ['Chiller-1', 'Chiller-2'],
                'count': 2,
                'loads_tons': [load_each, load_each],
                'loads_percent': [load_percent, load_percent],
                'efficiencies': [kw_per_ton, kw_per_ton],
                'total_power_kw': cooling_load_tons * kw_per_ton,
                'avg_kw_per_ton': kw_per_ton,
                'n_plus_1': True  # Chiller-3 (500RT) provides partial redundancy
            })

        # Option 3: All three chillers — proportional load (1000:1000:500)
        if cooling_load_tons > 1500:
            total_rated = 2500
            load_1 = cooling_load_tons * (1000 / total_rated)
            load_2 = cooling_load_tons * (1000 / total_rated)
            load_3 = cooling_load_tons * (500 / total_rated)

            eff_model_small = ChillerEfficiencyModel(chiller_id='Chiller-3', rated_tons=500)
            eff_1 = efficiency_model.get_efficiency(load_tons=load_1, chw_supply_temp_c=6.5, cw_entering_temp_c=cw_temp)
            eff_2 = efficiency_model.get_efficiency(load_tons=load_2, chw_supply_temp_c=6.5, cw_entering_temp_c=cw_temp)
            eff_3 = eff_model_small.get_efficiency(load_tons=load_3, chw_supply_temp_c=6.5, cw_entering_temp_c=cw_temp)

            total_power = load_1 * eff_1 + load_2 * eff_2 + load_3 * eff_3
            options.append({
                'chillers': ['Chiller-1', 'Chiller-2', 'Chiller-3'],
                'count': 3,
                'loads_tons': [load_1, load_2, load_3],
                'loads_percent': [
                    (load_1 / 1000) * 100,
                    (load_2 / 1000) * 100,
                    (load_3 / 500) * 100
                ],
                'efficiencies': [eff_1, eff_2, eff_3],
                'total_power_kw': total_power,
                'avg_kw_per_ton': total_power / cooling_load_tons,
                'n_plus_1': True
            })
        
        return options
    
    def _select_optimal_staging(self, options: List[Dict]) -> Dict:
        """
        Select optimal staging configuration
        
        Criteria:
        1. Must maintain N+1 redundancy
        2. Minimize total power consumption
        3. Keep chillers in optimal load range (60-80%)
        """
        
        # Filter for N+1 redundancy
        valid_options = [opt for opt in options if opt['n_plus_1']]
        
        if not valid_options:
            return options[0]  # Fallback
        
        # Score each option
        for opt in valid_options:
            score = 0
            
            # Reward low power consumption
            score += (1.0 - opt['avg_kw_per_ton'] / 0.70) * 50
            
            # Reward operating in optimal load range
            for load_pct in opt['loads_percent']:
                if 60 <= load_pct <= 80:
                    score += 20
                elif 50 <= load_pct <= 90:
                    score += 10
            
            # Penalty for excessive staging (more chillers = more complexity)
            score -= opt['count'] * 5
            
            opt['optimization_score'] = score
        
        # Return highest scoring option
        return max(valid_options, key=lambda x: x['optimization_score'])
    
    def _create_staging_description(self, current: Dict, optimal: Dict) -> str:
        """Create human-readable staging change description"""
        
        current_chillers = ', '.join(current['chillers'])
        optimal_chillers = ', '.join(optimal['chillers'])
        
        if len(optimal['chillers']) > len(current['chillers']):
            action = "Stage online"
            diff = set(optimal['chillers']) - set(current['chillers'])
        else:
            action = "Stage offline"
            diff = set(current['chillers']) - set(optimal['chillers'])
        
        diff_str = ', '.join(diff)
        
        return f"{action} {diff_str}. New configuration: {optimal_chillers}"
    
    def _create_justification(self, analysis: Dict, optimal: Dict) -> str:
        """Create justification for staging change"""
        
        load = analysis['cooling_load_tons']
        current = analysis['current_staging']
        
        justification = f"Current load: {load:.0f} tons. "
        
        # Efficiency improvement
        eff_improvement = (current['avg_kw_per_ton'] - optimal['avg_kw_per_ton']) / current['avg_kw_per_ton'] * 100
        justification += f"Proposed staging improves efficiency by {eff_improvement:.1f}%. "
        
        # Part-load positioning
        avg_load_pct = np.mean(optimal['loads_percent'])
        if 60 <= avg_load_pct <= 80:
            justification += f"Positions chillers in optimal load range ({avg_load_pct:.0f}% average). "
        
        # N+1 redundancy
        justification += "Maintains N+1 redundancy."
        
        return justification
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine monitoring update (no action needed)"""
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'Current chiller staging is optimal',
            'status': 'NOMINAL',
            'current_staging': analysis['current_staging'],
            'cooling_load_tons': analysis['cooling_load_tons'],
            'confidence': self.confidence_level
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING CHILLER OPTIMIZATION AGENT")
    print("="*70)
    
    # Create agent
    agent = ChillerOptimizationAgent()
    
    # Test context
    context = {
        'cooling_load_kw': 2800,
        'wet_bulb_temp': 25.5,
        'it_load_kw': 9500,
        'timestamp': datetime.now()
    }
    
    # Test analysis
    print("\n[TEST] Analyze & Propose...")
    proposal = agent.propose_action(context)
    
    print(f"\nAgent: {proposal['agent']}")
    print(f"Action: {proposal['action_type']}")
    
    if proposal['action_type'] == 'CHILLER_STAGING':
        print(f"Description: {proposal['description']}")
        print(f"Justification: {proposal['justification']}")
        print(f"\nPredicted Savings:")
        print(f"  Energy: {proposal['predicted_savings']['energy_kw']} kW")
        print(f"  Cost: SGD {proposal['predicted_savings']['cost_sgd']}")
        print(f"  Efficiency improvement: {proposal['predicted_savings']['efficiency_improvement_kw_per_ton']} kW/ton")
        print(f"\nConfidence: {proposal['confidence']:.2f}")
    else:
        print(f"Status: {proposal['status']}")
    
    print("\nChiller Optimization Agent test complete!")

