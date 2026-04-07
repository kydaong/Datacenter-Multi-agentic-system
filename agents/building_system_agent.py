"""
Building System Agent
Optimizes pumps, cooling towers, AHUs, and dampers for maximum distribution efficiency
Secondary equipment optimization to support chiller performance
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / 'simulations'))

try:
    from agents.base_agent import BaseAgent
except ImportError:
    from base_agent import BaseAgent
from typing import Dict, List, Optional
from datetime import datetime
import numpy as np

class BuildingSystemAgent(BaseAgent):
    """
    Agent 3: Building System Optimization
    
    Responsibilities:
    - Optimize CHW/CW pump speeds (VFD control)
    - Optimize cooling tower fan speeds
    - Optimize CHW delta-T (minimize pumping power)
    - Enable economizer mode when wet-bulb < 22°C
    - Optimize AHU damper positions
    
    Authority: Can adjust VFD speeds, damper positions (requires approval if savings uncertain)
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Building System Agent",
            agent_role="Optimize pumps, towers, AHUs for maximum distribution efficiency"
        )
        
        # Load agent-specific prompt
        self.load_prompt()
        
        # Equipment specifications
        self.pump_specs = {
            'PCHWP': {'rated_flow_lps': 150, 'rated_power_kw': 55, 'min_speed': 50, 'max_speed': 100},
            'SCHWP': {'rated_flow_lps': 120, 'rated_power_kw': 45, 'min_speed': 50, 'max_speed': 100},
            'CWP': {'rated_flow_lps': 180, 'rated_power_kw': 38, 'min_speed': 50, 'max_speed': 100}
        }
        
        self.tower_specs = {
            'fan_power_kw_each': 15,
            'min_speed': 40,
            'max_speed': 100
        }
        
        # Optimization targets
        self.targets = {
            'chw_delta_t': 5.5,  # °C - Target delta-T for optimal pumping
            'tower_approach': 3.5,  # °C - Target approach temp
            'economizer_threshold_wb': 22.0  # °C - Enable free cooling below this
        }
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/building_system_v1.txt', 'r') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

RESPONSIBILITIES:
1. Optimize CHW pump VFD speeds (minimize pumping power while maintaining flow)
2. Optimize CW pump VFD speeds (balance flow vs tower performance)
3. Optimize cooling tower fan speeds (minimize fan power, maintain approach temp)
4. Maximize CHW delta-T (reduce flow, reduce pumping power)
5. Enable economizer mode when outdoor conditions allow
6. Optimize AHU damper positions

AUTHORITY:
- Can adjust pump VFD speeds (50-100%)
- Can adjust tower fan speeds (40-100%)
- Can propose economizer mode
- Can adjust AHU dampers
- Requires approval if predicted savings < 20 kW

DECISION CRITERIA:
For pump optimization:
1. Calculate required flow based on cooling load and delta-T
2. Minimize VFD speed while maintaining differential pressure setpoint
3. Balance pumping power (speed³) vs chiller efficiency

For tower optimization:
1. Maintain approach temperature target (3.5°C)
2. Minimize fan power (speed³)
3. Enable economizer if wet-bulb < 22°C

For CHW delta-T:
1. Target 5.5°C delta-T (optimal for most systems)
2. Higher delta-T = lower flow = lower pumping power
3. But too high = poor heat transfer

OUTPUT FORMAT:
Provide proposals in JSON with:
- Equipment ID and current vs proposed speeds
- Predicted power savings
- Impact on system performance
- Confidence level
"""
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze building systems performance
        
        Args:
            context: Current system state
        
        Returns:
            Analysis with optimization opportunities
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Get current operating conditions
        cooling_load_kw = context.get('cooling_load_kw', current_metrics.get('total_cooling_load_kw', 2800))
        wet_bulb_temp = context.get('wet_bulb_temp', 25.0)
        
        # Analyze pump performance
        pump_analysis = self._analyze_pumps(cooling_load_kw, current_metrics)
        
        # Analyze tower performance
        tower_analysis = self._analyze_towers(cooling_load_kw, wet_bulb_temp, current_metrics)
        
        # Check economizer opportunity
        economizer_opportunity = self._check_economizer_opportunity(wet_bulb_temp)
        
        # Find historical precedents
        precedents = self.get_historical_precedents(
            cooling_load_kw=cooling_load_kw,
            wet_bulb_temp=wet_bulb_temp,
            days=30
        )
        
        # Search knowledge base
        sop_results = self.search_knowledge(
            query="pump VFD optimization delta-T control",
            knowledge_type="sops"
        )
        
        return {
            'current_state': current_metrics,
            'cooling_load_kw': cooling_load_kw,
            'wet_bulb_temp': wet_bulb_temp,
            'pump_analysis': pump_analysis,
            'tower_analysis': tower_analysis,
            'economizer_opportunity': economizer_opportunity,
            'historical_precedents': precedents,
            'sop_guidance': sop_results
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose building system optimization
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action
        """
        
        # Analyze current situation
        analysis = self.analyze_situation(context)
        
        # Prioritize opportunities
        proposals = []
        
        # Opportunity 1: Economizer mode (highest priority)
        if analysis['economizer_opportunity']['viable']:
            proposals.append(self._propose_economizer(analysis))
        
        # Opportunity 2: Pump VFD optimization
        pump_savings = analysis['pump_analysis']['potential_savings_kw']
        if pump_savings > 5.0:
            proposals.append(self._propose_pump_optimization(analysis))
        
        # Opportunity 3: Tower fan optimization
        tower_savings = analysis['tower_analysis']['potential_savings_kw']
        if tower_savings > 3.0:
            proposals.append(self._propose_tower_optimization(analysis))
        
        # If no opportunities, return monitoring update
        if not proposals:
            return self._create_monitoring_update(analysis)
        
        # Return highest-value proposal
        return max(proposals, key=lambda x: x['predicted_savings']['energy_kw'])
    
    def _analyze_pumps(self, cooling_load_kw: float, metrics: Dict) -> Dict:
        """Analyze pump performance and optimization potential using live DB data"""

        # Calculate required CHW flow
        # Q = Load / (ρ × Cp × ΔT)
        chw_delta_t = 5.5  # Target
        required_flow_lps = cooling_load_kw / (4.18 * chw_delta_t)

        # Fetch live pump speeds from DB
        pump_rows = self.live_data.get_pump_telemetry()
        current_speeds = {}
        if pump_rows:
            for row in pump_rows:
                pid = row.get('PumpID', '')
                spd = row.get('VFDSpeedPercent')
                if pid and spd is not None:
                    current_speeds[pid] = float(spd)

        # Fall back to representative defaults only if DB unavailable
        if not current_speeds:
            current_speeds = {
                'PCHWP-1': 75.0, 'PCHWP-2': 75.0,
                'SCHWP-1': 70.0, 'SCHWP-2': 70.0, 'SCHWP-3': 70.0
            }

        # Calculate current power from live speeds
        current_power = 0
        for pump_id, speed in current_speeds.items():
            pump_type = pump_id.split('-')[0]
            rated_power = self.pump_specs.get(pump_type, {}).get('rated_power_kw', 45)
            current_power += rated_power * (speed / 100) ** 3
        
        # Optimize speeds based on required flow
        # Primary pumps: 2 running
        optimal_pchwp_speed = min(100, max(50, (required_flow_lps / 2 / 150) * 100))
        
        # Secondary pumps: 3 running
        optimal_schwp_speed = min(100, max(50, (required_flow_lps / 3 / 120) * 100))
        
        # Calculate optimized power
        optimized_power = 0
        optimized_power += 2 * 55 * (optimal_pchwp_speed / 100) ** 3  # PCHWP
        optimized_power += 3 * 45 * (optimal_schwp_speed / 100) ** 3  # SCHWP
        
        savings_kw = current_power - optimized_power
        
        return {
            'required_flow_lps': round(required_flow_lps, 1),
            'current_speeds': current_speeds,
            'optimal_speeds': {
                'PCHWP': round(optimal_pchwp_speed, 1),
                'SCHWP': round(optimal_schwp_speed, 1)
            },
            'current_power_kw': round(current_power, 1),
            'optimized_power_kw': round(optimized_power, 1),
            'potential_savings_kw': round(savings_kw, 1)
        }
    
    def _analyze_towers(self, cooling_load_kw: float, wet_bulb_temp: float, metrics: Dict) -> Dict:
        """Analyze cooling tower performance using live DB data"""

        # Calculate heat rejection
        heat_rejection_kw = cooling_load_kw * 1.3  # Includes compressor heat

        # Fetch live tower data from DB
        tower_rows = self.live_data.get_tower_telemetry()
        if tower_rows:
            # Average across all active towers
            approaches = [r.get('ApproachTempCelsius') for r in tower_rows if r.get('ApproachTempCelsius') is not None]
            fan_speeds = []
            for r in tower_rows:
                if r.get('Fan1VFDSpeedPercent') is not None:
                    fan_speeds.append(float(r['Fan1VFDSpeedPercent']))
                if r.get('Fan2VFDSpeedPercent') is not None:
                    fan_speeds.append(float(r['Fan2VFDSpeedPercent']))
            current_approach = float(np.mean(approaches)) if approaches else 3.8
            current_fan_speed = float(np.mean(fan_speeds)) if fan_speeds else 75.0
        else:
            # Fall back only when DB is unavailable
            current_approach = 3.8
            current_fan_speed = 75.0

        target_approach = self.targets['tower_approach']

        # Current fan power
        num_fans = len(tower_rows) * 2 if tower_rows else 4  # 2 fans per tower
        current_fan_power = num_fans * self.tower_specs['fan_power_kw_each'] * (current_fan_speed / 100) ** 3
        
        # Optimize fan speed
        # If approach is good, can reduce fan speed
        if current_approach <= target_approach:
            # Can reduce fan speed
            optimal_fan_speed = current_fan_speed * 0.85
            optimal_fan_speed = max(40, optimal_fan_speed)
        else:
            # Need to increase fan speed
            optimal_fan_speed = current_fan_speed * 1.1
            optimal_fan_speed = min(100, optimal_fan_speed)
        
        # Optimized fan power
        optimized_fan_power = num_fans * self.tower_specs['fan_power_kw_each'] * (optimal_fan_speed / 100) ** 3
        
        savings_kw = current_fan_power - optimized_fan_power
        
        return {
            'heat_rejection_kw': round(heat_rejection_kw, 1),
            'wet_bulb_temp': wet_bulb_temp,
            'current_approach': current_approach,
            'target_approach': target_approach,
            'current_fan_speed': current_fan_speed,
            'optimal_fan_speed': round(optimal_fan_speed, 1),
            'current_fan_power_kw': round(current_fan_power, 1),
            'optimized_fan_power_kw': round(optimized_fan_power, 1),
            'potential_savings_kw': round(savings_kw, 1)
        }
    
    def _check_economizer_opportunity(self, wet_bulb_temp: float) -> Dict:
        """Check if economizer mode is viable"""
        
        threshold = self.targets['economizer_threshold_wb']
        viable = wet_bulb_temp < threshold
        
        if viable:
            # Estimate savings (30-40% of chiller power)
            potential_savings_kw = 150  # Typical savings
            
            return {
                'viable': True,
                'wet_bulb_temp': wet_bulb_temp,
                'threshold': threshold,
                'margin': round(threshold - wet_bulb_temp, 1),
                'potential_savings_kw': potential_savings_kw,
                'description': f'Wet-bulb {wet_bulb_temp}°C < {threshold}°C threshold'
            }
        else:
            return {
                'viable': False,
                'wet_bulb_temp': wet_bulb_temp,
                'threshold': threshold,
                'margin': round(wet_bulb_temp - threshold, 1),
                'description': f'Wet-bulb {wet_bulb_temp}°C > {threshold}°C threshold'
            }
    
    def _propose_economizer(self, analysis: Dict) -> Dict:
        """Propose economizer mode activation"""
        
        eco = analysis['economizer_opportunity']
        
        confidence = self.calculate_confidence(
            historical_matches=5,
            data_quality=0.95,
            risk_level='LOW'
        )
        
        proposal = self.format_proposal(
            action_type='ENABLE_ECONOMIZER',
            description=f"Enable economizer mode (free cooling) - wet-bulb {eco['wet_bulb_temp']}°C",
            justification=f"Wet-bulb temperature {eco['margin']:.1f}°C below threshold. Can leverage outside air for cooling, significantly reducing chiller load.",
            predicted_savings={
                'energy_kw': eco['potential_savings_kw'],
                'cost_sgd': round(eco['potential_savings_kw'] * 0.20, 2),
                'pue_improvement': 0.05,
                'chiller_load_reduction_percent': 35
            },
            evidence=[
                self.cite_evidence(
                    'WEATHER',
                    'Current Conditions',
                    {
                        'wet_bulb_temp': eco['wet_bulb_temp'],
                        'threshold': eco['threshold']
                    }
                ),
                self.cite_evidence(
                    'REGULATION',
                    'NEA Requirements',
                    'Mandatory free cooling when wet-bulb < 22°C'
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_pump_optimization(self, analysis: Dict) -> Dict:
        """Propose pump VFD speed optimization"""
        
        pump_data = analysis['pump_analysis']
        
        confidence = self.calculate_confidence(
            historical_matches=len(analysis['historical_precedents']),
            data_quality=0.90,
            risk_level='MEDIUM'
        )
        
        description = f"Optimize pump VFD speeds: PCHWP → {pump_data['optimal_speeds']['PCHWP']}%, SCHWP → {pump_data['optimal_speeds']['SCHWP']}%"
        
        proposal = self.format_proposal(
            action_type='PUMP_VFD_OPTIMIZATION',
            description=description,
            justification=f"Current pumping power: {pump_data['current_power_kw']} kW. Required flow: {pump_data['required_flow_lps']} L/s. Reducing VFD speeds to match actual demand reduces pumping power by {pump_data['potential_savings_kw']} kW (affinity laws: Power ∝ Speed³).",
            predicted_savings={
                'energy_kw': pump_data['potential_savings_kw'],
                'cost_sgd': round(pump_data['potential_savings_kw'] * 0.20, 2),
                'pue_improvement': round(pump_data['potential_savings_kw'] / 9500 * 0.01, 3)
            },
            evidence=[
                self.cite_evidence(
                    'ANALYTICAL',
                    'Pump Affinity Laws',
                    pump_data
                ),
                self.cite_evidence(
                    'HISTORICAL',
                    'Medium-term Memory',
                    analysis['historical_precedents'][:2]
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_tower_optimization(self, analysis: Dict) -> Dict:
        """Propose cooling tower fan optimization"""
        
        tower_data = analysis['tower_analysis']
        
        confidence = self.calculate_confidence(
            historical_matches=len(analysis['historical_precedents']),
            data_quality=0.88,
            risk_level='LOW'
        )
        
        proposal = self.format_proposal(
            action_type='TOWER_FAN_OPTIMIZATION',
            description=f"Adjust tower fan speeds to {tower_data['optimal_fan_speed']:.0f}%",
            justification=f"Current approach: {tower_data['current_approach']}°C vs target {tower_data['target_approach']}°C. Fan speed can be adjusted to maintain optimal approach while minimizing fan power.",
            predicted_savings={
                'energy_kw': tower_data['potential_savings_kw'],
                'cost_sgd': round(tower_data['potential_savings_kw'] * 0.20, 2),
                'pue_improvement': round(tower_data['potential_savings_kw'] / 9500 * 0.01, 3)
            },
            evidence=[
                self.cite_evidence(
                    'ANALYTICAL',
                    'Tower Performance Model',
                    tower_data
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine monitoring update"""
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'Building systems operating optimally',
            'status': 'NOMINAL',
            'pump_power_kw': analysis['pump_analysis']['current_power_kw'],
            'tower_fan_power_kw': analysis['tower_analysis']['current_fan_power_kw'],
            'economizer_status': 'AVAILABLE' if analysis['economizer_opportunity']['viable'] else 'NOT_VIABLE',
            'confidence': self.confidence_level
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING BUILDING SYSTEM AGENT")
    print("="*70)
    
    # Create agent
    agent = BuildingSystemAgent()
    
    # Test context - normal conditions
    print("\n[TEST 1] Normal Conditions:")
    context = {
        'cooling_load_kw': 2800,
        'wet_bulb_temp': 25.5,
        'timestamp': datetime.now()
    }
    
    proposal = agent.propose_action(context)
    print(f"  Action: {proposal['action_type']}")
    if 'predicted_savings' in proposal:
        print(f"  Savings: {proposal['predicted_savings']['energy_kw']} kW")
    
    # Test context - economizer opportunity
    print("\n[TEST 2] Economizer Opportunity:")
    context = {
        'cooling_load_kw': 2800,
        'wet_bulb_temp': 21.0,  # Below threshold
        'timestamp': datetime.now()
    }
    
    proposal = agent.propose_action(context)
    print(f"  Action: {proposal['action_type']}")
    print(f"  Description: {proposal['description']}")
    if 'predicted_savings' in proposal:
        print(f"  Savings: {proposal['predicted_savings']['energy_kw']} kW")
        print(f"  Confidence: {proposal['confidence']:.2f}")
    
    print("\n Building System Agent test complete!")
