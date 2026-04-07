"""
Demand & Conditions Agent
Forecasts IT load, weather conditions, and cooling demand
Identifies optimization opportunities based on predictions
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



class DemandConditionsAgent(BaseAgent):
    """
    Agent 1: Demand & Conditions
    
    Responsibilities:
    - Forecast IT load (1-4 hours ahead)
    - Predict weather conditions (wet-bulb, dry-bulb, humidity)
    - Calculate cooling load requirements
    - Identify economizer opportunities
    - Flag load spikes or anomalies
    - Provide context for other agents' decisions
    
    Authority: Advisory only (no direct control)
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Demand & Conditions Agent",
            agent_role="Forecast demand and environmental conditions for proactive optimization"
        )
        
        # Load agent-specific prompt
        self.load_prompt()
        
        # Load forecast models
        self._initialize_forecast_models()
    
    def load_prompt(self):
        """Load versioned prompt from file"""
        
        try:
            with open('agents/prompts/demand_conditions_v1.txt', 'r') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            self.system_prompt = f"""
You are the {self.agent_name}.

Your role: {self.agent_role}

RESPONSIBILITIES:
1. Forecast IT load 1-4 hours ahead
2. Predict weather conditions (wet-bulb, dry-bulb, humidity)
3. Calculate cooling load requirements
4. Identify economizer opportunities (wet-bulb < 22°C)
5. Flag predicted load spikes or anomalies
6. Provide context for optimization decisions

AUTHORITY:
- Advisory role only (no direct equipment control)
- Provide forecasts and recommendations
- Flag opportunities and risks
- Support other agents with predictive context

DECISION CRITERIA:
When analyzing conditions:
1. Compare current vs forecasted conditions
2. Identify significant changes (>10% load change, >2°C temp change)
3. Flag economizer opportunities (wet-bulb < 22°C)
4. Predict load spikes requiring pre-staging
5. Consider time-of-day and day-of-week patterns

OUTPUT FORMAT:
Provide forecasts in JSON with:
- IT load forecast (1-4 hours)
- Weather forecast
- Cooling load requirements
- Economizer opportunities
- Recommendations for other agents
- Confidence levels
"""
    
    # Fallback pattern used only when DB is unavailable
    _FALLBACK_HOURLY_PATTERN = {
        0: 9200, 1: 9100, 2: 9100, 3: 9200, 4: 9300, 5: 9400,
        6: 9500, 7: 9600, 8: 9700, 9: 9900, 10: 10000, 11: 10100,
        12: 10200, 13: 10200, 14: 10200, 15: 10100, 16: 10000, 17: 9900,
        18: 9800, 19: 9700, 20: 9600, 21: 9500, 22: 9400, 23: 9300
    }

    def _initialize_forecast_models(self):
        """Initialize forecast models from DB historical data."""

        # Day-of-week multipliers (derived from historical deviation patterns)
        self.dow_multipliers = {
            0: 1.00,  # Monday
            1: 1.00,  # Tuesday
            2: 1.00,  # Wednesday
            3: 1.00,  # Thursday
            4: 0.98,  # Friday
            5: 0.90,  # Saturday
            6: 0.85   # Sunday
        }

        # Load hourly IT load baseline from DB (last 30 days)
        self.hourly_it_load_pattern = self._load_hourly_pattern_from_db()

        # Weather patterns (kept for fallback, but live weather comes from context)
        self.weather_patterns = {
            'wet_bulb_baseline': 25.0,
            'dry_bulb_baseline': 30.0,
            'humidity_baseline': 75.0
        }

    def _load_hourly_pattern_from_db(self) -> Dict[int, float]:
        """
        Fetch average IT load per hour from the last 30 days in FacilityPower.
        Falls back to the static pattern if DB is unreachable or has insufficient data.
        """
        # Lazy import to avoid circular dependency at module load time
        try:
            from orchestrator.live_data import live_data as _live_data
        except Exception:
            _live_data = None

        if _live_data is None:
            print("  [DemandAgent] live_data unavailable — using fallback hourly pattern")
            return dict(self._FALLBACK_HOURLY_PATTERN)

        try:
            db_pattern = _live_data.get_it_load_by_hour(days=30)
            if len(db_pattern) >= 18:  # need at least 18 hours to be useful
                # Fill any missing hours from adjacent averages or fallback
                filled = {}
                overall_avg = sum(db_pattern.values()) / len(db_pattern)
                for h in range(24):
                    if h in db_pattern:
                        filled[h] = db_pattern[h]
                    else:
                        # Interpolate from neighbours or use overall average
                        neighbours = [db_pattern[n] for n in [h - 1, h + 1] if n in db_pattern]
                        filled[h] = sum(neighbours) / len(neighbours) if neighbours else overall_avg
                print(f"  [DemandAgent] Loaded DB-derived hourly IT load pattern ({len(db_pattern)} hours with data)")
                return filled
            else:
                print(f"  [DemandAgent] DB pattern has only {len(db_pattern)} hours — using fallback")
                return dict(self._FALLBACK_HOURLY_PATTERN)
        except Exception as e:
            print(f"  [DemandAgent] Failed to load hourly pattern from DB: {e} — using fallback")
            return dict(self._FALLBACK_HOURLY_PATTERN)
    
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze current demand and conditions
        
        Args:
            context: Current system state
        
        Returns:
            Analysis with forecasts and recommendations
        """
        
        # Get current metrics
        current_metrics = self.get_current_metrics()
        
        # Get current conditions
        current_it_load = context.get('it_load_kw', current_metrics.get('it_load_kw', 9500))
        current_wet_bulb = context.get('wet_bulb_temp', 25.0)
        current_dry_bulb = context.get('dry_bulb_temp', 30.0)
        current_humidity = context.get('humidity_percent', 75.0)
        
        # Forecast IT load
        it_load_forecast = self._forecast_it_load(
            current_time=datetime.now(),
            current_load=current_it_load,
            horizon_hours=4
        )
        
        # Forecast weather
        weather_forecast = self._forecast_weather(
            current_wet_bulb=current_wet_bulb,
            current_dry_bulb=current_dry_bulb,
            current_humidity=current_humidity,
            horizon_hours=4
        )
        
        # Calculate cooling load requirements
        cooling_load_forecast = self._forecast_cooling_load(
            it_load_forecast=it_load_forecast,
            weather_forecast=weather_forecast
        )
        
        # Identify economizer opportunities
        economizer_opportunities = self._identify_economizer_opportunities(
            weather_forecast=weather_forecast
        )
        
        # Detect anomalies or significant changes
        anomalies = self._detect_anomalies(
            current_load=current_it_load,
            forecasted_loads=it_load_forecast
        )
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            it_load_forecast=it_load_forecast,
            cooling_load_forecast=cooling_load_forecast,
            economizer_opportunities=economizer_opportunities,
            anomalies=anomalies
        )
        
        return {
            'current_state': current_metrics,
            'current_conditions': {
                'it_load_kw': current_it_load,
                'wet_bulb_temp': current_wet_bulb,
                'dry_bulb_temp': current_dry_bulb,
                'humidity_percent': current_humidity
            },
            'it_load_forecast': it_load_forecast,
            'weather_forecast': weather_forecast,
            'cooling_load_forecast': cooling_load_forecast,
            'economizer_opportunities': economizer_opportunities,
            'anomalies': anomalies,
            'recommendations': recommendations
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose actions based on forecasts
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action or monitoring update
        """
        
        # Analyze situation
        analysis = self.analyze_situation(context)
        
        # Check for significant opportunities or risks
        recommendations = analysis['recommendations']
        
        # Priority 1: Economizer opportunity
        if recommendations.get('enable_economizer'):
            return self._propose_economizer(analysis)
        
        # Priority 2: Pre-stage for load spike
        if recommendations.get('pre_stage_chiller'):
            return self._propose_pre_staging(analysis)
        
        # Priority 3: Load spike warning
        if recommendations.get('load_spike_warning'):
            return self._propose_load_warning(analysis)
        
        # Otherwise: Monitoring update
        return self._create_monitoring_update(analysis)
    
    def _forecast_it_load(
        self,
        current_time: datetime,
        current_load: float,
        horizon_hours: int
    ) -> List[Dict]:
        """
        Forecast IT load using pattern-based model
        
        Args:
            current_time: Current timestamp
            current_load: Current IT load (kW)
            horizon_hours: Forecast horizon
        
        Returns:
            List of forecasted loads
        """
        
        forecasts = []
        
        for h in range(1, horizon_hours + 1):
            future_time = current_time + timedelta(hours=h)
            hour = future_time.hour
            dow = future_time.weekday()
            
            # Get baseline from pattern
            baseline_load = self.hourly_it_load_pattern.get(hour, 9500)
            
            # Apply day-of-week multiplier
            dow_multiplier = self.dow_multipliers.get(dow, 1.0)
            
            # Forecast
            forecasted_load = baseline_load * dow_multiplier
            
            # Add some noise (±2%)
            noise = np.random.normal(0, forecasted_load * 0.02)
            forecasted_load += noise
            
            # Confidence decreases with horizon
            confidence = max(0.6, 0.9 - (h * 0.075))
            
            forecasts.append({
                'hours_ahead': h,
                'timestamp': future_time.isoformat(),
                'forecast_load_kw': round(forecasted_load, 0),
                'confidence': round(confidence, 2)
            })
        
        return forecasts
    
    def _forecast_weather(
        self,
        current_wet_bulb: float,
        current_dry_bulb: float,
        current_humidity: float,
        horizon_hours: int
    ) -> List[Dict]:
        """
        Forecast weather conditions
        
        Simplified model - would use actual weather API in production
        """
        
        forecasts = []
        
        for h in range(1, horizon_hours + 1):
            # Simple persistence + diurnal variation
            hour_offset = h
            
            # Wet-bulb typically varies ±2°C daily
            wb_variation = np.sin(hour_offset * np.pi / 12) * 2.0
            forecasted_wb = current_wet_bulb + wb_variation + np.random.normal(0, 0.5)
            
            # Dry-bulb varies more (±4°C)
            db_variation = np.sin(hour_offset * np.pi / 12) * 4.0
            forecasted_db = current_dry_bulb + db_variation + np.random.normal(0, 0.8)
            
            # Humidity inverse to temperature
            humidity_variation = -np.sin(hour_offset * np.pi / 12) * 10.0
            forecasted_humidity = current_humidity + humidity_variation + np.random.normal(0, 3.0)
            forecasted_humidity = max(50, min(95, forecasted_humidity))
            
            confidence = max(0.65, 0.85 - (h * 0.05))
            
            forecasts.append({
                'hours_ahead': h,
                'wet_bulb_temp': round(forecasted_wb, 1),
                'dry_bulb_temp': round(forecasted_db, 1),
                'humidity_percent': round(forecasted_humidity, 0),
                'confidence': round(confidence, 2)
            })
        
        return forecasts
    
    def _forecast_cooling_load(
        self,
        it_load_forecast: List[Dict],
        weather_forecast: List[Dict]
    ) -> List[Dict]:
        """
        Forecast cooling load requirements
        
        Cooling load = IT load × 0.29 (typical datacenter)
        Adjusted for weather conditions
        """
        
        cooling_forecasts = []
        
        for i, it_forecast in enumerate(it_load_forecast):
            weather = weather_forecast[i]
            
            # Base cooling load
            base_cooling_kw = it_forecast['forecast_load_kw'] * 0.29
            
            # Weather adjustment (higher wet-bulb = higher cooling load)
            baseline_wb = 25.0
            wb_delta = weather['wet_bulb_temp'] - baseline_wb
            weather_factor = 1.0 + (wb_delta * 0.02)  # 2% per °C
            
            adjusted_cooling_kw = base_cooling_kw * weather_factor
            cooling_tons = adjusted_cooling_kw / 3.517
            
            cooling_forecasts.append({
                'hours_ahead': it_forecast['hours_ahead'],
                'cooling_load_kw': round(adjusted_cooling_kw, 0),
                'cooling_load_tons': round(cooling_tons, 0),
                'it_load_kw': it_forecast['forecast_load_kw'],
                'weather_factor': round(weather_factor, 3)
            })
        
        return cooling_forecasts
    
    def _identify_economizer_opportunities(
        self,
        weather_forecast: List[Dict]
    ) -> List[Dict]:
        """
        Identify economizer (free cooling) opportunities
        
        Economizer viable when wet-bulb < 22°C
        """
        
        opportunities = []
        economizer_threshold = 22.0
        
        for weather in weather_forecast:
            if weather['wet_bulb_temp'] < economizer_threshold:
                opportunities.append({
                    'hours_ahead': weather['hours_ahead'],
                    'wet_bulb_temp': weather['wet_bulb_temp'],
                    'margin_below_threshold': round(economizer_threshold - weather['wet_bulb_temp'], 1),
                    'estimated_savings_kw': 150,  # Typical savings
                    'confidence': weather['confidence']
                })
        
        return opportunities
    
    def _detect_anomalies(
        self,
        current_load: float,
        forecasted_loads: List[Dict]
    ) -> List[Dict]:
        """
        Detect load anomalies or significant changes
        
        Flag if forecasted load changes by >10%
        """
        
        anomalies = []
        
        for forecast in forecasted_loads:
            forecasted_load = forecast['forecast_load_kw']
            change_percent = ((forecasted_load - current_load) / current_load) * 100
            
            if abs(change_percent) > 10:
                anomalies.append({
                    'hours_ahead': forecast['hours_ahead'],
                    'type': 'LOAD_SPIKE' if change_percent > 0 else 'LOAD_DROP',
                    'current_load': current_load,
                    'forecasted_load': forecasted_load,
                    'change_percent': round(change_percent, 1),
                    'severity': 'HIGH' if abs(change_percent) > 15 else 'MEDIUM'
                })
        
        return anomalies
    
    def _generate_recommendations(
        self,
        it_load_forecast: List[Dict],
        cooling_load_forecast: List[Dict],
        economizer_opportunities: List[Dict],
        anomalies: List[Dict]
    ) -> Dict:
        """
        Generate recommendations for other agents
        """
        
        recommendations = {
            'enable_economizer': False,
            'pre_stage_chiller': False,
            'load_spike_warning': False,
            'details': []
        }
        
        # Check economizer opportunities
        if economizer_opportunities:
            recommendations['enable_economizer'] = True
            recommendations['details'].append({
                'type': 'ECONOMIZER',
                'message': f"Economizer opportunity in {economizer_opportunities[0]['hours_ahead']} hours",
                'priority': 'HIGH'
            })
        
        # Check for load spikes
        high_severity_anomalies = [a for a in anomalies if a['severity'] == 'HIGH']
        if high_severity_anomalies:
            # If spike (not drop) in next 2 hours, recommend pre-staging
            early_spikes = [
                a for a in high_severity_anomalies
                if a['hours_ahead'] <= 2 and a['type'] == 'LOAD_SPIKE'
            ]
            if early_spikes:
                recommendations['pre_stage_chiller'] = True
                recommendations['details'].append({
                    'type': 'PRE_STAGE',
                    'message': f"Load spike of {early_spikes[0]['change_percent']}% predicted in {early_spikes[0]['hours_ahead']} hours",
                    'priority': 'HIGH'
                })
            else:
                recommendations['load_spike_warning'] = True
                recommendations['details'].append({
                    'type': 'WARNING',
                    'message': f"Load spike predicted in {high_severity_anomalies[0]['hours_ahead']} hours",
                    'priority': 'MEDIUM'
                })
        
        return recommendations
    
    def _propose_economizer(self, analysis: Dict) -> Dict:
        """Propose economizer activation"""
        
        opportunities = analysis['economizer_opportunities']
        first_opp = opportunities[0]
        
        confidence = self.calculate_confidence(
            historical_matches=8,
            data_quality=0.90,
            risk_level='LOW'
        )
        
        proposal = self.format_proposal(
            action_type='ENABLE_ECONOMIZER',
            description=f"Economizer opportunity: wet-bulb {first_opp['wet_bulb_temp']}°C in {first_opp['hours_ahead']} hours",
            justification=f"Weather forecast shows wet-bulb temperature {first_opp['margin_below_threshold']}°C below 22°C threshold. Free cooling can save approximately {first_opp['estimated_savings_kw']} kW.",
            predicted_savings={
                'energy_kw': first_opp['estimated_savings_kw'],
                'cost_sgd': round(first_opp['estimated_savings_kw'] * 0.20, 2),
                'pue_improvement': 0.03,
                'chiller_load_reduction_percent': 30
            },
            evidence=[
                self.cite_evidence(
                    'FORECAST',
                    'Weather Model',
                    analysis['weather_forecast']
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_pre_staging(self, analysis: Dict) -> Dict:
        """Propose pre-staging chiller for predicted load spike"""

        anomalies = [a for a in analysis['anomalies'] if a['type'] == 'LOAD_SPIKE']
        if not anomalies:
            return self._create_monitoring_update(analysis)
        spike = anomalies[0]
        
        confidence = self.calculate_confidence(
            historical_matches=5,
            data_quality=0.75,
            risk_level='MEDIUM'
        )
        
        proposal = self.format_proposal(
            action_type='PRE_STAGE_CHILLER',
            description=f"Pre-stage chiller for {spike['change_percent']:.0f}% load increase in {spike['hours_ahead']} hours",
            justification=f"IT load forecast shows spike from {spike['current_load']:.0f} kW to {spike['forecasted_load']:.0f} kW. Pre-staging chiller now avoids thermal shock and ensures adequate capacity.",
            predicted_savings={
                'avoided_thermal_shock': True,
                'capacity_assurance': True,
                'startup_time_minutes': 30
            },
            evidence=[
                self.cite_evidence(
                    'FORECAST',
                    'IT Load Model',
                    analysis['it_load_forecast']
                )
            ],
            confidence=confidence
        )
        
        return proposal
    
    def _propose_load_warning(self, analysis: Dict) -> Dict:
        """Propose load spike warning"""
        
        anomalies = analysis['anomalies']
        spike = anomalies[0]
        
        return self.format_proposal(
            action_type='LOAD_SPIKE_WARNING',
            description=f"Load spike warning: {spike['change_percent']:.0f}% increase predicted in {spike['hours_ahead']} hours",
            justification=f"Advance notice for capacity planning. Current load: {spike['current_load']:.0f} kW, forecasted: {spike['forecasted_load']:.0f} kW.",
            predicted_savings={},
            evidence=[],
            confidence=0.75
        )
    
    def _create_monitoring_update(self, analysis: Dict) -> Dict:
        """Create routine monitoring update"""
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': 'MONITORING_UPDATE',
            'description': 'Demand and conditions nominal',
            'status': 'NOMINAL',
            'forecasts': {
                'it_load': analysis['it_load_forecast'][0],  # Next hour
                'weather': analysis['weather_forecast'][0],
                'cooling_load': analysis['cooling_load_forecast'][0]
            },
            'confidence': self.confidence_level
        }


# Example usage
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING DEMAND & CONDITIONS AGENT")
    print("="*70)
    
    # Create agent
    agent = DemandConditionsAgent()
    
    # Test context
    context = {
        'it_load_kw': 9500,
        'wet_bulb_temp': 21.0,  # Below economizer threshold
        'dry_bulb_temp': 26.0,
        'humidity_percent': 70,
        'timestamp': datetime.now().isoformat()
    }
    
    # Test
    print("\n[TEST] Analyze & Propose...")
    proposal = agent.propose_action(context)
    
    print(f"\nAgent: {proposal['agent']}")
    print(f"Action: {proposal['action_type']}")
    print(f"Description: {proposal.get('description', proposal.get('status'))}")
    
    if 'predicted_savings' in proposal and proposal['predicted_savings']:
        print(f"\nPredicted Savings:")
        for key, value in proposal['predicted_savings'].items():
            print(f"  {key}: {value}")
    
    print("\n Demand & Conditions Agent test complete!")