"""
Weather-Load Correlation Model
Analyzes correlation between weather conditions and cooling load
Helps predict cooling load based on weather forecasts
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pickle
from pathlib import Path
from scipy import stats

class WeatherLoadCorrelation:
    """
    Weather-Cooling Load Correlation Model
    
    Analyzes relationships between:
    - Wet-bulb temperature → Cooling tower performance
    - Dry-bulb temperature → IT load (indirect)
    - Humidity → Overall system efficiency
    
    Provides cooling load predictions based on weather forecasts
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize weather-load correlation model
        
        Args:
            model_path: Path to trained model (optional)
        """
        
        self.model_path = model_path
        self.trained_model = None
        
        # Try to load trained model
        if model_path and Path(model_path).exists():
            self._load_trained_model()
        
        # Empirical correlations (Singapore datacenter)
        # These are typical values - would be calibrated with actual data
        self.correlations = {
            'wet_bulb_tower_approach': {
                # Wet-bulb (°C) → Tower approach temp (°C)
                'slope': 0.15,
                'intercept': 1.5
            },
            'dry_bulb_it_load': {
                # Dry-bulb (°C) → IT load multiplier
                # Slight increase in IT load during hot weather (AC load)
                'slope': 0.002,
                'intercept': 0.94
            },
            'humidity_efficiency_penalty': {
                # Humidity (%) → Efficiency penalty factor
                'slope': 0.0005,
                'intercept': 0.96
            }
        }
        
        # Historical correlation coefficients (would be calculated from data)
        self.correlation_coefficients = {
            'wet_bulb_vs_cooling_load': 0.72,  # Strong positive correlation
            'dry_bulb_vs_it_load': 0.35,       # Weak positive correlation
            'humidity_vs_efficiency': -0.48    # Moderate negative correlation
        }
        
        print(f"✅ Weather-Load Correlation Model initialized")
        print(f"   Mode: {'Trained ML' if self.trained_model else 'Empirical'}")
    
    def predict_cooling_load(
        self,
        it_load_kw: float,
        wet_bulb_c: float,
        dry_bulb_c: float,
        humidity_percent: float
    ) -> Dict:
        """
        Predict cooling load based on IT load and weather conditions
        
        Args:
            it_load_kw: IT equipment load (kW)
            wet_bulb_c: Wet-bulb temperature (°C)
            dry_bulb_c: Dry-bulb temperature (°C)
            humidity_percent: Relative humidity (%)
        
        Returns:
            Cooling load prediction with breakdown
        """
        
        # Base cooling load (IT load * datacenter heat ratio)
        # Typical datacenter: ~29% of facility power is cooling
        base_cooling_load = it_load_kw * 0.29
        
        # Weather-based adjustments
        
        # 1. Wet-bulb impact on tower performance
        # Higher wet-bulb → Higher tower approach → Higher CW temp → Higher cooling load
        wb_impact = self._calculate_wetbulb_impact(wet_bulb_c)
        
        # 2. Dry-bulb impact on IT load (indirect)
        # Higher ambient → Slightly higher IT load (AC systems, server fans)
        db_impact = self._calculate_drybulb_impact(dry_bulb_c)
        
        # 3. Humidity impact on efficiency
        # Higher humidity → Lower efficiency → Higher power consumption
        humidity_impact = self._calculate_humidity_impact(humidity_percent)
        
        # Combined cooling load
        total_cooling_load = base_cooling_load * (1 + wb_impact + db_impact + humidity_impact)
        
        # Convert to tons
        cooling_load_tons = total_cooling_load / 3.517
        
        return {
            'base_cooling_load_kw': round(base_cooling_load, 1),
            'total_cooling_load_kw': round(total_cooling_load, 1),
            'cooling_load_tons': round(cooling_load_tons, 1),
            'weather_impacts': {
                'wet_bulb_impact_percent': round(wb_impact * 100, 2),
                'dry_bulb_impact_percent': round(db_impact * 100, 2),
                'humidity_impact_percent': round(humidity_impact * 100, 2),
                'total_impact_percent': round((wb_impact + db_impact + humidity_impact) * 100, 2)
            },
            'conditions': {
                'wet_bulb_c': wet_bulb_c,
                'dry_bulb_c': dry_bulb_c,
                'humidity_percent': humidity_percent
            }
        }
    
    def _calculate_wetbulb_impact(self, wet_bulb_c: float) -> float:
        """
        Calculate wet-bulb impact on cooling load
        
        Wet-bulb affects tower performance:
        - Low wet-bulb (< 24°C) → Better tower performance → Lower CW temp → Lower cooling load
        - High wet-bulb (> 26°C) → Worse tower performance → Higher CW temp → Higher cooling load
        """
        
        # Baseline wet-bulb for Singapore
        baseline_wb = 25.0
        
        # Every 1°C above baseline adds ~3% to cooling load
        # Every 1°C below baseline reduces ~2% from cooling load
        delta = wet_bulb_c - baseline_wb
        
        if delta > 0:
            # Penalty for high wet-bulb
            impact = delta * 0.03
        else:
            # Benefit for low wet-bulb
            impact = delta * 0.02
        
        return impact
    
    def _calculate_drybulb_impact(self, dry_bulb_c: float) -> float:
        """
        Calculate dry-bulb impact on IT load (indirect)
        
        Higher ambient temperature slightly increases:
        - Server fan speeds
        - CRAC/CRAH unit power
        - Auxiliary cooling
        """
        
        baseline_db = 30.0
        
        # Every 1°C above baseline adds ~0.5% to IT load
        delta = dry_bulb_c - baseline_db
        
        impact = delta * 0.005
        
        return max(0, impact)  # Only penalty, no benefit
    
    def _calculate_humidity_impact(self, humidity_percent: float) -> float:
        """
        Calculate humidity impact on system efficiency
        
        Higher humidity:
        - Reduces air density
        - Increases compressor work
        - Affects heat transfer
        """
        
        baseline_humidity = 70.0
        
        # Every 10% above baseline adds ~1% efficiency penalty
        delta = humidity_percent - baseline_humidity
        
        impact = (delta / 10) * 0.01
        
        return max(0, impact)  # Only penalty
    
    def analyze_correlation(
        self,
        weather_data: pd.DataFrame,
        cooling_load_data: pd.DataFrame
    ) -> Dict:
        """
        Analyze historical correlation between weather and cooling load
        
        Args:
            weather_data: DataFrame with columns [timestamp, wet_bulb, dry_bulb, humidity]
            cooling_load_data: DataFrame with columns [timestamp, cooling_load_kw]
        
        Returns:
            Correlation analysis results
        """
        
        # Merge datasets on timestamp
        merged = pd.merge(
            weather_data,
            cooling_load_data,
            on='timestamp',
            how='inner'
        )
        
        if merged.empty:
            return {
                'error': 'No overlapping data',
                'correlations': {}
            }
        
        # Calculate correlations
        correlations = {}
        
        # Wet-bulb vs Cooling Load
        if 'wet_bulb' in merged.columns and 'cooling_load_kw' in merged.columns:
            r, p_value = stats.pearsonr(merged['wet_bulb'], merged['cooling_load_kw'])
            correlations['wet_bulb_vs_cooling_load'] = {
                'coefficient': round(r, 3),
                'p_value': round(p_value, 4),
                'strength': self._interpret_correlation(r)
            }
        
        # Dry-bulb vs Cooling Load
        if 'dry_bulb' in merged.columns:
            r, p_value = stats.pearsonr(merged['dry_bulb'], merged['cooling_load_kw'])
            correlations['dry_bulb_vs_cooling_load'] = {
                'coefficient': round(r, 3),
                'p_value': round(p_value, 4),
                'strength': self._interpret_correlation(r)
            }
        
        # Humidity vs Cooling Load
        if 'humidity' in merged.columns:
            r, p_value = stats.pearsonr(merged['humidity'], merged['cooling_load_kw'])
            correlations['humidity_vs_cooling_load'] = {
                'coefficient': round(r, 3),
                'p_value': round(p_value, 4),
                'strength': self._interpret_correlation(r)
            }
        
        # Calculate regression coefficients
        if 'wet_bulb' in merged.columns:
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                merged['wet_bulb'],
                merged['cooling_load_kw']
            )
            
            correlations['wet_bulb_regression'] = {
                'slope': round(slope, 2),
                'intercept': round(intercept, 2),
                'r_squared': round(r_value**2, 3),
                'equation': f'CoolingLoad = {slope:.2f} * WetBulb + {intercept:.2f}'
            }
        
        return {
            'sample_size': len(merged),
            'correlations': correlations,
            'summary': self._generate_correlation_summary(correlations)
        }
    
    def _interpret_correlation(self, r: float) -> str:
        """Interpret correlation coefficient strength"""
        
        r_abs = abs(r)
        
        if r_abs >= 0.7:
            strength = "Strong"
        elif r_abs >= 0.4:
            strength = "Moderate"
        elif r_abs >= 0.2:
            strength = "Weak"
        else:
            strength = "Very Weak"
        
        direction = "positive" if r >= 0 else "negative"
        
        return f"{strength} {direction}"
    
    def _generate_correlation_summary(self, correlations: Dict) -> str:
        """Generate human-readable summary"""
        
        summary_lines = []
        
        for key, data in correlations.items():
            if 'coefficient' in data:
                summary_lines.append(
                    f"{key}: {data['strength']} (r={data['coefficient']})"
                )
        
        return "\n".join(summary_lines)
    
    def predict_tower_approach(
        self,
        wet_bulb_c: float,
        tower_loading_percent: float = 75.0
    ) -> float:
        """
        Predict cooling tower approach temperature
        
        Args:
            wet_bulb_c: Wet-bulb temperature (°C)
            tower_loading_percent: Tower loading (% of design)
        
        Returns:
            Predicted approach temperature (°C)
        """
        
        # Base approach from empirical correlation
        base_approach = (
            self.correlations['wet_bulb_tower_approach']['slope'] * wet_bulb_c +
            self.correlations['wet_bulb_tower_approach']['intercept']
        )
        
        # Adjust for tower loading
        # Higher loading → Worse approach
        loading_factor = 1.0 + (tower_loading_percent - 75.0) / 100 * 0.5
        
        approach = base_approach * loading_factor
        
        # Typical range: 2.5-5.5°C
        approach = max(2.5, min(5.5, approach))
        
        return round(approach, 2)
    
    def calculate_economizer_potential(
        self,
        wet_bulb_forecast: List[float],
        current_cooling_load_kw: float
    ) -> Dict:
        """
        Calculate free cooling (economizer) potential based on weather forecast
        
        Args:
            wet_bulb_forecast: List of forecasted wet-bulb temps (°C)
            current_cooling_load_kw: Current cooling load
        
        Returns:
            Economizer opportunity analysis
        """
        
        economizer_threshold = 22.0  # °C - Below this, economizer is viable
        
        opportunities = []
        total_potential_savings_kw = 0
        
        for hour, wb_temp in enumerate(wet_bulb_forecast):
            if wb_temp < economizer_threshold:
                # Estimate savings
                # At wet-bulb < 22°C, can save ~30-40% of chiller power
                chiller_power_kw = current_cooling_load_kw * 0.18  # Typical chiller efficiency
                potential_savings = chiller_power_kw * 0.35
                
                opportunities.append({
                    'hour': hour,
                    'wet_bulb_c': wb_temp,
                    'potential_savings_kw': round(potential_savings, 1)
                })
                
                total_potential_savings_kw += potential_savings
        
        return {
            'total_opportunities': len(opportunities),
            'total_potential_savings_kw': round(total_potential_savings_kw, 1),
            'total_potential_savings_sgd': round(total_potential_savings_kw * 0.20, 2),  # SGD 0.20/kWh
            'opportunities': opportunities,
            'recommendation': 'ENABLE_ECONOMIZER' if opportunities else 'NO_OPPORTUNITY'
        }
    
    def _load_trained_model(self):
        """Load trained correlation model"""
        
        try:
            with open(self.model_path, 'rb') as f:
                model_data = pickle.load(f)
            
            if isinstance(model_data, dict):
                self.trained_model = model_data.get('model')
                self.correlations = model_data.get('correlations', self.correlations)
            else:
                self.trained_model = model_data
            
            print(f"   Loaded trained model from {self.model_path}")
            
        except Exception as e:
            print(f"   ⚠️  Could not load model: {e}")
            self.trained_model = None
    
    def save_model(self, correlations: Dict, model_path: str):
        """
        Save calibrated correlations
        
        Args:
            correlations: Calibrated correlation parameters
            model_path: Save path
        """
        
        model_data = {
            'correlations': correlations,
            'metadata': {
                'calibrated_date': str(np.datetime64('today')),
                'sample_size': 'N/A'
            }
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"✅ Correlation model saved to {model_path}")


# Example usage and testing
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING WEATHER-LOAD CORRELATION MODEL")
    print("="*70)
    
    # Initialize model
    model = WeatherLoadCorrelation()
    
    # Test 1: Predict cooling load
    print("\n[TEST 1] Cooling Load Prediction:")
    
    test_conditions = [
        {'it_load': 9500, 'wb': 24.0, 'db': 28.0, 'humidity': 65, 'description': 'Optimal conditions'},
        {'it_load': 9500, 'wb': 27.0, 'db': 33.0, 'humidity': 85, 'description': 'Hot & humid'},
        {'it_load': 9500, 'wb': 22.0, 'db': 26.0, 'humidity': 60, 'description': 'Cool conditions'}
    ]
    
    for test in test_conditions:
        result = model.predict_cooling_load(
            it_load_kw=test['it_load'],
            wet_bulb_c=test['wb'],
            dry_bulb_c=test['db'],
            humidity_percent=test['humidity']
        )
        
        print(f"\n  {test['description']}:")
        print(f"    Base cooling load: {result['base_cooling_load_kw']} kW")
        print(f"    Total cooling load: {result['total_cooling_load_kw']} kW")
        print(f"    Weather impact: {result['weather_impacts']['total_impact_percent']}%")
        print(f"      - Wet-bulb: {result['weather_impacts']['wet_bulb_impact_percent']}%")
        print(f"      - Dry-bulb: {result['weather_impacts']['dry_bulb_impact_percent']}%")
        print(f"      - Humidity: {result['weather_impacts']['humidity_impact_percent']}%")
    
    # Test 2: Tower approach prediction
    print("\n[TEST 2] Cooling Tower Approach Prediction:")
    
    wet_bulbs = [22.0, 24.0, 26.0, 28.0]
    
    for wb in wet_bulbs:
        approach = model.predict_tower_approach(wet_bulb_c=wb)
        print(f"  Wet-bulb {wb}°C → Approach: {approach}°C")
    
    # Test 3: Economizer potential
    print("\n[TEST 3] Economizer Opportunity Analysis:")
    
    # Forecast: Night-time cooling window
    wb_forecast = [25.0, 24.0, 23.0, 22.0, 21.0, 21.5, 22.5, 24.0]
    
    result = model.calculate_economizer_potential(
        wet_bulb_forecast=wb_forecast,
        current_cooling_load_kw=2800
    )
    
    print(f"  Total opportunities: {result['total_opportunities']} hours")
    print(f"  Potential savings: {result['total_potential_savings_kw']} kW")
    print(f"  Potential cost savings: SGD {result['total_potential_savings_sgd']}")
    print(f"  Recommendation: {result['recommendation']}")
    
    if result['opportunities']:
        print("\n  Opportunity windows:")
        for opp in result['opportunities']:
            print(f"    Hour {opp['hour']}: WB {opp['wet_bulb_c']}°C → {opp['potential_savings_kw']} kW savings")
    
    # Test 4: Correlation analysis (simulated data)
    print("\n[TEST 4] Correlation Analysis:")
    
    # Generate simulated correlated data
    np.random.seed(42)
    n_samples = 100
    
    # Weather data
    wet_bulb = np.random.normal(25.0, 2.0, n_samples)
    dry_bulb = wet_bulb + np.random.normal(5.0, 1.0, n_samples)
    humidity = np.random.normal(75.0, 10.0, n_samples)
    
    # Cooling load (correlated with wet-bulb)
    cooling_load = 2700 + wet_bulb * 50 + np.random.normal(0, 100, n_samples)
    
    weather_df = pd.DataFrame({
        'timestamp': pd.date_range('2025-01-01', periods=n_samples, freq='H'),
        'wet_bulb': wet_bulb,
        'dry_bulb': dry_bulb,
        'humidity': humidity
    })
    
    cooling_df = pd.DataFrame({
        'timestamp': pd.date_range('2025-01-01', periods=n_samples, freq='H'),
        'cooling_load_kw': cooling_load
    })
    
    analysis = model.analyze_correlation(weather_df, cooling_df)
    
    print(f"  Sample size: {analysis['sample_size']}")
    print("\n  Correlations:")
    for key, data in analysis['correlations'].items():
        if 'coefficient' in data:
            print(f"    {key}:")
            print(f"      Coefficient: {data['coefficient']}")
            print(f"      Strength: {data['strength']}")
    
    print("\n All tests complete!")