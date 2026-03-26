"""
Test All Models Together
Comprehensive testing of all prediction models
"""

from chiller_efficiency_model import ChillerEfficiencyModel
from lstm_load_forecaster import LSTMLoadForecaster
from weather_load_correlation import WeatherLoadCorrelation
from datetime import datetime
import pandas as pd
import numpy as np

def test_all_models():
    """
    Test complete prediction workflow:
    IT Load → Weather → Cooling Load → Chiller Efficiency
    """
    
    print("="*70)
    print("COMPREHENSIVE MODELS TESTING")
    print("="*70)
    
    # Initialize all models
    print("\n[SETUP] Initializing models...")
    chiller_model = ChillerEfficiencyModel()
    load_forecaster = LSTMLoadForecaster()
    weather_model = WeatherLoadCorrelation()
    print("✅ All models initialized")
    
    # ====================
    # SCENARIO: Morning Peak Prediction
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO: Morning Peak Hour Prediction Workflow")
    print("="*70)
    
    current_time = datetime(2025, 1, 15, 9, 0)  # 9 AM Wednesday
    
    # Step 1: Forecast IT load
    print("\n[STEP 1] Forecasting IT Load...")
    it_forecasts = load_forecaster.forecast(current_time, horizon_hours=4)
    
    print("  IT Load Forecast:")
    for f in it_forecasts:
        print(f"    {f['hours_ahead']}hr: {f['forecast_load_kw']:.0f} kW")
    
    # Step 2: Predict cooling load from weather
    print("\n[STEP 2] Predicting Cooling Load from Weather...")
    
    # Simulated weather conditions
    weather_conditions = {
        'wet_bulb': 25.5,
        'dry_bulb': 31.0,
        'humidity': 78.0
    }
    
    cooling_prediction = weather_model.predict_cooling_load(
        it_load_kw=it_forecasts[0]['forecast_load_kw'],
        wet_bulb_c=weather_conditions['wet_bulb'],
        dry_bulb_c=weather_conditions['dry_bulb'],
        humidity_percent=weather_conditions['humidity']
    )
    
    print(f"  Weather: WB={weather_conditions['wet_bulb']}°C, DB={weather_conditions['dry_bulb']}°C, RH={weather_conditions['humidity']}%")
    print(f"  Base cooling load: {cooling_prediction['base_cooling_load_kw']} kW")
    print(f"  Weather-adjusted: {cooling_prediction['total_cooling_load_kw']} kW ({cooling_prediction['cooling_load_tons']:.0f} tons)")
    print(f"  Weather impact: {cooling_prediction['weather_impacts']['total_impact_percent']}%")
    
    # Step 3: Predict chiller efficiency
    print("\n[STEP 3] Predicting Chiller Efficiency...")
    
    # Assume 2 chillers running (each at ~50% load)
    chiller_load_percent = (cooling_prediction['cooling_load_tons'] / 2) / 400 * 100
    
    tower_approach = weather_model.predict_tower_approach(weather_conditions['wet_bulb'])
    cw_temp = weather_conditions['wet_bulb'] + tower_approach + 5.5  # Approach + range
    
    kw_per_ton = chiller_model.predict(
        load_percent=chiller_load_percent,
        chw_supply_temp_c=6.8,
        cw_entering_temp_c=cw_temp,
        chiller_age_years=6.0
    )
    
    print(f"  Chiller load: {chiller_load_percent:.1f}%")
    print(f"  CW entering temp: {cw_temp:.1f}°C")
    print(f"  Predicted efficiency: {kw_per_ton:.3f} kW/ton")
    
    # Step 4: Calculate total plant power
    print("\n[STEP 4] Calculating Total Plant Power...")
    
    chiller_power = cooling_prediction['cooling_load_tons'] * kw_per_ton
    pump_power = cooling_prediction['total_cooling_load_kw'] * 0.05  # ~5% for pumps
    tower_power = 30 * 2  # 2 towers, 30 kW each
    
    total_cooling_power = chiller_power + pump_power + tower_power
    
    print(f"  Chiller power: {chiller_power:.1f} kW")
    print(f"  Pump power: {pump_power:.1f} kW")
    print(f"  Tower power: {tower_power:.1f} kW")
    print(f"  Total cooling system: {total_cooling_power:.1f} kW")
    
    # Calculate PUE
    it_load = it_forecasts[0]['forecast_load_kw']
    total_facility = it_load + total_cooling_power + 100  # +100 for misc
    pue = total_facility / it_load
    
    print(f"\n  IT Load: {it_load:.1f} kW")
    print(f"  Total Facility: {total_facility:.1f} kW")
    print(f"  PUE: {pue:.3f}")
    
    # ====================
    # SCENARIO: Economizer Opportunity
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO: Economizer Opportunity Detection")
    print("="*70)
    
    # Simulated night-time weather forecast (cooling window)
    wb_forecast = [25.5, 24.8, 23.5, 22.8, 21.5, 21.0, 21.5, 22.8]
    
    eco_result = weather_model.calculate_economizer_potential(
        wet_bulb_forecast=wb_forecast,
        current_cooling_load_kw=cooling_prediction['total_cooling_load_kw']
    )
    
    print(f"  Forecast window: 8 hours (21:00 - 05:00)")
    print(f"  Economizer opportunities: {eco_result['total_opportunities']} hours")
    print(f"  Potential savings: {eco_result['total_potential_savings_kw']} kW")
    print(f"  Potential cost savings: SGD {eco_result['total_potential_savings_sgd']}")
    print(f"  Recommendation: {eco_result['recommendation']}")
    
    # ====================
    # SCENARIO: Optimal Chiller Staging
    # ====================
    
    print("\n" + "="*70)
    print("SCENARIO: Optimal Chiller Staging Analysis")
    print("="*70)
    
    total_load_tons = cooling_prediction['cooling_load_tons']
    
    # Test different staging configurations
    staging_configs = [
        {'chillers': 1, 'load_each': total_load_tons, 'capacity': 400},
        {'chillers': 2, 'load_each': total_load_tons / 2, 'capacity': 400},
        {'chillers': 3, 'load_each': total_load_tons / 3, 'capacity': [400, 400, 300]}
    ]
    
    print(f"  Total cooling load: {total_load_tons:.0f} tons\n")
    
    for config in staging_configs:
        n_chillers = config['chillers']
        
        if n_chillers == 3:
            # 2 large + 1 small
            load_1 = total_load_tons * 0.4
            load_2 = total_load_tons * 0.4
            load_3 = total_load_tons * 0.2
            
            eff_1 = chiller_model.predict(load_1 / 400 * 100, cw_entering_temp_c=cw_temp)
            eff_2 = chiller_model.predict(load_2 / 400 * 100, cw_entering_temp_c=cw_temp)
            eff_3 = chiller_model.predict(load_3 / 300 * 100, cw_entering_temp_c=cw_temp)
            
            total_power = load_1 * eff_1 + load_2 * eff_2 + load_3 * eff_3
            avg_eff = total_power / total_load_tons
        
        else:
            load_each = config['load_each']
            load_pct = (load_each / config['capacity']) * 100
            
            eff = chiller_model.predict(load_pct, cw_entering_temp_c=cw_temp)
            total_power = load_each * eff * n_chillers
            avg_eff = eff
        
        print(f"  Config: {n_chillers} chiller(s)")
        print(f"    Load per chiller: {load_each:.0f} tons ({load_pct:.1f}%)" if n_chillers <= 2 else f"    Loads: {load_1:.0f}, {load_2:.0f}, {load_3:.0f} tons")
        print(f"    Average efficiency: {avg_eff:.3f} kW/ton")
        print(f"    Total power: {total_power:.1f} kW\n")
    
    # ====================
    # FINAL SUMMARY
    # ====================
    
    print("="*70)
    print("FINAL SUMMARY")
    print("="*70)
    
    print(f"\nPrediction Accuracy:")
    print(f"     IT Load Forecaster: Pattern-based (MEDIUM confidence)")
    print(f"     Weather-Load Correlation: Empirical (HIGH confidence)")
    print(f"     Chiller Efficiency Model: Manufacturer curves (HIGH confidence)")
    
    print(f"\nKey Insights:")
    print(f"  • Current PUE: {pue:.3f}")
    print(f"  • Chiller efficiency: {kw_per_ton:.3f} kW/ton")
    print(f"  • Weather impact on load: {cooling_prediction['weather_impacts']['total_impact_percent']:+.1f}%")
    print(f"  • Economizer opportunities: {eco_result['total_opportunities']} hours tonight")
    
    print("\n" + "="*70)
    print(" ALL MODELS TESTED SUCCESSFULLY!")
    print("="*70)

if __name__ == "__main__":
    test_all_models()