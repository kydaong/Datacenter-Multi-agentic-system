"""
LSTM Load Forecaster
Predicts IT load for next 1-4 hours using historical patterns

Uses time-series patterns (day of week, hour, historical trends)
Can be replaced with trained LSTM model later
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pickle
from pathlib import Path

class LSTMLoadForecaster:
    """
    IT Load Forecaster using LSTM (or pattern-based fallback)
    
    Predicts IT load (kW) based on:
    - Time of day
    - Day of week
    - Historical patterns
    - Recent trends
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize load forecaster
        
        Args:
            model_path: Path to trained LSTM model (optional)
        """
        
        self.model_path = model_path
        self.trained_model = None
        
        # Try to load trained model
        if model_path and Path(model_path).exists():
            self._load_trained_model()
        
        # Typical load patterns (Singapore datacenter)
        # kW by hour of day
        self.hourly_pattern = {
            0: 9200, 1: 9100, 2: 9050, 3: 9000, 4: 9000, 5: 9100,
            6: 9300, 7: 9500, 8: 9700, 9: 9900, 10: 10100, 11: 10200,
            12: 10000, 13: 9900, 14: 10100, 15: 10200, 16: 10100, 17: 9900,
            18: 9800, 19: 9700, 20: 9600, 21: 9500, 22: 9400, 23: 9300
        }
        
        # Day of week multipliers (1=Monday, 7=Sunday)
        self.dow_multipliers = {
            1: 1.00,  # Monday
            2: 1.00,  # Tuesday
            3: 1.00,  # Wednesday
            4: 1.00,  # Thursday
            5: 0.98,  # Friday
            6: 0.90,  # Saturday
            7: 0.85   # Sunday
        }
        
        print(f" LSTM Load Forecaster initialized")
        print(f"   Mode: {'Trained LSTM' if self.trained_model else 'Pattern-based'}")
    
    def forecast(
        self,
        current_time: datetime,
        horizon_hours: int = 4,
        historical_data: Optional[pd.DataFrame] = None
    ) -> List[Dict]:
        """
        Forecast IT load for next N hours
        
        Args:
            current_time: Current timestamp
            horizon_hours: Forecast horizon (hours)
            historical_data: Recent historical data (optional)
        
        Returns:
            List of forecasts with timestamps and confidence intervals
        """
        
        # Use trained model if available
        if self.trained_model and historical_data is not None:
            return self._forecast_lstm(current_time, horizon_hours, historical_data)
        
        # Otherwise use pattern-based forecast
        return self._forecast_pattern_based(current_time, horizon_hours)
    
    def _forecast_pattern_based(
        self,
        current_time: datetime,
        horizon_hours: int
    ) -> List[Dict]:
        """
        Pattern-based forecast using typical load profiles
        """
        
        forecasts = []
        
        for h in range(1, horizon_hours + 1):
            forecast_time = current_time + timedelta(hours=h)
            
            # Get hour and day of week
            hour = forecast_time.hour
            dow = forecast_time.isoweekday()  # 1-7
            
            # Base load from hourly pattern
            base_load = self.hourly_pattern.get(hour, 9500)
            
            # Apply day of week multiplier
            dow_multiplier = self.dow_multipliers.get(dow, 1.0)
            forecast_load = base_load * dow_multiplier
            
            # Add some random variation (±2%)
            noise = np.random.normal(0, 0.02)
            forecast_load *= (1 + noise)
            
            # Calculate confidence interval (±5% for pattern-based)
            std_dev = forecast_load * 0.05
            
            forecasts.append({
                'timestamp': forecast_time.isoformat(),
                'hours_ahead': h,
                'forecast_load_kw': round(forecast_load, 1),
                'confidence_interval': {
                    'lower': round(forecast_load - 1.96 * std_dev, 1),
                    'upper': round(forecast_load + 1.96 * std_dev, 1)
                },
                'confidence': 'MEDIUM'
            })
        
        return forecasts
    
    def _forecast_lstm(
        self,
        current_time: datetime,
        horizon_hours: int,
        historical_data: pd.DataFrame
    ) -> List[Dict]:
        """
        LSTM-based forecast (when trained model is available)
        """
        
        # Prepare sequence for LSTM
        # (This would be implemented when actual LSTM model is trained)
        
        # For now, fallback to pattern-based
        return self._forecast_pattern_based(current_time, horizon_hours)
    
    def _load_trained_model(self):
        """Load trained LSTM model"""
        
        try:
            with open(self.model_path, 'rb') as f:
                model_data = pickle.load(f)
            
            if isinstance(model_data, dict):
                self.trained_model = model_data.get('model')
            else:
                self.trained_model = model_data
            
            print(f"   Loaded trained LSTM from {self.model_path}")
            
        except Exception as e:
            print(f"   ⚠️  Could not load LSTM model: {e}")
            self.trained_model = None
    
    def save_model(self, model, model_path: str):
        """
        Save trained LSTM model
        
        Args:
            model: Trained LSTM model
            model_path: Save path
        """
        
        model_data = {
            'model': model,
            'metadata': {
                'model_type': 'LSTM',
                'sequence_length': 24,  # 24 hours lookback
                'forecast_horizon': 4,   # 4 hours ahead
                'created_date': str(np.datetime64('today'))
            }
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f" LSTM model saved to {model_path}")
    
    def detect_anomaly(
        self,
        actual_load: float,
        forecast_load: float,
        threshold_percent: float = 15.0
    ) -> Dict:
        """
        Detect if actual load deviates significantly from forecast
        
        Args:
            actual_load: Actual measured load
            forecast_load: Forecasted load
            threshold_percent: Deviation threshold (%)
        
        Returns:
            Anomaly detection result
        """
        
        deviation_percent = abs(actual_load - forecast_load) / forecast_load * 100
        
        is_anomaly = deviation_percent > threshold_percent
        
        return {
            'is_anomaly': is_anomaly,
            'actual_load_kw': actual_load,
            'forecast_load_kw': forecast_load,
            'deviation_percent': round(deviation_percent, 2),
            'threshold_percent': threshold_percent,
            'severity': 'HIGH' if deviation_percent > 20 else 'MEDIUM' if is_anomaly else 'LOW'
        }


# Example usage and testing
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING LSTM LOAD FORECASTER")
    print("="*70)
    
    # Initialize forecaster
    forecaster = LSTMLoadForecaster()
    
    # Test 1: Forecast for next 4 hours
    print("\n[TEST 1] IT Load Forecast (4 hours ahead):")
    current_time = datetime.now()
    
    forecasts = forecaster.forecast(current_time, horizon_hours=4)
    
    for forecast in forecasts:
        print(f"\n  {forecast['hours_ahead']}hr ahead:")
        print(f"    Time: {forecast['timestamp'][:16]}")
        print(f"    Forecast: {forecast['forecast_load_kw']:.1f} kW")
        print(f"    Range: {forecast['confidence_interval']['lower']:.1f} - {forecast['confidence_interval']['upper']:.1f} kW")
    
    # Test 2: Anomaly detection
    print("\n[TEST 2] Anomaly Detection:")
    
    test_cases = [
        {'actual': 9500, 'forecast': 9400, 'description': 'Normal variation'},
        {'actual': 11000, 'forecast': 9500, 'description': 'Significant spike'},
        {'actual': 8000, 'forecast': 9500, 'description': 'Unexpected drop'}
    ]
    
    for test in test_cases:
        result = forecaster.detect_anomaly(
            actual_load=test['actual'],
            forecast_load=test['forecast']
        )
        
        print(f"\n  {test['description']}:")
        print(f"    Actual: {result['actual_load_kw']} kW")
        print(f"    Forecast: {result['forecast_load_kw']} kW")
        print(f"    Deviation: {result['deviation_percent']}%")
        print(f"    Anomaly: {result['is_anomaly']} ({result['severity']})")
    
    # Test 3: Different times of day
    print("\n[TEST 3] Forecast at Different Times:")
    
    test_times = [
        datetime(2025, 1, 15, 9, 0),   # Morning peak
        datetime(2025, 1, 15, 14, 0),  # Afternoon peak
        datetime(2025, 1, 15, 3, 0),   # Night low
        datetime(2025, 1, 18, 14, 0)   # Weekend
    ]
    
    for test_time in test_times:
        forecast = forecaster.forecast(test_time, horizon_hours=1)[0]
        
        print(f"\n  {test_time.strftime('%A %H:%M')}:")
        print(f"    Forecast: {forecast['forecast_load_kw']:.1f} kW")
    
    print("\n All tests complete!")