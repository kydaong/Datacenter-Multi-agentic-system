"""
Chiller Efficiency Model
Predicts chiller kW/ton based on load and operating conditions

Uses analytical model based on manufacturer curves
Can be replaced with trained ML model later
"""

import numpy as np
from typing import Dict, List, Optional
import pickle
from pathlib import Path

class ChillerEfficiencyModel:
    """
    Chiller efficiency prediction model
    
    Predicts kW/ton based on:
    - Load percentage (most important)
    - CHW supply temperature
    - CW entering temperature (wet-bulb dependent)
    - Equipment degradation
    
    Based on Trane Series R RTWD centrifugal chiller curves
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize chiller efficiency model
        
        Args:
            model_path: Path to trained .pkl file (optional)
        """
        
        self.model_path = model_path
        self.trained_model = None
        
        # Try to load trained model
        if model_path and Path(model_path).exists():
            self._load_trained_model()
        
        # Manufacturer efficiency curves (from your chart)
        # Load % → kW/ton at nominal conditions (CHW: 6.7°C, CW: 29°C)
        self.baseline_curve = {
            20: 0.680,
            30: 0.620,
            40: 0.560,
            50: 0.545,
            60: 0.535,  # Optimal zone starts
            70: 0.540,  # Optimal zone
            80: 0.545,
            90: 0.550,
            100: 0.545
        }
        
        print(f" Chiller Efficiency Model initialized")
        print(f"   Mode: {'Trained ML' if self.trained_model else 'Analytical'}")
    
    def predict(
        self,
        load_percent: float,
        chw_supply_temp_c: float = 6.8,
        cw_entering_temp_c: float = 29.0,
        chiller_age_years: float = 6.0
    ) -> float:
        """
        Predict chiller efficiency (kW/ton)
        
        Args:
            load_percent: Chiller load (0-100%)
            chw_supply_temp_c: CHW supply temp (°C)
            cw_entering_temp_c: CW entering temp (°C)
            chiller_age_years: Equipment age (years)
        
        Returns:
            Predicted efficiency (kW/ton)
        """
        
        # Use trained model if available
        if self.trained_model:
            return self._predict_ml(
                load_percent,
                chw_supply_temp_c,
                cw_entering_temp_c,
                chiller_age_years
            )
        
        # Otherwise use analytical model
        return self._predict_analytical(
            load_percent,
            chw_supply_temp_c,
            cw_entering_temp_c,
            chiller_age_years
        )
    
    def _predict_analytical(
        self,
        load_percent: float,
        chw_supply_temp_c: float,
        cw_entering_temp_c: float,
        chiller_age_years: float
    ) -> float:
        """
        Analytical prediction based on manufacturer curves
        """
        
        # Clamp load to valid range
        load_percent = max(20, min(100, load_percent))
        
        # 1. Get baseline efficiency from curve (interpolate)
        baseline_kw_per_ton = self._interpolate_baseline_curve(load_percent)
        
        # 2. CHW temperature penalty
        # Every 1°C increase in CHW supply = ~3% efficiency improvement
        nominal_chw_temp = 6.7
        chw_delta = chw_supply_temp_c - nominal_chw_temp
        chw_penalty_factor = 1.0 - (chw_delta * 0.03)
        
        # 3. CW temperature penalty
        # Every 1°C increase in CW entering = ~2% efficiency penalty
        nominal_cw_temp = 29.0
        cw_delta = cw_entering_temp_c - nominal_cw_temp
        cw_penalty_factor = 1.0 + (cw_delta * 0.02)
        
        # 4. Degradation factor
        # ~0.5% efficiency loss per year of operation
        degradation_factor = 1.0 + (chiller_age_years * 0.005)
        
        # Combined prediction
        predicted_kw_per_ton = (
            baseline_kw_per_ton *
            chw_penalty_factor *
            cw_penalty_factor *
            degradation_factor
        )
        
        # Add measurement noise (±1.5%)
        noise = np.random.normal(0, 0.015)
        predicted_kw_per_ton *= (1.0 + noise)
        
        # Clamp to realistic bounds
        predicted_kw_per_ton = max(0.45, min(0.75, predicted_kw_per_ton))
        
        return round(predicted_kw_per_ton, 3)
    
    def _interpolate_baseline_curve(self, load_percent: float) -> float:
        """
        Interpolate baseline efficiency from manufacturer curve
        """
        
        # Find surrounding points
        loads = sorted(self.baseline_curve.keys())
        
        if load_percent <= loads[0]:
            return self.baseline_curve[loads[0]]
        
        if load_percent >= loads[-1]:
            return self.baseline_curve[loads[-1]]
        
        # Linear interpolation
        for i in range(len(loads) - 1):
            if loads[i] <= load_percent <= loads[i + 1]:
                x0, x1 = loads[i], loads[i + 1]
                y0, y1 = self.baseline_curve[x0], self.baseline_curve[x1]
                
                # Interpolate
                t = (load_percent - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)
        
        return 0.55  # Fallback
    
    def _predict_ml(
        self,
        load_percent: float,
        chw_supply_temp_c: float,
        cw_entering_temp_c: float,
        chiller_age_years: float
    ) -> float:
        """
        Prediction using trained ML model
        """
        
        # Prepare features
        features = np.array([[
            load_percent,
            chw_supply_temp_c,
            cw_entering_temp_c,
            chiller_age_years
        ]])
        
        # Predict
        prediction = self.trained_model.predict(features)[0]
        
        return round(prediction, 3)
    
    def _load_trained_model(self):
        """Load trained ML model from pickle file"""
        
        try:
            with open(self.model_path, 'rb') as f:
                model_data = pickle.load(f)
            
            if isinstance(model_data, dict):
                self.trained_model = model_data.get('model')
            else:
                self.trained_model = model_data
            
            print(f"   Loaded trained model from {self.model_path}")
            
        except Exception as e:
            print(f"   ⚠️  Could not load trained model: {e}")
            self.trained_model = None
    
    def save_model(self, model, model_path: str):
        """
        Save trained model to pickle file
        
        Args:
            model: Trained model object
            model_path: Save path
        """
        
        model_data = {
            'model': model,
            'feature_names': [
                'load_percent',
                'chw_supply_temp_c',
                'cw_entering_temp_c',
                'chiller_age_years'
            ],
            'metadata': {
                'model_type': type(model).__name__,
                'created_date': str(np.datetime64('today'))
            }
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"✅ Model saved to {model_path}")
    
    def get_optimal_load_range(self) -> tuple:
        """
        Get optimal load range for this chiller
        
        Returns:
            (min_load, max_load) percentages
        """
        
        # Find minimum kW/ton in baseline curve
        optimal_load = min(self.baseline_curve, key=self.baseline_curve.get)
        
        # Optimal range is ±10% of optimal point
        return (optimal_load - 10, optimal_load + 10)
    
    def predict_batch(
        self,
        load_percents: List[float],
        chw_supply_temp_c: float = 6.8,
        cw_entering_temp_c: float = 29.0,
        chiller_age_years: float = 6.0
    ) -> List[float]:
        """
        Predict efficiency for multiple load points
        
        Args:
            load_percents: List of load percentages
            chw_supply_temp_c: CHW supply temp
            cw_entering_temp_c: CW entering temp
            chiller_age_years: Equipment age
        
        Returns:
            List of predicted kW/ton values
        """
        
        predictions = []
        
        for load in load_percents:
            kw_per_ton = self.predict(
                load,
                chw_supply_temp_c,
                cw_entering_temp_c,
                chiller_age_years
            )
            predictions.append(kw_per_ton)
        
        return predictions


# Example usage and testing
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING CHILLER EFFICIENCY MODEL")
    print("="*70)
    
    # Initialize model
    model = ChillerEfficiencyModel()
    
    # Test 1: Predict at different loads
    print("\n[TEST 1] Efficiency at Different Loads:")
    loads = [30, 50, 70, 90, 100]
    
    for load in loads:
        kw_per_ton = model.predict(load_percent=load)
        print(f"  {load:3d}% load: {kw_per_ton:.3f} kW/ton")
    
    # Test 2: Impact of CHW temperature
    print("\n[TEST 2] Impact of CHW Supply Temperature:")
    chw_temps = [6.0, 6.5, 7.0, 7.5]
    
    for temp in chw_temps:
        kw_per_ton = model.predict(
            load_percent=70,
            chw_supply_temp_c=temp
        )
        print(f"  CHW {temp:.1f}°C: {kw_per_ton:.3f} kW/ton")
    
    # Test 3: Impact of CW temperature (wet-bulb)
    print("\n[TEST 3] Impact of CW Entering Temperature:")
    cw_temps = [27.0, 29.0, 31.0, 33.0]
    
    for temp in cw_temps:
        kw_per_ton = model.predict(
            load_percent=70,
            cw_entering_temp_c=temp
        )
        print(f"  CW {temp:.1f}°C: {kw_per_ton:.3f} kW/ton")
    
    # Test 4: Equipment degradation
    print("\n[TEST 4] Equipment Degradation Over Time:")
    ages = [0, 3, 6, 9, 12]
    
    for age in ages:
        kw_per_ton = model.predict(
            load_percent=70,
            chiller_age_years=age
        )
        print(f"  Age {age:2d} years: {kw_per_ton:.3f} kW/ton")
    
    # Test 5: Optimal load range
    print("\n[TEST 5] Optimal Load Range:")
    optimal_range = model.get_optimal_load_range()
    print(f"  Optimal range: {optimal_range[0]}-{optimal_range[1]}%")
    
    # Test 6: Batch prediction
    print("\n[TEST 6] Batch Prediction:")
    load_range = list(range(20, 101, 10))
    predictions = model.predict_batch(load_range)
    
    print("  Load% | kW/ton")
    print("  ------|-------")
    for load, kw_per_ton in zip(load_range, predictions):
        print(f"  {load:5d} | {kw_per_ton:.3f}")
    
    print("\n All tests complete!")