"""
Equipment performance models based on manufacturer data
"""

import numpy as np
from scipy.interpolate import interp1d

class ChillerEfficiencyModel:
    """
    Trane Series R RTWD centrifugal chiller efficiency model
    Based on manufacturer performance curves
    """
    
    def __init__(self, chiller_id, rated_tons=400):
        self.chiller_id = chiller_id
        self.rated_tons = rated_tons
        
        # Manufacturer efficiency curves at different CHW temperatures
        # X-axis: Chiller load as % of rated capacity (normalized to 7 points: 20%-120%)
        # Y-axis: Efficiency (kW/RT)
        # Scaled by rated_tons so the optimal efficiency point tracks chiller size
        scale = rated_tons / 400.0
        self.load_points_rt = np.array([200, 250, 300, 350, 400, 450, 500]) * scale

        # At 44°F (6.7°C) CHW - RED LINE
        self.efficiency_44F = np.array([0.620, 0.580, 0.560, 0.545, 0.535, 0.540, 0.545])

        # At 45°F (7.2°C) CHW - MAGENTA LINE
        self.efficiency_45F = np.array([0.615, 0.575, 0.555, 0.540, 0.530, 0.525, 0.535])

        # At 46°F (7.8°C) CHW - BLUE LINE
        self.efficiency_46F = np.array([0.610, 0.570, 0.550, 0.535, 0.525, 0.520, 0.530])

        # At 47°F (8.3°C) CHW - PURPLE LINE
        self.efficiency_47F = np.array([0.605, 0.565, 0.545, 0.530, 0.520, 0.515, 0.525])
        
        # Create interpolation functions
        self.interp_44F = interp1d(self.load_points_rt, self.efficiency_44F, 
                                   kind='cubic', fill_value='extrapolate')
        self.interp_45F = interp1d(self.load_points_rt, self.efficiency_45F, 
                                   kind='cubic', fill_value='extrapolate')
        self.interp_46F = interp1d(self.load_points_rt, self.efficiency_46F, 
                                   kind='cubic', fill_value='extrapolate')
        self.interp_47F = interp1d(self.load_points_rt, self.efficiency_47F, 
                                   kind='cubic', fill_value='extrapolate')
    
    def get_efficiency(self, load_tons, chw_supply_temp_c, cw_entering_temp_c=None):
        """
        Get chiller efficiency (kW/RT) based on load and temperatures
        
        Args:
            load_tons: Actual cooling load in refrigeration tons 
        
            chw_supply_temp_c: CHW supply temperature in °C
            cw_entering_temp_c: Condenser water entering temp (optional penalty)
        
        Returns:
            kw_per_ton: Power consumption per ton of cooling
        """
        
        # Ensure load is within reasonable bounds
        load_tons = max(self.rated_tons * 0.2, min(self.rated_tons * 1.25, load_tons))
        
        # Convert CHW temp to Fahrenheit for curve lookup
        chw_temp_f = (chw_supply_temp_c * 9/5) + 32
        
        # Interpolate between temperature curves
        if chw_temp_f <= 44:
            kw_per_ton = float(self.interp_44F(load_tons))
            
        elif chw_temp_f <= 45:
            # Interpolate between 44F and 45F
            weight = chw_temp_f - 44
            kw_44 = float(self.interp_44F(load_tons))
            kw_45 = float(self.interp_45F(load_tons))
            kw_per_ton = kw_44 + weight * (kw_45 - kw_44)
            
        elif chw_temp_f <= 46:
            # Interpolate between 45F and 46F
            weight = chw_temp_f - 45
            kw_45 = float(self.interp_45F(load_tons))
            kw_46 = float(self.interp_46F(load_tons))
            kw_per_ton = kw_45 + weight * (kw_46 - kw_45)
            
        elif chw_temp_f <= 47:
            # Interpolate between 46F and 47F
            weight = chw_temp_f - 46
            kw_46 = float(self.interp_46F(load_tons))
            kw_47 = float(self.interp_47F(load_tons))
            kw_per_ton = kw_46 + weight * (kw_47 - kw_46)
            
        else:
            # Above 47F, use 47F curve with penalty
            kw_per_ton = float(self.interp_47F(load_tons))
            # Add 1% penalty per degree F above 47
            penalty = (chw_temp_f - 47) * 0.01
            kw_per_ton *= (1 + penalty)
        
        # Condenser water temperature penalty (if provided)
        if cw_entering_temp_c is not None:
            # For every 1°C above 29°C (84°F), add 2% penalty
            if cw_entering_temp_c > 29:
                cw_penalty = (cw_entering_temp_c - 29) * 0.02
                kw_per_ton *= (1 + cw_penalty)
        
        # Add realistic measurement noise (±1.5%)
        noise = np.random.normal(0, 0.015)
        kw_per_ton *= (1 + noise)
        
        # Physical bounds
        kw_per_ton = max(0.45, min(0.70, kw_per_ton))
        
        return kw_per_ton
    
    def get_optimal_load(self, chw_supply_temp_c=6.7):
        """
        Find the optimal load point (highest efficiency)
        
        Returns:
            optimal_load_tons: Load in tons where efficiency is best
            optimal_kw_per_ton: Best efficiency achievable
        """
        
        # From the curves, optimal point is around 400-450 RT
        test_loads = np.linspace(200, 500, 100)
        efficiencies = [self.get_efficiency(load, chw_supply_temp_c) 
                       for load in test_loads]
        
        optimal_idx = np.argmin(efficiencies)
        optimal_load = test_loads[optimal_idx]
        optimal_efficiency = efficiencies[optimal_idx]
        
        return optimal_load, optimal_efficiency


class PumpAffinityLaws:
    """
    Pump performance based on affinity laws
    """
    
    @staticmethod
    def calculate_power(rated_power_kw, speed_percent):
        """
        Power varies with cube of speed
        P2 = P1 * (N2/N1)^3
        """
        speed_ratio = speed_percent / 100
        return rated_power_kw * (speed_ratio ** 3)
    
    @staticmethod
    def calculate_flow(rated_flow, speed_percent):
        """
        Flow varies linearly with speed
        Q2 = Q1 * (N2/N1)
        """
        speed_ratio = speed_percent / 100
        return rated_flow * speed_ratio
    
    @staticmethod
    def calculate_head(rated_head, speed_percent):
        """
        Head varies with square of speed
        H2 = H1 * (N2/N1)^2
        """
        speed_ratio = speed_percent / 100
        return rated_head * (speed_ratio ** 2)


class CoolingTowerModel:
    """
    Cooling tower performance model
    """
    
    @staticmethod
    def calculate_approach(fan_speed_percent, water_loading_gpm_per_sqft, wet_bulb_temp_c):
        """
        Calculate tower approach temperature
        
        Approach = Leaving Water Temp - Wet Bulb Temp
        
        Factors:
        - Fan speed: Higher speed → Better heat transfer → Lower approach
        - Water loading: Higher flow → More load → Higher approach
        - Wet-bulb: Higher WB → Harder to cool → Higher approach
        """
        
        # Base approach at design conditions (3.5°C)
        base_approach = 3.5
        
        # Fan speed effect (lower speed = higher approach)
        fan_penalty = (100 - fan_speed_percent) * 0.025
        
        # Water loading effect (design = 3 GPM/sqft)
        loading_penalty = max(0, (water_loading_gpm_per_sqft - 3.0) * 0.4)
        
        approach = base_approach + fan_penalty + loading_penalty
        
        # Add noise
        approach += np.random.normal(0, 0.2)
        
        return max(2.5, min(6.5, approach))
    
    @staticmethod
    def calculate_effectiveness(range_temp, entering_water_temp, wet_bulb_temp):
        """
        Tower effectiveness = Range / (Entering - Wet Bulb) * 100%
        
        Typical: 60-75%
        """
        effectiveness = (range_temp / (entering_water_temp - wet_bulb_temp)) * 100
        return max(50, min(85, effectiveness))