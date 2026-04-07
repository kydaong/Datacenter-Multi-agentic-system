# Dont run this. this is old version
"""
FINAL DATA GENERATION - Matches Live Data with Manufacturer Efficiency Curves
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
from dotenv import load_dotenv
import os
from tqdm import tqdm
import sys

# Import equipment models
from equipment_models import ChillerEfficiencyModel, PumpAffinityLaws, CoolingTowerModel

load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

START_DATE = "2025-10-01 00:00:00"
END_DATE = "2025-10-07 23:59:00"
INTERVAL_MINUTES = 1

# Chiller specifications (2 large + 1 small)
CHILLERS = {
    'Chiller-1': {
        'rated_tons': 400,
        'rated_kw': 1406,
        'type': 'large',
        'model': ChillerEfficiencyModel('Chiller-1', 400)
    },
    'Chiller-2': {
        'rated_tons': 400,
        'rated_kw': 1406,
        'type': 'large',
        'model': ChillerEfficiencyModel('Chiller-2', 400)
    },
    'Chiller-3': {
        'rated_tons': 300,
        'rated_kw': 1055,
        'type': 'small',
        'model': ChillerEfficiencyModel('Chiller-3', 300)
    }
}

# Pump specifications
PUMPS = {
    'PCHWP': {'rated_power_kw': 55, 'rated_flow_lps': 150},
    'SCHWP': {'rated_power_kw': 45, 'rated_flow_lps': 120},
    'CWP': {'rated_power_kw': 38, 'rated_flow_lps': 180}
}

# Tower specifications
TOWER_AREA_SQFT = 1600  # Per tower

# ============================================
# COOLING LOAD PATTERN
# ============================================

def get_cooling_load_for_time(timestamp):
    """
    Realistic datacenter cooling load pattern
    
    Range: 2700-2900 kW (matches your live data)
    """
    
    hour = timestamp.hour
    minute = timestamp.minute
    day_of_week = timestamp.dayofweek
    
    if day_of_week < 5:  # Weekday
        if 0 <= hour < 6:
            base_cooling_kw = 2720
        elif 6 <= hour < 9:
            progress = (hour - 6) + (minute / 60)
            base_cooling_kw = 2720 + (160 * (progress / 3))
        elif 9 <= hour < 12:
            # PEAK (matches your 9:00-9:30 sample)
            base_cooling_kw = 2760 + np.random.uniform(0, 100)
        elif 12 <= hour < 14:
            base_cooling_kw = 2780
        elif 14 <= hour < 18:
            base_cooling_kw = 2850
        elif 18 <= hour < 22:
            progress = (hour - 18) + (minute / 60)
            base_cooling_kw = 2850 - (120 * (progress / 4))
        else:
            base_cooling_kw = 2730
    else:  # Weekend
        base_cooling_kw = 2600
    
    # Minute-to-minute variation
    variation = np.random.normal(0, 20)
    
    # Periodic bursts (batch jobs every 15 min)
    if minute % 15 == 0:
        variation += np.random.uniform(0, 30)
    
    cooling_load_kw = base_cooling_kw + variation
    
    # Match your data range
    return max(2700, min(2900, cooling_load_kw))

# ============================================
# WEATHER SIMULATION
# ============================================

def get_weather_for_time(timestamp):
    """
    Singapore tropical weather with daily cycle
    """
    
    hour = timestamp.hour
    month = timestamp.month
    
    # Daily cycle (peak at 3 PM)
    hour_angle = ((hour - 15) / 24) * 2 * np.pi
    daily_cycle = 2.5 * np.sin(hour_angle)
    
    dry_bulb = 30.5 + daily_cycle + np.random.normal(0, 1.5)
    wet_bulb = 25.0 + daily_cycle * 0.6 + np.random.normal(0, 1.0)
    humidity = 75 + np.random.normal(0, 8)
    
    # Monsoon adjustment
    if month in [11, 12, 1, 2, 3]:
        wet_bulb += 0.8
        humidity += 5
    
    # Clamp
    dry_bulb = max(28, min(34, dry_bulb))
    wet_bulb = max(23, min(27, wet_bulb))
    humidity = max(60, min(90, humidity))
    
    if wet_bulb >= dry_bulb:
        wet_bulb = dry_bulb - 1.5
    
    return {
        'dry_bulb': dry_bulb,
        'wet_bulb': wet_bulb,
        'humidity': humidity
    }

# ============================================
# CHILLER STAGING
# ============================================

def determine_active_chillers(cooling_load_kw, wet_bulb_temp):
    """
    Determine which chillers to run
    
    Strategy:
    - Prefer running at optimal efficiency point (400-450 RT per chiller)
    - Consider N+1 redundancy
    - Account for ambient conditions
    """
    
    cooling_load_tons = cooling_load_kw / 3.517
    
    # Base staging
    if cooling_load_tons < 350:
        active = ['Chiller-1']
    elif cooling_load_tons < 700:
        # Most common - matches your data
        active = ['Chiller-1', 'Chiller-2']
    elif cooling_load_tons < 900:
        active = ['Chiller-1', 'Chiller-2', 'Chiller-3']
    else:
        active = ['Chiller-1', 'Chiller-2', 'Chiller-3']
    
    # High ambient adjustment
    if wet_bulb_temp > 26 and len(active) < 3:
        load_per_chiller_tons = cooling_load_tons / len(active)
        if load_per_chiller_tons > 380:  # Near capacity
            if len(active) == 1:
                active.append('Chiller-2')
            elif len(active) == 2:
                active.append('Chiller-3')
    
    return active

# ============================================
# GENERATE CHILLER RECORD
# ============================================

def generate_chiller_record(timestamp, chiller_id, cooling_load_share_kw, wet_bulb_temp):
    """
    Generate single chiller operating record using manufacturer efficiency curves
    """
    
    chiller_spec = CHILLERS[chiller_id]
    chiller_model = chiller_spec['model']
    
    # Convert to tons
    cooling_load_tons = cooling_load_share_kw / 3.517
    
    # CHW temperatures (your data: supply ~6.7°C, return ~12.1°C)
    chw_supply_temp = 6.7 + np.random.normal(0, 0.1)
    chw_return_temp = 12.1 + np.random.normal(0, 0.15)
    chw_delta_t = chw_return_temp - chw_supply_temp
    
    # Tower approach
    tower_approach = 3.5 + np.random.normal(0, 0.3)
    cw_supply_temp = wet_bulb_temp + tower_approach
    
    # Get efficiency from manufacturer curve
    kw_per_ton = chiller_model.get_efficiency(
        load_tons=cooling_load_tons,
        chw_supply_temp_c=chw_supply_temp,
        cw_entering_temp_c=cw_supply_temp
    )
    
    # Chiller power
    chiller_power_kw = cooling_load_tons * kw_per_ton
    
    # CHW flow (Q = m·Cp·ΔT)
    cp_water = 4.18
    chw_flow_lps = cooling_load_share_kw / (cp_water * chw_delta_t)
    
    # Heat rejection
    heat_in_kw = cooling_load_share_kw
    heat_rejected_kw = heat_in_kw + chiller_power_kw
    
    # CW flow & temps
    cw_flow_lps = chw_flow_lps * 1.2
    cw_delta_t = heat_rejected_kw / (cw_flow_lps * cp_water)
    cw_return_temp = cw_supply_temp + cw_delta_t
    
    # Energy balance
    unbalanced_heat_pct = ((heat_rejected_kw - (heat_in_kw + chiller_power_kw)) / heat_rejected_kw) * 100
    unbalanced_heat_pct += np.random.normal(0, 2)
    
    # COP
    cop = cooling_load_share_kw / chiller_power_kw if chiller_power_kw > 0 else 0
    
    # Load percentage
    load_percent = (cooling_load_tons / chiller_spec['rated_tons']) * 100
    
    return {
        'Timestamp': timestamp,
        'ChillerID': chiller_id,
        'CHWFHeaderFlowLPS': round(chw_flow_lps, 0),
        'CHWSTHeaderTempC': round(chw_supply_temp, 1),
        'CHWRTHeaderTempC': round(chw_return_temp, 1),
        'CDWFHeaderFlowLPS': round(cw_flow_lps, 0),
        'CWSTHeaderTempC': round(cw_supply_temp, 1),
        'CWRTHeaderTempC': round(cw_return_temp, 1),
        'ChillerPowerKW': round(chiller_power_kw, 0),
        'CoolingLoadKW': round(cooling_load_share_kw, 0),
        'HeatInKW': round(heat_in_kw, 0),
        'HeatRejectedKW': round(heat_rejected_kw, 0),
        'PercentUnbalancedHeat': round(unbalanced_heat_pct, 2),
        'COP': round(cop, 3),
        'KWPerTon': round(kw_per_ton, 3),
        'LoadPercentage': round(load_percent, 2),
        'RunningStatus': 'ON',
        'LoadCondition': 'PART_LOAD_HIGH' if load_percent > 70 else 'PART_LOAD_MEDIUM'
    }

# ============================================
# MAIN GENERATION
# ============================================

def generate_all_data():
    """
    Generate 1 week of per-minute data
    """
    
    timestamps = pd.date_range(
        start=START_DATE,
        end=END_DATE,
        freq=f'{INTERVAL_MINUTES}min'
    )
    
    print("="*70)
    print("CHILLER DATA GENERATION - FINAL VERSION")
    print("="*70)
    print(f"Records: {len(timestamps):,}")
    print(f"Duration: {len(timestamps) / 60 / 24:.1f} days")
    print(f"Interval: {INTERVAL_MINUTES} minute(s)")
    print("="*70)
    
    all_records = []
    
    for ts in tqdm(timestamps, desc="Generating per-minute data"):
        
        # Get cooling load
        cooling_load_kw = get_cooling_load_for_time(ts)
        
        # Get weather
        weather = get_weather_for_time(ts)
        wet_bulb_temp = weather['wet_bulb']
        
        # Determine active chillers
        active_chillers = determine_active_chillers(cooling_load_kw, wet_bulb_temp)
        num_active = len(active_chillers)
        
        # Distribute load
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        # Generate records
        for chiller_id in active_chillers:
            record = generate_chiller_record(
                ts, chiller_id, cooling_load_per_chiller, wet_bulb_temp
            )
            all_records.append(record)
    
    df = pd.DataFrame(all_records)
    
    print(f"\n✅ Generated {len(df):,} records")
    
    return df

# ============================================
# SAVE & INSERT
# ============================================

if __name__ == "__main__":
    
    # Generate
    df = generate_all_data()
    
    # Save CSV
    print("\nSaving to CSV...")
    os.makedirs('data/raw', exist_ok=True)
    df.to_csv('data/raw/chiller_operating_points_final.csv', index=False)
    print("✅ Saved: data/raw/chiller_operating_points_final.csv")
    
    # Display sample
    print("\n" + "="*70)
    print("SAMPLE DATA (9:00-9:10 AM):")
    print("="*70)
    sample = df[(df['Timestamp'] >= '2025-10-01 09:00:00') & 
                (df['Timestamp'] <= '2025-10-01 09:10:00')]
    
    print(sample[['Timestamp', 'ChillerID', 'CHWFHeaderFlowLPS', 'CHWSTHeaderTempC', 
                  'CHWRTHeaderTempC', 'CWSTHeaderTempC', 'CWRTHeaderTempC',
                  'ChillerPowerKW', 'CoolingLoadKW', 'KWPerTon']].to_string(index=False))
    
    print("\n" + "="*70)
    print("✅ GENERATION COMPLETE!")
    print("="*70)