"""
COMPLETE DATA GENERATION FOR ALL TABLES
Generates data for entire chiller plant system
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
from dotenv import load_dotenv
import os
from tqdm import tqdm
import json

# Import equipment models
from equipment_models import ChillerEfficiencyModel, PumpAffinityLaws, CoolingTowerModel

load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

START_DATE = "2025-10-01 00:00:00"
END_DATE = "2025-10-07 23:59:00"
INTERVAL_MINUTES = 1

# Equipment specs
CHILLERS = {
    'Chiller-1': {'rated_tons': 400, 'rated_kw': 1406, 'type': 'large'},
    'Chiller-2': {'rated_tons': 400, 'rated_kw': 1406, 'type': 'large'},
    'Chiller-3': {'rated_tons': 300, 'rated_kw': 1055, 'type': 'small'}
}

PUMPS = {
    'PCHWP-1': {'type': 'PCHWP', 'rated_power_kw': 55, 'rated_flow_lps': 150},
    'PCHWP-2': {'type': 'PCHWP', 'rated_power_kw': 55, 'rated_flow_lps': 150},
    'PCHWP-3': {'type': 'PCHWP', 'rated_power_kw': 55, 'rated_flow_lps': 150},
    'SCHWP-1': {'type': 'SCHWP', 'rated_power_kw': 45, 'rated_flow_lps': 120},
    'SCHWP-2': {'type': 'SCHWP', 'rated_power_kw': 45, 'rated_flow_lps': 120},
    'SCHWP-3': {'type': 'SCHWP', 'rated_power_kw': 45, 'rated_flow_lps': 120},
    'CWP-1': {'type': 'CWP', 'rated_power_kw': 38, 'rated_flow_lps': 180},
    'CWP-2': {'type': 'CWP', 'rated_power_kw': 38, 'rated_flow_lps': 180},
    'CWP-3': {'type': 'CWP', 'rated_power_kw': 38, 'rated_flow_lps': 180}
}

TOWERS = {
    'CT-1': {'capacity_tons': 450, 'fan_count': 2, 'fan_power_kw_each': 15},
    'CT-2': {'capacity_tons': 450, 'fan_count': 2, 'fan_power_kw_each': 15},
    'CT-3': {'capacity_tons': 450, 'fan_count': 2, 'fan_power_kw_each': 15}
}

# Output directory
OUTPUT_DIR = 'data/raw'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================
# HELPER FUNCTIONS (from previous script)
# ============================================

def get_cooling_load_for_time(timestamp):
    """Realistic datacenter cooling load pattern"""
    hour = timestamp.hour
    minute = timestamp.minute
    day_of_week = timestamp.dayofweek
    
    if day_of_week < 5:
        if 0 <= hour < 6:
            base_cooling_kw = 2720
        elif 6 <= hour < 9:
            progress = (hour - 6) + (minute / 60)
            base_cooling_kw = 2720 + (160 * (progress / 3))
        elif 9 <= hour < 12:
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
    else:
        base_cooling_kw = 2600
    
    variation = np.random.normal(0, 20)
    if minute % 15 == 0:
        variation += np.random.uniform(0, 30)
    
    return max(2700, min(2900, base_cooling_kw + variation))

def get_weather_for_time(timestamp):
    """Singapore tropical weather"""
    hour = timestamp.hour
    month = timestamp.month
    
    hour_angle = ((hour - 15) / 24) * 2 * np.pi
    daily_cycle = 2.5 * np.sin(hour_angle)
    
    dry_bulb = 30.5 + daily_cycle + np.random.normal(0, 1.5)
    wet_bulb = 25.0 + daily_cycle * 0.6 + np.random.normal(0, 1.0)
    humidity = 75 + np.random.normal(0, 8)
    
    if month in [11, 12, 1, 2, 3]:
        wet_bulb += 0.8
        humidity += 5
    
    dry_bulb = max(28, min(34, dry_bulb))
    wet_bulb = max(23, min(27, wet_bulb))
    humidity = max(60, min(90, humidity))
    
    if wet_bulb >= dry_bulb:
        wet_bulb = dry_bulb - 1.5
    
    return {
        'dry_bulb': dry_bulb,
        'wet_bulb': wet_bulb,
        'humidity': humidity,
        'dew_point': wet_bulb - 2,
        'pressure': 1013 + np.random.normal(0, 3),
        'wind_speed': max(0, np.random.normal(2, 1)),
        'rainfall': 0 if np.random.random() > 0.15 else np.random.exponential(2)
    }

def determine_active_chillers(cooling_load_kw, wet_bulb_temp):
    """Determine which chillers to run"""
    cooling_load_tons = cooling_load_kw / 3.517
    
    if cooling_load_tons < 350:
        active = ['Chiller-1']
    elif cooling_load_tons < 700:
        active = ['Chiller-1', 'Chiller-2']
    elif cooling_load_tons < 900:
        active = ['Chiller-1', 'Chiller-2', 'Chiller-3']
    else:
        active = ['Chiller-1', 'Chiller-2', 'Chiller-3']
    
    if wet_bulb_temp > 26 and len(active) < 3:
        load_per_chiller_tons = cooling_load_tons / len(active)
        if load_per_chiller_tons > 380:
            if len(active) == 1:
                active.append('Chiller-2')
            elif len(active) == 2:
                active.append('Chiller-3')
    
    return active

# ============================================
# TABLE 1: CHILLER OPERATING POINTS
# ============================================

def generate_chiller_operating_points():
    """Generate ChillerOperatingPoints table data"""
    
    print("\n[1/14] Generating ChillerOperatingPoints...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Chiller Operating Points"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        wet_bulb_temp = weather['wet_bulb']
        
        active_chillers = determine_active_chillers(cooling_load_kw, wet_bulb_temp)
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        for chiller_id in active_chillers:
            chiller_spec = CHILLERS[chiller_id]
            cooling_load_tons = cooling_load_per_chiller / 3.517
            
            # Efficiency model
            chiller_model = ChillerEfficiencyModel(chiller_id, chiller_spec['rated_tons'])
            
            chw_supply_temp = 6.7 + np.random.normal(0, 0.1)
            chw_return_temp = 12.1 + np.random.normal(0, 0.15)
            chw_delta_t = chw_return_temp - chw_supply_temp
            
            tower_approach = 3.5 + np.random.normal(0, 0.3)
            cw_supply_temp = wet_bulb_temp + tower_approach
            
            kw_per_ton = chiller_model.get_efficiency(cooling_load_tons, chw_supply_temp, cw_supply_temp)
            chiller_power_kw = cooling_load_tons * kw_per_ton
            
            cp_water = 4.18
            chw_flow_lps = cooling_load_per_chiller / (cp_water * chw_delta_t)
            
            heat_in_kw = cooling_load_per_chiller
            heat_rejected_kw = heat_in_kw + chiller_power_kw
            
            cw_flow_lps = chw_flow_lps * 1.2
            cw_delta_t = heat_rejected_kw / (cw_flow_lps * cp_water)
            cw_return_temp = cw_supply_temp + cw_delta_t
            
            unbalanced_heat_pct = ((heat_rejected_kw - (heat_in_kw + chiller_power_kw)) / heat_rejected_kw) * 100
            unbalanced_heat_pct += np.random.normal(0, 2)
            
            cop = cooling_load_per_chiller / chiller_power_kw if chiller_power_kw > 0 else 0
            load_percent = (cooling_load_tons / chiller_spec['rated_tons']) * 100
            
            records.append({
                'Timestamp': ts,
                'ChillerID': chiller_id,
                'CHWFHeaderFlowLPS': round(chw_flow_lps, 0),
                'CHWSTHeaderTempC': round(chw_supply_temp, 1),
                'CHWRTHeaderTempC': round(chw_return_temp, 1),
                'CDWFHeaderFlowLPS': round(cw_flow_lps, 0),
                'CWSTHeaderTempC': round(cw_supply_temp, 1),
                'CWRTHeaderTempC': round(cw_return_temp, 1),
                'ChillerPowerKW': round(chiller_power_kw, 0),
                'CoolingLoadKW': round(cooling_load_per_chiller, 0),
                'HeatInKW': round(heat_in_kw, 0),
                'HeatRejectedKW': round(heat_rejected_kw, 0),
                'PercentUnbalancedHeat': round(unbalanced_heat_pct, 2),
                'COP': round(cop, 3),
                'KWPerTon': round(kw_per_ton, 3),
                'LoadPercentage': round(load_percent, 2),
                'RunningStatus': 'ON',
                'LoadCondition': 'PART_LOAD_HIGH' if load_percent > 70 else 'PART_LOAD_MEDIUM'
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 2: CHILLER TELEMETRY (Basic)
# ============================================

def generate_chiller_telemetry():
    """Generate ChillerTelemetry table data (basic format)"""
    
    print("\n[2/14] Generating ChillerTelemetry...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    runtime_hours = {'Chiller-1': 8500, 'Chiller-2': 8200, 'Chiller-3': 6800}
    runtime_since_service = {'Chiller-1': 450, 'Chiller-2': 380, 'Chiller-3': 520}
    
    for ts in tqdm(timestamps, desc="Chiller Telemetry"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, weather['wet_bulb'])
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        for chiller_id in active_chillers:
            chiller_spec = CHILLERS[chiller_id]
            cooling_load_tons = cooling_load_per_chiller / 3.517
            
            chiller_model = ChillerEfficiencyModel(chiller_id, chiller_spec['rated_tons'])
            chw_supply_temp = 6.7 + np.random.normal(0, 0.1)
            kw_per_ton = chiller_model.get_efficiency(cooling_load_tons, chw_supply_temp)
            
            capacity_percent = (cooling_load_tons / chiller_spec['rated_tons']) * 100
            power_kw = cooling_load_tons * kw_per_ton
            
            # Increment runtime
            runtime_hours[chiller_id] += INTERVAL_MINUTES / 60
            runtime_since_service[chiller_id] += INTERVAL_MINUTES / 60
            
            records.append({
                'Timestamp': ts,
                'ChillerID': chiller_id,
                'RunningStatus': 'ON',
                'CapacityPercent': round(capacity_percent, 2),
                'PowerConsumptionKW': round(power_kw, 2),
                'EfficiencyKwPerTon': round(kw_per_ton, 3),
                'CHWSupplyTempCelsius': round(chw_supply_temp, 2),
                'CHWReturnTempCelsius': round(12.1 + np.random.normal(0, 0.15), 2),
                'CHWFlowRateLPM': round(cooling_load_per_chiller / (4.18 * 5.4) * 60, 2),
                'EvaporatorPressureBar': round(4.5 + np.random.normal(0, 0.2), 3),
                'CWSupplyTempCelsius': round(weather['wet_bulb'] + 3.5, 2),
                'CWReturnTempCelsius': round(weather['wet_bulb'] + 3.5 + 5.5, 2),
                'CWFlowRateLPM': round(cooling_load_per_chiller / (4.18 * 5.4) * 60 * 1.2, 2),
                'CondenserPressureBar': round(12.5 + np.random.normal(0, 0.5), 3),
                'CompressorCurrentAmps': round(power_kw * 1.45, 2),
                'OilPressureBar': round(3.2 + np.random.normal(0, 0.2), 3),
                'OilTempCelsius': round(55 + np.random.normal(0, 3), 2),
                'VibrationMmS': round(2.5 + np.random.normal(0, 0.3), 2),
                'BearingTempCelsius': round(58 + np.random.normal(0, 3), 2),
                'SuperheatCelsius': round(5.5 + np.random.normal(0, 0.8), 2),
                'SubcoolingCelsius': round(4.2 + np.random.normal(0, 0.6), 2),
                'StartsToday': 2 if ts.hour < 12 else 3,
                'RuntimeHoursTotal': round(runtime_hours[chiller_id], 2),
                'RuntimeHoursSinceService': round(runtime_since_service[chiller_id], 2),
                'ActiveAlarms': '[]'
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 3: CHILLER PERFORMANCE MONITORING
# ============================================

def generate_chiller_performance_monitoring():
    """Generate ChillerPerformanceMonitoring table data"""
    
    print("\n[3/14] Generating ChillerPerformanceMonitoring...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Chiller Performance"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, weather['wet_bulb'])
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        for chiller_id in active_chillers:
            chiller_spec = CHILLERS[chiller_id]
            rated_tons = chiller_spec['rated_tons']
            cooling_load_tons = cooling_load_per_chiller / 3.517
            
            chiller_model = ChillerEfficiencyModel(chiller_id, rated_tons)
            chw_supply_temp = 6.7 + np.random.normal(0, 0.1)
            kw_per_ton = chiller_model.get_efficiency(cooling_load_tons, chw_supply_temp)
            
            load_percent = (cooling_load_tons / rated_tons) * 100
            part_load_ratio = cooling_load_tons / rated_tons
            
            # Full load reference (at 100%)
            full_load_kw_per_ton = 0.545  # From curve at 100% load
            full_load_cop = 3.517 / full_load_kw_per_ton
            full_load_power = rated_tons * full_load_kw_per_ton
            
            # Part load actual
            part_load_power = cooling_load_tons * kw_per_ton
            part_load_cop = cooling_load_per_chiller / part_load_power if part_load_power > 0 else 0
            
            # Degradation
            efficiency_degradation = ((kw_per_ton - 0.52) / 0.52) * 100  # vs optimal 0.52
            
            # IPLV (Integrated Part Load Value)
            iplv = 0.01*full_load_kw_per_ton + 0.42*kw_per_ton*0.95 + 0.45*kw_per_ton*0.90 + 0.12*kw_per_ton*0.85
            
            records.append({
                'Timestamp': ts,
                'ChillerID': chiller_id,
                'LoadCategory': 'FULL_LOAD' if load_percent > 85 else ('PART_LOAD_HIGH' if load_percent > 60 else 'PART_LOAD_MEDIUM'),
                'LoadPercentage': round(load_percent, 2),
                'RatedCapacityTons': rated_tons,
                'ActualCapacityTons': round(cooling_load_tons, 2),
                'FullLoadEfficiencyKWPerTon': full_load_kw_per_ton,
                'FullLoadCOP': round(full_load_cop, 3),
                'FullLoadPowerKW': round(full_load_power, 2),
                'PartLoadRatio': round(part_load_ratio, 3),
                'PartLoadEfficiencyKWPerTon': round(kw_per_ton, 3),
                'PartLoadCOP': round(part_load_cop, 3),
                'PartLoadPowerKW': round(part_load_power, 2),
                'EfficiencyDegradationPercent': round(efficiency_degradation, 2),
                'PerformanceFactorIPLV': round(iplv, 3),
                'CompressorLoadPercent': round(load_percent, 2),
                'CompressorPowerKW': round(part_load_power * 0.85, 2),
                'CompressorEfficiencyPercent': round(85 + np.random.normal(0, 2), 2),
                'SlideValvePosition': round(load_percent, 2),
                'VanePosition': None,
                'EvaporatorHeatTransferKW': round(cooling_load_per_chiller, 2),
                'EvaporatorApproachTempC': round(1.5 + np.random.normal(0, 0.3), 2),
                'EvaporatorFoulingFactor': round(0.00008 + np.random.normal(0, 0.00002), 5),
                'CondenserHeatRejectionKW': round(cooling_load_per_chiller + part_load_power, 2),
                'CondenserApproachTempC': round(2.0 + np.random.normal(0, 0.4), 2),
                'CondenserFoulingFactor': round(0.00010 + np.random.normal(0, 0.00003), 5),
                'RefrigerantType': 'R-134a',
                'RefrigerantChargeKg': round(850 if rated_tons == 400 else 650, 1),
                'EvaporatorRefrigTempC': round(chw_supply_temp - 2, 1),
                'CondenserRefrigTempC': round(weather['wet_bulb'] + 3.5 + 5.5 + 2, 1),
                'SuperheatC': round(5.5 + np.random.normal(0, 1.0), 1),
                'SubcoolingC': round(4.2 + np.random.normal(0, 0.8), 1),
                'OilPressureBar': round(3.2 + np.random.normal(0, 0.3), 2),
                'OilTempC': round(55 + np.random.normal(0, 3), 1),
                'OilLevelPercent': round(75 + np.random.normal(0, 5), 1),
                'OilFilterDifferentialPressureBar': round(0.15 + np.random.normal(0, 0.05), 3)
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# Continue with remaining tables...
# (I'll continue in the next message with pumps, towers, weather, etc.)

# ============================================
# TABLE 4: PUMP TELEMETRY (Basic)
# ============================================

def generate_pump_telemetry():
    """Generate PumpTelemetry table data (basic format)"""
    
    print("\n[4/14] Generating PumpTelemetry...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Pump Telemetry"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, 25)
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        # Total CHW flow needed
        total_chw_flow_lps = (cooling_load_kw / (4.18 * 5.4))
        
        # Primary CHW Pumps (2 running, 1 standby)
        pchwp_flow_each = total_chw_flow_lps / 2
        
        for i in [1, 2]:  # PCHWP-1 and PCHWP-2 running
            pump_id = f"PCHWP-{i}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 70 + (pchwp_flow_each / pump_spec['rated_flow_lps']) * 30
            vfd_speed = min(100, max(60, vfd_speed))
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            flow_lpm = pchwp_flow_each * 60
            diff_pressure = 2.5 + (vfd_speed / 100) * 0.5
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'PowerConsumptionKW': round(power_kw, 2),
                'FlowRateLPM': round(flow_lpm, 2),
                'DifferentialPressureBar': round(diff_pressure, 3),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'VibrationMmS': round(2.2 + np.random.normal(0, 0.3), 2)
            })
        
        # PCHWP-3 standby
        records.append({
            'Timestamp': ts,
            'PumpID': 'PCHWP-3',
            'RunningStatus': 'OFF',
            'VFDSpeedPercent': 0,
            'PowerConsumptionKW': 0,
            'FlowRateLPM': 0,
            'DifferentialPressureBar': None,
            'MotorCurrentAmps': 0,
            'VibrationMmS': None
        })
        
        # Secondary CHW Pumps (all 3 running)
        schwp_flow_each = total_chw_flow_lps / 3
        
        for i in [1, 2, 3]:
            pump_id = f"SCHWP-{i}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 65 + (schwp_flow_each / pump_spec['rated_flow_lps']) * 35
            vfd_speed = min(100, max(60, vfd_speed))
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            flow_lpm = schwp_flow_each * 60
            diff_pressure = 1.8 + (vfd_speed / 100) * 0.4
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'PowerConsumptionKW': round(power_kw, 2),
                'FlowRateLPM': round(flow_lpm, 2),
                'DifferentialPressureBar': round(diff_pressure, 3),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'VibrationMmS': round(2.0 + np.random.normal(0, 0.3), 2)
            })
        
        # Condenser Water Pumps (one per active chiller)
        cw_flow_per_chiller = (cooling_load_per_chiller / (4.18 * 5.4)) * 1.2
        
        for idx, chiller_id in enumerate(active_chillers, 1):
            pump_id = f"CWP-{idx}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 65 + (cw_flow_per_chiller / pump_spec['rated_flow_lps']) * 35
            vfd_speed = min(100, max(60, vfd_speed))
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            flow_lpm = cw_flow_per_chiller * 60
            diff_pressure = 2.5 + (vfd_speed / 100) * 1.0
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'PowerConsumptionKW': round(power_kw, 2),
                'FlowRateLPM': round(flow_lpm, 2),
                'DifferentialPressureBar': round(diff_pressure, 3),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'VibrationMmS': round(2.5 + np.random.normal(0, 0.4), 2)
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 5: PUMP OPERATING DATA (Enhanced)
# ============================================

def generate_pump_operating_data():
    """Generate PumpOperatingData table data (enhanced format)"""
    
    print("\n[5/14] Generating PumpOperatingData...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Pump Operating Data"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, 25)
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        total_chw_flow_lps = (cooling_load_kw / (4.18 * 5.4))
        pchwp_flow_each = total_chw_flow_lps / 2
        
        # Primary CHW Pumps
        for i in [1, 2]:
            pump_id = f"PCHWP-{i}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 70 + (pchwp_flow_each / pump_spec['rated_flow_lps']) * 30
            vfd_speed = min(100, max(60, vfd_speed))
            vfd_hz = 50 * (vfd_speed / 100)
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            flow_lps = pchwp_flow_each
            flow_lpm = flow_lps * 60
            
            discharge_pressure = 5.0 + np.random.normal(0, 0.2)
            suction_pressure = 2.5 + np.random.normal(0, 0.1)
            diff_pressure = discharge_pressure - suction_pressure
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'PumpType': 'PCHWP',
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'VFDSpeedHz': round(vfd_hz, 2),
                'FlowRateLPS': round(flow_lps, 2),
                'FlowRateLPM': round(flow_lpm, 2),
                'DischargePressureBar': round(discharge_pressure, 3),
                'SuctionPressureBar': round(suction_pressure, 3),
                'DifferentialPressureBar': round(diff_pressure, 3),
                'DifferentialPressureSetpointBar': 2.5,
                'PowerConsumptionKW': round(power_kw, 2),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'MotorVoltageVolts': 400,
                'PowerFactor': round(0.85 + np.random.normal(0, 0.02), 3),
                'PumpEfficiencyPercent': round(78 + np.random.normal(0, 2), 2),
                'MotorEfficiencyPercent': round(93 + np.random.normal(0, 1), 2),
                'WireToWaterEfficiencyPercent': round(72 + np.random.normal(0, 2), 2),
                'BearingTempFrontC': round(53 + np.random.normal(0, 3), 2),
                'BearingTempRearC': round(55 + np.random.normal(0, 3), 2),
                'VibrationMmS': round(2.2 + np.random.normal(0, 0.3), 2)
            })
        
        # PCHWP-3 standby
        records.append({
            'Timestamp': ts,
            'PumpID': 'PCHWP-3',
            'PumpType': 'PCHWP',
            'RunningStatus': 'OFF',
            'VFDSpeedPercent': 0,
            'VFDSpeedHz': 0,
            'FlowRateLPS': 0,
            'FlowRateLPM': 0,
            'DischargePressureBar': None,
            'SuctionPressureBar': None,
            'DifferentialPressureBar': None,
            'DifferentialPressureSetpointBar': 2.5,
            'PowerConsumptionKW': 0,
            'MotorCurrentAmps': 0,
            'MotorVoltageVolts': 400,
            'PowerFactor': None,
            'PumpEfficiencyPercent': None,
            'MotorEfficiencyPercent': None,
            'WireToWaterEfficiencyPercent': None,
            'BearingTempFrontC': None,
            'BearingTempRearC': None,
            'VibrationMmS': None
        })
        
        # Secondary CHW Pumps (similar pattern)
        schwp_flow_each = total_chw_flow_lps / 3
        
        for i in [1, 2, 3]:
            pump_id = f"SCHWP-{i}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 65 + (schwp_flow_each / pump_spec['rated_flow_lps']) * 35
            vfd_speed = min(100, max(60, vfd_speed))
            vfd_hz = 50 * (vfd_speed / 100)
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'PumpType': 'SCHWP',
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'VFDSpeedHz': round(vfd_hz, 2),
                'FlowRateLPS': round(schwp_flow_each, 2),
                'FlowRateLPM': round(schwp_flow_each * 60, 2),
                'DischargePressureBar': round(4.2 + np.random.normal(0, 0.2), 3),
                'SuctionPressureBar': round(2.4 + np.random.normal(0, 0.1), 3),
                'DifferentialPressureBar': round(1.8, 3),
                'DifferentialPressureSetpointBar': 1.8,
                'PowerConsumptionKW': round(power_kw, 2),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'MotorVoltageVolts': 400,
                'PowerFactor': round(0.85 + np.random.normal(0, 0.02), 3),
                'PumpEfficiencyPercent': round(76 + np.random.normal(0, 2), 2),
                'MotorEfficiencyPercent': round(92 + np.random.normal(0, 1), 2),
                'WireToWaterEfficiencyPercent': round(70 + np.random.normal(0, 2), 2),
                'BearingTempFrontC': round(52 + np.random.normal(0, 3), 2),
                'BearingTempRearC': round(54 + np.random.normal(0, 3), 2),
                'VibrationMmS': round(2.0 + np.random.normal(0, 0.3), 2)
            })
        
        # Condenser Water Pumps
        cw_flow_per_chiller = (cooling_load_per_chiller / (4.18 * 5.4)) * 1.2
        
        for idx, chiller_id in enumerate(active_chillers, 1):
            pump_id = f"CWP-{idx}"
            pump_spec = PUMPS[pump_id]
            
            vfd_speed = 65 + (cw_flow_per_chiller / pump_spec['rated_flow_lps']) * 35
            vfd_speed = min(100, max(60, vfd_speed))
            vfd_hz = 50 * (vfd_speed / 100)
            
            power_kw = PumpAffinityLaws.calculate_power(pump_spec['rated_power_kw'], vfd_speed)
            
            records.append({
                'Timestamp': ts,
                'PumpID': pump_id,
                'PumpType': 'CWP',
                'RunningStatus': 'ON',
                'VFDSpeedPercent': round(vfd_speed, 2),
                'VFDSpeedHz': round(vfd_hz, 2),
                'FlowRateLPS': round(cw_flow_per_chiller, 2),
                'FlowRateLPM': round(cw_flow_per_chiller * 60, 2),
                'DischargePressureBar': round(4.5 + np.random.normal(0, 0.2), 3),
                'SuctionPressureBar': round(2.0 + np.random.normal(0, 0.1), 3),
                'DifferentialPressureBar': round(2.5, 3),
                'DifferentialPressureSetpointBar': 2.5,
                'PowerConsumptionKW': round(power_kw, 2),
                'MotorCurrentAmps': round(power_kw * 1.45, 2),
                'MotorVoltageVolts': 400,
                'PowerFactor': round(0.85 + np.random.normal(0, 0.02), 3),
                'PumpEfficiencyPercent': round(75 + np.random.normal(0, 3), 2),
                'MotorEfficiencyPercent': round(92 + np.random.normal(0, 1), 2),
                'WireToWaterEfficiencyPercent': round(70 + np.random.normal(0, 3), 2),
                'BearingTempFrontC': round(55 + np.random.normal(0, 3), 2),
                'BearingTempRearC': round(57 + np.random.normal(0, 3), 2),
                'VibrationMmS': round(2.5 + np.random.normal(0, 0.4), 2)
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 6: COOLING TOWER TELEMETRY (Basic)
# ============================================

def generate_cooling_tower_telemetry():
    """Generate CoolingTowerTelemetry table data (basic format)"""
    
    print("\n[6/14] Generating CoolingTowerTelemetry...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Cooling Tower Telemetry"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, weather['wet_bulb'])
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        # One tower per active chiller
        for idx, chiller_id in enumerate(active_chillers, 1):
            tower_id = f"CT-{idx}"
            
            # CW flow from chiller
            cw_flow_lps = (cooling_load_per_chiller / (4.18 * 5.4)) * 1.2
            cw_flow_lpm = cw_flow_lps * 60
            
            # Tower temperatures
            tower_approach = 3.5 + np.random.normal(0, 0.3)
            leaving_temp = weather['wet_bulb'] + tower_approach
            entering_temp = leaving_temp + 5.5  # Range ~5.5°C
            
            # Fan control
            if tower_approach > 4.0:
                fan_speed = min(100, 75 + (tower_approach - 4.0) * 10)
            else:
                fan_speed = max(50, 75 - (4.0 - tower_approach) * 5)
            
            fan_power = 15 * (fan_speed / 100)**3  # Per fan
            total_fan_power = fan_power * 2  # 2 fans per tower
            
            # Makeup water
            evap_rate = 0.00085 * (cooling_load_per_chiller * 1.3)  # L/min
            makeup_rate = evap_rate * 1.25  # Including blowdown
            
            records.append({
                'Timestamp': ts,
                'TowerID': tower_id,
                'Fan1Status': 'ON',
                'Fan1VFDSpeedPercent': round(fan_speed, 2),
                'Fan2Status': 'ON',
                'Fan2VFDSpeedPercent': round(fan_speed, 2),
                'TotalFanPowerKW': round(total_fan_power, 2),
                'BasinTempCelsius': round(leaving_temp + 0.5, 2),
                'InletTempCelsius': round(entering_temp, 2),
                'ApproachTempCelsius': round(tower_approach, 2),
                'WaterFlowRateLPM': round(cw_flow_lpm, 2),
                'MakeupWaterFlowLPM': round(makeup_rate, 2),
                'BasinLevelPercent': round(75 + np.random.normal(0, 5), 2)
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 7: COOLING TOWER OPERATING DATA (Enhanced)
# ============================================

def generate_cooling_tower_operating_data():
    """Generate CoolingTowerOperatingData table data (enhanced format)"""
    
    print("\n[7/14] Generating CoolingTowerOperatingData...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Cooling Tower Operating Data"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, weather['wet_bulb'])
        num_active = len(active_chillers)
        cooling_load_per_chiller = cooling_load_kw / num_active
        
        for idx, chiller_id in enumerate(active_chillers, 1):
            tower_id = f"CT-{idx}"
            tower_spec = TOWERS[tower_id]
            
            # Water flow
            cw_flow_lps = (cooling_load_per_chiller / (4.18 * 5.4)) * 1.2
            cw_flow_lpm = cw_flow_lps * 60
            cw_flow_gpm = cw_flow_lps * 15.85
            
            # Temperatures
            tower_approach = CoolingTowerModel.calculate_approach(75, cw_flow_gpm / 1600, weather['wet_bulb'])
            leaving_temp = weather['wet_bulb'] + tower_approach
            
            # Heat rejected
            heat_rejected_kw = cooling_load_per_chiller * 1.3  # includes compressor heat
            range_temp = heat_rejected_kw / (cw_flow_lps * 4.18)
            entering_temp = leaving_temp + range_temp
            
            # Tower effectiveness
            effectiveness = CoolingTowerModel.calculate_effectiveness(range_temp, entering_temp, weather['wet_bulb'])
            
            # Cooling capacity
            cooling_capacity_tons = heat_rejected_kw / 3.517
            
            # Fan operation
            if tower_approach > 4.0:
                fan_speed = min(100, 75 + (tower_approach - 4.0) * 10)
            else:
                fan_speed = max(50, 75 - (4.0 - tower_approach) * 5)
            
            fan_rpm = 900 * (fan_speed / 100)
            fan_power_each = 15 * (fan_speed / 100)**3
            total_fan_power = fan_power_each * 2
            
            # Air flow
            air_flow_cfm = 180000 * (fan_speed / 100)
            air_flow_m3h = air_flow_cfm * 1.699
            air_velocity = 3.5 * (fan_speed / 100)
            
            # Water management
            evap_rate = 0.00085 * heat_rejected_kw
            blowdown_rate = evap_rate / 4
            makeup_rate = evap_rate + blowdown_rate
            cycles_of_conc = 5.0
            
            # Performance metrics
            water_loading_gpm_per_sqft = cw_flow_gpm / 1600
            power_per_ton = total_fan_power / cooling_capacity_tons if cooling_capacity_tons > 0 else 0
            
            records.append({
                'Timestamp': ts,
                'TowerID': tower_id,
                'WaterFlowRateLPS': round(cw_flow_lps, 2),
                'WaterFlowRateLPM': round(cw_flow_lpm, 2),
                'WaterFlowRateGPM': round(cw_flow_gpm, 2),
                'EnteringWaterTempC': round(entering_temp, 2),
                'LeavingWaterTempC': round(leaving_temp, 2),
                'WetBulbAirTempC': round(weather['wet_bulb'], 2),
                'DryBulbAirTempC': round(weather['dry_bulb'], 2),
                'RelativeHumidityPercent': round(weather['humidity'], 2),
                'EffectivenessPercent': round(effectiveness, 2),
                'CoolingCapacityTons': round(cooling_capacity_tons, 2),
                'Fan1Status': 'ON',
                'Fan1SpeedPercent': round(fan_speed, 2),
                'Fan1SpeedRPM': round(fan_rpm, 0),
                'Fan1PowerKW': round(fan_power_each, 2),
                'Fan2Status': 'ON',
                'Fan2SpeedPercent': round(fan_speed, 2),
                'Fan2SpeedRPM': round(fan_rpm, 0),
                'Fan2PowerKW': round(fan_power_each, 2),
                'TotalFanPowerKW': round(total_fan_power, 2),
                'AirFlowCFM': round(air_flow_cfm, 0),
                'AirFlowM3H': round(air_flow_m3h, 0),
                'AirVelocityMPS': round(air_velocity, 2),
                'MakeupWaterFlowLPM': round(makeup_rate, 2),
                'BlowdownFlowLPM': round(blowdown_rate, 2),
                'CyclesOfConcentration': round(cycles_of_conc, 2),
                'BasinWaterLevelPercent': round(75 + np.random.normal(0, 5), 2),
                'BasinWaterTempC': round(leaving_temp + 0.5, 2),
                'WaterLoadingGPMPerSqFt': round(water_loading_gpm_per_sqft, 2),
                'HeatRejectionRateKW': round(heat_rejected_kw, 2),
                'PowerPerTonKW': round(power_per_ton, 3)
            })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 8: WEATHER CONDITIONS
# ============================================

def generate_weather_conditions():
    """Generate WeatherConditions table data"""
    
    print("\n[8/14] Generating WeatherConditions...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Weather Conditions"):
        weather = get_weather_for_time(ts)
        
        records.append({
            'Timestamp': ts,
            'OutdoorTempCelsius': round(weather['dry_bulb'], 2),
            'WetBulbTempCelsius': round(weather['wet_bulb'], 2),
            'RelativeHumidityPercent': round(weather['humidity'], 2),
            'DewPointCelsius': round(weather['dew_point'], 2),
            'BarometricPressureMbar': round(weather['pressure'], 2),
            'WindSpeedMPS': round(weather['wind_speed'], 2),
            'RainfallMM': round(weather['rainfall'], 2)
        })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 9: FACILITY POWER
# ============================================

def generate_facility_power():
    """Generate FacilityPower table data"""
    
    print("\n[9/14] Generating FacilityPower...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="Facility Power"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        
        # IT load (cooling load / 0.29)
        it_load_kw = cooling_load_kw / 0.29
        
        # Total cooling system power (chillers + pumps + towers)
        # Estimate: ~15-20% of cooling load
        cooling_system_power_kw = cooling_load_kw * 0.18
        
        # Total facility power
        lighting_misc_kw = 100
        total_facility_power_kw = it_load_kw + cooling_system_power_kw + lighting_misc_kw
        
        # PUE
        pue = total_facility_power_kw / it_load_kw if it_load_kw > 0 else 0
        
        records.append({
            'Timestamp': ts,
            'TotalFacilityPowerKW': round(total_facility_power_kw, 2),
            'ITLoadPowerKW': round(it_load_kw, 2),
            'CoolingSystemPowerKW': round(cooling_system_power_kw, 2),
            'PUE': round(pue, 3)
        })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# ============================================
# TABLE 10: SYSTEM PERFORMANCE METRICS
# ============================================

def generate_system_performance_metrics():
    """Generate SystemPerformanceMetrics table data"""
    
    print("\n[10/14] Generating SystemPerformanceMetrics...")
    
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    records = []
    
    for ts in tqdm(timestamps, desc="System Performance Metrics"):
        cooling_load_kw = get_cooling_load_for_time(ts)
        weather = get_weather_for_time(ts)
        
        active_chillers = determine_active_chillers(cooling_load_kw, weather['wet_bulb'])
        num_active = len(active_chillers)
        
        cooling_load_tons = cooling_load_kw / 3.517
        
        # Chiller power (estimate based on efficiency)
        avg_kw_per_ton = 0.52
        total_chiller_power = cooling_load_tons * avg_kw_per_ton
        
        # Plant efficiency
        plant_efficiency_kw_per_ton = total_chiller_power / cooling_load_tons if cooling_load_tons > 0 else 0
        plant_cop = cooling_load_kw / total_chiller_power if total_chiller_power > 0 else 0
        
        # Auxiliary power
        total_pump_power = cooling_load_kw * 0.05  # ~5% of cooling load
        chw_pumps_power = total_pump_power * 0.6
        cw_pumps_power = total_pump_power * 0.4
        total_tower_fan_power = num_active * 2 * 15 * 0.7  # 2 fans per tower, 70% speed average
        
        # System totals
        total_cooling_system_power = total_chiller_power + total_pump_power + total_tower_fan_power
        system_efficiency = total_cooling_system_power / cooling_load_tons if cooling_load_tons > 0 else 0
        system_cop = cooling_load_kw / total_cooling_system_power if total_cooling_system_power > 0 else 0
        
        # PUE & WUE
        it_load_kw = cooling_load_kw / 0.29
        total_facility_power = it_load_kw + total_cooling_system_power + 100
        pue = total_facility_power / it_load_kw if it_load_kw > 0 else 0
        
        # Water consumption
        total_makeup_water = num_active * (0.00085 * (cooling_load_kw / num_active) * 1.3) * 1.25
        total_water_liters = total_makeup_water * 60  # L/hour
        wue = (total_water_liters) / it_load_kw if it_load_kw > 0 else 0
        
        # Carbon
        grid_carbon_intensity = 420 + np.random.normal(0, 20)
        total_carbon_kg = (total_facility_power * grid_carbon_intensity / 1000) / 60
        carbon_per_ton = (total_cooling_system_power * grid_carbon_intensity / 1000) / cooling_load_tons / 60 if cooling_load_tons > 0 else 0
        
        # Operating mode
        redundancy_level = 'N+1' if (3 - num_active) >= 1 else 'N+0'
        
        # Weather normalized PUE
        weather_normalized_pue = pue * (24 / weather['wet_bulb'])
        
        records.append({
            'Timestamp': ts,
            'TotalChillerPowerKW': round(total_chiller_power, 2),
            'TotalCoolingLoadTons': round(cooling_load_tons, 2),
            'TotalCoolingLoadKW': round(cooling_load_kw, 2),
            'PlantEfficiencyKWPerTon': round(plant_efficiency_kw_per_ton, 3),
            'PlantCOP': round(plant_cop, 3),
            'TotalPumpPowerKW': round(total_pump_power, 2),
            'CHWPumpsPowerKW': round(chw_pumps_power, 2),
            'CWPumpsPowerKW': round(cw_pumps_power, 2),
            'TotalTowerFanPowerKW': round(total_tower_fan_power, 2),
            'TotalCoolingSystemPowerKW': round(total_cooling_system_power, 2),
            'SystemEfficiencyKWPerTon': round(system_efficiency, 3),
            'SystemCOP': round(system_cop, 3),
            'ITLoadPowerKW': round(it_load_kw, 2),
            'TotalFacilityPowerKW': round(total_facility_power, 2),
            'PUE': round(pue, 3),
            'TotalWaterConsumptionLiters': round(total_water_liters, 2),
            'WUE': round(wue, 3),
            'GridCarbonIntensityGCO2PerKWh': round(grid_carbon_intensity, 2),
            'TotalCarbonEmissionsKgCO2': round(total_carbon_kg, 2),
            'CarbonPerCoolingTonKgCO2': round(carbon_per_ton, 3),
            'ChillersOnline': num_active,
            'RedundancyLevel': redundancy_level,
            'EconomizerMode': 0,
            'OutdoorWetBulbC': round(weather['wet_bulb'], 2),
            'WeatherNormalizedPUE': round(weather_normalized_pue, 3)
        })
    
    df = pd.DataFrame(records)
    print(f"✅ Generated {len(df):,} records")
    return df

# Continue with remaining tables in next message...

# ============================================
# TABLE 11: MAINTENANCE LOGS (Historical)
# ============================================

def generate_maintenance_logs():
    """Generate MaintenanceLogs table data (historical maintenance records)"""
    
    print("\n[11/14] Generating MaintenanceLogs...")
    
    records = []
    
    # Generate historical maintenance for past 2 years
    maintenance_types = {
        'Chiller': [
            ('Oil Change', 1200, 800, 1200),
            ('Refrigerant Top-up', 2400, 300, 2400),
            ('Compressor Inspection', 4000, 1500, 4000),
            ('Tube Cleaning (Evaporator)', 8760, 2000, 8760),
            ('Tube Cleaning (Condenser)', 8760, 2000, 8760),
            ('Annual Service', 8760, 3500, 8760)
        ],
        'Pump': [
            ('Bearing Replacement', 8760, 600, 8760),
            ('Seal Replacement', 4380, 400, 4380),
            ('VFD Calibration', 4380, 300, 4380),
            ('Motor Inspection', 8760, 500, 8760)
        ],
        'CoolingTower': [
            ('Fill Media Cleaning', 4380, 800, 4380),
            ('Fan Motor Service', 8760, 700, 8760),
            ('Basin Cleaning', 2190, 500, 2190),
            ('Water Treatment', 720, 200, 720)
        ]
    }
    
    # Generate maintenance records
    current_date = datetime.strptime(END_DATE, "%Y-%m-%d %H:%M:%S")
    
    # Chillers
    for chiller_id in CHILLERS.keys():
        runtime_hours = 8500 if chiller_id == 'Chiller-1' else (8200 if chiller_id == 'Chiller-2' else 6800)
        
        for service_type, interval_hours, cost, next_interval in maintenance_types['Chiller']:
            # How many times should this service have been done?
            num_services = int(runtime_hours / interval_hours)
            
            for i in range(num_services):
                service_hours = (i + 1) * interval_hours
                days_ago = int((runtime_hours - service_hours) / 24)
                service_date = current_date - timedelta(days=days_ago)
                
                records.append({
                    'Timestamp': service_date,
                    'EquipmentID': chiller_id,
                    'EquipmentType': 'CHILLER',
                    'ServiceType': service_type,
                    'HoursAtService': round(service_hours, 2),
                    'PartsReplaced': json.dumps(['Filter', 'Gaskets']) if 'Oil' in service_type else json.dumps([]),
                    'TechnicianNotes': f'{service_type} completed successfully',
                    'CostSGD': round(cost + np.random.uniform(-100, 100), 2),
                    'NextServiceDueHours': round(service_hours + next_interval, 2)
                })
    
    # Pumps
    for pump_id in PUMPS.keys():
        runtime_hours = 7800
        
        for service_type, interval_hours, cost, next_interval in maintenance_types['Pump']:
            num_services = int(runtime_hours / interval_hours)
            
            for i in range(num_services):
                service_hours = (i + 1) * interval_hours
                days_ago = int((runtime_hours - service_hours) / 24)
                service_date = current_date - timedelta(days=days_ago)
                
                records.append({
                    'Timestamp': service_date,
                    'EquipmentID': pump_id,
                    'EquipmentType': 'PUMP',
                    'ServiceType': service_type,
                    'HoursAtService': round(service_hours, 2),
                    'PartsReplaced': json.dumps(['Bearing', 'Seal']) if 'Bearing' in service_type else json.dumps([]),
                    'TechnicianNotes': f'{service_type} completed',
                    'CostSGD': round(cost + np.random.uniform(-50, 50), 2),
                    'NextServiceDueHours': round(service_hours + next_interval, 2)
                })
    
    # Cooling Towers
    for tower_id in TOWERS.keys():
        runtime_hours = 7800
        
        for service_type, interval_hours, cost, next_interval in maintenance_types['CoolingTower']:
            num_services = int(runtime_hours / interval_hours)
            
            for i in range(num_services):
                service_hours = (i + 1) * interval_hours
                days_ago = int((runtime_hours - service_hours) / 24)
                service_date = current_date - timedelta(days=days_ago)
                
                records.append({
                    'Timestamp': service_date,
                    'EquipmentID': tower_id,
                    'EquipmentType': 'TOWER',
                    'ServiceType': service_type,
                    'HoursAtService': round(service_hours, 2),
                    'PartsReplaced': json.dumps([]),
                    'TechnicianNotes': f'{service_type} completed',
                    'CostSGD': round(cost + np.random.uniform(-50, 50), 2),
                    'NextServiceDueHours': round(service_hours + next_interval, 2)
                })
    
    df = pd.DataFrame(records)
    df = df.sort_values('Timestamp')
    print(f"✅ Generated {len(df):,} maintenance records")
    return df

# ============================================
# TABLE 12: EQUIPMENT ALARMS (Realistic)
# ============================================

def generate_equipment_alarms():
    """Generate EquipmentAlarms table data (realistic alarm events)"""
    
    print("\n[12/14] Generating EquipmentAlarms...")
    
    records = []
    
    # Generate realistic alarms during the simulation period
    timestamps = pd.date_range(START_DATE, END_DATE, freq=f'{INTERVAL_MINUTES}min')
    
    alarm_templates = {
        'CHILLER': [
            ('ALM-CH-001', 'High Discharge Pressure', 'WARNING', 13.0, 12.5, 'Bar'),
            ('ALM-CH-002', 'Low Suction Pressure', 'WARNING', 3.8, 4.0, 'Bar'),
            ('ALM-CH-003', 'High Oil Temperature', 'WARNING', 62, 60, '°C'),
            ('ALM-CH-004', 'High Vibration', 'CRITICAL', 3.5, 3.0, 'mm/s'),
            ('ALM-CH-005', 'Low Oil Pressure', 'CRITICAL', 2.5, 3.0, 'Bar')
        ],
        'PUMP': [
            ('ALM-P-001', 'High Bearing Temperature', 'WARNING', 68, 65, '°C'),
            ('ALM-P-002', 'High Vibration', 'WARNING', 3.2, 3.0, 'mm/s'),
            ('ALM-P-003', 'Low Flow Rate', 'CRITICAL', 800, 1000, 'LPM')
        ],
        'TOWER': [
            ('ALM-T-001', 'High Approach Temperature', 'WARNING', 5.5, 5.0, '°C'),
            ('ALM-T-002', 'Low Basin Water Level', 'CRITICAL', 45, 50, '%'),
            ('ALM-T-003', 'Fan Motor Overload', 'CRITICAL', 18, 16, 'kW')
        ]
    }
    
    # Generate ~1-2 alarms per day
    alarm_count = 0
    for day_offset in range(7):  # 7 days
        day_start = datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S") + timedelta(days=day_offset)
        
        # Random number of alarms per day (1-3)
        num_alarms = np.random.randint(1, 4)
        
        for _ in range(num_alarms):
            # Random time during the day
            alarm_time = day_start + timedelta(hours=np.random.randint(0, 24), minutes=np.random.randint(0, 60))
            
            # Random equipment type
            equip_type = np.random.choice(['CHILLER', 'PUMP', 'TOWER'])
            
            if equip_type == 'CHILLER':
                equip_id = np.random.choice(list(CHILLERS.keys()))
            elif equip_type == 'PUMP':
                equip_id = np.random.choice(list(PUMPS.keys()))
            else:
                equip_id = np.random.choice(list(TOWERS.keys()))
            
            # Random alarm from templates
            alarm_code, alarm_desc, severity, triggered_val, threshold_val, unit = alarm_templates[equip_type][np.random.randint(len(alarm_templates[equip_type]))]
            
            # Alarm duration (5 min to 2 hours)
            alarm_duration_min = np.random.randint(5, 120)
            cleared_time = alarm_time + timedelta(minutes=alarm_duration_min)
            
            # Acknowledged time (1-10 min after trigger)
            ack_time = alarm_time + timedelta(minutes=np.random.randint(1, 10))
            
            records.append({
                'Timestamp': alarm_time,
                'EquipmentID': equip_id,
                'EquipmentType': equip_type,
                'AlarmCode': alarm_code,
                'AlarmDescription': alarm_desc,
                'AlarmSeverity': severity,
                'AlarmStatus': 'CLEARED',
                'TriggeredValue': triggered_val,
                'ThresholdValue': threshold_val,
                'Unit': unit,
                'AcknowledgedBy': 'Operator' if np.random.random() > 0.3 else 'Auto-Ack',
                'AcknowledgedTime': ack_time,
                'ClearedTime': cleared_time
            })
            alarm_count += 1
    
    df = pd.DataFrame(records)
    df = df.sort_values('Timestamp')
    print(f"✅ Generated {alarm_count} alarm records")
    return df

# ============================================
# TABLE 13 & 14: AGENT TABLES (Empty - populated at runtime)
# ============================================

def generate_agent_prompts():
    """Generate initial AgentPrompts (v1 baseline)"""
    
    print("\n[13/14] Generating AgentPrompts (baseline)...")
    
    agent_prompts = [
        {
            'AgentName': 'Demand & Conditions Forecasting Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Demand & Conditions Forecasting Agent. Your role is to predict IT load, weather conditions, and grid carbon intensity to enable proactive optimization.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        },
        {
            'AgentName': 'Chiller Optimization Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Chiller Optimization Agent. Your role is to optimize chiller sequencing, staging, and capacity allocation to minimize kW/ton while maintaining reliability.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        },
        {
            'AgentName': 'Building Systems Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Building Systems Agent. Your role is to optimize CHW/CW pumps, cooling towers, AHUs, and dampers for maximum distribution efficiency.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        },
        {
            'AgentName': 'Energy & Cost Optimization Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Energy & Cost Optimization Agent. Your role is to optimize for business outcomes: total cost, PUE, WUE, carbon footprint, and demand charge management.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        },
        {
            'AgentName': 'Maintenance & Compliance Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Maintenance & Compliance Agent. Your role is to ensure equipment health, preventive maintenance, and regulatory compliance.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        },
        {
            'AgentName': 'Operations & Safety Agent',
            'Version': 'v1.0',
            'PromptText': 'You are the Operations & Safety Agent. Your role is to enforce SOPs, N+1 redundancy, and real-time safety. You have VETO POWER.',
            'PerformanceNotes': 'Baseline version',
            'EvolvedFromVersion': None
        }
    ]
    
    df = pd.DataFrame(agent_prompts)
    print(f"✅ Generated {len(df)} agent prompt baselines")
    return df

def generate_agent_decisions():
    """Generate empty AgentDecisions table (populated at runtime)"""
    
    print("\n[14/14] Generating AgentDecisions (empty - runtime only)...")
    
    # Create empty dataframe with correct schema
    df = pd.DataFrame(columns=[
        'DecisionID', 'Timestamp', 'ScenarioID', 'ProposedByAgent', 'DecisionType',
        'Proposal', 'AgentsVotes', 'Approved', 'Executed', 'PredictedEnergySavingsKW',
        'PredictedPUE', 'PredictedCostSavingsSGD', 'ActualEnergySavingsKW',
        'ActualPUE', 'ActualCostSavingsSGD', 'EnergyPredictionErrorPct', 'PUEPredictionError'
    ])
    
    print(f"✅ Empty table created (will be populated by agents)")
    return df

# ============================================
# MASTER GENERATION FUNCTION
# ============================================

def generate_all_data():
    """
    Generate all tables and return as dictionary
    """
    
    print("="*70)
    print("GENERATING ALL DATA TABLES")
    print("="*70)
    print(f"Time Range: {START_DATE} to {END_DATE}")
    print(f"Interval: {INTERVAL_MINUTES} minute(s)")
    print(f"Total Minutes: {(datetime.strptime(END_DATE, '%Y-%m-%d %H:%M:%S') - datetime.strptime(START_DATE, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60:.0f}")
    print("="*70)
    
    data_dict = {}
    
    # Generate all tables
    data_dict['ChillerOperatingPoints'] = generate_chiller_operating_points()
    data_dict['ChillerTelemetry'] = generate_chiller_telemetry()
    data_dict['ChillerPerformanceMonitoring'] = generate_chiller_performance_monitoring()
    data_dict['PumpTelemetry'] = generate_pump_telemetry()
    data_dict['PumpOperatingData'] = generate_pump_operating_data()
    data_dict['CoolingTowerTelemetry'] = generate_cooling_tower_telemetry()
    data_dict['CoolingTowerOperatingData'] = generate_cooling_tower_operating_data()
    data_dict['WeatherConditions'] = generate_weather_conditions()
    data_dict['FacilityPower'] = generate_facility_power()
    data_dict['SystemPerformanceMetrics'] = generate_system_performance_metrics()
    data_dict['MaintenanceLogs'] = generate_maintenance_logs()
    data_dict['EquipmentAlarms'] = generate_equipment_alarms()
    data_dict['AgentPrompts'] = generate_agent_prompts()
    data_dict['AgentDecisions'] = generate_agent_decisions()
    
    print("\n" + "="*70)
    print("DATA GENERATION SUMMARY")
    print("="*70)
    
    total_records = 0
    for table_name, df in data_dict.items():
        record_count = len(df)
        total_records += record_count
        print(f"{table_name:40s}: {record_count:>10,} records")
    
    print("="*70)
    print(f"{'TOTAL RECORDS':40s}: {total_records:>10,}")
    print("="*70)
    
    return data_dict

# ============================================
# SAVE TO CSV
# ============================================

def save_to_csv(data_dict):
    """
    Save all dataframes to CSV files
    """
    
    print("\n" + "="*70)
    print("SAVING TO CSV FILES")
    print("="*70)
    
    for table_name, df in tqdm(data_dict.items(), desc="Saving CSV files"):
        csv_path = os.path.join(OUTPUT_DIR, f'{table_name}.csv')
        df.to_csv(csv_path, index=False)
        file_size_mb = os.path.getsize(csv_path) / 1024 / 1024
        print(f"✅ {table_name:40s}: {file_size_mb:>8.2f} MB")
    
    total_size_mb = sum([os.path.getsize(os.path.join(OUTPUT_DIR, f'{t}.csv')) for t in data_dict.keys()]) / 1024 / 1024
    
    print("="*70)
    print(f"{'TOTAL CSV SIZE':40s}: {total_size_mb:>8.2f} MB")
    print(f"{'LOCATION':40s}: {OUTPUT_DIR}")
    print("="*70)

# ============================================
# INSERT TO SQL SERVER
# ============================================

def insert_to_sqlserver(data_dict):
    """
    Insert all data into SQL Server database
    """
    
    print("\n" + "="*70)
    print("SQL SERVER DATABASE INSERTION")
    print("="*70)
    
    
    # Connection string
    conn_str = (
        f"DRIVER={{{os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')}}};"
        f"SERVER={os.getenv('DB_SERVER', 'localhost')};"
        f"DATABASE={os.getenv('DB_NAME', 'ChillerOptimizationDB')};"
        f"UID={os.getenv('DB_USER', 'sa')};"
        f"PWD={{{os.getenv('DB_PASSWORD')}}};"
        f"TrustServerCertificate=yes;"
    )
    
    print(f"\nConnecting to SQL Server...")
    print(f"  Server: {os.getenv('DB_SERVER', 'localhost')}")
    print(f"  Database: {os.getenv('DB_NAME', 'ChillerOptimizationDB')}")
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    # Insert each table
    table_insert_queries = {
        'ChillerOperatingPoints': """
            INSERT INTO ChillerOperatingPoints 
            (Timestamp, ChillerID, CHWFHeaderFlowLPS, CHWSTHeaderTempC, CHWRTHeaderTempC,
             CDWFHeaderFlowLPS, CWSTHeaderTempC, CWRTHeaderTempC, ChillerPowerKW,
             CoolingLoadKW, HeatInKW, HeatRejectedKW, PercentUnbalancedHeat,
             COP, KWPerTon, LoadPercentage, RunningStatus, LoadCondition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'ChillerTelemetry': """
            INSERT INTO ChillerTelemetry
            (Timestamp, ChillerID, RunningStatus, CapacityPercent, PowerConsumptionKW,
             EfficiencyKwPerTon, CHWSupplyTempCelsius, CHWReturnTempCelsius, CHWFlowRateLPM,
             EvaporatorPressureBar, CWSupplyTempCelsius, CWReturnTempCelsius, CWFlowRateLPM,
             CondenserPressureBar, CompressorCurrentAmps, OilPressureBar, OilTempCelsius,
             VibrationMmS, BearingTempCelsius, SuperheatCelsius, SubcoolingCelsius,
             StartsToday, RuntimeHoursTotal, RuntimeHoursSinceService, ActiveAlarms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'ChillerPerformanceMonitoring': """
            INSERT INTO ChillerPerformanceMonitoring
            (Timestamp, ChillerID, LoadCategory, LoadPercentage, RatedCapacityTons,
             ActualCapacityTons, FullLoadEfficiencyKWPerTon, FullLoadCOP, FullLoadPowerKW,
             PartLoadRatio, PartLoadEfficiencyKWPerTon, PartLoadCOP, PartLoadPowerKW,
             EfficiencyDegradationPercent, PerformanceFactorIPLV, CompressorLoadPercent,
             CompressorPowerKW, CompressorEfficiencyPercent, SlideValvePosition, VanePosition,
             EvaporatorHeatTransferKW, EvaporatorApproachTempC, EvaporatorFoulingFactor,
             CondenserHeatRejectionKW, CondenserApproachTempC, CondenserFoulingFactor,
             RefrigerantType, RefrigerantChargeKg, EvaporatorRefrigTempC, CondenserRefrigTempC,
             SuperheatC, SubcoolingC, OilPressureBar, OilTempC, OilLevelPercent,
             OilFilterDifferentialPressureBar)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        # Add more INSERT queries for other tables...
        'WeatherConditions': """
            INSERT INTO WeatherConditions
            (Timestamp, OutdoorTempCelsius, WetBulbTempCelsius, RelativeHumidityPercent,
             DewPointCelsius, BarometricPressureMbar, WindSpeedMPS, RainfallMM)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'FacilityPower': """
            INSERT INTO FacilityPower
            (Timestamp, TotalFacilityPowerKW, ITLoadPowerKW, CoolingSystemPowerKW, PUE)
            VALUES (?, ?, ?, ?, ?)
        """,
        'SystemPerformanceMetrics': """
            INSERT INTO SystemPerformanceMetrics
            (Timestamp, TotalChillerPowerKW, TotalCoolingLoadTons, TotalCoolingLoadKW,
             PlantEfficiencyKWPerTon, PlantCOP, TotalPumpPowerKW, CHWPumpsPowerKW,
             CWPumpsPowerKW, TotalTowerFanPowerKW, TotalCoolingSystemPowerKW,
             SystemEfficiencyKWPerTon, SystemCOP, ITLoadPowerKW, TotalFacilityPowerKW,
             PUE, TotalWaterConsumptionLiters, WUE, GridCarbonIntensityGCO2PerKWh,
             TotalCarbonEmissionsKgCO2, CarbonPerCoolingTonKgCO2, ChillersOnline,
             RedundancyLevel, EconomizerMode, OutdoorWetBulbC, WeatherNormalizedPUE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'MaintenanceLogs': """
            INSERT INTO MaintenanceLogs
            (Timestamp, EquipmentID, EquipmentType, ServiceType, HoursAtService,
             PartsReplaced, TechnicianNotes, CostSGD, NextServiceDueHours)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'EquipmentAlarms': """
            INSERT INTO EquipmentAlarms
            (Timestamp, EquipmentID, EquipmentType, AlarmCode, AlarmDescription,
             AlarmSeverity, AlarmStatus, TriggeredValue, ThresholdValue, Unit,
             AcknowledgedBy, AcknowledgedTime, ClearedTime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        'AgentPrompts': """
            INSERT INTO AgentPrompts
            (AgentName, Version, PromptText, PerformanceNotes, EvolvedFromVersion)
            VALUES (?, ?, ?, ?, ?)
        """
    }
    
    # Insert data for each table
    for table_name, df in data_dict.items():
        if table_name == 'AgentDecisions':
            # Skip empty table
            continue
            
        if table_name not in table_insert_queries:
            print(f"⚠️  Skipping {table_name} (no INSERT query defined)")
            continue
        
        print(f"\nInserting {table_name}...")
        
        insert_sql = table_insert_queries[table_name]
        batch_size = 1000
        total_batches = (len(df) // batch_size) + 1
        
        try:
            for i in tqdm(range(0, len(df), batch_size), total=total_batches, desc=f"  {table_name}"):
                batch = df.iloc[i:i+batch_size]
                
                batch_data = [tuple(row) for _, row in batch.iterrows()]
                cursor.executemany(insert_sql, batch_data)
                conn.commit()
            
            print(f"✅ {table_name}: {len(df):,} records inserted")
            
        except Exception as e:
            print(f"❌ {table_name} failed: {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*70)
    print("✅ DATABASE INSERTION COMPLETE!")
    print("="*70)
    
    return True

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    
    print("="*70)
    print("CHILLER PLANT COMPLETE DATA GENERATION")
    print("="*70)
    print("\nThis script will:")
    print("  1. Generate data for ALL 14 tables")
    print("  2. Save to CSV files (backup)")
    print("  3. Insert into SQL Server database")
    print("\n" + "="*70)
    
    input("Press ENTER to start generation...")
    
    # Step 1: Generate all data
    print("\n[STEP 1/3] GENERATING ALL DATA...")
    data_dict = generate_all_data()
    
    # Step 2: Save to CSV
    print("\n[STEP 2/3] SAVING TO CSV...")
    save_to_csv(data_dict)
    
    # Step 3: Insert to database
    print("\n[STEP 3/3] INSERTING TO SQL SERVER...")
    response = input("\nProceed with database insertion? (y/n): ")
    
    if response.lower() == 'y':
        success = insert_to_sqlserver(data_dict)
        
        if success:
            print("\n" + "="*70)
            print("✅ ALL STEPS COMPLETED SUCCESSFULLY!")
            print("="*70)
            print("\nData Location:")
            print(f"  CSV Files: {OUTPUT_DIR}")
            print(f"  Database: {os.getenv('DB_NAME', 'ChillerOptimizationDB')}")
            print("\nNext Steps:")
            print("  1. Verify data in SQL Server Management Studio")
            print("  2. Run data quality checks")
            print("  3. Begin MAGS agent development")
    else:
        print("\n⚠️  Skipped database insertion")
        print(f"  CSV files saved to: {OUTPUT_DIR}")
        print("\nTo insert later:")
        print("  python insert_all_from_csv.py")
    
    print("\n" + "="*70)
    print("COMPLETE!")
    print("="*70)