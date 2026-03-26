-- ============================================================
-- AOM-Dev Database Schema
-- Generated from generate_all_tables.py column definitions
-- ============================================================

USE [AOM-Dev];
GO

-- ============================================================
-- TABLE 1: ChillerOperatingPoints
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ChillerOperatingPoints')
CREATE TABLE ChillerOperatingPoints (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp               DATETIME2 NOT NULL,
    ChillerID               NVARCHAR(20) NOT NULL,
    CHWFHeaderFlowLPS       DECIMAL(10,2),
    CHWSTHeaderTempC        DECIMAL(10,2),
    CHWRTHeaderTempC        DECIMAL(10,2),
    CDWFHeaderFlowLPS       DECIMAL(10,2),
    CWSTHeaderTempC         DECIMAL(10,2),
    CWRTHeaderTempC         DECIMAL(10,2),
    ChillerPowerKW          DECIMAL(10,2),
    CoolingLoadKW           DECIMAL(10,2),
    HeatInKW                DECIMAL(10,2),
    HeatRejectedKW          DECIMAL(10,2),
    PercentUnbalancedHeat   DECIMAL(10,2),
    COP                     DECIMAL(10,3),
    KWPerTon                DECIMAL(10,3),
    LoadPercentage          DECIMAL(10,2),
    RunningStatus           NVARCHAR(10),
    LoadCondition           NVARCHAR(30)
);
GO

-- ============================================================
-- TABLE 2: ChillerTelemetry
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ChillerTelemetry')
CREATE TABLE ChillerTelemetry (
    ID                          INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                   DATETIME2 NOT NULL,
    ChillerID                   NVARCHAR(20) NOT NULL,
    RunningStatus               NVARCHAR(10),
    CapacityPercent             DECIMAL(10,2),
    PowerConsumptionKW          DECIMAL(10,2),
    EfficiencyKwPerTon          DECIMAL(10,3),
    CHWSupplyTempCelsius        DECIMAL(10,2),
    CHWReturnTempCelsius        DECIMAL(10,2),
    CHWFlowRateLPM              DECIMAL(10,2),
    EvaporatorPressureBar       DECIMAL(10,3),
    CWSupplyTempCelsius         DECIMAL(10,2),
    CWReturnTempCelsius         DECIMAL(10,2),
    CWFlowRateLPM               DECIMAL(10,2),
    CondenserPressureBar        DECIMAL(10,3),
    CompressorCurrentAmps       DECIMAL(10,2),
    OilPressureBar              DECIMAL(10,3),
    OilTempCelsius              DECIMAL(10,2),
    VibrationMmS                DECIMAL(10,2),
    BearingTempCelsius          DECIMAL(10,2),
    SuperheatCelsius            DECIMAL(10,2),
    SubcoolingCelsius           DECIMAL(10,2),
    StartsToday                 INT,
    RuntimeHoursTotal           DECIMAL(10,2),
    RuntimeHoursSinceService    DECIMAL(10,2),
    ActiveAlarms                NVARCHAR(255)
);
GO

-- ============================================================
-- TABLE 3: ChillerPerformanceMonitoring
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ChillerPerformanceMonitoring')
CREATE TABLE ChillerPerformanceMonitoring (
    ID                                  INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                           DATETIME2 NOT NULL,
    ChillerID                           NVARCHAR(20) NOT NULL,
    LoadCategory                        NVARCHAR(30),
    LoadPercentage                      DECIMAL(10,2),
    RatedCapacityTons                   INT,
    ActualCapacityTons                  DECIMAL(10,2),
    FullLoadEfficiencyKWPerTon          DECIMAL(10,3),
    FullLoadCOP                         DECIMAL(10,3),
    FullLoadPowerKW                     DECIMAL(10,2),
    PartLoadRatio                       DECIMAL(10,3),
    PartLoadEfficiencyKWPerTon          DECIMAL(10,3),
    PartLoadCOP                         DECIMAL(10,3),
    PartLoadPowerKW                     DECIMAL(10,2),
    EfficiencyDegradationPercent        DECIMAL(10,2),
    PerformanceFactorIPLV               DECIMAL(10,3),
    CompressorLoadPercent               DECIMAL(10,2),
    CompressorPowerKW                   DECIMAL(10,2),
    CompressorEfficiencyPercent         DECIMAL(10,2),
    SlideValvePosition                  DECIMAL(10,2),
    VanePosition                        DECIMAL(10,2) NULL,
    EvaporatorHeatTransferKW            DECIMAL(10,2),
    EvaporatorApproachTempC             DECIMAL(10,2),
    EvaporatorFoulingFactor             DECIMAL(12,8),
    CondenserHeatRejectionKW            DECIMAL(10,2),
    CondenserApproachTempC              DECIMAL(10,2),
    CondenserFoulingFactor              DECIMAL(12,8),
    RefrigerantType                     NVARCHAR(20),
    RefrigerantChargeKg                 DECIMAL(10,1),
    EvaporatorRefrigTempC               DECIMAL(10,1),
    CondenserRefrigTempC                DECIMAL(10,1),
    SuperheatC                          DECIMAL(10,1),
    SubcoolingC                         DECIMAL(10,1),
    OilPressureBar                      DECIMAL(10,2),
    OilTempC                            DECIMAL(10,1),
    OilLevelPercent                     DECIMAL(10,1),
    OilFilterDifferentialPressureBar    DECIMAL(10,3)
);
GO

-- ============================================================
-- TABLE 4: PumpTelemetry
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PumpTelemetry')
CREATE TABLE PumpTelemetry (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp               DATETIME2 NOT NULL,
    PumpID                  NVARCHAR(20) NOT NULL,
    RunningStatus           NVARCHAR(10),
    VFDSpeedPercent         DECIMAL(10,2),
    PowerConsumptionKW      DECIMAL(10,2),
    FlowRateLPM             DECIMAL(10,2),
    DifferentialPressureBar DECIMAL(10,3) NULL,
    MotorCurrentAmps        DECIMAL(10,2),
    VibrationMmS            DECIMAL(10,2) NULL
);
GO

-- ============================================================
-- TABLE 5: PumpOperatingData
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PumpOperatingData')
CREATE TABLE PumpOperatingData (
    ID                              INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                       DATETIME2 NOT NULL,
    PumpID                          NVARCHAR(20) NOT NULL,
    PumpType                        NVARCHAR(10),
    RunningStatus                   NVARCHAR(10),
    VFDSpeedPercent                 DECIMAL(10,2),
    VFDSpeedHz                      DECIMAL(10,2),
    FlowRateLPS                     DECIMAL(10,2),
    FlowRateLPM                     DECIMAL(10,2),
    DischargePressureBar            DECIMAL(10,3) NULL,
    SuctionPressureBar              DECIMAL(10,3) NULL,
    DifferentialPressureBar         DECIMAL(10,3) NULL,
    DifferentialPressureSetpointBar DECIMAL(10,3),
    PowerConsumptionKW              DECIMAL(10,2),
    MotorCurrentAmps                DECIMAL(10,2),
    MotorVoltageVolts               INT,
    PowerFactor                     DECIMAL(10,3) NULL,
    PumpEfficiencyPercent           DECIMAL(10,2) NULL,
    MotorEfficiencyPercent          DECIMAL(10,2) NULL,
    WireToWaterEfficiencyPercent    DECIMAL(10,2) NULL,
    BearingTempFrontC               DECIMAL(10,2) NULL,
    BearingTempRearC                DECIMAL(10,2) NULL,
    VibrationMmS                    DECIMAL(10,2) NULL
);
GO

-- ============================================================
-- TABLE 6: CoolingTowerTelemetry
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CoolingTowerTelemetry')
CREATE TABLE CoolingTowerTelemetry (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp               DATETIME2 NOT NULL,
    TowerID                 NVARCHAR(10) NOT NULL,
    Fan1Status              NVARCHAR(10),
    Fan1VFDSpeedPercent     DECIMAL(10,2),
    Fan2Status              NVARCHAR(10),
    Fan2VFDSpeedPercent     DECIMAL(10,2),
    TotalFanPowerKW         DECIMAL(10,2),
    BasinTempCelsius        DECIMAL(10,2),
    InletTempCelsius        DECIMAL(10,2),
    ApproachTempCelsius     DECIMAL(10,2),
    WaterFlowRateLPM        DECIMAL(10,2),
    MakeupWaterFlowLPM      DECIMAL(10,2),
    BasinLevelPercent       DECIMAL(10,2)
);
GO

-- ============================================================
-- TABLE 7: CoolingTowerOperatingData
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CoolingTowerOperatingData')
CREATE TABLE CoolingTowerOperatingData (
    ID                          INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                   DATETIME2 NOT NULL,
    TowerID                     NVARCHAR(10) NOT NULL,
    WaterFlowRateLPS            DECIMAL(10,2),
    WaterFlowRateLPM            DECIMAL(10,2),
    WaterFlowRateGPM            DECIMAL(10,2),
    EnteringWaterTempC          DECIMAL(10,2),
    LeavingWaterTempC           DECIMAL(10,2),
    WetBulbAirTempC             DECIMAL(10,2),
    DryBulbAirTempC             DECIMAL(10,2),
    RelativeHumidityPercent     DECIMAL(10,2),
    EffectivenessPercent        DECIMAL(10,2),
    CoolingCapacityTons         DECIMAL(10,2),
    Fan1Status                  NVARCHAR(10),
    Fan1SpeedPercent            DECIMAL(10,2),
    Fan1SpeedRPM                DECIMAL(10,0),
    Fan1PowerKW                 DECIMAL(10,2),
    Fan2Status                  NVARCHAR(10),
    Fan2SpeedPercent            DECIMAL(10,2),
    Fan2SpeedRPM                DECIMAL(10,0),
    Fan2PowerKW                 DECIMAL(10,2),
    TotalFanPowerKW             DECIMAL(10,2),
    AirFlowCFM                  DECIMAL(10,0),
    AirFlowM3H                  DECIMAL(10,0),
    AirVelocityMPS              DECIMAL(10,2),
    MakeupWaterFlowLPM          DECIMAL(10,2),
    BlowdownFlowLPM             DECIMAL(10,2),
    CyclesOfConcentration       DECIMAL(10,2),
    BasinWaterLevelPercent      DECIMAL(10,2),
    BasinWaterTempC             DECIMAL(10,2),
    WaterLoadingGPMPerSqFt      DECIMAL(10,2),
    HeatRejectionRateKW         DECIMAL(10,2),
    PowerPerTonKW               DECIMAL(10,3)
);
GO

-- ============================================================
-- TABLE 8: WeatherConditions
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'WeatherConditions')
CREATE TABLE WeatherConditions (
    ID                          INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                   DATETIME2 NOT NULL,
    OutdoorTempCelsius          DECIMAL(10,2),
    WetBulbTempCelsius          DECIMAL(10,2),
    RelativeHumidityPercent     DECIMAL(10,2),
    DewPointCelsius             DECIMAL(10,2),
    BarometricPressureMbar      DECIMAL(10,2),
    WindSpeedMPS                DECIMAL(10,2),
    RainfallMM                  DECIMAL(10,2)
);
GO

-- ============================================================
-- TABLE 9: FacilityPower
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FacilityPower')
CREATE TABLE FacilityPower (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp               DATETIME2 NOT NULL,
    TotalFacilityPowerKW    DECIMAL(10,2),
    ITLoadPowerKW           DECIMAL(10,2),
    CoolingSystemPowerKW    DECIMAL(10,2),
    PUE                     DECIMAL(10,3)
);
GO

-- ============================================================
-- TABLE 10: SystemPerformanceMetrics
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SystemPerformanceMetrics')
CREATE TABLE SystemPerformanceMetrics (
    ID                              INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                       DATETIME2 NOT NULL,
    TotalChillerPowerKW             DECIMAL(10,2),
    TotalCoolingLoadTons            DECIMAL(10,2),
    TotalCoolingLoadKW              DECIMAL(10,2),
    PlantEfficiencyKWPerTon         DECIMAL(10,3),
    PlantCOP                        DECIMAL(10,3),
    TotalPumpPowerKW                DECIMAL(10,2),
    CHWPumpsPowerKW                 DECIMAL(10,2),
    CWPumpsPowerKW                  DECIMAL(10,2),
    TotalTowerFanPowerKW            DECIMAL(10,2),
    TotalCoolingSystemPowerKW       DECIMAL(10,2),
    SystemEfficiencyKWPerTon        DECIMAL(10,3),
    SystemCOP                       DECIMAL(10,3),
    ITLoadPowerKW                   DECIMAL(10,2),
    TotalFacilityPowerKW            DECIMAL(10,2),
    PUE                             DECIMAL(10,3),
    TotalWaterConsumptionLiters     DECIMAL(10,2),
    WUE                             DECIMAL(10,3),
    GridCarbonIntensityGCO2PerKWh   DECIMAL(10,2),
    TotalCarbonEmissionsKgCO2       DECIMAL(10,2),
    CarbonPerCoolingTonKgCO2        DECIMAL(10,3),
    ChillersOnline                  INT,
    RedundancyLevel                 NVARCHAR(10),
    EconomizerMode                  BIT,
    OutdoorWetBulbC                 DECIMAL(10,2),
    WeatherNormalizedPUE            DECIMAL(10,3)
);
GO

-- ============================================================
-- TABLE 11: MaintenanceLogs
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MaintenanceLogs')
CREATE TABLE MaintenanceLogs (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp               DATETIME2 NOT NULL,
    EquipmentID             NVARCHAR(20) NOT NULL,
    EquipmentType           NVARCHAR(20),
    ServiceType             NVARCHAR(100),
    HoursAtService          DECIMAL(10,2),
    PartsReplaced           NVARCHAR(500),
    TechnicianNotes         NVARCHAR(500),
    CostSGD                 DECIMAL(10,2),
    NextServiceDueHours     DECIMAL(10,2)
);
GO

-- ============================================================
-- TABLE 12: EquipmentAlarms
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'EquipmentAlarms')
CREATE TABLE EquipmentAlarms (
    ID                  INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp           DATETIME2 NOT NULL,
    EquipmentID         NVARCHAR(20) NOT NULL,
    EquipmentType       NVARCHAR(20),
    AlarmCode           NVARCHAR(20),
    AlarmDescription    NVARCHAR(200),
    AlarmSeverity       NVARCHAR(20),
    AlarmStatus         NVARCHAR(20),
    TriggeredValue      DECIMAL(10,3),
    ThresholdValue      DECIMAL(10,3),
    Unit                NVARCHAR(20),
    AcknowledgedBy      NVARCHAR(50),
    AcknowledgedTime    DATETIME2 NULL,
    ClearedTime         DATETIME2 NULL
);
GO

-- ============================================================
-- TABLE 13: AgentPrompts
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AgentPrompts')
CREATE TABLE AgentPrompts (
    ID                  INT IDENTITY(1,1) PRIMARY KEY,
    AgentName           NVARCHAR(100) NOT NULL,
    Version             NVARCHAR(20),
    PromptText          NVARCHAR(MAX),
    PerformanceNotes    NVARCHAR(500),
    EvolvedFromVersion  NVARCHAR(20) NULL
);
GO

-- ============================================================
-- TABLE 14: AgentDecisions (populated at runtime by agents)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AgentDecisions')
CREATE TABLE AgentDecisions (
    DecisionID                  INT IDENTITY(1,1) PRIMARY KEY,
    Timestamp                   DATETIME2 NOT NULL,
    ScenarioID                  NVARCHAR(50),
    ProposedByAgent             NVARCHAR(100),
    DecisionType                NVARCHAR(50),
    Proposal                    NVARCHAR(MAX),
    AgentsVotes                 NVARCHAR(MAX),
    Approved                    BIT,
    Executed                    BIT,
    PredictedEnergySavingsKW    DECIMAL(10,2),
    PredictedPUE                DECIMAL(10,3),
    PredictedCostSavingsSGD     DECIMAL(10,2),
    ActualEnergySavingsKW       DECIMAL(10,2),
    ActualPUE                   DECIMAL(10,3),
    ActualCostSavingsSGD        DECIMAL(10,2),
    EnergyPredictionErrorPct    DECIMAL(10,2),
    PUEPredictionError          DECIMAL(10,3)
);
GO

-- ============================================================
-- TABLE 15: EquipmentRegistry (Static metadata)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'EquipmentRegistry')
CREATE TABLE EquipmentRegistry (
    ID                      INT IDENTITY(1,1) PRIMARY KEY,
    EquipmentID             NVARCHAR(20)  NOT NULL UNIQUE,
    EquipmentType           NVARCHAR(20)  NOT NULL,   -- CHILLER, PUMP, COOLING_TOWER
    PumpSubType             NVARCHAR(10)  NULL,        -- PCHWP, SCHWP, CWP
    Manufacturer            NVARCHAR(50)  NOT NULL,
    ModelNumber             NVARCHAR(50)  NOT NULL,
    SerialNumber            NVARCHAR(50)  NOT NULL,
    InstallDate             DATE          NOT NULL,
    RatedCapacityTons       DECIMAL(10,2) NULL,
    RatedPowerKW            DECIMAL(10,2) NOT NULL,
    RatedFlowLPS            DECIMAL(10,2) NULL,
    RatedHeadM              DECIMAL(10,2) NULL,
    DesignCHWSTCelsius      DECIMAL(10,2) NULL,
    DesignCHWRTCelsius      DECIMAL(10,2) NULL,
    DesignCWSTCelsius       DECIMAL(10,2) NULL,
    DesignCWRTCelsius       DECIMAL(10,2) NULL,
    DesignApproachCelsius   DECIMAL(10,2) NULL,
    RefrigerantType         NVARCHAR(20)  NULL,
    RatedCOPFull            DECIMAL(10,3) NULL,
    RatedIPLV               DECIMAL(10,3) NULL,
    HasVFD                  BIT           NOT NULL DEFAULT 0,
    FanCount                INT           NULL,
    FanPowerKWEach          DECIMAL(10,2) NULL,
    ServiceIntervalHours    INT           NOT NULL,
    LastServiceDate         DATE          NULL,
    NextServiceDueDate      DATE          NULL,
    WarrantyExpiryDate      DATE          NULL,
    Location                NVARCHAR(50)  NOT NULL,
    Status                  NVARCHAR(10)  NOT NULL    -- ACTIVE, STANDBY
);
GO
