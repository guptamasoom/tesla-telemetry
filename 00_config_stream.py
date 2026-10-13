# Databricks notebook source
# DBTITLE 1,Config — Tesla Telemetry Streaming
# MAGIC %md
# MAGIC # 00_CONFIG_STREAM — Tesla Vehicle Telemetry Streaming Pipeline
# MAGIC
# MAGIC Shared configuration for the Tesla telemetry **streaming** pipeline (Structured Streaming + Auto Loader).
# MAGIC
# MAGIC **Notebooks in this pipeline:**
# MAGIC 1. `00_config_stream` — this notebook (paths, schema, constants)
# MAGIC 2. `01_stream_generator` — continuous event generator (writes JSON to S3)
# MAGIC 3. `02_bronze_stream` — Auto Loader → Bronze Delta (streaming)
# MAGIC 4. `03_silver_stream` — Bronze → Silver Delta (streaming transform)
# MAGIC 5. `04_scoring_stream` — Silver → anomaly scoring (streaming)
# MAGIC 6. `05_gold_stream` — Silver → Gold aggregations (streaming windows)
# MAGIC 7. `06_validation_stream` — streaming DQ + query monitoring
# MAGIC
# MAGIC **Usage:** `%run ./00_config_stream` from any downstream notebook.

# COMMAND ----------

# DBTITLE 1,S3 Paths
# ============================================================
# S3 PATHS
# ============================================================

BUCKET = "tesla-vehicle-telemetry-511976431596"

BASE_PATH = f"s3://{BUCKET}/telemetry/"

# Streaming source — generator writes JSON files here
STREAM_RAW_PATH = f"{BASE_PATH}stream/raw/"

# Vehicle state (persisted across runs)
VEHICLE_STATE_PATH = f"{BASE_PATH}vehicle_state/"

# Medallion layer paths
BRONZE_PATH = f"{BASE_PATH}bronze_stream/"
SILVER_PATH = f"{BASE_PATH}silver_stream/telemetry/"
GOLD_PATH = f"{BASE_PATH}gold_stream/"
SCORING_PATH = f"{BASE_PATH}scoring_stream/"
QUARANTINE_PATH = f"{BASE_PATH}quarantine_stream/"

# Streaming checkpoint locations (one per query)
CHECKPOINT_BASE = f"{BASE_PATH}checkpoints/stream/"
BRONZE_CHECKPOINT = f"{CHECKPOINT_BASE}bronze/"
SILVER_CHECKPOINT = f"{CHECKPOINT_BASE}silver/"
GOLD_CHECKPOINT = f"{CHECKPOINT_BASE}gold/"
SCORING_CHECKPOINT = f"{CHECKPOINT_BASE}scoring/"

print("S3 paths configured.")
print(f"  Stream raw   : {STREAM_RAW_PATH}")
print(f"  Bronze       : {BRONZE_PATH}")
print(f"  Silver       : {SILVER_PATH}")
print(f"  Gold         : {GOLD_PATH}")
print(f"  Scoring      : {SCORING_PATH}")
print(f"  Checkpoints  : {CHECKPOINT_BASE}")

# COMMAND ----------

# DBTITLE 1,Streaming Config
# ============================================================
# STREAMING CONFIGURATION
# ============================================================

# Auto Loader
AUTO_LOADER_FORMAT = "json"
AUTO_LOADER_SCHEMA_LOCATION = f"{CHECKPOINT_BASE}schema/"

# Trigger intervals
BRONZE_TRIGGER_SECONDS = 10
SILVER_TRIGGER_SECONDS = 10
GOLD_TRIGGER_SECONDS = 30
SCORING_TRIGGER_SECONDS = 10

# Watermark for late data
WATERMARK_DELAY_SECONDS = 60

# Generator config
BATCH_SIZE = 500
BATCH_INTERVAL_S = 5
GENERATOR_DURATION_S = 0  # 0 = infinite

print("Streaming configuration set.")
print(f"  Bronze trigger   : {BRONZE_TRIGGER_SECONDS}s")
print(f"  Silver trigger   : {SILVER_TRIGGER_SECONDS}s")
print(f"  Gold trigger     : {GOLD_TRIGGER_SECONDS}s")
print(f"  Watermark delay  : {WATERMARK_DELAY_SECONDS}s")
print(f"  Generator batch  : {BATCH_SIZE} events / {BATCH_INTERVAL_S}s")

# COMMAND ----------

# DBTITLE 1,Telemetry Schema
# ============================================================
# TELEMETRY SCHEMA (explicit — same as batch pipeline)
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType
)

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("vehicle_number", LongType(), True),
    StructField("vin", StringType(), True),
    StructField("model", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("software_version", StringType(), True),
    StructField("battery_capacity_kwh", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("battery_soc", DoubleType(), True),
    StructField("battery_temp_c", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("charging_state", StringType(), True),
    StructField("gear", StringType(), True),
    StructField("autopilot_engaged", BooleanType(), True),
    StructField("alert_code", StringType(), True),
])

print(f"Telemetry schema defined: {len(telemetry_schema.fieldNames())} fields.")

# COMMAND ----------

# DBTITLE 1,Vehicle Constants
# ============================================================
# VEHICLE CONSTANTS
# ============================================================

CITIES = {
    "Berlin": (52.5200, 13.4050),
    "Munich": (48.1351, 11.5820),
    "Frankfurt": (50.1109, 8.6820),
    "Hamburg": (53.5511, 9.9937),
    "Stuttgart": (48.7758, 9.1829),
}

MODELS = ["Model 3", "Model Y", "Model S", "Model X"]

SOFTWARE_VERSIONS = [
    "2026.10.1", "2026.11.2", "2026.12.1", "2026.13.3", "2026.14.1"
]

BATTERY_CAPACITIES = [50, 60, 75, 82, 100]

ALERT_CODES = [
    "TIRE_PRESSURE_LOW",
    "BATTERY_HIGH_TEMP",
    "AUTOpilot_WARNING",
    "CHARGING_ERROR",
]

print("Vehicle constants defined.")
print(f"  Cities   : {len(CITIES)}")
print(f"  Models   : {len(MODELS)}")
print(f"  Alerts   : {len(ALERT_CODES)}")