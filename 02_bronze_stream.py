# Databricks notebook source
# DBTITLE 1,Bronze Stream — Tesla Telemetry
# MAGIC %md
# MAGIC # 02_BRONZE_STREAM — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Streaming Bronze layer using **Auto Loader** to ingest JSON files from S3 in real-time.
# MAGIC
# MAGIC 1. Auto Loader reads JSON from `stream/raw/`
# MAGIC 2. Explicit schema applied (no schema inference drift)
# MAGIC 3. Basic data quality checks (null event_id, null timestamps)
# MAGIC 4. Valid records → Bronze Delta (append)
# MAGIC 5. Invalid records → Quarantine Delta (append)
# MAGIC 6. Streaming write with checkpoint for exactly-once semantics

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports & Paths
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Bronze Stream — starting...")
print(f"  Source     : {STREAM_RAW_PATH}")
print(f"  Bronze     : {BRONZE_PATH}")
print(f"  Quarantine : {QUARANTINE_PATH}")
print(f"  Checkpoint : {BRONZE_CHECKPOINT}")

# COMMAND ----------

# DBTITLE 1,Auto Loader Stream
# ============================================================
# AUTO LOADER — READ STREAMING JSON FROM S3
# ============================================================

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", AUTO_LOADER_FORMAT)
    .option("cloudFiles.schemaLocation", AUTO_LOADER_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "false")
    .schema(telemetry_schema)
    .load(STREAM_RAW_PATH)
)

# Add ingestion metadata
raw_stream = (
    raw_stream
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

print("Auto Loader stream defined.")
print(f"  Schema fields: {len(telemetry_schema.fieldNames())}")

# COMMAND ----------

# DBTITLE 1,Data Quality Rules
# ============================================================
# DATA QUALITY CHECKS
# ============================================================

validated_stream = (
    raw_stream

    # Flag invalid records
    .withColumn("_invalid_event_id", F.col("event_id").isNull())
    .withColumn("_invalid_event_ts", F.col("event_ts").isNull())
    .withColumn("_invalid_speed", (F.col("speed_kmh") < 0) | (F.col("speed_kmh") > 300))
    .withColumn("_invalid_soc", (F.col("battery_soc") < 0) | (F.col("battery_soc") > 100))
    .withColumn("_invalid_temp", (F.col("battery_temp_c") < -20) | (F.col("battery_temp_c") > 80))

    # Overall validity
    .withColumn(
        "_is_valid",
        ~(
            F.col("_invalid_event_id")
            | F.col("_invalid_event_ts")
            | F.col("_invalid_speed")
            | F.col("_invalid_soc")
            | F.col("_invalid_temp")
        )
    )

    # Build rejection reason for invalid records
    .withColumn(
        "_rejection_reason",
        F.when(F.col("_is_valid"), F.lit(None).cast("string"))
        .otherwise(
            F.concat_ws(
                "; ",
                F.when(F.col("_invalid_event_id"), F.lit("NULL_EVENT_ID")),
                F.when(F.col("_invalid_event_ts"), F.lit("NULL_EVENT_TS")),
                F.when(F.col("_invalid_speed"), F.lit("INVALID_SPEED")),
                F.when(F.col("_invalid_soc"), F.lit("INVALID_SOC")),
                F.when(F.col("_invalid_temp"), F.lit("INVALID_TEMP")),
            )
        )
    )
)

# Split into valid and invalid
valid_stream = validated_stream.filter(F.col("_is_valid"))
quarantine_stream = (
    validated_stream
    .filter(~F.col("_is_valid"))
    .select(
        "event_id", "event_ts", "vehicle_number", "vin",
        "event_type", "speed_kmh", "battery_soc", "battery_temp_c",
        "_rejection_reason", "_ingest_ts", "_source_file",
    )
)

# Clean up valid stream — drop helper columns
bronze_stream = (
    valid_stream
    .drop(
        "_invalid_event_id", "_invalid_event_ts",
        "_invalid_speed", "_invalid_soc", "_invalid_temp",
        "_is_valid", "_rejection_reason",
    )
)

print("DQ checks applied.")
print("  Valid records → Bronze Delta")
print("  Invalid records → Quarantine Delta")

# COMMAND ----------

# DBTITLE 1,Write Quarantine Stream
# ============================================================
# WRITE STREAM — QUARANTINE (append)
# ============================================================

quarantine_query = (
    quarantine_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{BRONZE_CHECKPOINT}quarantine/")
    .trigger(processingTime=f"{BRONZE_TRIGGER_SECONDS} seconds")
    .queryName("bronze_quarantine_stream")
    .start(QUARANTINE_PATH)
)

print("Quarantine stream started.")

# COMMAND ----------

# DBTITLE 1,Write Bronze Stream
# ============================================================
# WRITE STREAM — BRONZE (append)
# ============================================================

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(processingTime=f"{BRONZE_TRIGGER_SECONDS} seconds")
    .queryName("bronze_telemetry_stream")
    .start(BRONZE_PATH)
)

print("Bronze stream started.")
print(f"  Output  : {BRONZE_PATH}")
print(f"  Trigger : {BRONZE_TRIGGER_SECONDS}s")
print()
print("Monitor with:")
print("  bronze_query.status")
print("  bronze_query.recentProgress")