# Databricks notebook source
# DBTITLE 1,Silver Stream — Tesla Telemetry
# MAGIC %md
# MAGIC # 03_SILVER_STREAM — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Streaming Silver layer: Bronze → Silver transformation using Structured Streaming.
# MAGIC
# MAGIC 1. Read from Bronze Delta table (streaming)
# MAGIC 2. Standardize text columns (city, event_type, charging_state, gear)
# MAGIC 3. Derive columns (event_date, event_hour, is_moving, is_charging, battery_band, speed_category)
# MAGIC 4. Watermark for late data handling
# MAGIC 5. Append to Silver Delta table (streaming)

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports & Paths
from pyspark.sql import functions as F

print("Silver Stream — starting...")
print(f"  Source     : {BRONZE_PATH}")
print(f"  Silver     : {SILVER_PATH}")
print(f"  Checkpoint : {SILVER_CHECKPOINT}")

# COMMAND ----------

# DBTITLE 1,Read Bronze Stream
# ============================================================
# READ BRONZE STREAM
# ============================================================

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# Apply watermark for late data
bronze_stream = (
    bronze_stream
    .withWatermark("event_ts", f"{WATERMARK_DELAY_SECONDS} seconds")
)

print("Bronze stream read.")
print(f"  Watermark : {WATERMARK_DELAY_SECONDS}s")

# COMMAND ----------

# DBTITLE 1,Silver Transformation
# ============================================================
# SILVER TRANSFORMATION
# ============================================================

silver_stream = (
    bronze_stream

    # ----------------------------------------------------
    # Standardize text columns
    # ----------------------------------------------------
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("software_version", F.trim(F.col("software_version")))
    .withColumn("event_type", F.lower(F.col("event_type")))
    .withColumn("charging_state", F.initcap(F.col("charging_state")))
    .withColumn("gear", F.upper(F.col("gear")))

    # ----------------------------------------------------
    # Derived columns
    # ----------------------------------------------------
    .withColumn("event_date", F.to_date(F.col("event_ts")))
    .withColumn("event_hour", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_moving", F.col("speed_kmh") > 1)
    .withColumn("is_charging", F.col("charging_state") == "Connected")

    # Battery band
    .withColumn(
        "battery_band",
        F.when(F.col("battery_soc") < 20, "CRITICAL")
        .when(F.col("battery_soc") < 40, "LOW")
        .when(F.col("battery_soc") < 70, "MEDIUM")
        .otherwise("HIGH")
    )

    # Speed category
    .withColumn(
        "speed_category",
        F.when(F.col("speed_kmh") < 1, "STATIONARY")
        .when(F.col("speed_kmh") < 50, "SLOW")
        .when(F.col("speed_kmh") < 100, "NORMAL")
        .when(F.col("speed_kmh") < 130, "FAST")
        .otherwise("VERY_FAST")
    )

    # Battery temperature status
    .withColumn(
        "battery_temperature_status",
        F.when(F.col("battery_temp_c") < 10, "COLD")
        .when(F.col("battery_temp_c") < 35, "NORMAL")
        .when(F.col("battery_temp_c") < 45, "WARM")
        .otherwise("HOT")
    )

    # Silver processed timestamp
    .withColumn("silver_processed_at", F.current_timestamp())
)

print("Silver transformation defined.")
print("  Standardized: city, event_type, charging_state, gear")
print("  Derived: event_date, event_hour, is_moving, is_charging")
print("  Derived: battery_band, speed_category, battery_temperature_status")

# COMMAND ----------

# DBTITLE 1,Write Silver Stream
# ============================================================
# WRITE SILVER STREAM (append)
# ============================================================

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime=f"{SILVER_TRIGGER_SECONDS} seconds")
    .queryName("silver_telemetry_stream")
    .partitionBy("event_date")
    .start(SILVER_PATH)
)

print("Silver stream started.")
print(f"  Output    : {SILVER_PATH}")
print(f"  Trigger   : {SILVER_TRIGGER_SECONDS}s")
print(f"  Partition : event_date")
print()
print("Monitor with:")
print("  silver_query.status")
print("  silver_query.recentProgress")