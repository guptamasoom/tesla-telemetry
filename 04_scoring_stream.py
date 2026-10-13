# Databricks notebook source
# DBTITLE 1,Scoring Stream — Tesla Telemetry
# MAGIC %md
# MAGIC # 04_SCORING_STREAM — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Streaming anomaly scoring on Silver telemetry data.
# MAGIC
# MAGIC 1. Read from Silver Delta table (streaming)
# MAGIC 2. Apply real-time anomaly detection rules:
# MAGIC    - Battery temperature critical (>55°C)
# MAGIC    - Battery SOC critical (<10%)
# MAGIC    - Speed anomaly (>200 km/h)
# MAGIC    - Charging error alerts
# MAGIC    - Autopilot disengagement at high speed
# MAGIC 3. Assign risk score (0-100) and risk level (LOW/MEDIUM/HIGH/CRITICAL)
# MAGIC 4. Write scored events to Scoring Delta table (streaming)

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports & Paths
from pyspark.sql import functions as F

print("Scoring Stream — starting...")
print(f"  Source     : {SILVER_PATH}")
print(f"  Scoring    : {SCORING_PATH}")
print(f"  Checkpoint : {SCORING_CHECKPOINT}")

# COMMAND ----------

# DBTITLE 1,Read Silver Stream
# ============================================================
# READ SILVER STREAM
# ============================================================

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
    .withWatermark("event_ts", f"{WATERMARK_DELAY_SECONDS} seconds")
)

print("Silver stream read for scoring.")

# COMMAND ----------

# DBTITLE 1,Anomaly Detection Rules
# ============================================================
# ANOMALY DETECTION RULES
# ============================================================

scored_stream = (
    silver_stream

    # ----------------------------------------------------
    # Individual anomaly flags
    # ----------------------------------------------------

    .withColumn(
        "anomaly_battery_temp_critical",
        F.col("battery_temp_c") > 55
    )

    .withColumn(
        "anomaly_battery_soc_critical",
        F.col("battery_soc") < 10
    )

    .withColumn(
        "anomaly_speed_high",
        F.col("speed_kmh") > 200
    )

    .withColumn(
        "anomaly_charging_error",
        (F.col("event_type") == "alert") &
        (F.col("alert_code") == "CHARGING_ERROR")
    )

    .withColumn(
        "anomaly_autopilot_high_speed",
        (F.col("autopilot_engaged")) &
        (F.col("speed_kmh") > 120)
    )

    .withColumn(
        "anomaly_tire_pressure",
        (F.col("event_type") == "alert") &
        (F.col("alert_code") == "TIRE_PRESSURE_LOW")
    )

    # ----------------------------------------------------
    # Risk score (0-100)
    # ----------------------------------------------------

    .withColumn(
        "risk_score",
        (
            F.when(F.col("anomaly_battery_temp_critical"), 40).otherwise(0)
            + F.when(F.col("anomaly_battery_soc_critical"), 30).otherwise(0)
            + F.when(F.col("anomaly_speed_high"), 25).otherwise(0)
            + F.when(F.col("anomaly_charging_error"), 35).otherwise(0)
            + F.when(F.col("anomaly_autopilot_high_speed"), 20).otherwise(0)
            + F.when(F.col("anomaly_tire_pressure"), 15).otherwise(0)
        )
    )

    # Cap at 100
    .withColumn(
        "risk_score",
        F.when(F.col("risk_score") > 100, 100).otherwise(F.col("risk_score"))
    )

    # ----------------------------------------------------
    # Risk level
    # ----------------------------------------------------

    .withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= 70, "CRITICAL")
        .when(F.col("risk_score") >= 40, "HIGH")
        .when(F.col("risk_score") >= 15, "MEDIUM")
        .otherwise("LOW")
    )

    # ----------------------------------------------------
    # Anomaly summary (concatenated flags)
    # ----------------------------------------------------

    .withColumn(
        "anomaly_flags",
        F.concat_ws(
            "; ",
            F.when(F.col("anomaly_battery_temp_critical"), F.lit("BATTERY_TEMP_CRITICAL")),
            F.when(F.col("anomaly_battery_soc_critical"), F.lit("BATTERY_SOC_CRITICAL")),
            F.when(F.col("anomaly_speed_high"), F.lit("SPEED_HIGH")),
            F.when(F.col("anomaly_charging_error"), F.lit("CHARGING_ERROR")),
            F.when(F.col("anomaly_autopilot_high_speed"), F.lit("AUTOPILOT_HIGH_SPEED")),
            F.when(F.col("anomaly_tire_pressure"), F.lit("TIRE_PRESSURE_LOW")),
        )
    )

    .withColumn("scored_at", F.current_timestamp())
)

print("Anomaly scoring rules defined.")
print("  Rules: battery_temp, battery_soc, speed, charging, autopilot, tire_pressure")
print("  Output: risk_score (0-100), risk_level (LOW/MEDIUM/HIGH/CRITICAL)")

# COMMAND ----------

# DBTITLE 1,Write Scoring Stream
# ============================================================
# WRITE SCORING STREAM (append)
# ============================================================

scoring_query = (
    scored_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SCORING_CHECKPOINT)
    .trigger(processingTime=f"{SCORING_TRIGGER_SECONDS} seconds")
    .queryName("scoring_telemetry_stream")
    .partitionBy("event_date")
    .start(SCORING_PATH)
)

print("Scoring stream started.")
print(f"  Output    : {SCORING_PATH}")
print(f"  Trigger   : {SCORING_TRIGGER_SECONDS}s")
print(f"  Partition : event_date")
print()
print("Monitor with:")
print("  scoring_query.status")
print("  scoring_query.recentProgress")