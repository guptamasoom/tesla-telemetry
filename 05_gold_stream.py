# Databricks notebook source
# DBTITLE 1,Gold Stream — Tesla Telemetry
# MAGIC %md
# MAGIC # 05_GOLD_STREAM — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Streaming Gold layer: windowed aggregations from Silver telemetry data using Structured Streaming.
# MAGIC
# MAGIC 1. Read from Silver Delta table (streaming)
# MAGIC 2. Compute real-time windowed aggregations:
# MAGIC    - **vehicle_hourly_metrics**: per-vehicle per-hour aggregates (avg speed, avg SOC, event count)
# MAGIC    - **city_hourly_metrics**: per-city per-hour aggregates (event count, avg speed, alerts)
# MAGIC    - **alert_summary**: alert counts by type per window
# MAGIC 3. Use tumbling windows (1 hour) with watermark
# MAGIC 4. Write to Gold Delta tables (update mode for aggregations)

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports & Paths
from pyspark.sql import functions as F

# Gold table paths
GOLD_VEHICLE_HOURLY = f"{GOLD_PATH}vehicle_hourly_metrics/"
GOLD_CITY_HOURLY = f"{GOLD_PATH}city_hourly_metrics/"
GOLD_ALERT_SUMMARY = f"{GOLD_PATH}alert_summary/"

print("Gold Stream — starting...")
print(f"  Source       : {SILVER_PATH}")
print(f"  Vehicle hourly : {GOLD_VEHICLE_HOURLY}")
print(f"  City hourly    : {GOLD_CITY_HOURLY}")
print(f"  Alert summary  : {GOLD_ALERT_SUMMARY}")

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

print("Silver stream read for Gold aggregations.")

# COMMAND ----------

# DBTITLE 1,Table 1 — Vehicle Hourly Metrics
# ============================================================
# TABLE 1: VEHICLE HOURLY METRICS (tumbling 1-hour window)
# ============================================================

vehicle_hourly = (
    silver_stream
    .groupBy(
        F.window(F.col("event_ts"), "1 hour"),
        F.col("vin"),
        F.col("vehicle_number"),
    )
    .agg(
        F.count("event_id").alias("event_count"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.avg("battery_soc").alias("avg_battery_soc"),
        F.min("battery_soc").alias("min_battery_soc"),
        F.avg("battery_temp_c").alias("avg_battery_temp_c"),
        F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("charging_events"),
        F.sum(F.when(F.col("autopilot_engaged"), 1).otherwise(0)).alias("autopilot_events"),
    )
    .select(
        F.col("vin"),
        F.col("vehicle_number"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_count",
        "avg_speed_kmh",
        "max_speed_kmh",
        "avg_battery_soc",
        "min_battery_soc",
        "avg_battery_temp_c",
        "charging_events",
        "autopilot_events",
    )
)

print("Table 1 defined: vehicle_hourly_metrics (1-hour tumbling window)")

# COMMAND ----------

# DBTITLE 1,Table 2 — City Hourly Metrics
# ============================================================
# TABLE 2: CITY HOURLY METRICS (tumbling 1-hour window)
# ============================================================

city_hourly = (
    silver_stream
    .groupBy(
        F.window(F.col("event_ts"), "1 hour"),
        F.col("city"),
    )
    .agg(
        F.count("event_id").alias("event_count"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.avg("battery_soc").alias("avg_battery_soc"),
        F.avg("battery_temp_c").alias("avg_battery_temp_c"),
        F.sum(F.when(F.col("event_type") == "alert", 1).otherwise(0)).alias("alert_count"),
        F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("charging_events"),
    )
    .select(
        F.col("city"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_count",
        "avg_speed_kmh",
        "avg_battery_soc",
        "avg_battery_temp_c",
        "alert_count",
        "charging_events",
    )
)

print("Table 2 defined: city_hourly_metrics (1-hour tumbling window)")

# COMMAND ----------

# DBTITLE 1,Table 3 — Alert Summary
# ============================================================
# TABLE 3: ALERT SUMMARY (tumbling 1-hour window)
# ============================================================

alert_summary = (
    silver_stream
    .filter(F.col("event_type") == "alert")
    .groupBy(
        F.window(F.col("event_ts"), "1 hour"),
        F.col("alert_code"),
        F.col("city"),
    )
    .agg(
        F.count("event_id").alias("alert_count"),
        F.collect_set("vin").alias("affected_vehicles"),
    )
    .select(
        F.col("alert_code"),
        F.col("city"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "alert_count",
        F.size("affected_vehicles").alias("affected_vehicle_count"),
    )
)

print("Table 3 defined: alert_summary (1-hour tumbling window)")

# COMMAND ----------

# DBTITLE 1,Write Gold Streams
# ============================================================
# WRITE STREAMS — Gold tables (update mode for aggregations)
# ============================================================

# Vehicle hourly metrics
vehicle_hourly_query = (
    vehicle_hourly.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{GOLD_CHECKPOINT}vehicle_hourly/")
    .trigger(processingTime=f"{GOLD_TRIGGER_SECONDS} seconds")
    .queryName("gold_vehicle_hourly_stream")
    .start(GOLD_VEHICLE_HOURLY)
)

print("Vehicle hourly stream started.")

# City hourly metrics
city_hourly_query = (
    city_hourly.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{GOLD_CHECKPOINT}city_hourly/")
    .trigger(processingTime=f"{GOLD_TRIGGER_SECONDS} seconds")
    .queryName("gold_city_hourly_stream")
    .start(GOLD_CITY_HOURLY)
)

print("City hourly stream started.")

# Alert summary
alert_summary_query = (
    alert_summary.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{GOLD_CHECKPOINT}alert_summary/")
    .trigger(processingTime=f"{GOLD_TRIGGER_SECONDS} seconds")
    .queryName("gold_alert_summary_stream")
    .start(GOLD_ALERT_SUMMARY)
)

print("Alert summary stream started.")
print()
print("All Gold streams running.")
print(f"  Trigger : {GOLD_TRIGGER_SECONDS}s")
print(f"  Mode    : update (aggregations)")
print()
print("Monitor with:")
print("  vehicle_hourly_query.status")
print("  city_hourly_query.status")
print("  alert_summary_query.status")