# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 04_GOLD — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Gold layer: Silver → Gold transformation with:
# MAGIC
# MAGIC 1. Read only new Silver records (incremental via checkpoint)
# MAGIC 2. Full rebuild of 5 Gold tables from ALL Silver data
# MAGIC 3. Tables: telemetry_enriched, vehicle_hourly_metrics, trip_metrics, battery_health, alerts
# MAGIC 4. Overwrite mode (idempotent by design)
# MAGIC 5. Checkpoint updated only after successful Gold write

# COMMAND ----------

# DBTITLE 1,Imports & Paths
# ============================================================
# 1. IMPORTS & PATHS
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable


BASE_PATH = (
    "s3://tesla-vehicle-telemetry-511976431596/"
    "telemetry/"
)

SILVER_PATH = f"{BASE_PATH}silver/telemetry/"

GOLD_PATH = f"{BASE_PATH}gold/"


GOLD_TABLES = {
    "telemetry_enriched":
        f"{GOLD_PATH}telemetry_enriched/",
    "vehicle_hourly_metrics":
        f"{GOLD_PATH}vehicle_hourly_metrics/",
    "trip_metrics":
        f"{GOLD_PATH}trip_metrics/",
    "battery_health":
        f"{GOLD_PATH}battery_health/",
    "alerts":
        f"{GOLD_PATH}alerts/",
}

CHECKPOINT_PATH = (
    f"{BASE_PATH}checkpoints/gold/"
)

# COMMAND ----------

# DBTITLE 1,Gold Checkpoint Read
# ============================================================
# 2. READ GOLD CHECKPOINT
# ============================================================

checkpoint_exists = False
last_processed_run_id = None


try:

    checkpoint_df = (
        spark.read
        .format("delta")
        .load(CHECKPOINT_PATH)
    )

    checkpoint_row = (
        checkpoint_df
        .orderBy(
            F.col("checkpoint_updated_at").desc()
        )
        .limit(1)
        .collect()
    )

    checkpoint_exists = True

    if checkpoint_row:

        last_processed_run_id = (
            checkpoint_row[0][
                "last_processed_run_id"
            ]
        )

except Exception:

    checkpoint_exists = False


print("Gold checkpoint:")
print("Last processed run:", last_processed_run_id)

# COMMAND ----------

# DBTITLE 1,Discover & Select Silver Runs
# ============================================================
# 3. DISCOVER NEW SILVER RUNS
# ============================================================
#
# Silver is partitioned by event_date, not source_run_id.
# So we read distinct source_run_ids from Silver Delta.
#

silver_runs = []


try:

    silver_run_rows = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
        .select("source_run_id")
        .distinct()
        .orderBy("source_run_id")
        .collect()
    )

    silver_runs = [
        row["source_run_id"]
        for row in silver_run_rows
    ]

except Exception as e:

    print("Unable to discover Silver runs.")
    print(str(e))


silver_runs = sorted(set(silver_runs))


# ============================================================
# 4. SELECT ONLY NEW RUNS
# ============================================================

if checkpoint_exists:

    new_runs = [
        run_id
        for run_id in silver_runs
        if run_id > last_processed_run_id
    ]

else:

    new_runs = silver_runs


print("\nSilver runs discovered:", len(silver_runs))
print("New runs selected:", len(new_runs))

for run_id in new_runs:
    print(run_id)


if not new_runs:

    print(
        "\nNo new Silver data."
        " Gold pipeline skipped."
    )

    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# DBTITLE 1,Read Silver Data
# ============================================================
# 5. READ SILVER DATA (FULL REBUILD)
# ============================================================
#
# Gold aggregation tables need ALL Silver records
# for correct aggregates — not just new records.
# So we read the entire Silver table.
#

if not new_runs:

    silver_df = None
    silver_count = 0

    print("No new runs to process.")

else:

    silver_df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    silver_count = silver_df.count()

    print(
        f"Silver records read (full rebuild): "
        f"{silver_count:,}"
    )

# COMMAND ----------

# DBTITLE 1,Build Gold Tables
# ============================================================
# 6. BUILD GOLD TABLES
# ============================================================

if silver_df is None:

    gold_tables = {}

    print("No data — skipping Gold table builds.")

else:

    gold_tables = {}


    # --------------------------------------------------------------
    # Table 1: telemetry_enriched
    # Event-level data from Silver (all columns)
    # --------------------------------------------------------------
    gold_tables["telemetry_enriched"] = silver_df


    # --------------------------------------------------------------
    # Table 2: vehicle_hourly_metrics
    # One row per vehicle per hour
    # --------------------------------------------------------------
    gold_tables["vehicle_hourly_metrics"] = (
        silver_df
        .groupBy("vin", "event_hour")
        .agg(
            F.count("event_id").alias("events"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.avg("battery_soc").alias("avg_battery_soc"),
            F.min("battery_soc").alias("min_battery_soc"),
            F.max("battery_temp_c").alias("max_battery_temp_c"),
            F.sum(F.col("is_moving").cast("int")).alias("moving_events"),
            F.sum(F.col("is_charging").cast("int")).alias("charging_events"),
            F.max("odometer_km").alias("latest_odometer_km"),
        )
    )


    # --------------------------------------------------------------
    # Table 3: trip_metrics
    # One row per vehicle per day, only moving events
    # --------------------------------------------------------------
    moving_df = silver_df.filter(F.col("is_moving"))

    gold_tables["trip_metrics"] = (
        moving_df
        .groupBy("vin", "event_date")
        .agg(
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
            F.min("odometer_km").alias("start_odometer_km"),
            F.max("odometer_km").alias("end_odometer_km"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.sum(
                F.col("autopilot_engaged").cast("int")
            ).alias("autopilot_events"),
        )
        .withColumn(
            "distance_km",
            F.col("end_odometer_km")
            - F.col("start_odometer_km")
        )
    )


    # --------------------------------------------------------------
    # Table 4: battery_health
    # One row per vehicle per day
    # --------------------------------------------------------------
    gold_tables["battery_health"] = (
        silver_df
        .groupBy("vin", "event_date")
        .agg(
            F.avg("battery_soc").alias("avg_battery_soc"),
            F.min("battery_soc").alias("min_battery_soc"),
            F.avg("battery_temp_c").alias("avg_battery_temp_c"),
            F.max("battery_temp_c").alias("max_battery_temp_c"),
            F.sum(
                F.col("is_charging").cast("int")
            ).alias("charge_events"),
        )
    )


    # --------------------------------------------------------------
    # Table 5: alerts
    # Only alert events
    # (Note: our generator only produces "telemetry" events,
    #  so this table will be empty for current data.)
    # --------------------------------------------------------------
    gold_tables["alerts"] = silver_df.filter(
        F.col("event_type") == "alert"
    )

    print("Gold tables built:")
    for name in gold_tables:
        print(f"  {name}")

# COMMAND ----------

# DBTITLE 1,Write Gold Tables
# ============================================================
# 7. WRITE GOLD TABLES
# ============================================================
#
# Strategy:
# - telemetry_enriched & alerts: append with idempotency
# - aggregation tables: MERGE (upsert by key)
#
# First run (table doesn't exist): create with append.
# Subsequent runs: MERGE or idempotent append.
#

if silver_df is None:

    print("No data — skipping Gold writes.")

else:

    for name, df in gold_tables.items():

        table_path = GOLD_TABLES[name]

        # Check if table exists
        table_exists = False

        try:
            spark.read.format("delta").load(table_path).limit(1).count()
            table_exists = True
        except Exception:
            table_exists = False


        if not table_exists:

            # First run: create table with append
            if name in ("telemetry_enriched", "alerts"):
                df.write.format("delta").mode("append").partitionBy("event_date").save(table_path)
            else:
                df.write.format("delta").mode("append").save(table_path)

            print(f"  {name}: created (first run)")

        elif name in ("telemetry_enriched", "alerts"):

            # Idempotent append: only new event_ids
            existing_ids = (
                spark.read.format("delta").load(table_path)
                .select("event_id")
            )

            new_df = df.join(existing_ids, on="event_id", how="left_anti")

            new_count = new_df.count()

            if new_count > 0:
                new_df.write.format("delta").mode("append").partitionBy("event_date").save(table_path)
                print(f"  {name}: {new_count:,} new records appended")
            else:
                print(f"  {name}: no new records (all already in Gold)")

        else:

            # MERGE for aggregation tables (upsert by key)
            if name == "vehicle_hourly_metrics":
                merge_keys = ["vin", "event_hour"]
            elif name == "trip_metrics":
                merge_keys = ["vin", "event_date"]
            elif name == "battery_health":
                merge_keys = ["vin", "event_date"]

            delta_table = DeltaTable.forPath(spark, table_path)

            merge_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )

            update_set = {
                c: f"source.{c}"
                for c in df.columns
                if c not in merge_keys
            }

            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(f"  {name}: merged (upserted)")

    print("\nAll Gold tables written.")

# COMMAND ----------

# DBTITLE 1,Verify, Checkpoint & Final Result
# ============================================================
# 8. VERIFY GOLD TABLES
# ============================================================

if silver_df is None:

    print("No data — skipping verify & checkpoint.")

else:

    print("Gold table record counts:")

    for name, path in GOLD_TABLES.items():

        count = (
            spark.read.format("delta").load(path).count()
        )

        print(f"  {name}: {count:,} records")


    # ============================================================
    # 9. UPDATE CHECKPOINT
    # ============================================================

    latest_run_id = max(new_runs)

    checkpoint_record = (
        spark.createDataFrame(
            [(latest_run_id,)],
            ["last_processed_run_id"]
        )
        .withColumn(
            "checkpoint_updated_at",
            F.current_timestamp()
        )
    )

    if checkpoint_exists:

        checkpoint_record.write.format("delta").mode("append").save(CHECKPOINT_PATH)

    else:

        checkpoint_record.write.format("delta").mode("overwrite").save(CHECKPOINT_PATH)

    print(f"\nGold checkpoint updated: {latest_run_id}")


    # ============================================================
    # 10. FINAL SUMMARY
    # ============================================================

    print("\n===================================")
    print("GOLD PIPELINE COMPLETED")
    print("===================================")
    print(f"Silver records (full rebuild): {silver_count:,}")
    print(f"Gold tables written: {len(GOLD_TABLES)}")
    print(f"Checkpoint: {latest_run_id}")
    print("===================================")