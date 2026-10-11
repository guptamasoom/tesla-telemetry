# Databricks notebook source
# DBTITLE 1,Incremental Gold — Title
# MAGIC %md
# MAGIC # 04_GOLD_INCREMENTAL — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Incremental replacement for 04_gold. Same 5 Gold tables, same checkpoint (`checkpoints/gold/`), same write semantics — but only processes what changed:
# MAGIC
# MAGIC 1. Event-grain tables (`telemetry_enriched`, `alerts`): idempotent append of the **new batch only** (cheap anti-join, no full-Silver shuffle)
# MAGIC 2. Aggregation tables: **affected-key recompute** — keys touched by the batch are recomputed from full history and MERGEd, producing values **identical to a full rebuild** (correct + idempotent + retry-safe)
# MAGIC 3. Silver reads use `event_date` partition pruning + key semi-joins, so untouched time windows are never re-read
# MAGIC 4. Checkpoint still updates only after a successful write (same crash-window safety as 04_gold)

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

# SAME checkpoint as 04_gold — the two notebooks are
# interchangeable on the same Gold tables.
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

    last_processed_run_id = (
        checkpoint_row[0]["last_processed_run_id"]
    )

    checkpoint_exists = True

    print("Gold checkpoint:")
    print(f"Last processed run: {last_processed_run_id}")

except Exception:

    print(
        "No gold checkpoint found — "
        "first run will process all Silver runs."
    )

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


new_runs = [
    r
    for r in silver_runs
    if (
        last_processed_run_id is None
        or r > last_processed_run_id
    )
]

print(f"\nSilver runs discovered: {len(silver_runs)}")
print(f"New runs selected: {len(new_runs)}")

for r in new_runs:
    print(r)

# COMMAND ----------

# DBTITLE 1,Read New Silver Batch
# ============================================================
# 4. READ NEW SILVER BATCH (INCREMENTAL)
# ============================================================
#
# Only the NEW runs — NOT the whole Silver table.
# (04_gold reads all of Silver here; that is the cost
#  this notebook removes.)
#

if not new_runs:

    silver_batch_df = None
    batch_count = 0

    print("No new runs to process.")

else:

    silver_batch_df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
        .filter(
            F.col("source_run_id").isin(new_runs)
        )
    )

    batch_count = silver_batch_df.count()

    print(
        f"New Silver records (incremental batch): "
        f"{batch_count:,}"
    )

# COMMAND ----------

# DBTITLE 1,Build Gold Tables (Incremental)
# ============================================================
# 5. BUILD GOLD TABLES (INCREMENTAL)
# ============================================================
#
# Event-grain tables come from the NEW BATCH only.
#
# Aggregation tables use AFFECTED-KEY RECOMPUTE:
#   1. Keys touched by the batch (vin+hour / vin+date)
#   2. Re-read Silver ONLY for those keys (event_date
#      partition pruning + semi-join)
#   3. Aggregate affected keys from full history → MERGE
#
# Recomputing from full history means the values are
# IDENTICAL to a full rebuild, so this stays idempotent
# and retry-safe (a reprocessed batch MERGEs the same
# values again — no drift, no double counting).
#

if silver_batch_df is None:

    gold_tables = {}

    print("No data — skipping Gold table builds.")

else:

    gold_tables = {}


    # --------------------------------------------------------------
    # Table 1: telemetry_enriched (event grain — batch only)
    # --------------------------------------------------------------
    gold_tables["telemetry_enriched"] = silver_batch_df


    # --------------------------------------------------------------
    # Table 5: alerts (event grain — batch only)
    # --------------------------------------------------------------
    gold_tables["alerts"] = silver_batch_df.filter(
        F.col("event_type") == "alert"
    )


    # --------------------------------------------------------------
    # Affected keys for aggregation tables
    # --------------------------------------------------------------
    affected_hourly_keys = (
        silver_batch_df
        .select("vin", "event_hour")
        .distinct()
    )

    affected_daily_keys = (
        silver_batch_df
        .select("vin", "event_date")
        .distinct()
    )

    affected_dates = [
        row["event_date"]
        for row in (
            silver_batch_df
            .select("event_date")
            .distinct()
            .collect()
        )
    ]

    print(
        f"Affected keys: "
        f"{affected_hourly_keys.count():,} vehicle-hours, "
        f"{affected_daily_keys.count():,} vehicle-days, "
        f"{len(affected_dates)} event_date partitions"
    )


    # --------------------------------------------------------------
    # Silver rows for affected keys only
    # (partition pruning on event_date + key semi-join)
    # --------------------------------------------------------------
    silver_for_keys = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
        .filter(
            F.col("event_date").isin(affected_dates)
        )
    )

    hourly_source = (
        silver_for_keys
        .join(
            affected_hourly_keys,
            on=["vin", "event_hour"],
            how="left_semi"
        )
    )

    daily_source = (
        silver_for_keys
        .join(
            affected_daily_keys,
            on=["vin", "event_date"],
            how="left_semi"
        )
    )


    # --------------------------------------------------------------
    # Table 2: vehicle_hourly_metrics (affected vehicle-hours)
    # --------------------------------------------------------------
    gold_tables["vehicle_hourly_metrics"] = (
        hourly_source
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
    # Table 3: trip_metrics (affected vehicle-days, moving only)
    # --------------------------------------------------------------
    moving_df = daily_source.filter(F.col("is_moving"))

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
    # Table 4: battery_health (affected vehicle-days)
    # --------------------------------------------------------------
    gold_tables["battery_health"] = (
        daily_source
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


    print("Gold tables built (incremental):")
    for name in gold_tables:
        print(f"  {name}")

# COMMAND ----------

# DBTITLE 1,Write Gold Tables
# ============================================================
# 6. WRITE GOLD TABLES
# ============================================================
#
# Strategy (same as 04_gold):
# - telemetry_enriched & alerts: append with idempotency
#   (source is the small BATCH now — cheap anti-join)
# - aggregation tables: MERGE (upsert by key)
#   (source is only the AFFECTED KEYS — small MERGE)
#
# First run (table doesn't exist): create with append.
# Subsequent runs: MERGE or idempotent append.
#

if silver_batch_df is None:

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
# 7. VERIFY GOLD TABLES
# ============================================================

if silver_batch_df is None:

    print("No data — skipping verify & checkpoint.")

else:

    print("Gold table record counts:")

    for name, path in GOLD_TABLES.items():

        count = (
            spark.read.format("delta").load(path).count()
        )

        print(f"  {name}: {count:,} records")


    # ============================================================
    # 8. UPDATE CHECKPOINT
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
    # 9. FINAL SUMMARY
    # ============================================================

    print("\n===================================")
    print("GOLD PIPELINE (INCREMENTAL) COMPLETED")
    print("===================================")
    print(f"New Silver records (batch) : {batch_count:,}")
    print(f"Gold tables written        : {len(GOLD_TABLES)}")
    print(f"Checkpoint                 : {latest_run_id}")
    print("===================================")

# COMMAND ----------

