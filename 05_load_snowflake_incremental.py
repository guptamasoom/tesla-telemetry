# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 05_LOAD_SNOWFLAKE_INCREMENTAL — Final Load Step (Fully Incremental)
# MAGIC
# MAGIC Loads Gold Delta tables into Snowflake — every table incrementally:
# MAGIC
# MAGIC 1. **Event tables** (telemetry_enriched, alerts): only new `source_run_id`s since the last load checkpoint
# MAGIC 2. **Aggregation tables** (vehicle_hourly_metrics, trip_metrics, battery_health): **affected-key load** — only keys (vin+hour / vin+date) touched by new runs. Gold MERGE keeps those rows current, so upserting them into Snowflake curated yields the same result as a full load, at a fraction of the data volume
# MAGIC 3. TRUNCATE → PUT → COPY INTO Snowflake staging tables (staging now holds only the delta)
# MAGIC 4. Call MERGE stored procedure (upserts into curated)
# MAGIC 5. Verify + update checkpoint (only after successful MERGE)
# MAGIC
# MAGIC **Retry-safe:** a crash before the checkpoint means the same batch is re-exported and re-merged — MERGE upserts identical values, no duplicates.

# COMMAND ----------

# DBTITLE 1,Install Snowflake Connector
# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

# DBTITLE 1,Imports & Config
# ============================================================
# IMPORTS & CONFIG
# ============================================================

import os
from pathlib import Path
import snowflake.connector


# --------------------------------------------------------------
# Snowflake connection parameters
# --------------------------------------------------------------
# NOTE: For production, move these to Databricks secrets:
#   dbutils.secrets.get(scope='snowflake', key='password')
# --------------------------------------------------------------

SNOWFLAKE_ACCOUNT = 'PISXLZQ-HA24013'
SNOWFLAKE_USER = 'MASOOMSNOW'
SNOWFLAKE_PASSWORD = 'Mylo@1234567890'

SNOWFLAKE_ROLE = "SYSADMIN"
SNOWFLAKE_WAREHOUSE = "TELEMETRY_WH"
SNOWFLAKE_DATABASE = "TESLA_TELEMETRY"


# --------------------------------------------------------------
# Gold table paths (S3)
# --------------------------------------------------------------

BASE_PATH = (
    "s3://tesla-vehicle-telemetry-511976431596/"
    "telemetry/"
)

GOLD_PATH = f"{BASE_PATH}gold/"

SILVER_PATH = f"{BASE_PATH}silver/telemetry/"


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


# --------------------------------------------------------------
# Snowflake staging tables
# --------------------------------------------------------------

STAGING_TABLES = {
    "telemetry_enriched":
        "STAGING.TELEMETRY_ENRICHED",
    "vehicle_hourly_metrics":
        "STAGING.VEHICLE_HOURLY_METRICS",
    "trip_metrics":
        "STAGING.TRIP_METRICS",
    "battery_health":
        "STAGING.BATTERY_HEALTH",
    "alerts":
        "STAGING.ALERTS",
}


# --------------------------------------------------------------
# Snowflake load checkpoint (tracks last loaded source_run_id)
# Same checkpoint as 05_load_snowflake — the two notebooks
# are interchangeable.
# --------------------------------------------------------------

SNOWFLAKE_CHECKPOINT_PATH = f"{BASE_PATH}checkpoints/snowflake_load/"


print("Config loaded.")
print(f"Snowflake account: {SNOWFLAKE_ACCOUNT}")
print(f"Snowflake user: {SNOWFLAKE_USER}")
print(f"Gold tables: {len(GOLD_TABLES)}")

# COMMAND ----------

# DBTITLE 1,Export Gold Delta to Parquet (Incremental)
# ============================================================
# EXPORT GOLD DELTA TO PARQUET (FULLY INCREMENTAL)
# ============================================================
#
# Incremental strategy:
# - Event-level tables (telemetry_enriched, alerts): only new
#   source_run_ids since last Snowflake load checkpoint
# - Aggregation tables (vehicle_hourly_metrics, trip_metrics,
#   battery_health): AFFECTED-KEY load — only keys touched by
#   the new runs. Gold's MERGE keeps exactly those rows
#   current, so upserting them into curated Snowflake gives
#   the same result as a full load at a fraction of the volume.
# - If no new Gold runs, skip the load entirely
#

from pyspark.sql.functions import current_timestamp

# --- Read Snowflake load checkpoint ---
try:
    ckpt_df = spark.read.format("delta").load(SNOWFLAKE_CHECKPOINT_PATH)
    last_loaded_run_id = ckpt_df.orderBy("loaded_at", ascending=False).first()["last_loaded_run_id"]
    print(f"Last Snowflake load checkpoint: {last_loaded_run_id}")
except Exception:
    last_loaded_run_id = None
    print("No checkpoint found — first run, loading all data")

# --- Discover new runs (Silver is the authoritative source) ---
silver_df = spark.read.format("delta").load(SILVER_PATH)

silver_run_rows = (
    silver_df
    .select("source_run_id")
    .distinct()
    .orderBy("source_run_id")
    .collect()
)

silver_runs = [row["source_run_id"] for row in silver_run_rows]

if last_loaded_run_id is None:
    new_run_ids = silver_runs
else:
    new_run_ids = [r for r in silver_runs if r > last_loaded_run_id]


# --- Check if there's new data ---
if not new_run_ids:

    print(f"\nNo new Gold data since last load.")
    print("Skipping Parquet export and Snowflake load.")
    parquet_files = {}
    new_run_id = last_loaded_run_id

else:

    print(
        f"\nNew Gold data available: {len(new_run_ids)} run(s) "
        f"(after {last_loaded_run_id})"
    )
    new_run_id = max(new_run_ids)

    # --- New batch since last Snowflake load ---
    if last_loaded_run_id is None:
        batch_df = silver_df
    else:
        batch_df = silver_df.filter(
            f"source_run_id > '{last_loaded_run_id}'"
        )

    # --- Affected keys for aggregation tables ---
    affected_hourly_keys = (
        batch_df
        .select("vin", "event_hour")
        .distinct()
    )

    affected_daily_keys = (
        batch_df
        .select("vin", "event_date")
        .distinct()
    )

    print(
        f"Affected keys: "
        f"{affected_hourly_keys.count():,} vehicle-hours, "
        f"{affected_daily_keys.count():,} vehicle-days"
    )

    # --- Create UC Volume for temp Parquet files ---
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.temp_parquet")
    VOLUME_PATH = "/Volumes/workspace/default/temp_parquet"

    parquet_files = {}

    # Event-level tables: incremental (filter by source_run_id)
    INCREMENTAL_TABLES = ["telemetry_enriched", "alerts"]

    # Aggregation tables: affected-key load
    AGGREGATION_TABLES = [
        "vehicle_hourly_metrics",
        "trip_metrics",
        "battery_health",
    ]

    for name in INCREMENTAL_TABLES + AGGREGATION_TABLES:
        path = GOLD_TABLES[name]
        df = spark.read.format("delta").load(path)

        if name in INCREMENTAL_TABLES and last_loaded_run_id is not None:
            df = df.filter(f"source_run_id > '{last_loaded_run_id}'")
            load_type = "incremental"

        elif name in AGGREGATION_TABLES:

            if name == "vehicle_hourly_metrics":
                df = df.join(
                    affected_hourly_keys,
                    on=["vin", "event_hour"],
                    how="left_semi"
                )
            else:
                df = df.join(
                    affected_daily_keys,
                    on=["vin", "event_date"],
                    how="left_semi"
                )

            load_type = "affected-keys"

        else:
            load_type = "full"

        count = df.count()

        if count == 0:
            print(f"  {name}: 0 new records — skipping")
            continue

        output_dir = f"{VOLUME_PATH}/{name}/"
        df.coalesce(1).write.mode("overwrite").parquet(output_dir)

        parquet_parts = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
        local_file = f"{output_dir}{parquet_parts[0]}"
        parquet_files[name] = local_file

        print(f"  {name}: {count:,} records ({load_type}) -> {local_file}")

    print(f"\n{len(parquet_files)} tables exported to Parquet.")
    print(f"New checkpoint will be: {new_run_id}")

# COMMAND ----------

# DBTITLE 1,Connect to Snowflake
# ============================================================
# CONNECT TO SNOWFLAKE
# ============================================================

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    host="PISXLZQ-HA24013.snowflakecomputing.com",
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    session_parameters={"TIMEZONE": "UTC"},
)

print("Connected to Snowflake.")

# COMMAND ----------

# DBTITLE 1,Load Tables into Snowflake
# ============================================================
# LOAD TABLES INTO SNOWFLAKE
# ============================================================
#
# Tables already exist in Snowflake (created via SQL scripts).
# This cell loads data: TRUNCATE -> PUT -> COPY -> MERGE
# Staging now holds only the DELTA (new events + affected
# aggregation keys). MERGE upserts it into curated.
# After MERGE, updates the Snowflake load checkpoint.
#

from pyspark.sql.functions import current_timestamp

# Guard: skip if no new data
if not parquet_files:
    print("No new data to load — skipping Snowflake load.")
else:
    cursor = conn.cursor()

    # Ensure the database and warehouse are set
    cursor.execute("USE DATABASE TESLA_TELEMETRY")
    cursor.execute("USE WAREHOUSE TELEMETRY_WH")

    # --- Load each table ---
    print("Loading tables...")

    for name, file_path in parquet_files.items():

        table = STAGING_TABLES[name]
        file_name = Path(file_path).name

        # Move 1: TRUNCATE staging table
        cursor.execute(f"TRUNCATE TABLE {table}")

        # Move 2: PUT file to Snowflake stage
        cursor.execute(
            f"PUT 'file://{file_path}' "
            "@RAW.TELEMETRY_STAGE "
            "AUTO_COMPRESS = FALSE "
            "OVERWRITE = TRUE"
        )

        # Move 3: COPY INTO staging table
        cursor.execute(
            f"COPY INTO {table} "
            f"FROM @RAW.TELEMETRY_STAGE/{file_name} "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        )

        print(f"  {name}: loaded into {table}")

    # Move 4: MERGE staging into curated
    print("\nRunning MERGE...")
    cursor.execute(
        "CALL TESLA_TELEMETRY.CURATED.MERGE_STAGING_INTO_CURATED()"
    )
    print("  MERGE stored procedure executed.")

    cursor.close()

    # --- Update Snowflake load checkpoint ---
    checkpoint_df = spark.createDataFrame(
        [(new_run_id,)],
        ["last_loaded_run_id"]
    ).withColumn("loaded_at", current_timestamp())

    checkpoint_df.write.format("delta").mode("append").save(SNOWFLAKE_CHECKPOINT_PATH)

    print(f"\nCheckpoint updated: {new_run_id}")
    print("All tables loaded into Snowflake.")

# COMMAND ----------

# DBTITLE 1,Verify & Summary
# ============================================================
# VERIFY & SUMMARY
# ============================================================
#
# NOTE: staging counts are now DELTAS, not full tables:
#   - event tables: new events only
#   - aggregation tables: affected keys only
# Curated counts are the source of truth after MERGE.
#

if not parquet_files:
    print("No data loaded this run — skipping verification.")
    conn.close()
else:
    cursor = conn.cursor()
    cursor.execute("USE DATABASE TESLA_TELEMETRY")

    print("Snowflake staging table counts (delta this load):")
    for name, table in STAGING_TABLES.items():

        cursor.execute(f"SELECT COUNT(*) FROM {table}")

        count = cursor.fetchone()[0]

        print(f"  {table}: {count:,} records")

    print("\nSnowflake CURATED table counts (after MERGE):")
    for name, table in STAGING_TABLES.items():
        curated_table = table.replace("STAGING.", "CURATED.")
        cursor.execute(f"SELECT COUNT(*) FROM {curated_table}")
        curated_count = cursor.fetchone()[0]
        print(f"  {curated_table}: {curated_count:,} records")

    cursor.close()
    conn.close()

    print("\n===================================")
    print("SNOWFLAKE INCREMENTAL LOAD COMPLETED")
    print("===================================")
    print(f"Tables loaded: {len(parquet_files)}")
    print(f"New checkpoint: {new_run_id}")
    print("MERGE procedure: executed")
    print("===================================")

# COMMAND ----------

