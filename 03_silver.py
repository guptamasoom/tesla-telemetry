# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 03_SILVER — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Silver layer: Bronze → Silver transformation with:
# MAGIC
# MAGIC 1. Read only new Bronze records (incremental)
# MAGIC 2. Standardize and enrich telemetry data
# MAGIC 3. Derived columns (event_date, event_hour, is_moving, is_charging, battery_band, speed_category)
# MAGIC 4. Append-only Silver Delta
# MAGIC 5. Cross-run event_id idempotency
# MAGIC 6. Checkpoint updated only after successful Silver write

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

BRONZE_PATH = f"{BASE_PATH}bronze/"

SILVER_PATH = f"{BASE_PATH}silver/telemetry/"

CHECKPOINT_PATH = (
    f"{BASE_PATH}checkpoints/silver/"
)

# COMMAND ----------

# DBTITLE 1,Silver Checkpoint Read
# ============================================================
# 2. READ SILVER CHECKPOINT
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


print("Silver checkpoint:")
print("Last processed run:", last_processed_run_id)

# COMMAND ----------

# DBTITLE 1,Discover & Select Bronze Runs
# ============================================================
# 3. DISCOVER NEW BRONZE RUNS
# ============================================================
#
# Bronze is partitioned by source_run_id=...
# (not ingestion_date=.../run_id=... like RAW)
#

bronze_runs = []


try:

    bronze_dirs = dbutils.fs.ls(BRONZE_PATH)

    for d in bronze_dirs:

        if d.isDir() and d.name.startswith("source_run_id="):

            run_id = (
                d.name
                .replace("source_run_id=", "")
                .rstrip("/")
            )

            bronze_runs.append(run_id)

except Exception as e:

    print("Unable to discover Bronze runs.")
    print(str(e))


bronze_runs = sorted(set(bronze_runs))


# ============================================================
# 4. SELECT ONLY NEW RUNS
# ============================================================

if checkpoint_exists:

    new_runs = [
        run_id
        for run_id in bronze_runs
        if run_id > last_processed_run_id
    ]

else:

    new_runs = bronze_runs


print("\nBronze runs discovered:", len(bronze_runs))
print("New runs selected:", len(new_runs))

for run_id in new_runs:
    print(run_id)


if not new_runs:

    print(
        "\nNo new Bronze data."
        " Silver pipeline skipped."
    )

    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
# ============================================================
# 5. READ NEW BRONZE DATA
# ============================================================

if not new_runs:

    bronze_df = None
    raw_count = 0

    print("No new runs to process.")

else:

    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
        .filter(
            F.col("source_run_id").isin(new_runs)
        )
    )

    raw_count = bronze_df.count()

    print(
        f"Bronze records read: {raw_count:,}"
    )

# COMMAND ----------

# DBTITLE 1,Silver Transformation
# ============================================================
# 6. SILVER TRANSFORMATION
# ============================================================
#
# Standardize text columns and add derived columns.
#

if bronze_df is None:

    silver_df = None

    print("No data — skipping Silver transformation.")

else:

    silver_df = (
        bronze_df

        # -------------------------------------------------------------
        # Standardize city names
        # -------------------------------------------------------------
        .withColumn(
            "city",
            F.initcap(F.trim(F.col("city")))
        )

        # -------------------------------------------------------------
        # Standardize software version
        # -------------------------------------------------------------
        .withColumn(
            "software_version",
            F.trim(F.col("software_version"))
        )

        # -------------------------------------------------------------
        # Standardize event type
        # -------------------------------------------------------------
        .withColumn(
            "event_type",
            F.lower(F.trim(F.col("event_type")))
        )

        # -------------------------------------------------------------
        # Standardize charging state
        # -------------------------------------------------------------
        .withColumn(
            "charging_state",
            F.initcap(F.trim(F.col("charging_state")))
        )

        # -------------------------------------------------------------
        # Standardize gear
        # -------------------------------------------------------------
        .withColumn(
            "gear",
            F.upper(F.trim(F.col("gear")))
        )

        # -------------------------------------------------------------
        # Derived: event date
        # -------------------------------------------------------------
        .withColumn(
            "event_date",
            F.to_date(F.col("event_ts"))
        )

        # -------------------------------------------------------------
        # Derived: event hour
        # -------------------------------------------------------------
        .withColumn(
            "event_hour",
            F.date_trunc("hour", F.col("event_ts"))
        )

        # -------------------------------------------------------------
        # Derived: moving flag
        # speed > 1 km/h means vehicle is moving
        # -------------------------------------------------------------
        .withColumn(
            "is_moving",
            F.col("speed_kmh") > 1
        )

        # -------------------------------------------------------------
        # Derived: charging flag
        # -------------------------------------------------------------
        .withColumn(
            "is_charging",
            F.lower(F.col("charging_state")) == "charging"
        )

        # -------------------------------------------------------------
        # Derived: battery band
        # -------------------------------------------------------------
        .withColumn(
            "battery_band",
            F.when(F.col("battery_soc") <= 20, "critical")
             .when(F.col("battery_soc") <= 50, "low")
             .when(F.col("battery_soc") <= 80, "normal")
             .otherwise("high")
        )

        # -------------------------------------------------------------
        # Derived: speed category
        # -------------------------------------------------------------
        .withColumn(
            "speed_category",
            F.when(F.col("speed_kmh") == 0, "stationary")
             .when(F.col("speed_kmh") <= 50, "slow")
             .when(F.col("speed_kmh") <= 100, "normal")
             .otherwise("high_speed")
        )

        # -------------------------------------------------------------
        # Derived: battery temperature status
        # -------------------------------------------------------------
        .withColumn(
            "battery_temperature_status",
            F.when(F.col("battery_temp_c") < 10, "cold")
             .when(F.col("battery_temp_c") <= 45, "normal")
             .otherwise("high")
        )

        # -------------------------------------------------------------
        # Processing metadata
        # -------------------------------------------------------------
        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )

# COMMAND ----------

# DBTITLE 1,Idempotency Check
# ============================================================
# 7. CHECK DUPLICATES AGAINST SILVER
# ============================================================
#
# Same event_id may already exist in Silver
# from a previous run. Use left_anti join to
# prevent duplicates on retry.
#

if silver_df is None:

    silver_new_count = 0

    print("No data — skipping idempotency check.")

else:

    silver_exists = False

    try:

        spark.read.format("delta").load(SILVER_PATH).limit(1).count()

        silver_exists = True

    except Exception:

        silver_exists = False


    if silver_exists:

        existing_event_ids = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
            .select("event_id")
        )

        silver_df = (
            silver_df
            .join(
                existing_event_ids,
                on="event_id",
                how="left_anti"
            )
        )


    # ============================================================
    # 8. COUNT NEW SILVER RECORDS
    # ============================================================

    silver_new_count = silver_df.count()

    print(
        f"\nNew Silver records: {silver_new_count:,}"
    )

# COMMAND ----------

# DBTITLE 1,Write Silver
# ============================================================
# 9. WRITE SILVER
# ============================================================

if silver_df is None:

    print("No data — skipping Silver write.")

else:

    if silver_new_count > 0:

        (
            silver_df

            .write

            .format("delta")

            .mode("append")

            .partitionBy("event_date")

            .save(SILVER_PATH)
        )

        print("Silver write completed.")

    else:

        print("No new records to write.")

# COMMAND ----------

# DBTITLE 1,Verify, Checkpoint & Final Result
# ============================================================
# 10. VERIFY SILVER WRITE
# ============================================================

if bronze_df is None:

    print("No data — skipping verify & checkpoint.")

else:

    silver_written_count = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
        .filter(
            F.col("source_run_id").isin(new_runs)
        )
        .count()
    )

    print(
        f"Records present in Silver for current runs: "
        f"{silver_written_count:,}"
    )


    # ============================================================
    # 11. UPDATE CHECKPOINT
    #
    # Only update checkpoint AFTER successful
    # Silver write + verification.
    # ============================================================

    latest_run_id = max(new_runs)

    checkpoint_record = (
        spark.createDataFrame(

            [
                (latest_run_id,)
            ],

            ["last_processed_run_id"]
        )

        .withColumn(
            "checkpoint_updated_at",
            F.current_timestamp()
        )
    )

    if checkpoint_exists:

        (
            checkpoint_record

            .write

            .format("delta")

            .mode("append")

            .save(CHECKPOINT_PATH)
        )

    else:

        (
            checkpoint_record

            .write

            .format("delta")

            .mode("overwrite")

            .save(CHECKPOINT_PATH)
        )

    print(
        f"Silver checkpoint updated: {latest_run_id}"
    )


    # ============================================================
    # 12. FINAL SUMMARY
    # ============================================================

    print("\n===================================")
    print("SILVER PIPELINE COMPLETED")
    print("===================================")

    print(
        f"Runs processed : {len(new_runs)}"
    )

    print(
        f"Bronze records : {raw_count:,}"
    )

    print(
        f"Silver records : {silver_new_count:,}"
    )

    print(
        f"Checkpoint     : {latest_run_id}"
    )

    print("===================================")