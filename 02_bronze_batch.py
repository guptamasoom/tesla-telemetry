# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 02_BRONZE — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Production-style incremental Bronze ingestion with:
# MAGIC
# MAGIC 1. Explicit schema
# MAGIC 2. Incremental RAW run discovery
# MAGIC 3. Checkpoint
# MAGIC 4. Cross-run event_id idempotency
# MAGIC 5. Data-quality quarantine
# MAGIC 6. Append-only Bronze Delta
# MAGIC 7. Checkpoint updated only after successful Bronze write

# COMMAND ----------

# DBTITLE 1,Imports, Paths & Schema
# ============================================================
# 1. IMPORTS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    BooleanType
)
from delta.tables import DeltaTable


# ============================================================
# 2. PATH CONFIGURATION
# ============================================================

BASE_PATH = (
    "s3://tesla-vehicle-telemetry-511976431596/"
    "telemetry/"
)

RAW_PATH = f"{BASE_PATH}raw/"

BRONZE_PATH = f"{BASE_PATH}bronze/"

QUARANTINE_PATH = f"{BASE_PATH}quarantine/"

CHECKPOINT_PATH = (
    f"{BASE_PATH}checkpoints/bronze/"
)


# ============================================================
# 3. EXPLICIT TELEMETRY SCHEMA
# ============================================================

telemetry_schema = StructType([

    StructField(
        "event_id",
        StringType(),
        True
    ),

    StructField(
        "vin",
        StringType(),
        True
    ),

    StructField(
        "event_ts",
        TimestampType(),
        True
    ),

    StructField(
        "ingest_ts",
        TimestampType(),
        True
    ),

    StructField(
        "event_type",
        StringType(),
        True
    ),

    StructField(
        "city",
        StringType(),
        True
    ),

    StructField(
        "latitude",
        DoubleType(),
        True
    ),

    StructField(
        "longitude",
        DoubleType(),
        True
    ),

    StructField(
        "speed_kmh",
        DoubleType(),
        True
    ),

    StructField(
        "battery_soc",
        DoubleType(),
        True
    ),

    StructField(
        "battery_temp_c",
        DoubleType(),
        True
    ),

    StructField(
        "odometer_km",
        DoubleType(),
        True
    ),

    StructField(
        "charging_state",
        StringType(),
        True
    ),

    StructField(
        "gear",
        StringType(),
        True
    ),

    StructField(
        "autopilot_engaged",
        BooleanType(),
        True
    ),

    StructField(
        "alert_code",
        StringType(),
        True
    ),

    StructField(
        "software_version",
        StringType(),
        True
    )
])

# COMMAND ----------

# DBTITLE 1,Checkpoint Read
# ============================================================
# 4. CHECKPOINT READ
# ============================================================

checkpoint_exists = False
last_processed_run_id = None


try:

    checkpoint_df = (
        spark.read
        .format("delta")
        .load(CHECKPOINT_PATH)
    )

    latest_checkpoint = (
        checkpoint_df
        .filter(
            F.col("pipeline_name") == "bronze"
        )
        .orderBy(
            F.col("checkpoint_updated_at").desc()
        )
        .select(
            "last_processed_run_id"
        )
        .first()
    )

    checkpoint_exists = True

    if latest_checkpoint:

        last_processed_run_id = (
            latest_checkpoint[
                "last_processed_run_id"
            ]
        )

        print(
            "Last successful run:",
            last_processed_run_id
        )

    else:

        print(
            "Checkpoint table exists "
            "but no Bronze checkpoint found."
        )

except Exception:

    print(
        "No checkpoint found."
        " Treating this as first load."
    )

# COMMAND ----------

# DBTITLE 1,Discover & Select Runs
# ============================================================
# 5. DISCOVER RAW RUNS
# ============================================================

raw_dates = dbutils.fs.ls(RAW_PATH)

raw_runs = []


for date_path in raw_dates:

    if "ingestion_date=" not in date_path.name:
        continue

    run_paths = dbutils.fs.ls(
        date_path.path
    )

    for run_path in run_paths:

        if "run_id=" not in run_path.name:
            continue

        run_id = (
            run_path.name
            .rstrip("/")
            .replace(
                "run_id=",
                ""
            )
        )

        raw_runs.append({

            "run_id": run_id,

            "path": run_path.path

        })


# Always process runs in chronological order.

raw_runs = sorted(
    raw_runs,
    key=lambda x: x["run_id"]
)


print("RAW runs discovered:")

for run in raw_runs:

    print(
        run["run_id"],
        "→",
        run["path"]
    )


# ============================================================
# 6. SELECT UNPROCESSED RUNS
# ============================================================

if last_processed_run_id is None:

    new_runs = raw_runs

else:

    new_runs = [

        run

        for run in raw_runs

        if run["run_id"]
        > last_processed_run_id

    ]


print("\nRuns selected for processing:")

for run in new_runs:

    print(
        run["run_id"]
    )


if not new_runs:

    print(
        "\nNo new RAW runs."
    )

    dbutils.notebook.exit(
        "NO_NEW_DATA"
    )

# COMMAND ----------

# DBTITLE 1,Read RAW Data
# ============================================================
# 7. READ ONLY NEW RAW RUNS
# ============================================================

raw_df = None


for run in new_runs:

    print(
        f"Reading run {run['run_id']}"
    )

    current_df = (

        spark.read

        .schema(
            telemetry_schema
        )

        .json(
            run["path"]
        )

        .withColumn(
            "source_run_id",
            F.lit(
                run["run_id"]
            )
        )

    )


    if raw_df is None:

        raw_df = current_df

    else:

        raw_df = (
            raw_df
            .unionByName(
                current_df
            )
        )


# ============================================================
# 8. BASIC INPUT COUNT
# ============================================================

if raw_df is None:

    raw_count = 0

    print(
        "No RAW records to read."
    )

else:

    raw_count = raw_df.count()

    print(
        f"RAW records read: {raw_count:,}"
    )

# COMMAND ----------

# DBTITLE 1,Data Quality Rules
# ============================================================
# 9. DATA QUALITY RULES
# ============================================================

if raw_df is None:

    validated_df = None

    print("No data — skipping DQ rules.")

else:

    validated_df = (

        raw_df

        # --------------------------------------------------------
        # Required fields
        # --------------------------------------------------------

        .withColumn(
            "invalid_event_id",
            F.col("event_id").isNull()
        )

        .withColumn(
            "invalid_vin",
            F.col("vin").isNull()
        )

        .withColumn(
            "invalid_event_ts",
            F.col("event_ts").isNull()
        )

        .withColumn(
            "invalid_ingest_ts",
            F.col("ingest_ts").isNull()
        )

        # --------------------------------------------------------
        # Speed
        # --------------------------------------------------------

        .withColumn(
            "invalid_speed",

            F.col("speed_kmh").isNotNull()
            &
            (
                F.col("speed_kmh") < 0
            )
        )

        # --------------------------------------------------------
        # Battery
        # --------------------------------------------------------

        .withColumn(
            "invalid_battery_soc",

            F.col("battery_soc").isNotNull()
            &
            (
                (F.col("battery_soc") < 0)
                |
                (F.col("battery_soc") > 100)
            )
        )

        # --------------------------------------------------------
        # Timestamp relationship
        # --------------------------------------------------------

        .withColumn(
            "invalid_time",

            F.col("event_ts").isNotNull()
            &
            F.col("ingest_ts").isNotNull()
            &
            (
                F.col("event_ts")
                >
                F.col("ingest_ts")
            )
        )

        # --------------------------------------------------------
        # Odometer
        # --------------------------------------------------------

        .withColumn(
            "invalid_odometer",

            (
                F.col("speed_kmh") > 0
            )
            &
            (
                F.col("odometer_km").isNull()
                |
                (
                    F.col("odometer_km")
                    <= 0
                )
            )
        )

        # --------------------------------------------------------
        # Alert
        # --------------------------------------------------------

        .withColumn(
            "invalid_alert",

            (
                F.col("event_type")
                == "alert"
            )
            &
            F.col("alert_code").isNull()
        )
    )

# COMMAND ----------

# DBTITLE 1,Duplicates & Idempotency
# ============================================================
# 10. DUPLICATES INSIDE CURRENT BATCH
# ============================================================

if validated_df is None:

    print("No data — skipping duplicate & idempotency checks.")

else:

    duplicate_ids = (

        raw_df

        .groupBy(
            "event_id"
        )

        .count()

        .filter(
            F.col("count") > 1
        )

        .select(
            "event_id"
        )

        .withColumn(
            "duplicate_event_id",
            F.lit(True)
        )
    )


    validated_df = (

        validated_df

        .join(
            duplicate_ids,
            on="event_id",
            how="left"
        )

        .withColumn(
            "duplicate_event_id",

            F.coalesce(
                F.col(
                    "duplicate_event_id"
                ),
                F.lit(False)
            )
        )
    )


    # ============================================================
    # 11. CROSS-RUN IDEMPOTENCY
    # ============================================================
    #
    # IMPORTANT:
    #
    # Same event_id may already exist in Bronze
    # from a previous run.
    #
    # We don't want to insert it again.
    #


    if DeltaTable.isDeltaTable(
        spark,
        BRONZE_PATH
    ):

        existing_event_ids = (

            spark.read

            .format("delta")

            .load(
                BRONZE_PATH
            )

            .select(
                "event_id"
            )
        )

        validated_df = (

            validated_df

            .join(
                existing_event_ids
                .withColumn(
                    "already_in_bronze",
                    F.lit(True)
                ),

                on="event_id",

                how="left"
            )

            .withColumn(
                "already_in_bronze",

                F.coalesce(
                    F.col(
                        "already_in_bronze"
                    ),
                    F.lit(False)
                )
            )
        )

    else:

        validated_df = (

            validated_df

            .withColumn(
                "already_in_bronze",
                F.lit(False)
            )
        )

# COMMAND ----------

# DBTITLE 1,Rejection Reason & Split
# ============================================================
# 12. BUILD REJECTION REASON
# ============================================================

if validated_df is None:

    valid_df = None
    quarantine_df = None

    print("No data — skipping rejection & split.")

else:

    validated_df = (

        validated_df

        .withColumn(
            "rejection_reason",

            F.concat_ws(

                "; ",

                F.when(
                    F.col(
                        "invalid_event_id"
                    ),
                    "NULL_EVENT_ID"
                ),

                F.when(
                    F.col(
                        "invalid_vin"
                    ),
                    "NULL_VIN"
                ),

                F.when(
                    F.col(
                        "invalid_event_ts"
                    ),
                    "NULL_EVENT_TS"
                ),

                F.when(
                    F.col(
                        "invalid_ingest_ts"
                    ),
                    "NULL_INGEST_TS"
                ),

                F.when(
                    F.col(
                        "invalid_speed"
                    ),
                    "INVALID_SPEED"
                ),

                F.when(
                    F.col(
                        "invalid_battery_soc"
                    ),
                    "INVALID_BATTERY_SOC"
                ),

                F.when(
                    F.col(
                        "invalid_time"
                    ),
                    "EVENT_TS_AFTER_INGEST_TS"
                ),

                F.when(
                    F.col(
                        "invalid_odometer"
                    ),
                    "INVALID_ODOMETER"
                ),

                F.when(
                    F.col(
                        "invalid_alert"
                    ),
                    "ALERT_CODE_MISSING"
                ),

                F.when(
                    F.col(
                        "duplicate_event_id"
                    ),
                    "DUPLICATE_EVENT_ID_IN_BATCH"
                ),

                F.when(
                    F.col(
                        "already_in_bronze"
                    ),
                    "EVENT_ALREADY_IN_BRONZE"
                )
            )
        )

        .withColumn(
            "is_invalid",

            F.length(
                F.col(
                    "rejection_reason"
                )
            ) > 0
        )
    )


    # ============================================================
    # 13. VALID DATA
    # ============================================================

    valid_df = (

        validated_df

        .filter(
            ~F.col(
                "is_invalid"
            )
        )

        .drop(
            "invalid_event_id",
            "invalid_vin",
            "invalid_event_ts",
            "invalid_ingest_ts",
            "invalid_speed",
            "invalid_battery_soc",
            "invalid_time",
            "invalid_odometer",
            "invalid_alert",
            "duplicate_event_id",
            "already_in_bronze",
            "rejection_reason",
            "is_invalid"
        )
    )


    # ============================================================
    # 14. QUARANTINE DATA
    # ============================================================

    quarantine_df = (

        validated_df

        .filter(
            F.col(
                "is_invalid"
            )
        )

        .withColumn(
            "quarantine_ts",
            F.current_timestamp()
        )
    )

# COMMAND ----------

# DBTITLE 1,Quality Summary
# ============================================================
# 15. QUALITY SUMMARY
# ============================================================

if valid_df is None:

    valid_count = 0
    quarantine_count = 0

    print("No data — skipping quality summary.")

else:

    valid_count = valid_df.count()

    quarantine_count = (
        quarantine_df.count()
    )


    print("\n==============================")
    print("BRONZE QUALITY SUMMARY")
    print("==============================")

    print(
        f"RAW records       : {raw_count:,}"
    )

    print(
        f"Valid records     : {valid_count:,}"
    )

    print(
        f"Quarantine        : {quarantine_count:,}"
    )

    print("==============================")

# COMMAND ----------

# DBTITLE 1,Write Quarantine & Bronze
# ============================================================
# 16. WRITE QUARANTINE
# ============================================================

if raw_df is None:

    print("No data — skipping writes.")

else:

    if quarantine_count > 0:

        (

            quarantine_df

            .write

            .format("delta")

            .mode("append")

            .partitionBy(
                "source_run_id"
            )

            .save(
                QUARANTINE_PATH
            )
        )

        print(
            "Quarantine write successful."
        )

    else:

        print(
            "No invalid records."
        )


    # ============================================================
    # 17. WRITE BRONZE
    # ============================================================

    if valid_count > 0:

        (

            valid_df

            .write

            .format("delta")

            .mode("append")

            .partitionBy(
                "source_run_id"
            )

            .save(
                BRONZE_PATH
            )
        )

        print(
            "Bronze write successful."
        )

    else:

        print(
            "No new valid records."
        )

# COMMAND ----------

# DBTITLE 1,Verify, Checkpoint & Final Result
# ============================================================
# 18. VERIFY BRONZE WRITE
# ============================================================
#
# We verify that Bronze is readable before checkpoint update.
#


if raw_df is None:

    print("No data — skipping verify & checkpoint.")

else:

    bronze_after_write = (

        spark.read

        .format("delta")

        .load(
            BRONZE_PATH
        )
    )


    written_run_count = (

        bronze_after_write

        .filter(
            F.col(
                "source_run_id"
            ).isin(
                [
                    run["run_id"]
                    for run in new_runs
                ]
            )
        )

        .count()
    )


    print(
        f"Records present in Bronze "
        f"for current runs: {written_run_count:,}"
    )


    # ============================================================
    # 19. CHECKPOINT UPDATE
    # ============================================================
    #
    # IMPORTANT:
    #
    # This happens ONLY after Bronze write succeeds.
    #
    # If anything above fails:
    #     checkpoint is NOT updated.
    #
    # Therefore retry will process the same run again.
    #
    # Cross-run event_id protection prevents duplicates.
    #


    latest_run_id = max(
        run["run_id"]
        for run in new_runs
    )


    checkpoint_record = (

        spark.createDataFrame(

            [
                (
                    "bronze",
                    latest_run_id
                )
            ],

            [
                "pipeline_name",
                "last_processed_run_id"
            ]
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

            .save(
                CHECKPOINT_PATH
            )
        )

    else:

        (

            checkpoint_record

            .write

            .format("delta")

            .mode("overwrite")

            .save(
                CHECKPOINT_PATH
            )
        )


    print(
        f"Checkpoint updated successfully: "
        f"{latest_run_id}"
    )


    # ============================================================
    # 20. FINAL RESULT
    # ============================================================

    print("\n===================================")
    print("BRONZE PIPELINE COMPLETED")
    print("===================================")

    print(
        f"Runs processed : {len(new_runs)}"
    )

    print(
        f"RAW records    : {raw_count:,}"
    )

    print(
        f"Bronze records : {valid_count:,}"
    )

    print(
        f"Quarantine     : {quarantine_count:,}"
    )

    print(
        f"Checkpoint     : {latest_run_id}"
    )

    print("===================================")