# Databricks notebook source
# DBTITLE 1,Generate Telemetry (1M events per run)
# ============================================================
# 01_GENERATE_TELEMETRY_1M
# Tesla Telemetry CDC-Aware Generator (1M events per run)
#
# Copy of 01_generate_telemetry with EVENT_COUNT = 1,000,000
# Databricks Serverless + PySpark + S3
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime, timezone
import time


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "tesla-vehicle-telemetry-511976431596"

BASE_PATH = (
    f"s3://{BUCKET}/telemetry/"
)

# Persistent state of vehicles
VEHICLE_STATE_PATH = (
    f"{BASE_PATH}vehicle_state/"
)

# Raw telemetry output
RAW_TELEMETRY_PATH = (
    f"{BASE_PATH}raw/"
)

# ------------------------------------------------------------
# Records per run
# ------------------------------------------------------------

EVENT_COUNT = 1_000_000

# ------------------------------------------------------------
# New vehicles introduced in every run
# ------------------------------------------------------------

NEW_VEHICLES_PER_RUN = 50

# ------------------------------------------------------------
# First run initial population
# ------------------------------------------------------------

INITIAL_VEHICLE_COUNT = 10_000


# ============================================================
# RUN INFORMATION
# ============================================================

pipeline_start = time.perf_counter()

run_id = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%d_%H%M%S"
)

ingestion_date = datetime.now(
    timezone.utc
).strftime(
    "%Y-%m-%d"
)

OUTPUT_PATH = (
    f"{RAW_TELEMETRY_PATH}"
    f"ingestion_date={ingestion_date}/"
    f"run_id={run_id}/"
)


print("=" * 70)
print("TESLA TELEMETRY GENERATOR")
print("=" * 70)

print(f"Run ID                  : {run_id}")
print(f"Events per run         : {EVENT_COUNT:,}")
print(f"New vehicles per run   : {NEW_VEHICLES_PER_RUN:,}")
print(f"Initial vehicles       : {INITIAL_VEHICLE_COUNT:,}")

print(f"Vehicle state          : {VEHICLE_STATE_PATH}")
print(f"Raw telemetry output   : {OUTPUT_PATH}")

print("=" * 70)


# ============================================================
# 1. TRY TO READ EXISTING VEHICLE STATE
# ============================================================

try:

    vehicle_state = (
        spark.read
        .format("delta")
        .load(VEHICLE_STATE_PATH)
    )

    existing_state_count = vehicle_state.count()

    print(
        f"Existing vehicle state found: "
        f"{existing_state_count:,} vehicles"
    )

    first_run = False

except Exception:

    first_run = True

    print(
        "No existing vehicle state found."
    )

    print(
        "Creating initial vehicle population..."
    )


# ============================================================
# 2. CREATE INITIAL VEHICLE STATE
# ============================================================

if first_run:

    vehicle_state = (
        spark.range(
            1,
            INITIAL_VEHICLE_COUNT + 1
        )
        .withColumnRenamed(
            "id",
            "vehicle_number"
        )

        # ----------------------------------------------------
        # VIN
        # ----------------------------------------------------

        .withColumn(
            "vin",
            F.format_string(
                "WVW%014d",
                F.col("vehicle_number")
            )
        )

        # ----------------------------------------------------
        # MODEL
        # ----------------------------------------------------

        .withColumn(
            "model",
            F.when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 0,
                "Model 3"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 1,
                "Model Y"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 2,
                "Model S"
            )
            .otherwise(
                "Model X"
            )
        )

        # ----------------------------------------------------
        # CITY
        # ----------------------------------------------------

        .withColumn(
            "city",
            F.when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 0,
                "Berlin"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 1,
                "Munich"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 2,
                "Frankfurt"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 3,
                "Hamburg"
            )
            .otherwise(
                "Stuttgart"
            )
        )

        # ----------------------------------------------------
        # LATITUDE
        # ----------------------------------------------------

        .withColumn(
            "latitude",
            F.when(
                F.col("city") == "Berlin",
                52.5200
            )
            .when(
                F.col("city") == "Munich",
                48.1351
            )
            .when(
                F.col("city") == "Frankfurt",
                50.1109
            )
            .when(
                F.col("city") == "Hamburg",
                53.5511
            )
            .otherwise(
                48.7758
            )
        )

        # ----------------------------------------------------
        # LONGITUDE
        # ----------------------------------------------------

        .withColumn(
            "longitude",
            F.when(
                F.col("city") == "Berlin",
                13.4050
            )
            .when(
                F.col("city") == "Munich",
                11.5820
            )
            .when(
                F.col("city") == "Frankfurt",
                8.6820
            )
            .when(
                F.col("city") == "Hamburg",
                9.9937
            )
            .otherwise(
                9.1829
            )
        )

        # ----------------------------------------------------
        # SOFTWARE
        # ----------------------------------------------------

        .withColumn(
            "software_version",
            F.when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 0,
                "2026.10.1"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 1,
                "2026.11.2"
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    4
                ) == 2,
                "2026.12.1"
            )
            .otherwise(
                "2026.13.3"
            )
        )

        # ----------------------------------------------------
        # BATTERY CAPACITY
        # ----------------------------------------------------

        .withColumn(
            "battery_capacity_kwh",
            F.when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 0,
                50
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 1,
                60
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 2,
                75
            )
            .when(
                F.pmod(
                    F.col("vehicle_number"),
                    5
                ) == 3,
                82
            )
            .otherwise(
                100
            )
        )

        .select(
            "vehicle_number",
            "vin",
            "model",
            "city",
            "latitude",
            "longitude",
            "software_version",
            "battery_capacity_kwh"
        )
    )

    print(
        f"Initial vehicles created: "
        f"{INITIAL_VEHICLE_COUNT:,}"
    )


# ============================================================
# 3. GET CURRENT MAX VEHICLE NUMBER
# ============================================================

max_vehicle_number = (
    vehicle_state
    .agg(
        F.max(
            "vehicle_number"
        ).alias(
            "max_vehicle_number"
        )
    )
    .collect()[0]["max_vehicle_number"]
)

print(
    f"Current max vehicle number: "
    f"{max_vehicle_number:,}"
)


# ============================================================
# 4. GENERATE NEW VEHICLES
# ============================================================

new_vehicle_start = (
    max_vehicle_number + 1
)

new_vehicle_end = (
    max_vehicle_number
    + NEW_VEHICLES_PER_RUN
)


new_vehicles = (
    spark.range(
        new_vehicle_start,
        new_vehicle_end + 1
    )
    .withColumnRenamed(
        "id",
        "vehicle_number"
    )

    .withColumn(
        "vin",
        F.format_string(
            "WVW%014d",
            F.col("vehicle_number")
        )
    )

    .withColumn(
        "model",
        F.when(
            F.pmod(
                F.col("vehicle_number"),
                4
            ) == 0,
            "Model 3"
        )
        .when(
            F.pmod(
                F.col("vehicle_number"),
                4
            ) == 1,
            "Model Y"
        )
        .when(
            F.pmod(
                F.col("vehicle_number"),
                4
            ) == 2,
            "Model S"
        )
        .otherwise(
            "Model X"
        )
    )

    .withColumn(
        "city",
        F.when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 0,
            "Berlin"
        )
        .when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 1,
            "Munich"
        )
        .when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 2,
            "Frankfurt"
        )
        .when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 3,
            "Hamburg"
        )
        .otherwise(
            "Stuttgart"
        )
    )

    .withColumn(
        "latitude",
        F.when(
            F.col("city") == "Berlin",
            52.5200
        )
        .when(
            F.col("city") == "Munich",
            48.1351
        )
        .when(
            F.col("city") == "Frankfurt",
            50.1109
        )
        .when(
            F.col("city") == "Hamburg",
            53.5511
        )
        .otherwise(
            48.7758
        )
    )

    .withColumn(
        "longitude",
        F.when(
            F.col("city") == "Berlin",
            13.4050
        )
        .when(
            F.col("city") == "Munich",
            11.5820
        )
        .when(
            F.col("city") == "Frankfurt",
            8.6820
        )
        .when(
            F.col("city") == "Hamburg",
            9.9937
        )
        .otherwise(
            9.1829
        )
    )

    .withColumn(
        "software_version",
        F.lit("2026.14.1")
    )

    .withColumn(
        "battery_capacity_kwh",
        F.lit(75)
    )

    .select(
        "vehicle_number",
        "vin",
        "model",
        "city",
        "latitude",
        "longitude",
        "software_version",
        "battery_capacity_kwh"
    )
)


print(
    f"New vehicles generated: "
    f"{NEW_VEHICLES_PER_RUN:,}"
)


# ============================================================
# 5. CHANGE EXISTING VEHICLE STATE
# ============================================================
#
# Every 5th existing vehicle gets changed.
#
# These changes will appear in NEW telemetry events.
# We NEVER modify old raw telemetry.
# ============================================================

updated_existing = (
    vehicle_state

    .withColumn(
        "city",
        F.when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 0,

            F.when(
                F.col("city") == "Berlin",
                "Munich"
            )
            .when(
                F.col("city") == "Munich",
                "Frankfurt"
            )
            .when(
                F.col("city") == "Frankfurt",
                "Hamburg"
            )
            .when(
                F.col("city") == "Hamburg",
                "Stuttgart"
            )
            .otherwise(
                "Berlin"
            )
        )
        .otherwise(
            F.col("city")
        )
    )

    .withColumn(
        "software_version",
        F.when(
            F.pmod(
                F.col("vehicle_number"),
                5
            ) == 0,
            "2026.14.1"
        )
        .otherwise(
            F.col("software_version")
        )
    )

    .withColumn(
        "latitude",
        F.when(
            F.col("city") == "Berlin",
            52.5200
        )
        .when(
            F.col("city") == "Munich",
            48.1351
        )
        .when(
            F.col("city") == "Frankfurt",
            50.1109
        )
        .when(
            F.col("city") == "Hamburg",
            53.5511
        )
        .otherwise(
            48.7758
        )
    )

    .withColumn(
        "longitude",
        F.when(
            F.col("city") == "Berlin",
            13.4050
        )
        .when(
            F.col("city") == "Munich",
            11.5820
        )
        .when(
            F.col("city") == "Frankfurt",
            8.6820
        )
        .when(
            F.col("city") == "Hamburg",
            9.9937
        )
        .otherwise(
            9.1829
        )
    )
)


# ============================================================
# 6. COMBINE CURRENT + NEW VEHICLES
# ============================================================

current_vehicle_state = (
    updated_existing
    .unionByName(
        new_vehicles
    )
)


# ============================================================
# 7. GENERATE 1M TELEMETRY EVENTS
# ============================================================

print(
    "\nGenerating telemetry events..."
)

events = (
    spark.range(
        1,
        EVENT_COUNT + 1
    )
    .withColumnRenamed(
        "id",
        "event_number"
    )

    # --------------------------------------------------------
    # Assign vehicle
    # --------------------------------------------------------

    .withColumn(
        "vehicle_number",
        (
            F.pmod(
                F.col("event_number") - 1,
                F.lit(
                    max_vehicle_number
                    + NEW_VEHICLES_PER_RUN
                )
            ) + 1
        )
    )

    # --------------------------------------------------------
    # Join to current vehicle state
    # --------------------------------------------------------

    .join(
        current_vehicle_state,
        on="vehicle_number",
        how="inner"
    )
)


# ============================================================
# 8. EVENT ID
# ============================================================

events = events.withColumn(
    "event_id",
    F.concat(
        F.lit("evt_"),
        F.lit(run_id),
        F.lit("_"),
        F.lpad(
            F.col("event_number")
            .cast("string"),
            9,
            "0"
        )
    )
)


# ============================================================
# 9. EVENT TIMESTAMP
# ============================================================

start_timestamp = (
    "2026-10-07 08:00:00"
)

events = events.withColumn(
    "event_ts",
    F.expr(
        f"""
        timestamp('{start_timestamp}')
        + INTERVAL 1 SECOND *
        (event_number - 1)
        """
    )
)


# ============================================================
# 10. INGEST TIMESTAMP
# ============================================================

events = events.withColumn(
    "ingest_ts",
    F.col("event_ts")
    + F.expr(
        "INTERVAL 5 SECONDS"
    )
)


# ============================================================
# 11. EVENT TYPE
# ============================================================

events = events.withColumn(
    "event_type",
    F.when(
        F.pmod(
            F.col("event_number"),
            100
        ) < 85,
        "telemetry"
    )
    .when(
        F.pmod(
            F.col("event_number"),
            100
        ) < 95,
        "charge"
    )
    .otherwise(
        "alert"
    )
)


# ============================================================
# 12. SPEED
# ============================================================

events = events.withColumn(
    "speed_kmh",
    F.when(
        F.col("event_type") == "charge",
        F.lit(0.0)
    )
    .otherwise(
        F.round(
            F.rand(seed=100)
            * 130,
            2
        )
    )
)


# ============================================================
# 13. BATTERY SOC
# ============================================================

events = events.withColumn(
    "battery_soc",
    F.round(
        20
        + F.rand(seed=101) * 80,
        2
    )
)


# ============================================================
# 14. BATTERY TEMPERATURE
# ============================================================

events = events.withColumn(
    "battery_temp_c",
    F.round(
        20
        + F.rand(seed=102) * 25,
        2
    )
)


# ============================================================
# 15. ODOMETER
# ============================================================

events = events.withColumn(
    "odometer_km",
    F.round(
        1000
        + (
            F.col("vehicle_number")
            * 25
        )
        + (
            F.rand(seed=103)
            * 100000
        ),
        2
    )
)


# ============================================================
# 16. CHARGING STATE
# ============================================================

events = events.withColumn(
    "charging_state",
    F.when(
        F.col("event_type") == "charge",
        "Connected"
    )
    .otherwise(
        "Disconnected"
    )
)


# ============================================================
# 17. GEAR
# ============================================================

events = events.withColumn(
    "gear",
    F.when(
        F.col("speed_kmh") < 1,
        "P"
    )
    .otherwise(
        "D"
    )
)


# ============================================================
# 18. AUTOPILOT
# ============================================================

events = events.withColumn(
    "autopilot_engaged",
    (
        (
            F.col("speed_kmh") > 30
        )
        &
        (
            F.rand(seed=104) < 0.25
        )
    )
)


# ============================================================
# 19. ALERT CODE
# ============================================================

events = events.withColumn(
    "alert_code",
    F.when(
        F.col("event_type") == "alert",

        F.when(
            F.pmod(
                F.col("event_number"),
                4
            ) == 0,
            "TIRE_PRESSURE_LOW"
        )
        .when(
            F.pmod(
                F.col("event_number"),
                4
            ) == 1,
            "BATTERY_TEMP_HIGH"
        )
        .when(
            F.pmod(
                F.col("event_number"),
                4
            ) == 2,
            "BRAKE_SYSTEM_WARNING"
        )
        .otherwise(
            "CHARGING_ERROR"
        )
    )
    .otherwise(
        F.lit(None)
    )
)


# ============================================================
# 20. FINAL TELEMETRY SCHEMA
# ============================================================

events = events.select(
    "event_id",
    "vin",
    "event_ts",
    "ingest_ts",
    "event_type",
    "city",
    "latitude",
    "longitude",
    "speed_kmh",
    "battery_soc",
    "battery_temp_c",
    "odometer_km",
    "charging_state",
    "gear",
    "autopilot_engaged",
    "alert_code",
    "software_version"
)


# ============================================================
# 21. CHECK SAMPLE
# ============================================================

print("\nSample telemetry:")

display(
    events.limit(10)
)


# ============================================================
# 22. WRITE RAW TELEMETRY
# ============================================================

print(
    "\nWriting telemetry to S3..."
)

(
    events.write
    .mode("append")
    .format("json")
    .save(OUTPUT_PATH)
)


# ============================================================
# 23. VALIDATE OUTPUT
# ============================================================

print(
    "\nValidating S3 output..."
)

written_df = (
    spark.read
    .json(OUTPUT_PATH)
)

written_count = (
    written_df.count()
)


# ============================================================
# 24. PERSIST CURRENT VEHICLE STATE
# ============================================================

print(
    "\nSaving current vehicle state..."
)

(
    current_vehicle_state.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .save(VEHICLE_STATE_PATH)
)


# ============================================================
# 25. FINAL VEHICLE COUNT
# ============================================================

final_vehicle_count = (
    spark.read
    .format("delta")
    .load(VEHICLE_STATE_PATH)
    .count()
)


# ============================================================
# 26. FINAL SUMMARY
# ============================================================

elapsed = (
    time.perf_counter()
    - pipeline_start
)

print("\n" + "=" * 70)
print("TESLA TELEMETRY GENERATION COMPLETED")
print("=" * 70)

print(
    f"Expected events      : {EVENT_COUNT:,}"
)

print(
    f"Written events       : {written_count:,}"
)

print(
    f"Previous vehicles    : "
    f"{max_vehicle_number:,}"
)

print(
    f"New vehicles         : "
    f"{NEW_VEHICLES_PER_RUN:,}"
)

print(
    f"Final vehicle count  : {final_vehicle_count:,}"
)

print(
    f"Run ID               : {run_id}"
)

print(
    f"S3 output            : {OUTPUT_PATH}"
)

print(
    f"Execution time       : {elapsed:.2f} seconds"
)

print("=" * 70)

# COMMAND ----------

