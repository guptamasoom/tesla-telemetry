# Databricks notebook source
# DBTITLE 1,Validation Stream — Tesla Telemetry
# MAGIC %md
# MAGIC # 06_VALIDATION_STREAM — Tesla Vehicle Telemetry
# MAGIC
# MAGIC Streaming validation and monitoring for the Tesla telemetry streaming pipeline.
# MAGIC
# MAGIC 1. **Active query monitor** — check status of all running streaming queries
# MAGIC 2. **Data quality snapshot** — row counts, null rates, anomaly distribution per layer
# MAGIC 3. **Throughput metrics** — events/sec, batch latency from recent progress
# MAGIC 4. **Quarantine review** — count and sample rejected records
# MAGIC 5. **Scoring summary** — risk level distribution from scored events
# MAGIC
# MAGIC **Usage**: Run this notebook while the streaming pipeline is active to get a real-time health snapshot.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

print("=" * 60)
print("Tesla Telemetry — Streaming Pipeline Validation")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Active Query Monitor
# ============================================================
# 1. ACTIVE STREAMING QUERIES
# ============================================================

active_queries = spark.streams.active

print(f"\nActive streaming queries: {len(active_queries)}")
print("-" * 60)

for q in active_queries:
    status = q.status
    print(f"  Name   : {q.name}")
    print(f"  ID     : {q.id}")
    print(f"  Status : {status.get('message', 'unknown')}")
    print(f"  Is active : {q.isActive}")
    print()

# COMMAND ----------

# DBTITLE 1,Layer Row Counts
# ============================================================
# 2. LAYER ROW COUNTS
# ============================================================

layers = {
    "Bronze": BRONZE_PATH,
    "Silver": SILVER_PATH,
    "Scoring": SCORING_PATH,
    "Quarantine": QUARANTINE_PATH,
}

print("\nLayer row counts:")
print("-" * 60)

for name, path in layers.items():
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  {name:15s}: {count:>10,} records")
    except Exception:
        print(f"  {name:15s}: (not yet created)")

# Gold tables
gold_tables = {
    "Gold — Vehicle Hourly": f"{GOLD_PATH}vehicle_hourly_metrics/",
    "Gold — City Hourly": f"{GOLD_PATH}city_hourly_metrics/",
    "Gold — Alert Summary": f"{GOLD_PATH}alert_summary/",
}

for name, path in gold_tables.items():
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  {name:20s}: {count:>10,} records")
    except Exception:
        print(f"  {name:20s}: (not yet created)")

# COMMAND ----------

# DBTITLE 1,Bronze DQ Snapshot
# ============================================================
# 3. DATA QUALITY SNAPSHOT (Bronze)
# ============================================================

try:
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    total = bronze_df.count()
    null_event_ids = bronze_df.filter(F.col("event_id").isNull()).count()
    event_type_dist = (
        bronze_df
        .groupBy("event_type")
        .count()
        .orderBy(F.col("count").desc())
    )

    print(f"\nBronze data quality snapshot:")
    print("-" * 60)
    print(f"  Total records     : {total:,}")
    print(f"  Null event_ids    : {null_event_ids:,}")
    print(f"  Event type distribution:")
    display(event_type_dist)

except Exception as e:
    print(f"\nBronze not yet available: {e}")

# COMMAND ----------

# DBTITLE 1,Quarantine Review
# ============================================================
# 4. QUARANTINE REVIEW
# ============================================================

try:
    quarantine_df = spark.read.format("delta").load(QUARANTINE_PATH)
    q_count = quarantine_df.count()

    print(f"\nQuarantine review:")
    print("-" * 60)
    print(f"  Total quarantined : {q_count:,}")

    if q_count > 0:
        reason_dist = (
            quarantine_df
            .groupBy("_rejection_reason")
            .count()
            .orderBy(F.col("count").desc())
        )
        print("  Rejection reasons:")
        display(reason_dist)

except Exception as e:
    print(f"\nQuarantine not yet available: {e}")

# COMMAND ----------

# DBTITLE 1,Scoring Summary
# ============================================================
# 5. SCORING SUMMARY
# ============================================================

try:
    scoring_df = spark.read.format("delta").load(SCORING_PATH)
    s_count = scoring_df.count()

    print(f"\nScoring summary:")
    print("-" * 60)
    print(f"  Total scored events : {s_count:,}")

    if s_count > 0:
        risk_dist = (
            scoring_df
            .groupBy("risk_level")
            .count()
            .orderBy(F.col("count").desc())
        )
        print("  Risk level distribution:")
        display(risk_dist)

        # Critical anomalies
        critical_count = scoring_df.filter(F.col("risk_level") == "CRITICAL").count()
        print(f"  Critical anomalies   : {critical_count:,}")

except Exception as e:
    print(f"\nScoring not yet available: {e}")

# COMMAND ----------

# DBTITLE 1,Throughput Metrics
# ============================================================
# 6. THROUGHPUT METRICS (from recent progress)
# ============================================================

print(f"\nThroughput metrics:")
print("-" * 60)

for q in spark.streams.active:
    progress = q.recentProgress
    if progress:
        latest = progress[-1]
        processed_rows = latest.get("numInputRows", 0)
        batch_id = latest.get("batchId", "N/A")
        duration_ms = latest.get("durationMs", {}).get("triggerExecution", 0)

        if duration_ms > 0:
            throughput = (processed_rows / duration_ms) * 1000
        else:
            throughput = 0

        print(f"  {q.name}:")
        print(f"    Batch ID      : {batch_id}")
        print(f"    Input rows    : {processed_rows:,}")
        print(f"    Duration      : {duration_ms}ms")
        print(f"    Throughput    : {throughput:.1f} rows/sec")
        print()
    else:
        print(f"  {q.name}: no progress yet")
        print()

print("=" * 60)
print("Validation complete.")
print("=" * 60)