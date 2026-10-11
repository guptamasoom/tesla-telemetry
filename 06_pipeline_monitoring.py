# Databricks notebook source
# DBTITLE 1,Pipeline Monitoring Overview
# MAGIC %md
# MAGIC # Tesla Telemetry Pipeline — Monitoring
# MAGIC
# MAGIC Monitors the 5-task pipeline (generate → bronze → silver → gold → load_snowflake) across all telemetry jobs:
# MAGIC
# MAGIC * **Cell 2** — Per-task duration for every run (from `system.lakeflow.job_task_run_timeline`)
# MAGIC * **Cell 3** — Bottleneck trend: how each task's duration grows over time (watch `gold` and `load_snowflake`)
# MAGIC * **Cell 4** — Failed / cancelled runs in the last 7 days
# MAGIC * **Cell 5** — Data-volume & data-quality health: records per run, quarantine rate, layer totals
# MAGIC
# MAGIC Jobs covered: `818230914419317` (main), `288555998184699` (1M/2M scaling tests), `930495625009104` (daily).

# COMMAND ----------

# DBTITLE 1,Task durations per run
# MAGIC %sql
# MAGIC -- Per-task duration per run (system tables have ~hourly latency)
# MAGIC SELECT job_id,
# MAGIC        job_run_id,
# MAGIC        task_key,
# MAGIC        MAX(result_state) AS result_state,
# MAGIC        CAST(SUM(period_end_time - period_start_time) AS LONG) AS duration_seconds
# MAGIC FROM system.lakeflow.job_task_run_timeline
# MAGIC WHERE period_end_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC   AND job_id IN (818230914419317, 288555998184699, 930495625009104)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY job_run_id, task_key

# COMMAND ----------

# DBTITLE 1,Bottleneck trend by task
# MAGIC %sql
# MAGIC -- Bottleneck trend: average task duration per day.
# MAGIC -- gold and load_snowflake are expected to grow with CUMULATIVE table size;
# MAGIC -- bronze/silver should stay flat. If gold/snowflake climb run over run,
# MAGIC -- it is time for incremental gold aggregation / incremental Snowflake loads.
# MAGIC WITH task_runs AS (
# MAGIC   SELECT job_run_id,
# MAGIC          task_key,
# MAGIC          MIN(period_start_time) AS run_started,
# MAGIC          CAST(SUM(period_end_time - period_start_time) AS LONG) AS duration_seconds
# MAGIC   FROM system.lakeflow.job_task_run_timeline
# MAGIC   WHERE period_end_time > CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
# MAGIC     AND job_id IN (818230914419317, 288555998184699, 930495625009104)
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT CAST(run_started AS DATE) AS run_date,
# MAGIC        task_key,
# MAGIC        COUNT(DISTINCT job_run_id) AS runs,
# MAGIC        ROUND(AVG(duration_seconds), 1) AS avg_seconds
# MAGIC FROM task_runs
# MAGIC GROUP BY ALL
# MAGIC ORDER BY run_date, task_key

# COMMAND ----------

# DBTITLE 1,Failed runs (last 7 days)
# MAGIC %sql
# MAGIC -- Failed or cancelled runs in the last 7 days (empty result = all healthy)
# MAGIC SELECT job_id,
# MAGIC        run_id,
# MAGIC        result_state,
# MAGIC        termination_code,
# MAGIC        CAST(SUM(period_end_time - period_start_time) AS LONG) AS duration_seconds
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_end_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC   AND job_id IN (818230914419317, 288555998184699, 930495625009104)
# MAGIC   AND result_state IS NOT NULL
# MAGIC   AND result_state != 'SUCCEEDED'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY duration_seconds DESC

# COMMAND ----------

# DBTITLE 1,Data volumes & DQ health
from pyspark.sql import functions as F

BASE = "s3://tesla-vehicle-telemetry-511976431596/telemetry"

print("=" * 70)
print("LAYER VOLUMES & DATA QUALITY")
print("=" * 70)

# 1. Records per run in Bronze (ingestion throughput)
bronze = spark.read.format("delta").load(f"{BASE}/bronze/")
print("\n--- Bronze records per run ---")
display(bronze.groupBy("source_run_id").count().orderBy("source_run_id"))

# 2. Quarantine rate per run (DQ health — should be near 0 for clean batches)
quarantine = spark.read.format("delta").load(f"{BASE}/quarantine/")
print("\n--- Quarantined records per run ---")
display(
    quarantine
    .groupBy("source_run_id")
    .agg(
        F.count("*").alias("quarantined"),
        F.sum(F.when(F.col("already_in_bronze"), 1).otherwise(0)).alias("retry_duplicates")
    )
    .orderBy("source_run_id")
)

# 3. Layer totals (cumulative growth — feeds gold rebuild cost)
print("\n--- Layer totals ---")
print(f"Silver total records : {spark.read.format('delta').load(f'{BASE}/silver/telemetry/').count():,}")

for table in ["telemetry_enriched", "vehicle_hourly_metrics", "trip_metrics", "battery_health", "alerts"]:
    cnt = spark.read.format("delta").load(f"{BASE}/gold/{table}").count()
    print(f"Gold {table:<25}: {cnt:,}")

# COMMAND ----------

