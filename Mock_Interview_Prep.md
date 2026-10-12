# Mock Interview Prep — Tesla Telemetry + FraudStream V3

> **Two projects on Databricks**: Tesla Vehicle Telemetry (batch medallion ETL) + FraudStream V3 (real-time streaming fraud detection)
> 27 questions across 8 blueprint categories with detailed answers.

---

## 1. "Tell Me About Your Recent Project" (Q1-Q3)

### Q1: Walk me through a data engineering project you worked on end-to-end.

**Problem**: Tesla needed a telemetry analytics platform to monitor vehicle health (battery, driving, charging) across 10K+ vehicles in 5 German cities. Data arrives in discrete batches of 500K events per run.

**Architecture**: Built a batch medallion ETL on Databricks with 9 notebooks, 5 sequential tasks (generate -> bronze -> silver -> gold -> load_snowflake). Data stored in S3 (Delta format), running on serverless compute. Checkpoint-based incremental processing at every layer.

**Implementation**: I designed and implemented all 9 notebooks. Bronze uses dbutils.fs.ls for run discovery + 7 DQ rules + quarantine. Silver standardizes and derives columns (battery_band, speed_category, is_moving) with left_anti join dedup. Gold builds 5 tables using MERGE for aggregates + idempotent append for events. Snowflake load via PUT/COPY. DABs for dev/prod, GitHub Actions CI/CD, 3 SQL alerts, 4-widget monitoring dashboard.

**Challenges**: Ensuring idempotency across all layers — solved with checkpoint + left_anti join pattern. Gold originally used mode(overwrite) from pandas code — unsafe, replaced with MERGE. Optimized by removing .count() logging, using DeltaTable.isDeltaTable, filtered anti-joins.

**Impact**: Processes 500K events/run in ~5 min total. Tested at 1M and 2M scale. 3 layers crash-tested — all 0 duplicates on retry. 3 business alerts running every 15 min.

### Q2: Describe a pipeline you designed and implemented.

**Problem**: A fintech needed real-time fraud detection — transactions must be scored within seconds to block fraudulent transfers before they complete.

**Architecture**: Built FraudStream V3 — continuous streaming pipeline on Databricks using Structured Streaming + Auto Loader. 7 notebooks, 5 concurrent tasks in a continuous job (no dependencies — all stages run simultaneously). Data stored in UC Volume.

**Implementation**: 01_stream_generator (200 events/5s, 8% fraud rate) to 02_bronze_stream (Auto Loader cloudFiles + DQ + quarantine) to 03_silver_stream (10-min watermark + dropDuplicates) to 04_scoring_stream (2-day velocity window + 7 fraud rules) to 05_gold_stream (7 gold tables, 60s trigger, full recompute). 06_validation_stream runs 37 checks (37/37 passed). safe_foreachbatch pattern for error handling.

**Challenges**: Unbounded state from dropDuplicates — solved with 10-min watermark (originally 60s, too aggressive). Scoring read full Silver table — optimized to 2-day window. Gold trigger 10s to 60s (6x less compute). Bare except in Gold silently swallowing errors — fixed with proper logging.

**Impact**: ~10-15 second end-to-end latency. 3,914 transactions processed, 6 high-risk flagged, $1.8M total tracked. 37/37 validation checks passed.

### Q3: Tell me about a project where you processed large-scale data.

**Tesla**: Scaled from 500K to 2M events per run. 9M Silver rows, 3M Gold event-level rows, 663K hourly metrics. Affected-key recompute in Gold reduces Silver read from 9M to ~1M rows (9x reduction). Tested with 3 job variants — main (500K), 1M scaling test, daily scheduled.

**FraudStream**: Continuous streaming with 200 events/5s = 1,440 events/min. Watermark bounds state to 10 min. 2-day scoring window bounds Silver read regardless of table size. Gold tables grow continuously but are fully rebuilt every 60s.

---

## 2. Architecture & Design Focus (Q4-Q8)

### Q4: How did you design the data pipeline architecture?

**Tesla** — Batch medallion: 5 sequential tasks with depends_on chain. Bronze discovers new S3 runs via dbutils.fs.ls, processes only runs > checkpoint. Each layer reads only new data via checkpoint. Append-only Bronze/Silver, MERGE + append in Gold.

**FraudStream** — Continuous streaming: 5 concurrent tasks, no dependencies. All stages run simultaneously because streaming never finishes. Bronze uses Auto Loader (cloudFiles) for file discovery. Silver reads Bronze stream with watermark. Scoring reads Silver stream with 2-day window. Gold reads Scored stream every 60s.

Key difference: Tesla = batch incremental (each stage processes a complete batch). FraudStream = continuous streaming (all stages run simultaneously, real-time).

### Q5: What tools and technologies did you choose and why?

**Databricks** — Unified platform for both projects. Notebooks for development, DABs for deployment, Jobs for scheduling, Delta Lake for storage, SQL alerts for monitoring.

**Delta Lake** — ACID transactions, MERGE support (critical for Gold upserts), partition pruning, time travel. S3 for Tesla (cost-effective batch), UC Volume for FraudStream (integrated streaming).

**Structured Streaming** (FraudStream) — Built-in watermark, dropDuplicates, foreachBatch, checkpointing. Auto Loader for file discovery.

**Serverless compute** (Tesla) — No cluster management, auto-scaling, pay-per-use. Perfect for batch jobs.

**Snowflake** (Tesla) — External data warehouse. PUT/COPY pattern for loading Gold tables.

**GitHub Actions** — CI/CD for both projects. PR validation (bundle validate), auto-deploy on merge (bundle deploy).

### Q6: Can you explain your data flow from source to consumption?

**Tesla**: S3 raw JSON -> Bronze Delta (DQ + quarantine) -> Silver Delta (standardized + derived + deduped) -> Gold Delta (5 analytics tables) -> Snowflake (external warehouse) -> Dashboards + SQL alerts.

**FraudStream**: UC Volume JSON -> Bronze Delta (Auto Loader + DQ) -> Silver Delta (watermark + dedup) -> Scored Delta (fraud rules + velocity features) -> Gold Delta (7 analytics tables) -> Dashboards + SQL alerts.

Both follow medallion: raw -> bronze (DQ gate) -> silver (clean) -> gold (analytics-ready). Consumption layer = dashboards, alerts, Snowflake.

### Q7: How did you implement medallion architecture (Bronze/Silver/Gold)?

**Bronze**: Both projects enforce schema and apply DQ checks. Tesla: explicit schema on JSON read, 7 DQ rules, quarantine with rejection_reason. FraudStream: Auto Loader with schema inference + DQ checks + quarantine. Both append-only.

**Silver**: Both deduplicate and derive. Tesla: left_anti join on event_id, derives battery_band/speed_category/is_moving. FraudStream: watermark + dropDuplicates on event_id. Both partitioned by date.

**Gold**: Tesla: 5 tables, hybrid strategy (MERGE for aggregates, idempotent append for events). FraudStream: 7 tables, full recompute with 60s trigger, complete output mode. Both are the consumption layer.

### Q8: How did you handle schema evolution or changing data?

**Tesla**: Explicit schema enforcement in Bronze (StructType with 17 fields). JSON records not matching schema are quarantined. If new fields are needed, schema is updated in notebook code, tested in dev, deployed via CI/CD. Delta Lake supports mergeSchema if needed.

**FraudStream**: Auto Loader handles schema inference and evolution. New fields in incoming JSON are automatically detected. Schema location tracks schema changes. mergeSchema option allows new columns without breaking existing data.

---

## 3. AWS / Cloud-Specific (Q9-Q11)

### Q9: How did you use AWS services in your project?

**S3** (Tesla): Primary data store. Raw JSON lands in S3 via generator. Bronze/Silver/Gold stored as Delta tables on S3. Checkpoint tables on S3. Partitioned by source_run_id (Bronze), event_date (Silver/Gold). S3 provides durability, low cost, and seamless integration with Databricks Delta Lake.

**UC Volume** (FraudStream): Databricks-managed storage on cloud infrastructure. Used for streaming checkpoints, Bronze/Silver/Gold Delta tables. Tighter integration with Databricks than raw S3 — Unity Catalog governs access.

**Databricks on AWS**: Both projects run on Databricks AWS workspace. Serverless compute (Tesla) uses AWS infrastructure under the hood. No EC2/ECS/Lambda needed — Databricks abstracts compute management.

### Q10: How did you manage IAM roles and permissions?

Databricks manages IAM internally via instance profiles and service credentials. S3 access configured at the workspace level — no per-notebook IAM. Unity Catalog (FraudStream) manages data governance: volumes, tables, schemas with GRANT/REVOKE permissions.

For Snowflake (Tesla): service account authentication via Snowflake connector. Credentials stored as Databricks secrets (secrets scope), never hardcoded in notebooks.

DABs deploy with a service principal token (DATABRICKS_TOKEN in GitHub secrets) — no personal credentials in CI/CD.

### Q11: How did you handle data governance?

**Unity Catalog** (FraudStream): UC Volume for data storage. Table-level permissions via GRANT/REVOKE. Schema-level access control. All data assets cataloged.

**Delta Lake features** (both): Time travel for audit (DESCRIBE HISTORY, SELECT ... VERSION AS OF). Transaction logs provide full audit trail. Checkpoint tables track processing history per run.

**CI/CD audit trail**: All changes tracked in Git history. GitHub Actions logs show who deployed what and when. DABs ensure reproducible deployments — same config across environments.

---

## 4. Data Processing & Performance (Q12-Q15)

### Q12: How did you optimize your pipeline performance?

**Tesla optimizations:**
1. Checkpoint-based incremental — only new runs processed, not full history
2. Partition pruning — source_run_id (Bronze), event_date (Silver/Gold)
3. Affected-key recompute (Gold incremental) — only changed keys recomputed, 9x reduction in Silver read
4. Left-anti vs NOT IN — hash join, NULL-safe, faster for dedup
5. MERGE vs overwrite — incremental upsert, no full rebuild
6. DeltaTable.isDeltaTable — metadata-only existence check instead of .count()
7. Filtered anti-join — filter by source_run_id to reduce scan scope
8. Removed redundant .count() logging — eliminated repeated full-table scans

**FraudStream optimizations:**
1. 2-day scoring window — bounded Silver read regardless of table size
2. Gold trigger 60s (from 10s) — 6x less compute
3. Bronze single groupBy (from 5 count() calls) — one pass instead of five
4. Watermark state cleanup — 10-min window bounds state size
5. safe_foreachbatch — one bad batch doesnt kill the pipeline

### Q13: What challenges did you face with large datasets?

**Tesla**: Gold full recompute read all 9M Silver rows — slow at scale. Solved with affected-key recompute: extract distinct (vin, event_date) from new batch, filter Silver to affected keys only via left_semi join + partition pruning. 500K batch touching ~5K vehicles reads ~1M rows instead of 9M.

**Tesla**: .count() logging caused repeated full-table scans. Removed all .count() calls, replaced table existence checks with DeltaTable.isDeltaTable (metadata only).

**FraudStream**: dropDuplicates without watermark = unbounded state = OOM. Solved with 10-min watermark — state store only holds 10 min of event IDs. Originally 60s — too aggressive, legitimate late events dropped.

**FraudStream**: Scoring read entire Silver table for velocity features. Solved with 2-day window filter (date_sub). Bounded read regardless of Silver size.

### Q14: How did you handle partitioning and indexing?

**Tesla**: Bronze partitioned by source_run_id (enables checkpoint-based pruning). Silver/Gold partitioned by event_date (enables date-range pruning). Z-order on vin for Gold GROUP BY performance. Delta OPTIMIZE compacts small files.

**FraudStream**: Less partitioning focus (streaming is append-heavy). Bronze/Silver/Gold partitioned by event_date for batch reads. Auto Loader handles file discovery efficiently.

Both: Delta Liquid Clustering (available as alternative to partitioning) considered for adaptive layout on (vin, event_date) — auto-optimizes without manual partition management.

### Q15: Did you use Spark? How did you tune it?

Both projects use PySpark on Databricks. Key tuning:

**Tesla (serverless)**: Auto-scaling handles cluster sizing. No manual tuning needed. coalesce(1) for Parquet staging files in Snowflake load. Left-anti joins use broadcast hash join when table is small enough.

**FraudStream**: Trigger intervals control batch size — 10s for Bronze/Silver, 60s for Gold. Watermark controls state cleanup. foreachBatch allows custom logic per micro-batch. Spark checkpointing on all streaming queries for fault tolerance.

**Spark Connect compatibility**: Both projects work on serverless (Spark Connect). Avoided RDD APIs, used DataFrame API exclusively. Used spark.interruptAll() instead of sparkContext.cancelAllJobs() for graceful query shutdown.

---

## 5. Data Quality & Reliability (Q16-Q18)

### Q16: How did you ensure data quality?

**Tesla**: 7 DQ rules in Bronze: (1) required fields non-null, (2) VIN format validation, (3) timestamp range (not future, not >30 days old), (4) speed 0-300 km/h, (5) battery SOC 0-100%, (6) battery temp -40 to 120C, (7) event type in known set. Failed records go to quarantine table with rejection_reason. Quarantine partitioned by source_run_id for debugging.

**FraudStream**: Auto Loader with bad data handling. Bronze DQ checks on transaction fields (amount > 0, valid merchant, valid timestamp). Quarantine table with rejection reasons. Schema inference catches malformed JSON.

Both: quarantine is NOT discarded — its stored for analysis. DQ is a gate, not a filter — bad data never reaches Silver/Gold.

### Q17: How did you handle failures and retries?

**Tesla**: max_retries=1, 5-min cooldown. Checkpoint-based idempotency — crash leaves checkpoint stale, retry reprocesses same run, left_anti join catches duplicates, 0 new writes, checkpoint advances. Tested at all 3 layers (Bronze, Silver, Gold) — all 0 duplicates on retry.

**FraudStream**: task_retry_mode ON_FAILURE — streaming tasks auto-restart. Checkpoints on all streaming queries — resume from last successful offset. safe_foreachbatch catches errors, logs with exc_info, does NOT re-raise — stream continues to next batch. One bad batch doesnt kill the pipeline.

Both: email alerts on failure (task + job level). Business SQL alerts catch data anomalies even when pipeline is healthy.

### Q18: How did you monitor your pipelines?

**Three layers of monitoring:**

1. **Infrastructure**: Task-level + job-level email alerts. Auto-retry. System tables (system.lakeflow.job_task_run_timeline) for run history, durations, failure states.

2. **Business**: Tesla has 3 SQL alerts every 15 min (battery temp >55C, battery SOC <10%, charging errors >100/run). FraudStream has 1 alert (HIGH risk anomaly spike).

3. **Dashboards**: Tesla has 4-widget pipeline monitoring dashboard (task duration per run, bottleneck trend, failed runs, avg duration by task). FraudStream has real-time fraud monitoring dashboard.

---

## 6. Real-Time / Streaming (Q19-Q21)

### Q19: Did you work with streaming data (Kafka/Kinesis)?

**FraudStream V3** uses Databricks Structured Streaming with Auto Loader (cloudFiles source). Not Kafka/Kinesis directly, but the same streaming concepts apply: micro-batch processing, watermarks, checkpoints, state stores.

01_stream_generator writes JSON files to UC Volume at 200 events/5s. Auto Loader discovers new files incrementally. Each downstream stage reads from the previous stages Delta table as a stream.

If Kafka were needed: would use kafka source instead of cloudFiles, same watermark/dropDuplicates/foreachBatch patterns apply. Auto Loader is file-based Kafka alternative — simpler for development, same streaming semantics.

### Q20: How did you handle late-arriving data?

**FraudStream**: withWatermark on event_ts with 10 minutes. Any record with event_ts older than (max_event_ts - 10 min) is dropped as too late. Combined with dropDuplicates on event_id, the state store only holds 10 minutes of event IDs — bounded state, no OOM.

Originally 60 seconds — too aggressive, legitimate late events (network latency, retries) were dropped. 10 minutes balances late-data tolerance with state cleanup.

**Tesla**: batch mode — late data handled by re-running the pipeline. Checkpoint can be manually reset to reprocess old runs. left_anti join ensures no duplicates even when reprocessing.

### Q21: What is your approach to real-time analytics?

**FraudStream pattern**: streaming bronze (Auto Loader) -> streaming silver (watermark + dedup) -> streaming scoring (velocity window + rules) -> micro-batch gold (60s trigger, full recompute). End-to-end latency ~10-15 seconds.

**Tesla (if converted to streaming)**: would replace generator with Kafka source, Bronze with Auto Loader, Silver with watermark + dropDuplicates, Gold with foreachBatch + MERGE (affected-key recompute from 04_gold_incremental already implements this pattern), real-time alerting via foreachBatch to alert table. The batch design is a stepping stone to streaming — the incremental patterns are already streaming-compatible.

---

## Category 4: Scenario-Based Questions (Q23-Q25)

### Q23: Silver is getting slow after 3 days. How do you diagnose?

Check system.lakeflow.job_task_run_timeline for duration trends — is Silver duration growing run over run? Check Spark UI for scan sizes — is Silver reading more data than expected? Likely causes: (1) Delta small files — run OPTIMIZE on Silver table, (2) left_anti join scanning full Silver — ensure partition pruning on event_date is working, (3) checkpoint not advancing — verify checkpoint table is updated correctly, (4) Bronze discovering too many old runs — check if checkpoint is stale.

With partition pruning on event_date, Silver should only scan recent partitions. If the checkpoint is stale, Bronze re-discovers old runs, Silver re-processes them (left_anti join filters them out, but the scan still happens). Fix: ensure checkpoint advances atomically after successful write.

### Q24: Gold is slow with 9M Silver rows. How to optimize?

(1) Use 04_gold_incremental — affected-key recompute scans only affected partitions, not full 9M. With 500K batch touching ~5K vehicles, reads ~1M rows instead of 9M — 9x reduction.
(2) OPTIMIZE plus Z-order on vin for GROUP BY performance.
(3) Liquid Clustering on (vin, event_date) for adaptive layout.
(4) Materialized views for pre-computed aggregates.
(5) Batch multiple Silver runs before Gold to reduce MERGE frequency.
(6) Remove .count() logging calls that trigger full table scans.

The incremental variant was validated: identical results to full rebuild.

### Q25: Databricks platform upgrade. How do you handle it?

Job is already PAUSED (daily 2AM schedule). No action needed during upgrade. S3 data accumulates. Run manually after upgrade — Bronze discovers all RAW runs, checkpoint processes incrementally, no duplicates. If run was interrupted mid-execution then re-run, idempotency handles dedup via left_anti join. DABs ensure job config survives — redeploy via bundle deploy if needed.

If using serverless compute, the upgrade is transparent — Databricks manages the runtime. No cluster provisioning or configuration changes needed.

---

## Quick Reference

| Metric | Tesla Telemetry |
|--------|----------------|
| Architecture | Batch medallion |
| Notebooks | 9 |
| Job Mode | Scheduled (5 sequential) |
| Data Storage | S3 (Delta) |
| Incremental | Checkpoint-based (last_processed_run_id) |
| Deduplication | Left-anti join on event_id |
| DQ Rules | 7 rules + quarantine |
| Gold Tables | 5 (MERGE + idempotent append) |
| Gold Incremental | Affected-key recompute |
| Error Handling | Checkpoint retry-safe |
| Performance | Partition pruning, affected-key, MERGE, DeltaTable.isDeltaTable |
| Auto-Retry | max_retries 1, 5-min cooldown |
| SQL Alerts | 3 (temp, SOC, charging) |
| Dashboard | 4-widget pipeline monitoring |
| DABs | dev/prod, 2 jobs |
| CI/CD | GitHub Actions |
| GitHub | github.com/guptamasoom/tesla-telemetry |
| Git Files | 16 |
| Validation | 3 layers crash-tested |
| Scale Tested | 500K, 1M, 2M per run |
| Gold Volume | 5 tables (telemetry_enriched, hourly, trips, battery, alerts) |
| Compute | Serverless |

---

*Updated: October 12, 2026*