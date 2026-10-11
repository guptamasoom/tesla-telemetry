# Mock Interview Prep — Data Engineering (Batch + Streaming)

> **Projects**: FraudStream V3 (real-time streaming) + Tesla Vehicle Telemetry (batch medallion)
> 30 questions across 4 categories with detailed answers.

---

## Category 1: Architecture & Design Decisions (Q1-Q8)

### Q1: Walk me through both your pipeline architectures.

**FraudStream V3** — Real-time streaming fraud detection using Databricks Structured Streaming. 7 notebooks, 5 concurrent tasks in a continuous job (no dependencies). Data flows: 01_stream_generator (200 events/5s, 8% fraud rate) to 02_bronze_stream (Auto Loader cloudFiles + DQ checks + quarantine) to 03_silver_stream (10-min watermark + dropDuplicates) to 04_scoring_stream (2-day velocity window + 7 fraud rules) to 05_gold_stream (7 gold tables, 60s trigger, full recompute) to 06_validation_stream (37/37 checks passed). Data stored in UC Volume.

**Tesla Telemetry** — Batch medallion ETL. 9 notebooks, 5 sequential tasks with dependencies (generate, bronze, silver, gold, snowflake). Data flows: 01_generate_telemetry (500K events/run, 10K+ vehicles, 5 German cities) to 02_bronze_batch (checkpoint-based run discovery + 7 DQ rules + quarantine) to 03_silver (standardize + derive columns + left_anti join dedup) to 04_gold (5 tables, MERGE for aggregates + idempotent append for events) to 05_load_snowflake. Data stored in S3.

Key difference: FraudStream uses continuous streaming (all stages run simultaneously, real-time). Tesla uses batch incremental (each stage processes a complete batch, checkpoint advances, next run picks up only new data).

### Q2: Why streaming for FraudStream and batch for Tesla?

FraudStream needs real-time fraud detection — transactions must be scored within seconds to block fraudulent transfers. A 10-minute batch delay means a fraudulent transaction could complete before we flag it. Structured Streaming with 10-second triggers gives ~10-15 second end-to-end latency.

Tesla telemetry arrives in discrete batches — each generation run is a complete snapshot. No real-time requirement — battery health trends, trip metrics, alert distribution dont need sub-second latency. Checkpoint-based incremental is simpler to debug, reason about, and test. The architecture follows the data, not the other way around.

### Q3: Explain the medallion architecture in both projects.

**Bronze** (Raw + DQ): Both ingest raw data and apply DQ checks. FraudStream uses Auto Loader with schema inference; Tesla uses dbutils.fs.ls run discovery. Both quarantine bad records with rejection reasons. Bronze is append-only.

**Silver** (Cleaned + Deduplicated): FraudStream uses watermark + dropDuplicates for streaming dedup. Tesla uses left_anti join on event_id for batch dedup. Both add derived columns (FraudStream: fraud scores; Tesla: battery_band, speed_category, is_moving, event_date).

**Gold** (Analytics-ready): FraudStream builds 7 tables with 60s trigger (full recompute). Tesla builds 5 tables with MERGE (aggregates) + idempotent append (events). Gold is the consumption layer for dashboards and SQL alerts.

### Q4: How does checkpoint-based incremental work in Tesla? What happens on crash?

Each layer has a checkpoint Delta table tracking last_processed_run_id. On each run: (1) discover new runs greater than checkpoint, (2) process new runs, (3) write to next layer, (4) update checkpoint ONLY after successful write.

**Crash scenario**: If Bronze writes data but crashes before checkpoint update, the next run re-discovers the same run. The left_anti join on event_id catches all records already in Bronze resulting in 0 new writes, no duplicates. Checkpoint then advances. Pipeline is retry-safe by design.

Tested: simulated crash (wrote Bronze, skipped checkpoint) then retried. All 500K caught by already_in_bronze. 0 duplicates. Checkpoint updated.

### Q5: How does the watermark work in FraudStream? Why 10 minutes?

withWatermark on event_ts with 10 minutes on Silver stream. Any record with event_ts older than (max_event_ts minus 10 min) is dropped as too late. Combined with dropDuplicates on event_id, the state store only holds 10 minutes of event IDs — bounded state, no OOM.

Originally 60 seconds — too aggressive, legitimate late events were dropped. 10 minutes balances late-data tolerance with state cleanup. Without the watermark, dropDuplicates would store ALL event IDs ever seen resulting in unbounded state and OOM.

### Q6: Why 5 concurrent tasks (FraudStream) vs 5 sequential tasks (Tesla)?

FraudStream: streaming requires all stages running simultaneously. Bronze ingests from Auto Loader, Silver reads Bronze stream, Scoring reads Silver stream — if sequential, Silver would never start because streaming never finishes. Continuous mode with no dependencies.

Tesla: batch requires each stage to complete before next. Bronze must finish before Silver reads new partitions. Silver must finish before Gold aggregates. depends_on chain ensures correct ordering.

### Q7: How does Gold differ between the two projects?

FraudStream Gold: full recompute with 60s trigger. Reads entire Scored table every 60s, rebuilds all 7 tables. Simple and correct, but expensive at scale. Uses complete output mode.

Tesla Gold: hybrid strategy. Event-level tables (telemetry_enriched, alerts) use idempotent append (left_anti join). Aggregation tables (hourly, trips, battery) use MERGE — upsert affected keys with new aggregate values. The incremental variant (04_gold_incremental) uses affected-key recompute — only recomputes keys touched by new batch.

### Q8: How do you separate dev and prod?

Both use DABs with dev/prod targets. dev: mode=development (allows overwrites). prod: mode=production (prevents accidental overwrites). Variables parameterized with notebook_root for paths and alert_email for emails. dev target adds (DEV) suffix to job name. CI/CD: GitHub Actions validates on PR (dev target), deploys on main push (prod target).

---

## Category 2: Databricks and Spark Deep Dive (Q9-Q16)

### Q9: How does Auto Loader work? Advantages over readStream?

Auto Loader (cloudFiles format) incrementally discovers new files in cloud storage. Uses checkpoint and schema location to track processed files and infer schema. Advantages: (1) incremental file discovery — no manual tracking, (2) schema inference plus evolution — handles new fields gracefully, (3) exactly-once — each file processed exactly once via checkpoints, (4) bad data handling. In FraudStream, Auto Loader watches UC Volume for new JSON files from the generator.

### Q10: Explain dropDuplicates with watermark.

Spark maintains a state store for event_id deduplication. When a record arrives: (1) check if event_id is in state store within 10-min window, (2) if yes then drop (duplicate), (3) if no then add to state store and pass through. When watermark advances past a records event_ts, Spark removes that event_id from state — bounded state cleanup. Without watermark, state grows forever resulting in OOM.

### Q11: Why foreachBatch in FraudStream? What is safe_foreachbatch?

foreachBatch allows arbitrary batch processing on each micro-batch. Used because: (1) multiple sinks (write to Scored and Alerts from same batch), (2) MERGE operations not available in standard streaming sinks, (3) complex logic (velocity features need joining micro-batch with 2-day Silver window), (4) error handling.

safe_foreachbatch pattern: try/except wrapper that catches errors, logs with exc_info=True (full stack trace), and does NOT re-raise — lets the stream continue. One bad batch doesnt kill the entire pipeline. Structured logging via logging.basicConfig in 00_config_stream.

### Q12: What is the 2-day scoring window optimization?

Scoring computes velocity features (transactions per user in last 2 days). Originally read entire Silver table — O(n) where n = all Silver records. Optimization: filter Silver to last 2 days using date_sub(current_timestamp(), 2). Bounded read regardless of Silver size. With Delta partition pruning on event_date, only relevant partitions scanned. As pipeline runs for weeks, Silver grows but scoring read stays bounded.

### Q13: How does left_anti join idempotency work in Tesla?

Two levels: (1) Run-level — checkpoint tracks last_processed_run_id, only new runs processed. (2) Event-level — left_anti join on event_id against existing Bronze/Silver/Gold returns only events NOT already present. If a run was partially processed (crash), retry re-discovers the run but left_anti join excludes already-written events. Only unprocessed events written. No duplicates.

### Q14: Why MERGE instead of overwrite in Tesla Gold?

Aggregations change as new data arrives — a vehicles hourly avg speed updates when new events arrive for that hour. Overwrite would lose all existing Gold data (unsafe on crash). Append would create duplicate keys. MERGE upserts — updates existing keys, inserts new keys. Idempotent: reprocessing the same batch produces the same MERGE result.

Originally had mode(overwrite) from users pandas code — unsafe in production. Replaced with MERGE plus idempotent append.

### Q15: What is affected-key recompute in incremental Gold?

Instead of rebuilding all aggregates from full Silver: (1) extract distinct (vin, event_date) from new batch — these are affected keys, (2) read from full Silver but filter to affected keys only using left_semi join plus partition pruning, (3) recompute aggregates for affected keys, (4) MERGE into Gold. Reduces Silver read from full-scan to only affected partitions. Validated: full recompute and affected-key recompute produce identical counts.

### Q16: What Spark optimizations did you apply?

FraudStream: (1) 2-day scoring window — bounded Silver read, (2) Gold 60s trigger (from 10s) — 6x less compute, (3) Bronze single groupBy (from 5 count() calls) — one pass instead of five, (4) watermark state cleanup.

Tesla: (1) checkpoint-based incremental — only new runs processed, (2) partition pruning (source_run_id, event_date), (3) affected-key recompute — only changed keys recomputed, (4) left_anti vs NOT IN — hash join, NULL-safe, (5) MERGE vs overwrite — incremental upsert.

---

## Category 3: Production and Operations (Q17-Q23)

### Q17: How do you monitor both pipelines?

Three layers: (1) Infrastructure — task-level plus job-level email alerts, auto-retry (FraudStream: task_retry_mode ON_FAILURE; Tesla: max_retries 1, 5-min cooldown). (2) Business — SQL alerts: FraudStream has 1 (HIGH risk anomaly spike), Tesla has 3 (battery temp >55C, battery SOC <10%, charging error >100), all every 15 min. (3) Dashboards — FraudStream real-time fraud monitoring, Tesla pipeline monitoring (11 widgets: 4 counters + 7 charts). Tesla also has 06_pipeline_monitoring notebook querying system.lakeflow tables.

### Q18: What happens when a task fails at 3 AM?

Step 1: Auto-retry (automatic) — FraudStream task_retry_mode restarts task; Tesla max_retries retries after 5 min. Transient failures (network, OOM) self-heal.
Step 2: Email alert (automatic) — task-level email fires immediately identifying which task failed.
Step 3: Investigation (manual) — FraudStream has structured logs (logger.error with exc_info); Tesla has checkpoint-based retry (just re-run, idempotency handles dedup).
Step 4: Business alert check — SQL alerts run every 15 min, catch data anomalies even if pipeline is healthy.
Step 5: Fix via CI/CD — push fix, PR, validate, merge, auto-deploy.

### Q19: Walk me through your CI/CD pipeline.

Developer makes change, push to feature branch, create PR, GitHub Actions runs databricks bundle validate --target dev (validates YAML, resources, notebook paths), code review, merge to main, GitHub Actions runs databricks bundle deploy --target prod (updates Databricks job automatically). GitHub secrets: DATABRICKS_HOST, DATABRICKS_TOKEN. No manual UI changes. Full audit trail via Git history plus GitHub Actions.

### Q20: Explain safe_foreachbatch error handling.

safe_scoring_batch and safe_gold_batch wrap foreachBatch in try/except. On error: logger.error with exc_info=True (full stack trace in job logs). Does NOT re-raise — stream continues to next batch. One bad batch doesnt kill the pipeline. Checkpoint records last successful offset — on restart, stream resumes from last success, not from failed batch. Fixed a bare except in Gold chargeback_summary that was silently swallowing errors.

### Q21: Whats in your Git repos?

FraudStream (github.com/guptamasoom/fraudstream-v3): 11 files — 7 notebooks (.py), databricks.yml (DABs with parameterized paths, dev/prod), .github/workflows/deploy.yml (CI/CD), FraudStream_V3_Production_Guide.md, Layer_Architecture_Details.md.

Tesla (github.com/guptamasoom/tesla-telemetry): 15 files — 9 notebooks (.py), databricks.yml (DABs with 2 jobs: main + 1M test), .github/workflows/deploy.yml, 4 docs (Production Hardening Guide, Project Explained, Interview Prep, Scaling Study).

Both repos clean, all changes pushed to main.

### Q22: What SQL anomaly alerts did you create?

Tesla: 3 alerts every 15 min — Battery Temp >55C (thermal runaway), Battery SOC <10% (stranding risk), Charging Error >100/run (infrastructure issue). FraudStream: 1 alert — HIGH Risk Anomaly Spike. These are business alerts — they monitor data anomalies even when the pipeline is healthy. Infrastructure alerts (email on task failure) tell you when the pipeline breaks. Business alerts tell you when the data is unusual. Both are needed.

### Q23: How did you test both pipelines?

FraudStream: 06_validation_stream with 37 checks (table existence, row counts, schema, DQ metrics, fraud rules, gold completeness) — 37/37 passed (100%). Test notebook validates spark.interruptAll() graceful shutdown. KPIs: 3,914 txns, 6 high-risk, $1.8M total.

Tesla: Bronze retry test (500K reprocessed, 0 duplicates), Silver retry test (same), Gold retry test (event-level 0 new, aggregates MERGE correct, all counts identical). 1M scale test (separate job). 2M batch test. All 3 layers proven: incremental, append-only, idempotent, retry-safe.

---

## Category 4: Scenario-Based Questions (Q24-Q30)

### Q24: Silver is getting slow after 3 days. How do you diagnose?

Check system.lakeflow.job_task_run_timeline for duration trends. Check Spark UI streaming metrics (input rate vs processing rate). Check state store size. Likely causes: (1) state growth — verify watermark is working, (2) input rate greater than processing rate — reduce BATCH_SIZE, (3) Delta small files — run OPTIMIZE, (4) increase trigger interval. FraudStreams 10-min watermark should bound state. If state is unbounded, watermark column might be misconfigured.

### Q25: Tesla Bronze crashes mid-run, checkpoint not updated. What happens?

Next run re-discovers the same run (checkpoint still points to previous). Bronze reads same 500K events. left_anti join on event_id catches all events already in Bronze resulting in 0 new writes, no duplicates. Checkpoint then advances. Pipeline is idempotent and retry-safe — just re-run the job. Tested this exact scenario: 500K reprocessed, 0 duplicates, checkpoint updated.

### Q26: Add a new fraud rule to FraudStream. Full process?

(1) Write rule in 04_scoring_stream (or 00_config_stream for shared rules). (2) Run 06_validation_stream — all 37 checks must still pass. (3) Push to feature branch, PR, GitHub Actions validates DABs. (4) Review and merge, GitHub Actions deploys to prod. (5) Continuous mode auto-restarts (CONTINUOUS_RESTART trigger) with new code. (6) Monitor dashboard for alert distribution changes. No manual UI changes — everything automated through CI/CD. Rule is isolated to scoring layer, no changes needed in Bronze/Silver.

### Q27: Convert Tesla batch to streaming?

(1) Replace generator with Kafka source. (2) Bronze with Auto Loader or Kafka source plus DQ in foreachBatch. (3) Silver with watermark plus dropDuplicates (same as FraudStream). (4) Gold with foreachBatch plus MERGE (affected-key recompute from 04_gold_incremental already implements this pattern). (5) Real-time alerting via foreachBatch to alert table. (6) Schema evolution with mergeSchema for new firmware. (7) Watermark 10-30 min for tunnel/poor connectivity late data. The incremental Gold variant already has the streaming-compatible approach.

### Q28: Gold is slow with 9M Silver rows. How to optimize?

(1) Use 04_gold_incremental — affected-key recompute scans only affected partitions, not full 9M. (2) OPTIMIZE plus Z-order on vin for GROUP BY performance. (3) Liquid Clustering on (vin, event_date) for adaptive layout. (4) Materialized views for pre-computed aggregates. (5) Batch multiple Silver runs before Gold to reduce MERGE frequency. The incremental variant was validated: identical results to full rebuild.

### Q29: Databricks platform upgrade. How do you handle it?

FraudStream: Pause continuous job, all 5 tasks checkpoint and stop gracefully, incoming events accumulate in UC Volume, resume after upgrade, Auto Loader picks up accumulated files, watermark handles out-of-order events, no data loss. CONTINUOUS_RESTART trigger handles automatic restarts (seen in run history).

Tesla: Job already PAUSED (daily 2AM). No action needed. S3 data accumulates. Run manually after upgrade, Bronze discovers all RAW runs, checkpoint processes incrementally, no duplicates. If run was interrupted mid-execution then re-run, idempotency handles dedup. DABs ensure job config survives — redeploy via bundle deploy if needed.

### Q30: Why should we trust your pipeline? How do you know data is correct?

Data quality: FraudStream has Auto Loader plus Bronze DQ checks plus quarantine. Tesla has 7 DQ rules plus quarantine. Bad data is isolated, good data flows through.

Idempotency: FraudStream dropDuplicates with watermark — no duplicate events. Tesla left_anti join at every layer — no duplicates even on retry.

Validation: FraudStream 37/37 checks passed. Tesla 3 layers crash-tested (Bronze, Silver, Gold — all 0 duplicates on retry, all counts identical).

Monitoring: Infrastructure (email plus auto-retry) plus business (SQL alerts every 15 min) plus dashboards. Two levels of alerting — pipeline health and data health.

Reproducibility: Both on GitHub with DABs plus CI/CD. Exact same configuration deployable to any workspace. Checkpoints plus idempotency mean any run can be safely re-processed.

---

## Quick Reference Table

| Metric | FraudStream V3 | Tesla Telemetry |
|--------|---------------|----------------|
| Architecture | Real-time streaming | Batch medallion |
| Notebooks | 7 | 9 |
| Job Mode | Continuous (5 concurrent) | Scheduled (5 sequential) |
| Data Storage | UC Volume | S3 |
| Watermark | 10 minutes | N/A (checkpoint-based) |
| Deduplication | dropDuplicates + watermark | left_anti join on event_id |
| DQ Rules | Auto Loader + Bronze checks | 7 rules + quarantine |
| Gold Tables | 7 (full recompute, 60s) | 5 (MERGE + idempotent append) |
| Gold Incremental | N/A | Affected-key recompute |
| Error Handling | safe_foreachbatch | Checkpoint retry-safe |
| Performance | 2-day window, single groupBy, 60s trigger | Partition pruning, affected-key, MERGE |
| Auto-Retry | task_retry_mode ON_FAILURE | max_retries 1, 5-min |
| SQL Alerts | 1 (HIGH risk spike) | 3 (temp, SOC, charging) |
| Dashboards | 1 (real-time fraud) | 1 (11-widget monitoring) |
| DABs | dev/prod, parameterized | dev/prod, 2 jobs |
| CI/CD | GitHub Actions | GitHub Actions |
| GitHub | github.com/guptamasoom/fraudstream-v3 | github.com/guptamasoom/tesla-telemetry |
| Git Files | 11 | 15 |
| Validation | 37/37 passed | 3 layers crash-tested |
| Scale Tested | Continuous (200/5s) | 500K, 1M, 2M per run |
| Gold Volume | 7 tables (fraud_summary, etc.) | 5 tables (9M, 2.9M, 450K, 256K, 256K) |

---

*Generated: October 11, 2026*