# Project Gap Analysis — Both Interview Projects

> **Purpose**: Comprehensive gap review of both data engineering projects (FraudStream V3 = streaming, Tesla Telemetry = batch) to identify and fix weaknesses an interviewer might catch.

---

## Project Overview

| Aspect | FraudStream V3 | Tesla Telemetry |
|--------|---------------|-----------------|
| **Architecture** | Real-time streaming (Structured Streaming) | Batch medallion (Bronze→Silver→Gold→Snowflake) |
| **Notebooks** | 7 (config, generator, bronze, silver, scoring, gold, validation) | 9 (generator, 1M generator, bronze, silver, gold, gold_incremental, snowflake, snowflake_incremental, monitoring) |
| **Job Mode** | Continuous (5 concurrent tasks, no dependencies) | Sequential (5 tasks with dependencies) |
| **Data Storage** | UC Volume (Databricks managed) | S3 (external) |
| **GitHub** | https://github.com/guptamasoom/fraudstream-v3 | https://github.com/guptamasoom/tesla-telemetry |

---

## Gap Analysis: Before vs After

### FraudStream V3 (Streaming)

| Gap | Severity | Status | What Was Fixed |
|-----|----------|--------|----------------|
| DABs used hardcoded notebook paths | MEDIUM | ✅ FIXED | Replaced with `${var.notebook_root}` variable — now dev/prod targets can use different paths |
| DABs missing `max_concurrent_runs: 1` | MEDIUM | ✅ FIXED | Added to prevent concurrent continuous runs from conflicting |
| No `no_alert_for_skipped_runs` | LOW | ⏭️ N/A | Not needed — all 5 tasks run concurrently, no task can be "skipped" due to dependencies |
| Test notebook not in Git | LOW | ⏭️ OK | Utility notebook, not part of pipeline — correctly excluded |
| Dashboard/alert not in Git | LOW | ⏭️ OK | Expected — Databricks assets (dashboards, alerts) are not file-based |

### Tesla Telemetry (Batch)

| Gap | Severity | Status | What Was Fixed |
|-----|----------|--------|----------------|
| **No dashboard** | **CRITICAL** | ✅ FIXED | Created "Tesla Vehicle Telemetry — Pipeline Monitoring" dashboard with 7 widgets (gold table counts, alert distribution, battery health, vehicle activity, speed categories, daily alert trend, battery temp status) |
| DABs only defined main job (not 1M test) | MEDIUM | ✅ FIXED | Added `tesla_telemetry_1m_test` job to DABs — now both jobs are version-controlled as code |
| 9 duplicate `.py.py` files in Git folder | LOW | ⏭️ Cosmetic | Untracked files, not pushed to GitHub — cleanup optional |
| Monitoring notebook not in job | LOW | ⏭️ BY DESIGN | 06_pipeline_monitoring is ad-hoc, not part of ETL pipeline |

---

## Current State: Both Projects

### FraudStream V3 — What's Now Solid

| Component | Status | Details |
|-----------|--------|---------|
| 7 Notebooks | ✅ | Config → Generator → Bronze → Silver → Scoring → Gold → Validation |
| Continuous Job (5 concurrent) | ✅ | Job ID 50077675888111, task_retry_mode: ON_FAILURE |
| Task + Job Email Alerts | ✅ | All 5 tasks + job level → masoomgupta0715@gmail.com |
| DABs (parameterized) | ✅ | dev/prod targets, `${var.notebook_root}`, `${var.alert_email}` |
| CI/CD (GitHub Actions) | ✅ | PR validates, main push deploys |
| Real-Time Dashboard | ✅ | FraudStream V3 — Real-Time Fraud Monitoring |
| SQL Alert | ✅ | HIGH Risk Anomaly Spike |
| Structured Logging | ✅ | logging.basicConfig in config, all notebooks use logger |
| Error Handling | ✅ | safe_scoring_batch + safe_gold_batch wrappers |
| Performance Optimizations | ✅ | Scoring 2-day window, Gold 60s trigger, Bronze single groupBy |
| Watermark (late data) | ✅ | 10 minutes |
| Checkpoints | ✅ | All streaming queries checkpointed |
| Validation Notebook | ✅ | 06_validation_stream — 37/37 checks passed |
| Documentation | ✅ | Production Guide + Layer Architecture Details (in Git) |
| Git Version Control | ✅ | Clean, up-to-date main branch |

### Tesla Telemetry — What's Now Solid

| Component | Status | Details |
|-----------|--------|---------|
| 9 Notebooks | ✅ | Generator, 1M Generator, Bronze, Silver, Gold, Gold Incremental, Snowflake, Snowflake Incremental, Monitoring |
| 2 Jobs (main + 1M test) | ✅ | Job 818230914419317 (daily 2AM, PAUSED) + Job 288555998184699 (one-time) |
| Task + Job Email + Retry | ✅ | All 5 tasks per job, max_retries:1, retry_on_failure:true |
| DABs (both jobs) | ✅ | Main job + 1M test job, parameterized paths |
| CI/CD (GitHub Actions) | ✅ | PR validates, main push deploys |
| Monitoring Dashboard | ✅ | Tesla Vehicle Telemetry — Pipeline Monitoring (7 widgets) |
| 3 SQL Alerts | ✅ | Battery Temp >55°C, Battery SOC <10%, Charging Error >100 |
| Incremental Checkpoints | ✅ | Bronze/Silver/Gold all checkpoint-based |
| Idempotency | ✅ | event_id dedup (left_anti join) all layers |
| Retry-Safe | ✅ | Checkpoint not updated on crash → safe reprocess |
| Data Quality | ✅ | 7 DQ rules in Bronze, quarantine table |
| Documentation | ✅ | Production Guide + Project Explained + Interview Prep + Scaling Study (in Git) |
| Git Version Control | ✅ | 15 files committed, up-to-date main branch |

---

## Interview Readiness Scorecard

| Interview Question | FraudStream V3 | Tesla Telemetry |
|-------------------|----------------|-----------------|
| "How do you monitor the pipeline?" | ✅ Dashboard + SQL alert + email | ✅ Dashboard + 3 SQL alerts + email + monitoring notebook |
| "What happens if a task fails?" | ✅ Task email + auto-retry (ON_FAILURE) | ✅ Task email + auto-retry (max_retries:1) |
| "How do you handle late data?" | ✅ 10-min watermark + dropDuplicates | ✅ N/A (batch, checkpoint-based) |
| "How do you handle duplicates?" | ✅ Watermark + dropDuplicates in Silver | ✅ event_id left_anti join all layers |
| "How do you deploy changes?" | ✅ DABs + GitHub Actions CI/CD | ✅ DABs + GitHub Actions CI/CD |
| "How do you test at scale?" | ✅ Validation notebook (37/37) | ✅ 1M test job (DABs version-controlled) |
| "How do you visualize data?" | ✅ Real-time dashboard | ✅ 7-widget monitoring dashboard |
| "How do you handle DQ?" | ✅ Bronze DQ checks + quarantine | ✅ 7 DQ rules + quarantine table |
| "How do you handle errors?" | ✅ safe_foreachbatch wrappers + logging | ✅ Checkpoint-based retry-safe design |
| "Is it version-controlled?" | ✅ GitHub repo, clean main | ✅ GitHub repo, 15 files on main |
| "How do you scale?" | ✅ Continuous mode, 5 concurrent | ✅ Scaling study doc, 1M/2M tested |
| "How do you separate dev/prod?" | ✅ DABs dev/prod targets + variables | ✅ DABs dev/prod targets + variables |

---

## What Makes These Two Projects Interview-Strong Together

### 1. Batch + Streaming Coverage
FraudStream V3 demonstrates **real-time streaming** with Structured Streaming, watermarks, continuous mode, and concurrent task execution. Tesla Telemetry demonstrates **batch processing** with checkpoint-based incremental processing, idempotency, and sequential task dependencies. Together, they cover the two fundamental data engineering paradigms.

### 2. Production Hardening
Both projects have: email alerts (job + task level), auto-retry on failure, DABs (Infrastructure as Code), CI/CD (GitHub Actions), SQL anomaly alerts, dashboards, structured logging (FraudStream), and comprehensive documentation.

### 3. Data Quality
FraudStream has DQ checks in Bronze with quarantine. Tesla has 7 DQ rules with quarantine. Both demonstrate a focus on data quality, not just data movement.

### 4. Idempotency & Retry Safety
Both pipelines are idempotent — reprocessing the same data produces no duplicates. Both are retry-safe — a crash doesn't corrupt the pipeline state. This is a key interview differentiator.

### 5. Version Control & CI/CD
Both projects are on GitHub with DABs configs and GitHub Actions. This shows DevOps maturity — the interviewer can see that you treat data pipelines as software.

---

## Potential Interview Questions (and how to answer)

### Q: "Why did you use batch for Tesla and streaming for FraudStream?"
**A**: Tesla telemetry data arrives in discrete batches (each generation run is a complete snapshot), so checkpoint-based incremental batch processing is the natural fit — simpler, more reliable, and easier to debug. FraudStream needs real-time fraud detection — transactions arrive continuously and must be scored within seconds, so Structured Streaming with watermarks and concurrent processing is the right choice. The architecture follows the data, not the other way around.

### Q: "How would you convert the Tesla batch pipeline to streaming?"
**A**: I would replace the batch generator with a Kafka source, use Structured Streaming with Auto Loader for Bronze, add a watermark for late data in Silver, use `foreachBatch` in Gold for MERGE operations (not possible in pure streaming), and add real-time alerting. I documented this in the production hardening guide.

### Q: "What's the difference between your two DABs configs?"
**A**: The FraudStream DABs uses `continuous` mode with `task_retry_mode: ON_FAILURE` and 5 concurrent tasks with no dependencies. The Tesla DABs uses `schedule` mode with `max_retries: 1` and 5 sequential tasks with `depends_on` chains. Both use `${var.notebook_root}` for parameterization and have dev/prod targets. The Tesla DABs also includes a second job definition for 1M scale testing.

### Q: "How do you handle a pipeline failure at 3 AM?"
**A**: Both pipelines have task-level email alerts — I'd get an email immediately when a specific task fails, not just when the entire job fails. FraudStream auto-retries via `task_retry_mode: ON_FAILURE`. Tesla auto-retries via `max_retries: 1` with a 5-minute cooldown. For transient failures, the pipeline self-heals. For systematic failures, I'd check the email, look at the logs (FraudStream has structured logging), and fix the root cause. SQL alerts would also fire if business metrics are anomalous, giving me early warning even before a pipeline failure.

---

*Generated: October 11, 2026*
*Projects: FraudStream V3 (streaming) + Tesla Telemetry (batch)*
*GitHub: github.com/guptamasoom/fraudstream-v3 + github.com/guptamasoom/tesla-telemetry*