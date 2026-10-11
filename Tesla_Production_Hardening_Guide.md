# Tesla Vehicle Telemetry — Production Hardening Guide

> **Purpose**: Documents all production-level upgrades applied to the Tesla Telemetry pipeline on Databricks. This guide covers monitoring, alerting, version control, CI/CD, and configuration hardening — without changing any notebook code.

---

## Table of Contents

1. [Pipeline Overview](#pipeline-overview)
2. [Step 1: Task-Level Email Alerts + Retry](#step-1-task-level-email-alerts--retry)
3. [Step 2: SQL Anomaly Alerts](#step-2-sql-anomaly-alerts)
4. [Step 3: Declarative Automation Bundles (DABs)](#step-3-declarative-automation-bundles-dabs)
5. [Step 4: CI/CD Pipeline](#step-4-cicd-pipeline)
6. [Step 5: Git Version Control](#step-5-git-version-control)
7. [Architecture Diagram](#architecture-diagram)
8. [Interview Q&A](#interview-qa)

---

## Pipeline Overview

### Architecture: Batch Medallion (Bronze → Silver → Gold → Snowflake)

```
01_generate_telemetry → 02_bronze_batch → 03_silver → 04_gold → 05_load_snowflake
    (500K events/run)      (Auto Loader)     (dedup)     (5 tables)   (Snowflake)
                                                                         ↓
                                                               06_pipeline_monitoring
                                                                (system tables)
```

### Databricks Jobs

| Job ID | Name | Schedule | Mode |
|--------|------|---------|------|
| 818230914419317 | Tesla Telemetry ETL Pipeline | Daily 2AM UTC (PAUSED) | Sequential 5 tasks |
| 288555998184699 | Tesla Telemetry ETL Pipeline - 1M Test | One-time | Sequential 5 tasks |

### S3 Paths

| Layer | Path |
|-------|------|
| RAW | `s3://tesla-vehicle-telemetry-511976431596/telemetry/raw/` |
| Bronze | `s3://.../telemetry/bronze/` |
| Silver | `s3://.../telemetry/silver/` |
| Gold | `s3://.../telemetry/gold/` |
| Quarantine | `s3://.../telemetry/quarantine/` |
| Checkpoints | `s3://.../telemetry/checkpoints/` |

### Gold Tables (Current Data Volume)

| Table | Rows |
|-------|------|
| telemetry_enriched | 9,000,000 |
| vehicle_hourly_metrics | 2,994,250 |
| alerts | 450,000 |
| trip_metrics | 256,080 |
| battery_health | 256,080 |

---

## Step 1: Task-Level Email Alerts + Retry

### What We Did

Upgraded both Databricks jobs with:

1. **Task-level email notifications** — each of the 5 tasks now has `email_notifications: {on_failure: [masoomgupta0715@gmail.com]}`. Previously only the job-level had email alerts.

2. **Automatic retry on failure** — added `max_retries: 1`, `min_retry_interval_millis: 300000` (5 minutes), `retry_on_failure: true`. If any task fails, the job automatically retries once after a 5-minute cooldown.

### Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| Job-level email | ✅ Already configured | ✅ Unchanged |
| Task-level email | ❌ All tasks had `{}` | ✅ All 5 tasks have `on_failure` email |
| Retry on failure | ❌ No retry | ✅ `max_retries: 1`, 5-min interval |

### Advantage

- **Faster detection**: Task-level emails fire immediately when a specific task fails, not just when the entire job fails
- **Auto-recovery**: Transient failures (network, OOM) auto-retry without manual intervention
- **Two-level alerting**: Task-level for fast detection, job-level for escalation after retry exhaustion

### Interview Answer

> "I added task-level email notifications to all 5 tasks in both Tesla telemetry jobs. Previously, only the job-level had email alerts — so if task 3 (silver) failed, you'd only get an email after the entire job failed. Now each task has its own `on_failure` email, giving immediate notification. I also added `max_retries: 1` with a 5-minute cooldown, so transient failures like network blips or temporary OOM auto-retry without manual intervention. This gives us two levels of alerting: task-level for fast detection and job-level for escalation after retry exhaustion."

---

## Step 2: SQL Anomaly Alerts

### What We Did

Created 3 Databricks SQL Alert V2 queries that monitor Gold tables for business anomalies. These are **business-level alerts**, not infrastructure alerts — they fire when the data shows something unusual, even if the pipeline is healthy.

### Alert 1: Battery Temperature Critical

**Query**:
```sql
SELECT COUNT(DISTINCT vin) AS critical_vehicles_count
FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/battery_health`
WHERE max_battery_temp_c > 55
AND event_date = (SELECT MAX(event_date) FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/battery_health`)
```

**Configuration**:
- Column: `critical_vehicles_count`
- Operator: `GREATER_THAN`
- Threshold: `0`
- Schedule: Every 15 minutes (`0 0/15 * * * ?` UTC)

**Why**: Battery temperature above 55°C indicates potential thermal runaway risk. If any vehicle hits this threshold, the alert fires immediately.

### Alert 2: Battery SOC Critical

**Query**:
```sql
SELECT COUNT(DISTINCT vin) AS critical_soc_count
FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/battery_health`
WHERE min_battery_soc < 10
AND event_date = (SELECT MAX(event_date) FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/battery_health`)
```

**Configuration**:
- Column: `critical_soc_count`
- Operator: `GREATER_THAN`
- Threshold: `0`
- Schedule: Every 15 minutes (`0 0/15 * * * ?` UTC)

**Why**: Battery SOC below 10% means a vehicle is at risk of stranding. This could indicate a charging infrastructure issue or a vehicle firmware bug draining battery.

### Alert 3: Charging Error Spike

**Query**:
```sql
SELECT COUNT(*) AS charging_error_count
FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/alerts`
WHERE alert_code = 'CHARGING_ERROR'
AND event_date = (SELECT MAX(event_date) FROM delta.`s3://tesla-vehicle-telemetry-511976431596/telemetry/gold/alerts`)
```

**Configuration**:
- Column: `charging_error_count`
- Operator: `GREATER_THAN`
- Threshold: `100`
- Schedule: Every 15 minutes (`0 0/15 * * * ?` UTC)

**Why**: More than 100 CHARGING_ERROR alerts in a single day could indicate a systemic charging infrastructure issue — Supercharger outage, firmware bug, or payment system failure.

### Alert IDs

| Alert | Asset ID |
|-------|----------|
| Battery Temperature Critical | 166292400391621 |
| Battery SOC Critical | 166292400391623 |
| Charging Error Spike | 166292400391622 |

### Interview Answer

> "I created 3 SQL Alert V2 queries that monitor the Gold tables for business anomalies. The first checks for vehicles with battery temperature above 55°C — indicating thermal runaway risk. The second checks for vehicles with SOC below 10% — indicating stranding risk. The third monitors for charging error spikes — more than 100 CHARGING_ERROR alerts in a day suggests systemic infrastructure issues. All three run every 15 minutes and complement the infrastructure alerts (task failure emails) by monitoring the actual business metrics."

---

## Step 3: Declarative Automation Bundles (DABs)

### What We Did

Created `databricks.yml` — a Declarative Automation Bundle that defines the entire pipeline as code:

```yaml
bundle:
  name: tesla-telemetry

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production

resources:
  jobs:
    tesla_telemetry_pipeline:
      name: Tesla Telemetry ETL Pipeline
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: UTC
        pause_status: PAUSED
      max_retries: 1
      min_retry_interval_millis: 300000
      retry_on_failure: true
      email_notifications:
        on_failure:
          - masoomgupta0715@gmail.com
      tasks:
        - task_key: generate_telemetry
          notebook_task:
            notebook_path: ${var.notebook_root}/01_generate_telemetry
            source: WORKSPACE
        - task_key: bronze
          depends_on: [{task_key: generate_telemetry}]
          notebook_task:
            notebook_path: ${var.notebook_root}/02_bronze_batch
            source: WORKSPACE
        # ... silver, gold, load_snowflake
```

### Advantage

- **Infrastructure as Code**: The entire job configuration is versioned and reproducible
- **Dev/Prod separation**: Test changes in `dev` target, deploy to `prod` target
- **Parameterized**: `${var.notebook_root}` allows different paths per environment
- **Reproducible**: Same config deployed every time — no manual UI clicks

### Interview Answer

> "I created a Declarative Automation Bundle (DAB) that defines the entire Tesla telemetry pipeline as code. The `databricks.yml` file specifies the job name, schedule, retry configuration, email notifications, and all 5 sequential tasks with their notebook paths. It has `dev` and `prod` targets — developers validate against `dev` and deploy to `prod`. The notebook path is parameterized using a variable, so the same bundle works across environments. This means the entire pipeline configuration is version-controlled, reproducible, and deployable via CLI."

---

## Step 4: CI/CD Pipeline

### What We Did

Created `.github/workflows/deploy.yml` — a GitHub Actions workflow for automated deployment:

```yaml
name: Deploy Tesla Telemetry Pipeline
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: Validate bundle (PR)
        if: github.event_name == 'pull_request'
        run: databricks bundle validate --target dev
      - name: Deploy bundle (main push)
        if: github.event_name == 'push' && github.ref == 'refs/heads/main'
        run: databricks bundle deploy --target prod
```

### How CI/CD Works

```
Developer makes change → Push to feature branch → Create PR
                                                ↓
                                    GitHub Actions validates bundle (dev target)
                                                ↓
                                    ✅ Valid → Merge to main
                                                ↓
                                    GitHub Actions deploys to prod
                                                ↓
                                    Databricks job updated automatically
```

### Prerequisites

Add these secrets to the GitHub repository:
- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: A Databricks PAT

### Interview Answer

> "I set up CI/CD using Databricks DABs with GitHub Actions. The `databricks.yml` file defines the pipeline as code with dev and prod targets. When a PR is created, GitHub Actions runs `databricks bundle validate --target dev` to catch configuration errors. When the PR is merged to main, it runs `databricks bundle deploy --target prod` which updates the Databricks job automatically. This means a developer makes a change, creates a PR, GitHub Actions validates it, and on merge, the updated job deploys automatically — no manual steps needed."

---

## Step 5: Git Version Control

### What We Did

1. **Created GitHub repo**: `https://github.com/guptamasoom/tesla-telemetry` (empty repo)
2. **Cloned to Databricks**: Created a Databricks Git folder linked to the GitHub repo
3. **Copied all notebooks and files**: Used Databricks SDK to copy from workspace to Git folder
4. **Committed and pushed**: All files version-controlled on GitHub

### Files in Git Repo

```
tesla-telemetry/
├── .github/
│   └── workflows/
│       └── deploy.yml                          # CI/CD pipeline
├── 01_generate_telemetry.py                    # 500K synthetic Tesla events
├── 01_generate_telemetry_1m.py                  # 1M variant for scale testing
├── 02_bronze_batch.py                           # Auto Loader → DQ → Bronze Delta
├── 03_silver.py                                 # Bronze → normalize → dedup → Silver
├── 04_gold.py                                   # Silver → 5 gold analytics tables
├── 04_gold_incremental.py                      # Incremental gold (affected-key recompute)
├── 05_load_snowflake.py                         # Gold → Snowflake
├── 05_load_snowflake_incremental.py             # Incremental Snowflake load
├── 06_pipeline_monitoring.py                    # System table monitoring (durations, DQ)
├── databricks.yml                                # DABs bundle config
├── Tesla_Production_Hardening_Guide.md           # This document
├── PROJECT_EXPLAINED.md                          # Original project documentation
├── Interview_Prep_Tesla_Telemetry.md             # Interview Q&A guide
└── Scaling_Study_Tesla_Telemetry.md              # Scaling study notes
```

### Advantage

- **Version history**: Every change tracked — who, what, when
- **Rollback**: Bad change? Revert to previous commit
- **Code review**: Team members can review PRs before merging
- **Branching**: Experiment on a branch without breaking production
- **Audit trail**: Required for automotive compliance

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                    Databricks Serverless                         │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐        │
│  │ 01_generate_  │    │ 02_bronze_    │    │ 03_silver    │        │
│  │ telemetry     │───▶│ batch         │───▶│              │        │
│  │ 500K events   │    │ Auto Loader   │    │ dedup+wmark  │        │
│  │ S3 raw JSON   │    │ 7 DQ checks   │    │ append       │        │
│  └──────────────┘    │ quarantine    │    └──────┬───────┘        │
│                      └──────────────┘           │                  │
│                                                  ▼                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐        │
│  │ 05_load_      │    │ 04_gold      │    │ Silver Delta │        │
│  │ snowflake     │◀───│ 5 tables      │◀───│ Tables       │        │
│  │ MERGE append  │    │ MERGE + append│    └──────────────┘        │
│  └──────────────┘    └──────────────┘                            │
│         │                    │                                     │
│         ▼                    ▼                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐        │
│  │ Snowflake    │    │ Gold Delta   │    │ 06_monitor   │        │
│  │ Tables       │    │ Tables (5)   │    │ system tables│        │
│  └──────────────┘    └──────────────┘    └──────────────┘        │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Monitoring & Alerting                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Email    │  │ SQL Alert│  │ SQL Alert│  │ SQL Alert│         │
│  │ (fail)   │  │ (batt T) │  │ (batt SOC)│  │ (chg err)│         │
│  │ task+job │  │ >55°C    │  │ <10%     │  │ >100/run │         │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │
│  ┌──────────┐                                                    │
│  │ Retry    │  max_retries:1, 5-min interval                     │
│  └──────────┘                                                    │
├──────────────────────────────────────────────────────────────────┤
│  Git & CI/CD                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────┐       │
│  │ GitHub   │  │ DABs     │  │ GitHub Actions            │       │
│  │ Repo     │  │ Config   │  │ (validate on PR,          │       │
│  │ (15 files)│  │ (YAML)   │  │  deploy on main push)     │       │
│  └──────────┘  └──────────┘  └──────────────────────────┘       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Interview Q&A

### Q1: Why did you add task-level email alerts when the job already had job-level alerts?

**A**: Job-level emails only fire after the entire job fails — which in continuous mode means after all retries are exhausted. If task 3 (silver) fails, you'd wait for the entire job to fail before getting an email. With task-level alerts, each of the 5 tasks has its own `on_failure` email, so you get notified immediately when a specific task fails, not just when the whole pipeline goes down. This gives faster mean-time-to-detection (MTTD).

### Q2: Why add retry on failure? What's the trade-off?

**A**: Transient failures — network blips, temporary OOM, brief S3 throttling — are common in cloud environments. Without retry, a single transient failure kills the entire pipeline run and requires manual intervention. With `max_retries: 1` and a 5-minute cooldown, the job auto-retries once. The trade-off is that if the failure is systematic (e.g., bad code), you waste one extra run. But the benefit of auto-recovery for transient issues far outweighs the cost of one wasted run.

### Q3: Why did you create SQL anomaly alerts? What's the difference between infrastructure and business alerts?

**A**: Infrastructure alerts (email on task failure) tell you when the pipeline breaks. Business alerts (SQL alerts) tell you when the data shows something unusual, even if the pipeline is healthy. For example, a sudden spike in battery temperature across vehicles could indicate a firmware bug that's draining battery or a Supercharger issue. The pipeline would process everything correctly, but the data itself is showing a problem. I created 3 SQL alerts: battery temp >55°C (thermal runaway risk), battery SOC <10% (stranding risk), and charging errors >100/run (infrastructure issue). Both types of alerts are needed for complete monitoring.

### Q4: What is a DAB and why is it better than manual job configuration?

**A**: A Declarative Automation Bundle (DAB) defines Databricks resources — jobs, pipelines, dashboards — as code in a YAML file. It's better than manual UI configuration because: (1) it's version-controlled, (2) it's reproducible — same config every time, (3) it supports dev/prod targets for testing before production, (4) it enables CI/CD — GitHub Actions can validate and deploy automatically, (5) it supports variables for parameterization across environments. For the Tesla pipeline, the `databricks.yml` file captures the entire job configuration including schedule, retry, email alerts, and all 5 sequential tasks.

### Q5: How does the CI/CD pipeline work?

**A**: The GitHub Actions workflow in `.github/workflows/deploy.yml` has two triggers: PR and main push. On a PR, it runs `databricks bundle validate --target dev` to catch configuration errors. On a push to main (i.e., after PR merge), it runs `databricks bundle deploy --target prod` which updates the Databricks job automatically. GitHub secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) authenticate the CLI. This means a developer makes a change, creates a PR, GitHub Actions validates it, and on merge, the job deploys — no manual steps needed.

### Q6: What's the pipeline's data volume and how does it scale?

**A**: The generator produces 500K events per run (10K+ vehicles, 5 German cities). Current Gold tables hold: telemetry_enriched (9M rows), vehicle_hourly_metrics (2.9M), alerts (450K), trip_metrics (256K), battery_health (256K). The pipeline uses incremental processing with checkpoint-based run discovery — each layer only processes new runs, not the full history. Bronze and Silver use `event_id` idempotency (left-anti join) to prevent duplicates on retry. Gold uses MERGE for aggregation tables and idempotent append for event-level tables. The pipeline has been tested at 1M and 2M events per run for scale validation.

### Q7: Why is the pipeline batch mode, not streaming?

**A**: Tesla vehicle telemetry data arrives in batches — each generation run produces a complete snapshot of events. The pipeline uses incremental batch processing with checkpoints: each layer discovers new runs via the checkpoint table, processes them, and updates the checkpoint. This is different from the FraudStream V3 pipeline which uses Structured Streaming for real-time fraud detection. For telemetry data, batch mode is more appropriate because: (1) the data arrives in discrete batches, (2) full table rebuilds for Gold aggregations give correct results, (3) checkpoint-based idempotency is simpler than streaming watermarks for this use case.

### Q8: How would you extend this pipeline for real-time Tesla telemetry?

**A**: I would: (1) Replace the batch generator with a Kafka topic receiving real-time vehicle telemetry, (2) Use Structured Streaming with Auto Loader or Kafka source for Bronze, (3) Add a 10-minute watermark for late data handling in Silver, (4) Use `foreachBatch` in Gold for MERGE operations (not possible in pure streaming), (5) Add real-time alerting with Structured Streaming + Delta Live Tables, (6) Implement schema evolution for new vehicle firmware versions, (7) Add data retention policies for long-term storage.

---

*Document generated: October 11, 2026*
*Pipeline: Tesla Vehicle Telemetry — Batch Medallion ETL*
*GitHub: https://github.com/guptamasoom/tesla-telemetry*