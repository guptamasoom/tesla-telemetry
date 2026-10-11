# The Tesla Telemetry Project Explained, From Start to End

This document explains the whole project in order. It starts with the idea, follows one batch of car data from S3 to Snowflake, and then explains every notebook, the testing, the scaling study and the optimizations.
You can read it top to bottom while sharing your screen in an interview or a demo.

---

## 1. The idea in five sentences

1. Electric cars send small messages all day. We call each message an **event**.
2. The events are saved as JSON files in an **S3 bucket** on AWS. (In this project a generator notebook makes them, like real cars would.)
3. A **Databricks job** picks up new files, checks the data, cleans it, and turns it into 5 report tables, layer by layer (Bronze → Silver → Gold).
4. The Gold tables are loaded into **Snowflake**, a cloud data warehouse where analysts run SQL.
5. The pipeline remembers which batches it already loaded, so it never loads the same event twice — even if it crashes halfway and is retried.

This is called an **ETL** pipeline: **E**xtract, **T**ransform, **L**oad. It runs on Databricks serverless compute as one job with 5 tasks.

---

## 2. The big picture

```text
┌────────────────────────────────────────────────────────────┐
│ Amazon S3 (raw zone)                                       │
│ s3://tesla-vehicle-telemetry-511976431596/telemetry/raw/  │
│ one folder per run, e.g. 20261008_204747/ (JSON files)    │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│ Databricks Job (serverless) — the medallion pipeline      │
├────────────────────────────────────────────────────────────┤
│ 1. generate_telemetry   make a new batch (simulates cars)  │
│ 2. bronze               check + quarantine  -> Bronze      │
│ 3. silver               clean + enrich     -> Silver      │
│ 4. gold                 build 5 tables     -> Gold         │
│ 5. load_snowflake        export, copy, merge               │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│ Snowflake (database TESLA_TELEMETRY)                      │
├────────────────────────────────────────────────────────────┤
│ RAW      the file stage (parquet uploads land here)        │
│ STAGING  5 waiting tables (only the rows of this load)     │
│ CURATED  5 final tables for reports                        │
└────────────────────────────────────────────────────────────┘
```

The same flow in one line:

```text
S3 JSON -> Bronze Delta -> Silver Delta -> Gold Delta (5 tables) -> Parquet -> Snowflake stage -> STAGING tables -> MERGE -> CURATED tables
```

The three middle layers are called the **medallion architecture**: Bronze keeps raw data as it arrived, Silver keeps clean standardised data, Gold keeps ready-to-report tables.

---

## 3. Words you need to know

| Word | Simple meaning |
| --- | --- |
| ETL | Extract (get the data), Transform (clean and reshape it), Load (put it in the warehouse). |
| Event | One message from a car: time, speed, battery, position and so on. |
| Batch / run | All the events the pipeline processes together in one go. Each batch gets a name like `20261008_204747` (date + time), called the `source_run_id`. |
| VIN | Vehicle Identification Number. It says which car the event came from. |
| Amazon S3 | AWS file storage. Files live in a **bucket**. |
| Databricks | A data platform that runs Spark notebooks and jobs. We write PySpark in notebooks. |
| Serverless compute | Databricks starts and sizes the compute for us. We do not manage servers. |
| Spark / PySpark | The engine that processes big tables in parallel. PySpark is its Python API. |
| Notebook | A page of code cells that run one after another. Our pipeline is 6 notebooks. |
| Job / task | A job runs notebooks in order. Each step is a task. Our job has 5 tasks. |
| Delta Lake | The table format we store layers in. It keeps history, allows `MERGE`, and is the default on Databricks. |
| Medallion architecture | The Bronze → Silver → Gold layering pattern. Each layer is cleaner than the one before. |
| Checkpoint | A small Delta table where each layer writes the last `source_run_id` it processed. Next run starts after it. |
| Idempotency | Doing the same work twice gives the same result. Our layers deduplicate by `event_id`, so retries never create duplicate rows. |
| Data quality (DQ) rule | A check on the data, for example "speed must be inside a valid range". Bronze applies 7 rules. |
| Quarantine | A Delta table where invalid rows are kept for inspection instead of being thrown away. |
| MERGE | One SQL statement that updates rows that already exist and inserts new ones. Also called an upsert. |
| Parquet | A fast file format for tables. We export Gold tables to Parquet before uploading to Snowflake. |
| UC Volume | A managed folder in Unity Catalog. Serverless cannot write to `/tmp`, so Parquet exports go to a volume. |
| Snowflake stage | A place inside Snowflake where files are uploaded before loading. Ours is `RAW.TELEMETRY_STAGE`. |
| Staging table | A waiting table. It holds only the rows of the current load. |
| Curated table | The final, clean table that reports use. |
| Warehouse (Snowflake) | The compute engine that runs SQL there. Ours is `TELEMETRY_WH`. It costs money only while it runs. |
| System tables | Databricks tables about Databricks itself. Our monitoring reads job run history from them. |

---

## 4. The data

### 4.1 One event

Every line of the input file is one event like this:

```json
{"event_id":"evt_00000003","vin":"WVW00000000000123","event_ts":"2026-06-06T08:25:00Z","ingest_ts":"2026-06-06T08:25:05Z","event_type":"alert","city":"Berlin","latitude":52.52,"longitude":13.40,"speed_kmh":67.1,"battery_soc":80.9,"battery_temp_c":31.1,"odometer_km":20699.5,"charging_state":"Disconnected","gear":"D","autopilot_engaged":false,"alert_code":"TIRE_PRESSURE_LOW","software_version":"2026.14.3"}
```

| Field | Meaning | Rule we check |
| --- | --- | --- |
| `event_id` | unique id of the event | must exist, must be unique (no duplicates ever) |
| `vin` | which car sent it | must exist and look like a VIN |
| `event_ts` | when it happened in the car | must exist, must be a valid timestamp |
| `ingest_ts` | when our system received it | must exist, must not be before `event_ts` |
| `event_type` | `telemetry`, `alert` or `charging` | only these 3 words |
| `city` | one of 5 German cities | text, standardised later in Silver |
| `latitude`, `longitude` | GPS position | must be valid numbers in range |
| `speed_kmh` | speed in km/h | must be inside a valid range |
| `battery_soc` | battery charge in percent | 0 to 100 |
| `battery_temp_c` | battery temperature | inside a valid range |
| `odometer_km` | total km the car has driven | must not be negative |
| `charging_state` | for example `Charging` or `Disconnected` | text |
| `gear` | `P` park, `D` drive and so on | text |
| `autopilot_engaged` | was Autopilot on | true or false |
| `alert_code` | which alert, only for alerts | required when `event_type` is `alert` |
| `software_version` | car software version | text, trimmed later in Silver |

Seven of these checks are formal **DQ rules** in the Bronze notebook. Rows that break a rule go to the **quarantine** table with the reason attached, not into Bronze.

### 4.2 The numbers in one batch

A normal batch is about **500,000 events** from about **10,000 cars** in 5 cities (Berlin, Munich, Frankfurt, Hamburg, Stuttgart), with 3 event types: `telemetry`, `alert`, `charging`. In the scaling tests we also ran batches of 1,000,000 and 2,000,000 events.

### 4.3 Where the files live in S3

```text
s3://tesla-vehicle-telemetry-511976431596/telemetry/
├── raw/                      incoming JSON, one folder per run
├── bronze/                   Bronze Delta (partitioned by source_run_id)
├── silver/telemetry/         Silver Delta (partitioned by event_date)
├── gold/                     5 Gold Delta tables
│   ├── telemetry_enriched/
│   ├── vehicle_hourly_metrics/
│   ├── trip_metrics/
│   ├── battery_health/
│   └── alerts/
├── quarantine/               invalid rows + reasons
├── checkpoints/              one small Delta table per layer
│   ├── bronze/  silver/  gold/  snowflake_load/
└── vehicle_state/            generator state
```

Each layer reads only what is new since its checkpoint, so the folders keep growing but the work per run stays small.

---

## 5. The notebooks and files

```text
/Users/masoomgupta0715@gmail.com/tesla_telemetry/
├── 01_generate_telemetry         make ~500K events -> S3 raw (the original)
├── 01_generate_telemetry_1m      same, but 1M events (for the scaling tests)
├── 02_bronze_batch               Bronze layer: schema, 7 DQ rules, quarantine, idempotency
├── 03_silver                     Silver layer: standardise + derived columns
├── 04_gold                       Gold layer: 5 tables (full rebuild of aggregates)
├── 04_gold_incremental           Gold, but only affected keys are recomputed (the optimization)
├── 05_load_snowflake             load Gold -> Snowflake (aggregates fully reloaded)
├── 05_load_snowflake_incremental load only new events + changed aggregate keys (the optimization)
├── 06_pipeline_monitoring        run times, trends, failures, volumes, DQ health (system tables)
├── Scaling_Study_Tesla_Telemetry.md    the 500K / 1M / 2M results
├── Interview_Prep_Tesla_Telemetry.md   the full interview story
└── PROJECT_EXPLAINED.md          this document
```

Two Databricks jobs run these notebooks:

| Job | Tasks | What it is for |
| --- | --- | --- |
| Tesla Telemetry ETL Pipeline | 5 tasks: generate → bronze → silver → gold → load_snowflake | the original pipeline, left untouched |
| Tesla Telemetry ETL Pipeline - 1M Test | same, but the generator makes 1M events, and the gold + snowflake tasks use the incremental notebooks | testing and scaling experiments |

---

## 6. One batch from S3 to Snowflake, step by step

### Step 0: before the pipeline

These things exist already: the S3 bucket, the Databricks workspace, and in Snowflake the database `TESLA_TELEMETRY` with the schemas `RAW`, `STAGING` and `CURATED`, the stage `RAW.TELEMETRY_STAGE`, the 5 staging + 5 curated tables, the warehouse `TELEMETRY_WH`, and a stored procedure `MERGE_STAGING_INTO_CURATED()` that merges each staging table into its curated table.

### Step 1: `generate_telemetry` (make a batch)

This notebook simulates the cars. It writes ~500K JSON events to `raw/<new run_id>/`. Each event gets a unique `event_id`. In real production this step would not exist — the cars themselves would write to S3.

### Step 2: `bronze` (quality gate)

Bronze finds new run folders (the ones after its checkpoint), reads the JSON with a **fixed 17-field schema** (so a bad file can never silently break the pipeline), and checks the 7 DQ rules. The batch splits into valid and invalid rows:

* invalid rows → **quarantine** Delta table, with the rule they broke
* valid rows → **Bronze** Delta table, partitioned by `source_run_id`

Before writing, bronze also checks every `event_id` against the rows already in Bronze, so a repeated run writes nothing new. Only after a successful write does bronze update its checkpoint.

### Step 3: `silver` (clean and enrich)

Silver reads only the new Bronze runs. It fixes text columns (city names like `berlin` → `Berlin`, event type to lowercase, gear to uppercase, trims the software version) and adds columns that reports like: `event_date`, `event_hour`, `is_moving`, `is_charging`, `battery_band`, `speed_category`. It keeps only rows that are not yet in Silver (`left_anti` join on `event_id`), writes the Silver Delta partitioned by `event_date`, and updates its checkpoint.

### Step 4: `gold` (build the 5 report tables)

Gold reads the new Silver runs and builds 5 tables:

* two **event tables** (`telemetry_enriched`, `alerts`): appended, with dedup — one row per event, forever
* three **aggregation tables** (`vehicle_hourly_metrics` per car per hour, `trip_metrics` per car per day, `battery_health` per car per day): computed and **MERGEd** into the Gold tables by key, so re-running updates the same rows instead of duplicating them

The incremental version (used by the test job) recomputes only the keys the new batch touched, not everything.

### Step 5: `load_snowflake` (load)

The loader reads its own checkpoint (the last Gold run it loaded) and exports to Parquet in a Unity Catalog volume: the **new events** and, in the incremental version, the **changed aggregation keys** only. Then for each table:

1. `TRUNCATE` the staging table
2. `PUT` the Parquet file to the Snowflake stage
3. `COPY INTO` the staging table (match columns by name)
4. `CALL` the stored procedure — it MERGEs staging into curated

Only after a successful MERGE does it update its checkpoint. If anything fails before that, the next run simply loads the same batch again — the MERGE makes that harmless.

### Step 6: look at the result

The verify cell prints the row counts of the staging tables (what this load brought) and of the curated tables (the final state). The monitoring notebook (`06_pipeline_monitoring`) shows run times and trends over all past runs, straight from Databricks system tables.

---

## 7. The 5 output tables

| Table | One row is | Good question it answers | Rows today |
| --- | --- | --- | --- |
| `telemetry_enriched` | one event, cleaned and enriched | "What did car X report at 08:25?" | 9,000,000 |
| `vehicle_hourly_metrics` | one car, one hour | "How far and how fast did car X drive between 8 and 9?" | 2,994,250 |
| `trip_metrics` | one car, one day | "What was today's trip summary for car X?" | 256,080 |
| `battery_health` | one car, one day | "Is car X's battery getting worse over time?" | 256,080 |
| `alerts` | one alert event | "Which alerts happened today?" | 450,000 |

All five live twice: as Delta tables in Gold (on S3) and as tables in Snowflake CURATED.

---

## 8. Why we use staging tables and MERGE

We could load straight into the final tables, but we don't. Three reasons:

1. **Retry safety.** If a load fails halfway, the curated tables were never touched. The staging tables just get truncated and reloaded next time.
2. **Idempotency.** MERGE updates existing rows and inserts new ones by key. Loading the same rows again changes nothing. A retry or a crash can never create duplicates.
3. **Audit.** Staging always shows exactly what the last load brought, so you can compare "what came in" with "what we ended up with".

In the incremental version this gets better: staging holds only the delta (new events + changed aggregate keys), so we move a fraction of the data across systems but still end with the same curated tables as a full load.

---

## 9. How the pipeline avoids loading a file twice

Two mechanisms work together, in every layer:

1. **The checkpoint** says what was already processed. A layer reads only runs after its checkpoint.
2. **Idempotency** (`event_id` dedup and MERGE) protects the data itself. Even if a layer crashes after writing but before updating its checkpoint, the retry finds its own rows already there and writes nothing.

We proved this with **crash tests** on every layer: run all cells except the checkpoint update, then run everything again:

| Layer | Simulated crash | Retry result |
| --- | --- | --- |
| Bronze | checkpoint not updated | all 500K rows already known → 0 new writes |
| Silver | checkpoint rolled back one run | all 500K rows caught by the anti-join → 0 new writes |
| Gold | checkpoint rolled back one run | event tables 0 new; aggregates MERGEd to the same values; counts identical |
| Snowflake | checkpoint rolled back one run | same batch reloaded and MERGEd; curated counts identical |

No duplicates anywhere. The pipeline is incremental, append-only, idempotent and retry-safe.

---
## 10. What the monitoring notebook does

`06_pipeline_monitoring` reads **system tables** (Databricks' own job history: `system.lakeflow.job_run_timeline` and `job_task_run_timeline`) and shows:

* how long each task of each run took
* which layer is the bottleneck, and whether it is getting worse over time
* failed runs in the last 7 days
* data volumes: rows per Bronze run, quarantine rate, totals per layer

This is how we found the problem worth fixing: Bronze and Silver stayed flat (~40–60s) while Gold grew 137→174s and Snowflake 157→185s as the data accumulated.

---

## 11. How the pipeline scales (the 2M test)

We ran the same job with 3 batch sizes:

| Batch size | Total time | Bronze | Silver | Gold | Snowflake |
| --- | --- | --- | --- | --- | --- |
| 500K | 9.4 min | 61s | 48s | 173s | 168s |
| 1M | 8.3 min | — | — | — | — |
| 2M | 9.2 min | 42s | 46s | 154s | 185s |

**4x the data, almost the same time.** Bronze and Silver are dominated by fixed overhead (job startup, small file handling), so Spark's parallelism absorbs the batch size. The parts that grow are Gold and Snowflake — and they grow with the **cumulative** data (how much is already in the lakehouse), not with the batch size, because the original Gold rebuilt all aggregates from all Silver every run, and the Snowflake load reloaded the whole aggregation tables every run.

---

## 12. The two optimizations

Both optimizations replace "recompute/reload everything" with "only what changed". The unit of change is the **key**: a car+hour or a car+day.

1. **Incremental Gold (`04_gold_incremental`).** From the new batch, collect the keys it touches (car+hour, car+day). Recompute only those keys from the full Silver history (partition pruning helps), and MERGE just those rows into the Gold tables. The values are identical to a full rebuild, because MERGE writes the same recomputed rows either way. Verified: a crash-simulated 2M reprocess produced exactly the same counts as before.
2. **Incremental Snowflake (`05_load_snowflake_incremental`).** Event tables were already loaded incrementally. Now the aggregation tables also ship only their changed keys: export the affected rows to Parquet, load them into staging, MERGE. Same curated result, a fraction of the transferred data.

Both are wired into the 1M test job. The originals stay untouched, so we can always compare.

One honest note: our generator reuses the same time windows every run, so almost every key is "affected" on every batch — the worst case for this pattern. With real data (new hours every day), the affected-key sets would be tiny and the savings much larger.

---

## 13. Problems we met while building this, and the fixes

| Problem | What was wrong | Fix |
| --- | --- | --- |
| Silver found no new Bronze runs | Code assumed a `date/run` folder layout, but Bronze is partitioned by `source_run_id` | Discover runs by the `source_run_id=` prefix |
| Checkpoint write failed | A timestamp **column** was passed as a plain value into `createDataFrame` | Pass the value, add the timestamp with `.withColumn` |
| A fully-duplicate retry never advanced the checkpoint | The guard skipped the checkpoint when 0 new rows were written — so the same run would be reprocessed forever | Update the checkpoint whenever there was input, even if 0 new rows |
| Parquet export to `/tmp` failed | Serverless compute cannot write there | Export to a Unity Catalog volume |
| Snowflake columns loaded as NULL | Gold uses km (`speed_kmh`), Snowflake expected mph (`speed_mph`) | Align the column names, load with `MATCH_BY_COLUMN_NAME` |
| Gold overwrote tables every run | `mode("overwrite")` destroys history and is unsafe on retry | Idempotent append for event tables, MERGE for aggregates |

---

## 14. How to show it in a demo (about 30 minutes)

* **Minutes 0–5 — the idea.** Section 1 and 2 of this doc. Show the S3 bucket layout.
* **Minutes 5–10 — Bronze.** Open `02_bronze_batch`. Show one event, the 7 DQ rules, and the quarantine table.
* **Minutes 10–15 — Silver and Gold.** Open `03_silver` and `04_gold`. Show the derived columns and the 5 Gold tables with their row counts.
* **Minutes 15–20 — Snowflake.** Open `05_load_snowflake_incremental`. Show the flow: Parquet → stage → COPY → MERGE, then the curated counts in the verify cell.
* **Minutes 20–25 — the crash test.** Explain checkpoints + idempotency (section 9). This is the part people remember.
* **Minutes 25–30 — scaling and monitoring.** The table from section 11, then `06_pipeline_monitoring`: "Bronze and Silver are flat; Gold and Snowflake grow — so that is exactly what we optimized."

---

## 15. Questions people often ask

**Q: What happens if the pipeline crashes halfway?**
A: Nothing bad. The checkpoint was not updated, so the next run reprocesses the same batch. The `event_id` dedup and the MERGE make sure no duplicates appear. We tested this on every layer.

**Q: Can a duplicate event ever reach the final tables?**
A: Only if two different events share the same `event_id` with different content — the DQ rules catch that. Everything else is stopped by dedup at three levels (Bronze, Silver, Gold).

**Q: Why use Databricks AND Snowflake?**
A: Databricks is the workhorse: Spark processes millions of events in parallel and Delta gives us MERGE, history and cheap storage. Snowflake is the shop window: analysts query the curated tables with plain SQL. Each tool does what it is best at.

**Q: Why Bronze → Silver → Gold, not one big step?**
A: Each layer answers a different question. Bronze: "what exactly arrived?" (raw, quarantined bad rows). Silver: "is it clean and consistent?" Gold: "what do reports need?" If something breaks, you know exactly which layer to look at, and you never reprocess more than the new part.

**Q: What was the hardest part?**
A: Making incremental AND correct work together. A full rebuild is simple but slow; incremental is fast but easy to get subtly wrong (which keys changed? what if a crash happens between write and checkpoint?). That is why every optimization was followed by a test proving the counts stay identical.

**Q: What would you improve next?**
A: Compaction of the Bronze table (it grows many small files), moving the Snowflake password into Databricks secrets, and streaming ingestion with Auto Loader instead of batch.

---

## 16. Limits of this project

* The data is **synthetic** — generated, not real cars. The generator reuses the same time windows, which is the worst case for the affected-key optimization.
* It is **batch**, not streaming. A real system would run continuously or every few minutes.
* The Snowflake credentials sit in the notebook config, not in Databricks secrets. Fine for a demo, not for production.
* The Bronze table grows one Delta folder per run without compaction, so file counts rise over time.
* There is no automated test suite or CI/CD; correctness is proven by manual crash tests and count checks.
