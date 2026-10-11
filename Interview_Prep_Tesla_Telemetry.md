# Tesla Telemetry Pipeline — Interview Prep (Complete Story)

> Companion doc: `Scaling_Study_Tesla_Telemetry.md` (numbers + analysis). Ye doc poora narrative hai — problem → build → debug → test → scale → optimize.

---

## 1. Problem Statement (interview me aise shuru karo)

> "Tesla vehicles se telemetry data aa raha hai (speed, battery, charging, alerts). Mainne Databricks par ek **production-grade medallion pipeline** banaya — Bronze → Silver → Gold → Snowflake — jo incremental chalta hai, crash ho jaye to safely retry ho sakta hai, aur bina duplicates ke data load karta hai. Fir maine isko 500K → 2M tak scale-test kiya, system tables pe monitoring banaya, aur data-driven optimization se dono bottlenecks fix kiye."

**Data profile:** ~10K+ vehicles (VINs), 5 German cities (Berlin, Munich, Frankfurt, Hamburg, Stuttgart), 3 event types (telemetry / alert / charging), 17-field schema, 500K–2M events per batch. Raw JSON S3 pe land hota hai, har run ka apna `source_run_id`.

---

## 2. Architecture (what I built)

```
01_generate_telemetry  (synthetic data → S3, partitioned by run_id)
        ↓
02_bronze_batch   →   03_silver   →   04_gold   →   05_load_snowflake
  (Delta, DQ rules,     (cleaned,       (5 tables,     (Parquet → staging →
   quarantine, checkpoint) derived cols)   MERGE)        MERGE → CURATED)
```

**Stack:** Databricks Serverless (Spark Connect, Spark 4.2.0), Delta Lake on S3, PySpark; Snowflake side: staging + curated schemas, internal stage, MERGE stored procedure. Jobs: 5-task sequential pipeline (generate → bronze → silver → gold → load_snowflake).

### Har layer me 3 core patterns

| Pattern | Implementation | Kyu
| --- | --- | --- |
| **Checkpoint** | Delta table per layer (`last_processed_run_id`) | Sirf naye runs process hote hain — incremental
| **Idempotency** | `event_id` dedup: Bronze me existing-check, Silver/Gold me `left_anti` join | Retry pe duplicates nahi — safety data-level pe hai, orchestration-level pe nahi |
| **Checkpoint update only after successful write** | Last cell me checkpoint append | Crash → checkpoint stale → same batch reprocess → dedup catch karta hai |

### Layer-wise details

* **Bronze (02_bronze_batch, 11 cells):** explicit 17-field schema, RAW run discovery, 7 DQ rules (nulls, speed range, battery SOC, timestamps, odometer, alert checks) → invalid rows **quarantine** Delta me (rejected, not dropped), valid rows append-only Bronze Delta (partitioned by `source_run_id`), cross-run `event_id` idempotency.
* **Silver (03_silver, 9 cells):** standardization (city/software_version/event_type/charging_state/gear casing), derived columns (`event_date`, `event_hour`, `is_moving`, `is_charging`, `battery_band`, `speed_category`, `battery_temperature_status`), partitioned by `event_date`, `left_anti` idempotency.
* **Gold (04_gold, 8 cells → 04_gold_incremental):** 5 tables — `telemetry_enriched` (event grain, idempotent append), `vehicle_hourly_metrics` (vin+hour MERGE), `trip_metrics` (vin+date MERGE), `battery_health` (vin+date MERGE), `alerts` (event grain). Units fixed mph→kmh / miles→km.
* **Snowflake (05_load_snowflake → 05_load_snowflake_incremental):** Gold Delta → Parquet (UC Volume) → `PUT` + `COPY INTO` STAGING (`MATCH_BY_COLUMN_NAME`) → `CALL CURATED.MERGE_STAGING_INTO_CURATED()` → checkpoint update after successful MERGE.

---

## 3. Debugging — bugs found & fixed (interview ka sabse impressive part)

| # | Bug | Fix | Lesson
| --- | --- | --- | --- |
| 1 | Silver Bronze runs discover nahi kar raha tha — code `ingestion_date=.../run_id=...` structure assume karti thi, Bronze actually `source_run_id=` se partitioned hai | Partition prefix discovery `source_run_id=` pe change | Partition layout assumption verify karo, assume mat karo |
| 2 | Checkpoint Delta me `current_timestamp()` as **data value** pass ho raha tha (`createDataFrame` me Column expression, Python value nahi) | Value-only DataFrame + `.withColumn()` se timestamp | Column expressions vs Python values ka difference |
| 3 | Sab-duplicate retry pe checkpoint update **nahi** hota tha — guard `silver_new_count == 0` par exit kar deta tha → pipeline same run forever reprocess | Guard ko `bronze_df is None` tak narrow kiya | Idempotent retries bhi checkpoint advance karein, warna infinite loop |
| 4 | Serverless pe Parquet `/tmp/` me write nahi hota | UC Volume (`/Volumes/workspace/default/temp_parquet/`) | Serverless filesystem restrictions |
| 5 | Snowflake column mismatch — Gold `speed_kmh/odometer_km`, Snowflake `speed_mph/odometer_miles` expect karta tha → silent NULLs | Column mapping align + `MATCH_BY_COLUMN_NAME` | Cross-system schema contracts explicitly define karo |
| 6 | Gold me `mode("overwrite")` — har run pe poori tables rewrite (unsafe + wasteful) | MERGE (aggregations) + idempotent append (event tables) | Idempotent write patterns over destructive overwrites |

---

## 4. Testing Methodology — Crash Simulation ("fault tolerance" ka answer)

Har layer (Bronze, Silver, Gold, Snowflake) pe same test protocol:

1. **Crash simulate karo:** cells run karo, **checkpoint update wala last cell skip** karo (ya temp cell se checkpoint ek run peeche rollback karo)
2. **Retry karo:** poora notebook dobara chalao
3. **Verify karo:** same batch reprocess hua, **0 duplicates**, saare counts identical, checkpoint ab sahi jagah

**Layer-wise proofs:**

* **Bronze:** 500K records retry pe sab `EVENT_ALREADY_IN_BRONZE` flag me catch — 0 new writes, 500K quarantine me duplicate-flagged
* **Silver:** `left_anti` join ne sab 500K pakde — 0 new writes
* **Gold:** event tables 0 new, aggregation MERGE same values — counts identical
* **Gold incremental:** 2M batch crash-simulated reprocess — **saare counts identical** (9M / 2,994,250 / 256,080 / 256,080 / 450K), checkpoint restored
* **Snowflake incremental:** no-op run validated — checkpoint table me 0 appends (load cell skip proven), 59s SUCCESS

**Interview line:**

> "Maine har layer pe crash-simulation retry tests kiye — checkpoint ko intentionally rollback karke. Har baar zero duplicates, kyunki idempotency **data-level** pe hai (event_id dedup), sirf orchestration-level pe nahi. Checkpoint sirf successful write ke baad update hota hai, isliye crash hamesha safe retry me end hota hai."

---

## 5. Scaling Study (numbers yaad rakho)

| Batch | Total runtime | generate | bronze | silver | gold | snowflake |
| --- | --- | --- | --- | --- | --- | --- |
| 500K | 9.4 min | 98s | 61s | 48s | 173s | 168s |
| 1M | 8.3 min | — | — | — | — | — |
| 2M | 9.2 min | 108s | 42s | 46s | 154s | 185s |

**Key insights:**

* **4x data (500K → 2M), flat runtime** (~9 min) — bronze/silver overhead-dominated, Spark ne horizontal scaling ki
* **Gold + Snowflake = ~60% runtime** — aur ye **batch size ke saath nahi, cumulative data ke saath** grow kar rahe the (gold 137→174s, snowflake 157→185s run-over-run) — gold har run **poora Silver rebuild** karta tha, Snowflake aggregation tables har run **FULL reload** hoti thi
* Yahi dono bottlenecks the → optimization roadmap bana

**Testing discipline:** original notebooks/jobs kabhi touch nahi kiye — alag copies (1M generator notebook, 1M test job) banaye. 2M test ke liye generator ko temp-patch karke generate start hone ke baad revert kiya.

---

## 6. Monitoring (data-driven diagnosis)

`06_pipeline_monitoring` notebook — **system tables** (`system.lakeflow.job_run_timeline`, `job_task_run_timeline`) se:

* Per-task durations per run
* Bottleneck trend by day
* Failed runs (7d)
* Data volumes & DQ health (bronze per-run counts, quarantine rate, layer totals)
* Growth trend — isse hi gold/snowflake ka cumulative growth confirm hua

**Interview line:**

> "Maine optimization ko opinion-driven nahi, **system-table evidence-driven** kiya — pehle monitoring se bottleneck trend confirm ki (gold 137→174s, snowflake 157→185s rising; bronze/silver flat), phir exactly unhi do layers ko fix kiya."

---

## 7. Optimizations (implemented)

### 7.1 Incremental Gold (`04_gold_incremental`)

Full Silver rebuild ki jagah **affected-key recompute**:

1. Naye runs se jo keys touch hui hain — `(vin, event_hour)` hourly ke liye, `(vin, event_date)` trips/battery ke liye — nikaalo
2. Sirf wahi keys **full Silver history** se recompute karo (`event_date` partition pruning + `left_semi` join)
3. MERGE se sirf affected keys update karo

**Result:** values full rebuild jaisi hi (MERGE deterministic recompute), par Silver read pruned. Validated: 2M reprocess me saare counts identical.

### 7.2 Incremental Snowflake (`05_load_snowflake_incremental`)

Aggregation tables ka full load hata ke **affected-key staging**:

1. Event tables pehle se incremental the (`source_run_id` filter)
2. Ab aggregation tables bhi — sirf changed keys Parquet → staging (`TRUNCATE` → `PUT` → `COPY`)
3. MERGE procedure un keys ko curated me upsert karta hai

**Result:** full-load jaisa curated state, cross-system transfer volume fraction me. Verify cell me ab curated counts bhi print hote hain (monitoring bonus).

### 7.3 Wiring

Dono incremental notebooks **1M test job** me wired hain (task keys same, dependency chain same — silver → gold → load_snowflake). Original artifacts untouched.

**Honest caveat (interview me plus point):** synthetic generator same time windows reuse karta hai, to affected keys ≈ poori table — pattern ka worst case. Real-world disjoint windows me Silver read sirf naye partitions tak prune hota — savings near-total.

---

## 8. "Your role" — one-paragraph answer

> "Maine end-to-end design aur implementation kiya — medallion architecture, har layer me checkpoint + idempotency patterns, 7 DQ rules with quarantine in Bronze, MERGE-based aggregation upserts in Gold, aur Snowflake load with staging + stored-procedure MERGE. Production readiness crash-simulation tests se prove ki, system tables pe monitoring banaya, 500K→2M scaling study ki, aur evidence-driven optimization se dono bottlenecks (gold full rebuild, Snowflake full reload) ko incremental affected-key pattern me convert kiya — originals untouched rakhte hue copies pe test karke."

---

## 9. Expected follow-up questions + answers

**Q: Retry-safety kaise guaranteed?**
A: Triple safety — checkpoint sirf successful write ke baad; event_id dedup data-level pe; MERGE upserts deterministic. Crash → stale checkpoint → same batch reprocess → dedup catch → 0 duplicates. Har layer pe crash-simulation test se proven.

**Q: Late-arriving / corrected data?**
A: Affected-key pattern isi ke liye hai — agar ek key ki value change ho jaye, us key ka recompute + MERGE correct value le aayega, bina full rebuild ke.

**Q: Duplicates kabhi aa sakte hain?**
A: Only if event_id collides across different payloads — DQ rules me us case ke checks hain. Quarantine me rejected data audit ke liye preserve hota hai, drop nahi hota.

**Q: Snowflake me kyun staging + MERGE, direct load kyun nahi?**
A: Separation of concerns — COPY bulk load staging me fast hai, MERGE curated me idempotent upsert. Isse retry-safe bhi (partial load ho to re-MERGE safe) aur audit-friendly bhi.

**Q: Sabse bada challenge?**
A: Idempotency + incremental correctness ko saath me laana. Full rebuild simple hai par slow; incremental fast hai par correctness subtle hai (affected keys nikalna, gold-silver consistency). Isliye har optimization ke baad full-rebuild-vs-incremental counts identical hona verify kiya.

---

## 10. Metrics cheat sheet (2-minute revision)

* Pipeline: 5 tasks, sequential, Databricks serverless
* Scaling: 500K → 9.4 min, 1M → 8.3 min, 2M → 9.2 min (4x data, flat runtime)
* Gold + Snowflake = ~60% runtime (optimization targets)
* Gold tables @ Oct 9: telemetry_enriched 9M, vehicle_hourly_metrics 2,994,250, trip_metrics 256,080, battery_health 256,080, alerts 450K
* 14 Silver runs cumulative; latest batch 2M (20261008_204747)
* 7 DQ rules, quarantine Delta, 17-field schema, ~10K+ vehicles, 5 cities
* Bottleneck trend (evidence): gold 137→174s, snowflake 157→185s; bronze/silver flat

## 11. Future work (agar poochein "what next?")

* Bronze table compaction (small files problem) — roadmap me hai
* Credentials Databricks secrets me (abhi notebook config me hain — known gap, acknowledged)
* Real-world disjoint time windows me affected-key savings ka actual benchmark
* Auto Loader / streaming ingestion upgrade (currently batch + checkpoint)
