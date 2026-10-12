# Mock Interview Prep — Tesla Telemetry + FraudStream V3

> **Two projects on Databricks**: Tesla Vehicle Telemetry (batch medallion ETL) + FraudStream V3 (real-time streaming fraud detection)
> Company-wise questions (Capgemini, TCS, Accenture, Deloitte, PwC, Cognizant, HCL, Uber, IBM, Tiger Analytics, Virtusa, Happiest Minds, Annalect, J.P. Morgan) — sab answers Hinglish mein, dono projects se examples + code.

---

## A. Project Overview (Sabse common sawaal)

### Q1: Walk me through your data engineering project end-to-end. *(Capgemini, Cognizant, HCL, Deloitte, Uber)*

**Problem**: Tesla ko telemetry analytics platform chahiye tha — 10K+ vehicles ka battery health, driving patterns, charging behavior monitor karna tha. Data discrete batches mein aata tha (500K events per run).

**Architecture**: Batch medallion ETL on Databricks. 9 notebooks, 5 sequential tasks: generate -> bronze -> silver -> gold -> load_snowflake. S3 pe Delta format, serverless compute. Har layer pe checkpoint-based incremental processing.

**Implementation**: 9 notebooks khud design kiye. Bronze: dbutils.fs.ls se run discovery + 7 DQ rules + quarantine. Silver: standardize + derive columns (battery_band, speed_category) + left_anti join dedup. Gold: 5 tables — MERGE for aggregates + idempotent append for events. Snowflake load via PUT/COPY. DABs for dev/prod, GitHub Actions CI/CD.

**Challenges**: Idempotency ensure karna — mode(overwrite) tha Gold mein, unsafe. MERGE + idempotent append se replace kiya. .cache() serverless pe fail hua — removed. .count() logging full table scan karta tha — DeltaTable.isDeltaTable se replace kiya.

**Impact**: 500K events/run in ~5 min. 1M aur 2M scale tested. 3 layers crash-tested — 0 duplicates on retry. 3 SQL alerts every 15 min.

### Q2: Describe a pipeline you designed and implemented. *(Capgemini, Cognizant)*

**FraudStream V3** — Real-time fraud detection pipeline. Problem: fintech ko seconds mein fraud catch karna tha.

Architecture: Structured Streaming + Auto Loader on Databricks. 7 notebooks, 5 concurrent tasks (continuous job, no dependencies). UC Volume pe data.

Flow: 01_stream_generator (200 events/5s, 8% fraud) -> 02_bronze_stream (Auto Loader + DQ + quarantine) -> 03_silver_stream (10-min watermark + dropDuplicates) -> 04_scoring_stream (2-day velocity window + 7 fraud rules) -> 05_gold_stream (7 gold tables, 60s trigger).

Key challenges: watermark tuning (60s -> 10 min), 2-day scoring window optimization, safe_foreachbatch error handling. 37/37 validation checks passed.

---

## B. Spark / PySpark Questions *(Capgemini, Tiger Analytics, Accenture, Cognizant, HCL)*

### Q3: RDD vs DataFrame vs Dataset — difference kya hai? *(Tiger Analytics, Accenture, Cognizant)*

**RDD**: Low-level, untyped, no schema. Distributed collection of objects. No optimization engine. Slow.

**DataFrame**: Tabular, schema-aware, Catalyst optimizer + Tungsten execution. Ham dono projects mein DataFrame use kiya — type safety + optimization milta hai.

```python
# Tesla Bronze — DataFrame with explicit schema
raw_df = spark.read.schema(telemetry_schema).json(run_path)
```

**Dataset**: Typed API (Scala/Java). Python mein unavailable. DataFrame hi use kiya dono projects mein.

### Q4: Transformations vs Actions — difference? *(Tiger Analytics, HCL, Cognizant)*

**Transformations**: Naya DataFrame banate hain, lazy evaluate hote hain. map, filter, groupBy, join, withColumn.

**Actions**: Trigger execution, result return karte hain. count, collect, show, write, take.

```python
# FraudStream Silver — transformations (lazy)
silver_df = (bronze_stream
    .withWatermark("event_ts", "10 minutes")  # transformation
    .dropDuplicates(["event_id"])             # transformation
    .withColumn("is_high_risk", F.col("risk_score") > 0.8)  # transformation
)

# Tesla Gold — action triggers execution
affected_keys.write.format("delta").mode("append").save(gold_path)  # ACTION
```

Spark lazy evaluation use karta taaki execution plan optimize ho sake. Sab transformations queue mein rakte hain, action aane pe Catalyst optimizer best plan banata hai.

### Q5: Wide vs Narrow transformations? *(Tiger Analytics, Cognizant)*

**Narrow**: Same partition pe data process hota hai. No shuffle. filter, map, withColumn.

**Wide**: Data shuffle hota hai across partitions. groupBy, join, distinct, repartition.

```python
# Tesla Silver — NARROW (no shuffle)
silver_df = bronze_df.withColumn("battery_band", 
    F.when(F.col("battery_soc") < 20, "low")
     .when(F.col("battery_soc") < 80, "medium")
     .otherwise("high"))

# Tesla Gold — WIDE (shuffle for GROUP BY)
hourly_metrics = silver_df.groupBy("vin", "event_date", "event_hour").agg(
    F.avg("speed_kmh").alias("avg_speed"),
    F.avg("battery_soc").alias("avg_soc")
)
```

Shuffle minimize karne ke liye: partition pruning, broadcast join for small tables, reduce groupBy keys.

### Q6: cache() vs persist() vs checkpoint()? *(Capgemini, Accenture, HCL)*

**cache()**: DataFrame ko memory mein store karta hai (MEMORY_AND_DISK default). Repeated computations avoid karte hain.

**persist()**: Same as cache but storage level customize kar sakte ho (MEMORY_ONLY, DISK_ONLY, etc.).

**checkpoint()**: DataFrame ki lineage cut karta hai, reliable DFS pe save karta hai. Long lineage chains mein use hota hai.

```python
# Tesla — .cache() SERVERLESS PE KAAM NAHI KARTA (Spark Connect limitation)
# Ye bug fix kiya tha:
# BAD: affected_keys.cache()  # fails on serverless
# GOOD: affected_keys = affected_keys.distinct()  # reduce shuffle without cache
```

Real example: .cache() add kiya Gold optimization ke liye, but serverless compute (Spark Connect) pe fail hua. Fix: .cache() remove kiya, sirf .distinct() removal rakha. Pipeline stable + optimized.

### Q7: repartition() vs coalesce()? *(Tiger Analytics, HCL, Cognizant, Capgemini)*

**repartition()**: Full shuffle karta hai, partitions increase/decrease kar sakta hai. Expensive but balanced.

**coalesce()**: Shuffle nahi karta, existing partitions merge karta hai. Decrease only. Cheaper.

```python
# Tesla Snowflake load — coalesce(1) for single Parquet file
gold_df.coalesce(1).write 
    .format("parquet")
    .mode("overwrite")
    .save(staging_path)
# Ek file banati hai taaki Snowflake PUT + COPY efficient ho

# FraudStream Gold — repartition for parallel writes
gold_df.repartition("event_date")
    .write.format("delta").mode("append").save(gold_path)
```

### Q8: Broadcast join — kab use karte ho? *(Capgemini)*

Jab ek table choti (typically < 10MB) aur dusri badi ho. Choti table broadcast ho jaati hai to all worker nodes, shuffle avoid hota hai.

```python
# Tesla Gold — affected keys (small, ~5K rows) vs full Silver (9M rows)
# Spark auto-detects small table and broadcasts
silver_filtered = silver_df.join(
    affected_keys,  # small DataFrame — auto-broadcast
    on=["vin", "event_date"],
    how="left_semi"  # only keep Silver rows matching affected keys
)
```

Config: `spark.sql.autoBroadcastJoinThreshold` (default 10MB). Manual: `broadcast(small_df)`.

### Q9: map() vs flatMap()? *(Cognizant, HCL)*

**map**: 1 input -> 1 output. Same number of elements.
**flatMap**: 1 input -> 0 or more outputs. Flatten karta hai.

```python
# Tesla — map (1-to-1): VIN format validate karna
validated = raw_df.rdd.map(lambda row: validate_vin(row)).toDF()

# FraudStream — flatMap (1-to-many): alert description split into keywords
keywords = alerts_df.rdd.flatMap(lambda row: row.alert_desc.split())
```

### Q10: Duplicate records kaise remove karte ho? *(Accenture, Cognizant)*

```python
# Tesla — left_anti join for idempotent dedup (batch)
new_events = bronze_df.join(
    existing_silver,
    on="event_id",
    how="left_anti"  # sirf wahi events jo Silver mein nahi hain
)

# FraudStream — dropDuplicates with watermark (streaming)
silver_df = bronze_stream \
    .withWatermark("event_ts", "10 minutes") \
    .dropDuplicates(["event_id"])

# SQL alternative
df.dropDuplicates(["event_id", "vin"])
```

### Q11: Window functions in PySpark? *(Cognizant, Accenture, J.P. Morgan)*

```python
from pyspark.sql.window import Window

# Tesla — moving average of battery SOC per vehicle (last 3 events)
window_spec = Window.partitionBy("vin").orderBy("timestamp").rowsBetween(-2, 0)
silver_df = silver_df.withColumn(
    "soc_moving_avg", 
    F.avg("battery_soc").over(window_spec)
)

# FraudStream — rank transactions by amount per user
window_spec = Window.partitionBy("user_id").orderBy(F.col("amount").desc())
scored_df = scored_df.withColumn("amount_rank", F.rank().over(window_spec))
```

### Q12: Null values kaise handle karte ho? *(Capgemini)*

```python
# Tesla — fill nulls based on column type
cleaned_df = raw_df.fillna({
    "speed_kmh": 0,           # numeric -> 0
    "battery_soc": 0,         # numeric -> 0
    "city": "Unknown",        # string -> default
    "event_type": "telemetry" # string -> default
})

# Type-aware dynamic fill
for col_name, dtype in raw_df.dtypes:
    if dtype in ["int", "double", "float"]:
        raw_df = raw_df.fillna(0, subset=[col_name])
    elif dtype == "string":
        raw_df = raw_df.fillna("Unknown", subset=[col_name])
```

### Q13: Skewed data kaise handle karte ho? *(Cognizant, HCL)*

Skew = ek partition mein zyada data, baaki partitions khali. Join aur groupBy mein common.

```python
# Tesla — some vehicles (fleet vehicles) have 10x more events than average
# Solution: salting technique
from pyspark.sql.functions import concat, col, rand, lit, explode, array

# Add salt key to split skewed partition
salted = silver_df.withColumn("salt", (rand() * 10).cast("int"))
salted_keys = affected_keys.withColumn("salt", 
    explode(array([lit(i) for i in range(10)])))

# Join on vin + salt — distributes skew
result = salted.join(salted_keys, ["vin", "salt", "event_date"])
```

Dusra approach: broadcast join (small table skew avoid), Adaptive Query Execution (AQE) — Spark 3+ automatically handles skew.

### Q14: Spark job slow hai — debug kaise? *(Cognizant, Deloitte, HCL, Uber)*

1. **Spark UI** check karo — konsa stage slow hai, shuffle size kya hai
2. **Partition skew** — `df.rdd.getNumPartitions()`, `df.groupBy().count().orderBy(desc)` se check
3. **Shuffle operations** — unnecessary joins/groupBy remove karo
4. **Broadcast join** — small tables broadcast karo
5. **Partition count** — too many = overhead, too few = OOM
6. **Caching** — repeated computations cache karo (but NOT on serverless Spark Connect!)
7. **File format** — Parquet/Delta instead of CSV/JSON
8. **Predicate pushdown** — filter early, read less data

Tesla real example: Gold 237 seconds -> 206 seconds. .count() logging remove kiya (full table scans), DeltaTable.isDeltaTable use kiya, anti-join filter by source_run_id.

---

## C. AWS / Cloud Questions *(Deloitte, TCS, HCL, IBM, PwC, Capgemini)*

### Q15: AWS services kaise use kiye? *(Deloitte, HCL, IBM, Cognizant)*

**S3** (Tesla): Primary data store. Raw JSON -> Bronze/Silver/Gold Delta tables on S3. Partitioned by source_run_id (Bronze), event_date (Silver/Gold). S3 + Delta Lake = ACID transactions + low cost.

**UC Volume** (FraudStream): Databricks-managed storage. Streaming checkpoints + Delta tables. Unity Catalog governs access.

**Databricks on AWS**: Serverless compute = AWS infrastructure under the hood. No EC2/EMR/Lambda needed.

**Snowflake** (Tesla): Gold tables load kiye via PUT/COPY. S3 se Parquet staging -> Snowflake internal stage -> COPY INTO.

### Q16: S3 storage classes? *(Deloitte, Capgemini, HCL)*

- **S3 Standard**: Frequently accessed data. Tesla ka Bronze/Silver/Gold — frequent reads.
- **S3 Standard-IA**: Infrequent access. Old quarantine data.
- **S3 Glacier**: Archive. Raw data retention > 90 days.
- **S3 Intelligent-Tiering**: Auto-moves between tiers. Unknown access patterns.

Tesla: S3 Standard for hot data (recent runs), lifecycle rule moves old partitions to Standard-IA after 30 days.

```python
# S3 lifecycle rule (concept)
# Transition to Standard-IA after 30 days
# Transition to Glacier after 90 days
# Expire after 365 days
```

### Q17: AWS Glue vs EMR? *(TCS, Cognizant, HCL, IBM)*

**Glue**: Serverless ETL. No cluster management. Crawlers for schema discovery. Good for simple pipelines.

**EMR**: Managed Hadoop/Spark cluster. More control. Good for complex Spark jobs.

Mera case: Databricks use kiya (not Glue/EMR). Databricks = managed Spark + Delta Lake + MLflow + serverless. Glue/EMR se zyada features aur better Delta Lake support.

Agar Glue use karna pade: Glue ETL jobs mein PySpark likhunga, Crawlers se schema discover, Glue Data Catalog for metadata. Same medallion pattern.

### Q18: IAM roles and permissions kaise manage kiye? *(Deloitte, PwC)*

Databricks IAM internally manage karta hai via instance profiles. S3 access workspace level pe configured hai — no per-notebook IAM.

Snowflake: service account authentication via Snowflake connector. Credentials Databricks secrets scope mein store, never hardcoded.

DABs deploy: service principal token (DATABRICKS_TOKEN in GitHub secrets) — no personal credentials in CI/CD.

```yaml
# databricks.yml — DABs CI/CD
# GitHub Actions uses DATABRICKS_HOST + DATABRICKS_TOKEN secrets
targets:
  prod:
    mode: production
    variables:
      notebook_root: "/Users/masoomgupta0715@gmail.com/tesla_telemetry"
```

### Q19: AWS Lambda in data pipeline? *(KPMG, HCL, PwC)*

Lambda = serverless event-driven compute. Data pipeline mein use cases:
- S3 event trigger -> Lambda -> Databricks job trigger
- File validation on upload
- Lightweight ETL (small files)

Mera case: Databricks serverless compute use kiya — Lambda ki zarurat nahi. But agar Kafka + Lambda chahiye:

```python
# Lambda handler — trigger Databricks job on S3 upload
import boto3

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Trigger Databricks job via API
    client = boto3.client('databricks')
    client.run_now(job_id=288555998184699)
    return {'status': 'job_triggered', 'file': f'{bucket}/{key}'}
```

### Q20: Redshift vs Athena vs Snowflake? *(Deloitte, HCL, IBM)*

| Feature | Redshift | Athena | Snowflake |
|---------|----------|--------|----------|
| Type | MPP Data Warehouse | Serverless Query Engine | Cloud Data Warehouse |
| Storage | Own storage | S3 directly | Separate storage + compute |
| Best for | Complex analytics | Ad-hoc S3 queries | Multi-cloud analytics |

Tesla mein Snowflake use kiya — separation of storage/compute, auto-scaling, time travel. Redshift se zyada flexible.

### Q21: S3 data lake design kaise kiya? *(Deloitte, HCL, IBM)*

```
s3://tesla-vehicle-telemetry-511976431596/
  telemetry/
    raw/                    # Bronze source
      ingestion_date=2026-10-07/
        run_id=20261007_103944/
    bronze/                 # DQ validated, partitioned by source_run_id
    quarantine/             # Failed DQ records
    silver/                 # Clean + deduped, partitioned by event_date
    gold/                   # 5 analytics tables
      telemetry_enriched/
      vehicle_hourly_metrics/
      trip_metrics/
      battery_health/
      alerts/
    checkpoints/            # Incremental processing state
```

Medallion pattern: raw -> bronze (DQ gate) -> silver (clean) -> gold (analytics-ready). Partitioning enables pruning.

---

## D. SQL Questions *(Sab companies)*

### Q22: Second/third highest salary find karo. *(Tiger Analytics, KPMG, Accenture, Deloitte, Cognizant)*

```sql
-- Second highest (DENSE_RANK)
SELECT salary FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rn
    FROM employees
) WHERE rn = 2;

-- Third highest (LIMIT/OFFSET)
SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 2;

-- Tesla equivalent: top 3 vehicles by trip distance
SELECT vin, total_distance FROM (
    SELECT vin, DENSE_RANK() OVER (ORDER BY total_distance DESC) as rn, total_distance
    FROM trip_metrics
) WHERE rn <= 3;
```

### Q23: RANK() vs DENSE_RANK() vs ROW_NUMBER()? *(Tiger Analytics, KPMG)*

```sql
-- Tesla example: rank vehicles by battery degradation
SELECT vin, battery_degradation_rate,
    ROW_NUMBER() OVER (ORDER BY battery_degradation_rate DESC) as row_num,
    RANK() OVER (ORDER BY battery_degradation_rate DESC) as rank,
    DENSE_RANK() OVER (ORDER BY battery_degradation_rate DESC) as dense_rank
FROM battery_health;

-- ROW_NUMBER: 1,2,3,4,5 (unique, no gaps)
-- RANK: 1,2,2,4,5 (ties get same rank, gap after)
-- DENSE_RANK: 1,2,2,3,4 (ties get same rank, NO gap)
```

### Q24: Window functions — rolling average? *(J.P. Morgan, Deloitte)*

```sql
-- Tesla: 7-day rolling average of battery SOC per vehicle
SELECT vin, event_date, avg_soc,
    AVG(avg_soc) OVER (
        PARTITION BY vin 
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as soc_7day_avg
FROM battery_health
ORDER BY vin, event_date;

-- FraudStream: rolling 7-day fraud count
SELECT event_date,
    COUNT(*) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as fraud_7day_count
FROM scored
WHERE risk_level = 'HIGH';
```

### Q25: Joins explain karo. *(Annalect, Deloitte)*

```sql
-- INNER JOIN: sirf matching records
-- Tesla: Silver JOIN Gold on vin (vehicles with both Silver + Gold data)

-- LEFT JOIN: left table ke saare records + matching from right
-- Tesla: Silver LEFT JOIN alerts (all events, with alert info where available)

-- LEFT ANTI JOIN: left table records NOT in right table
-- Tesla: Bronze LEFT ANTI JOIN Silver (events not yet processed by Silver)
SELECT * FROM bronze_df
LEFT ANTI JOIN silver_df ON bronze_df.event_id = silver_df.event_id;

-- FULL OUTER JOIN: dono tables ke saathe records
-- FraudStream: transactions FULL OUTER JOIN alerts (transactions with/without alerts)
```

### Q26: DELETE vs TRUNCATE vs DROP? *(Deloitte)*

- **DELETE**: Row-level, DML, WHERE clause ke saath. Slow. Can rollback.
- **TRUNCATE**: Table empty karta hai, DDL. Fast. Cannot rollback (in some DBs).
- **DROP**: Table hi remove kar deta hai, DDL. Structure + data gone.

Tesla: Delta Lake mein DELETE use kiya for quarantine cleanup. TRUNCATE avoid — checkpoint data important.

### Q27: Duplicate records find + remove karo. *(Accenture, J.P. Morgan)*

```sql
-- Find duplicates
SELECT event_id, COUNT(*) as cnt
FROM bronze_telemetry
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Remove duplicates (keep latest)
DELETE FROM bronze_telemetry
WHERE event_id IN (
    SELECT event_id FROM (
        SELECT event_id, ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY timestamp DESC
        ) as rn
        FROM bronze_telemetry
    ) WHERE rn > 1
);

-- PySpark: dropDuplicates
df.dropDuplicates(["event_id"])
```

### Q28: HAVING vs WHERE? *(Deloitte)*

- **WHERE**: Individual rows filter karta hai, GROUP BY se pehle.
- **HAVING**: Groups filter karta hai, GROUP BY ke baad.

```sql
-- Tesla: vehicles with avg speed > 80 km/h on highway
SELECT vin, AVG(speed_kmh) as avg_speed
FROM silver_telemetry
WHERE speed_category = 'highway'  -- WHERE: row-level filter
GROUP BY vin
HAVING AVG(speed_kmh) > 80;        -- HAVING: group-level filter
```

### Q29: UNION vs UNION ALL? *(Deloitte)*

- **UNION**: Duplicates remove karta hai. Slow (sort + dedup).
- **UNION ALL**: Duplicates keep karta hai. Fast.

```sql
-- Tesla: combine alerts from two runs
SELECT * FROM alerts WHERE source_run_id = '20261007_103944'
UNION ALL
SELECT * FROM alerts WHERE source_run_id = '20261008_020015';
-- UNION ALL: faster, duplicates allowed (same alert in both runs = valid)
```

---

## E. Python Questions *(Deloitte, Cognizant, HCL, Uber)*

### Q30: List vs Tuple? *(Deloitte, Cognizant)*

- **List**: Mutable, [], append/remove possible. `my_list = [1, 2, 3]`
- **Tuple**: Immutable, (), cannot change. `my_tuple = (1, 2, 3)`

```python
# Tesla — tuple for fixed schema definition (immutable)
telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])  # Tuples inside StructField — immutable

# Tesla — list for dynamic run discovery (mutable)
new_runs = []
for run in all_runs:
    if run > checkpoint:
        new_runs.append(run)  # List: mutable, dynamic
```

### Q31: Shallow copy vs Deep copy? *(Deloitte)*

```python
import copy

# Shallow copy: nested objects share reference
a = [[1, 2], [3, 4]]
b = copy.copy(a)
b[0].append(99)  # a[0] also changes!

# Deep copy: nested objects fully independent
c = copy.deepcopy(a)
c[0].append(88)  # a unchanged
```

### Q32: Exception handling? *(Deloitte)*

```python
# FraudStream — safe_foreachbatch pattern
import logging
logger = logging.getLogger(__name__)

def safe_scoring_batch(batch_df, batch_id):
    try:
        # Score fraud rules
        scored = apply_fraud_rules(batch_df)
        scored.write.format("delta").mode("append").save(scored_path)
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
        # Do NOT re-raise — stream continues to next batch
        # One bad batch doesnt kill the pipeline
```

### Q33: Lambda functions? *(Deloitte)*

```python
# Tesla — lambda for quick transformations
is_critical = lambda soc, temp: soc < 10 or temp > 55

critical_alerts = silver_df.filter(
    F.udf(is_critical, BooleanType())(F.col("battery_soc"), F.col("battery_temp"))
)

# List comprehension — derive battery bands
battery_bands = [
    (soc, "low" if soc < 20 else "medium" if soc < 80 else "high")
    for soc in soc_readings
]
```

### Q34: Large file efficiently kaise read karo? *(Deloitte, Uber)*

```python
# Chunked reading for large CSV (100GB+)
import pandas as pd

chunk_size = 100000
for chunk in pd.read_csv("large_file.csv", chunksize=chunk_size):
    process_chunk(chunk)

# Tesla — Spark handles large files natively (distributed)
raw_df = spark.read.schema(telemetry_schema).json("s3://bucket/raw/")
# Spark automatically partitions + distributes the read
```

---

## F. Data Architecture & Modeling *(Happiest Minds, J.P. Morgan, PwC, Accenture, Capgemini L2)*

### Q35: Star schema vs Snowflake schema? *(Happiest Minds, J.P. Morgan, PwC)*

**Star Schema**: Fact table center mein, dimension tables around. Simple, fast queries, denormalized.
**Snowflake Schema**: Dimensions further normalized. Less redundancy, more joins.

Tesla Gold = Star schema:
- **Fact**: telemetry_enriched (event_id, vin, speed, soc, temperature)
- **Dimensions**: vehicle_hourly_metrics (by vin+hour), trip_metrics (by vin+date), battery_health (by vin+date)
- Joins on `vin` — simple, fast.

### Q36: SCD types kya hote hain? *(Happiest Minds, Annalect, PwC)*

- **SCD Type 1**: Overwrite old value. No history. Tesla: battery_health MERGE (latest snapshot only).
- **SCD Type 2**: Keep history. New row for change. Valid-from/to columns.
- **SCD Type 3**: Keep only previous value. One extra column.

```python
# Tesla Gold — SCD Type 1 via MERGE
from delta.tables import DeltaTable

gold_table = DeltaTable.forPath(spark, gold_path)
gold_table.alias("target").merge(
    new_aggregates.alias("source"),
    "target.vin = source.vin AND target.event_date = source.event_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# Overwrites existing values — SCD Type 1
```

### Q37: Fact vs Dimension tables? *(Happiest Minds)*

**Fact**: Measurable data (speed, SOC, temperature, transaction amount). Tesla: telemetry_enriched.
**Dimension**: Descriptive attributes (vehicle info, date, location). Tesla: vehicle metadata, date dimension.

### Q38: ETL vs ELT? *(Accenture)*

- **ETL**: Extract -> Transform -> Load. Transform before loading. Good for complex transformations.
- **ELT**: Extract -> Load -> Transform. Load raw, then transform in warehouse. Good for cloud data warehouses.

Tesla: ETL pattern (transform in Databricks -> load to Snowflake). FraudStream: ELT (load raw to Bronze -> transform in Silver/Gold).

### Q39: OLTP vs OLAP? *(Accenture, HCL)*

- **OLTP**: Transactional, many small writes, normalized. E.g., banking transactions.
- **OLAP**: Analytical, large reads, denormalized. E.g., Tesla Gold tables for dashboards.

FraudStream = OLTP (real-time transaction scoring) feeding OLAP (Gold analytics tables).

### Q40: Medallion architecture explain karo. *(Capgemini L2, Deloitte)*

```
Raw JSON -> Bronze (DQ + quarantine) -> Silver (clean + dedup + derive) -> Gold (analytics-ready)
```

**Bronze**: Schema enforce + 7 DQ rules. Bad records -> quarantine. Append-only. Partition: source_run_id.

**Silver**: Standardize (city initcap, event_type lower). Derive (battery_band, speed_category, is_moving). Dedup: left_anti join on event_id. Partition: event_date.

**Gold**: 5 tables. Event-level: idempotent append (telemetry_enriched, alerts). Aggregates: MERGE upsert (hourly, trips, battery_health). Consumption layer for dashboards, alerts, Snowflake.

### Q41: Partitioning vs Bucketing? *(Accenture, HCL)*

**Partitioning**: Directory-based split. Good for coarse filtering (date, category). Tesla: event_date partition.

**Bucketing**: File-based split within partition. Good for join optimization on specific columns. E.g., bucket by vin.

```sql
-- Hive-style partitioning
CREATE TABLE silver_telemetry
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://.../silver/';

-- Delta Liquid Clustering (better than partitioning for adaptive layout)
CREATE TABLE gold_metrics
USING DELTA
CLUSTER BY (vin, event_date)
LOCATION 's3://.../gold/';
```

---

## G. Kafka & Streaming *(Accenture, HCL, Uber)*

### Q42: Apache Kafka kya hai? Architecture explain karo. *(Accenture, HCL)*

Kafka = distributed event streaming platform. Producers write messages to topics, Consumers read from topics.

**Components**:
- **Topic**: Category/feed name. Partitioned for parallelism.
- **Partition**: Ordered, immutable sequence of records. Each partition on one broker.
- **Producer**: Publishes messages to topics.
- **Consumer**: Subscribes to topics, processes messages.
- **Broker**: Kafka server. Cluster of brokers for fault tolerance.
- **Zookeeper/KRaft**: Coordination service.

FraudStream mein Auto Loader use kiya (file-based), but Kafka equivalent:

```python
# FraudStream — Kafka integration (if using Kafka instead of Auto Loader)
from pyspark.sql import functions as F

# Read from Kafka topic
bronze_stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "fraud-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON from Kafka value column
parsed = bronze_stream.selectExpr("CAST(value AS STRING) as json_str")
transactions = parsed.select(
    F.from_json(F.col("json_str"), transaction_schema).alias("data")
).select("data.*")

# Apply DQ + write to Bronze Delta
transactions.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://.../checkpoints/bronze/") \
    .trigger(processingTime="10 seconds") \
    .start("s3://.../bronze/")
```

### Q43: Kafka Producer vs Consumer? *(Accenture)*

```python
# FraudStream — Kafka Producer (replaces 01_stream_generator)
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['broker1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate fraud transaction event
event = {
    "event_id": "txn_12345",
    "user_id": "user_789",
    "amount": 850.00,
    "merchant": "suspicious_store",
    "timestamp": "2026-10-12T10:30:00Z"
}
producer.send('fraud-transactions', event)
producer.flush()
```

```python
# FraudStream — Kafka Consumer (replaces Auto Loader)
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'fraud-transactions',
    bootstrap_servers=['broker1:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    event = message.value
    # Process event — apply DQ rules, score fraud
    process_transaction(event)
```

### Q44: Kafka mein message ordering kaise ensure karo? *(Accenture)*

- **Within partition**: Messages ordered by offset. Guaranteed.
- **Across partitions**: No ordering guarantee.
- **Solution**: Use same key (e.g., user_id) for related messages -> same partition.

```python
# FraudStream — same user's transactions go to same partition
producer.send('fraud-transactions', 
    key=str(event['user_id']).encode('utf-8'),  # partition key
    value=event
)
# Same user_id -> same partition -> ordered processing
```

### Q45: Kafka fault tolerance kaise handle karta hai? *(Accenture)*

- **Replication**: Each partition replicated across brokers (replication factor = 3).
- **Consumer groups**: If one consumer fails, rebalance to others.
- **Offset commit**: Consumer tracks last processed offset. Crash pe resume from last commit.
- **Exactly-once**: Kafka Transactions API + idempotent producer.

FraudStream equivalent: Spark Structured Streaming checkpoints. Crash pe resume from last checkpoint offset.

```python
# Spark + Kafka — exactly-once with checkpointing
stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "fraud-transactions")
    .load()
)

# Checkpoint ensures exactly-once
stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/fraud/") \
    .start("/bronze/fraud/")
# Crash pe checkpoint se resume — no data loss, no duplicates
```

---

## H. Airflow & Scheduling *(Accenture, Deloitte)*

### Q46: Apache Airflow kya hai? DAGs kya hain? *(Accenture, Deloitte)*

Airflow = workflow orchestration tool. DAGs (Directed Acyclic Graphs) mein tasks define karte ho, schedule kar sakte ho, dependencies set kar sakte ho.

Tesla pipeline mein: Databricks Jobs use kiya (depends_on chain). But Airflow equivalent:

```python
# Airflow DAG — Tesla Telemetry Pipeline
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'masoom',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'masoomgupta0715@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tesla_telemetry_pipeline',
    default_args=default_args,
    description='Tesla Vehicle Telemetry ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 10, 1),
    catchup=False,
)

# Task 1: Generate telemetry
generate = DatabricksSubmitRunOperator(
    task_id='generate_telemetry',
    databricks_conn_id='databricks_default',
    notebook_task={
        'notebook_path': '/Users/masoomgupta0715@gmail.com/tesla_telemetry/01_generate_telemetry',
    },
    dag=dag,
)

# Task 2: Bronze layer (depends on generate)
bronze = DatabricksSubmitRunOperator(
    task_id='bronze_layer',
    databricks_conn_id='databricks_default',
    notebook_task={
        'notebook_path': '/Users/masoomgupta0715@gmail.com/tesla_telemetry/02_bronze_batch',
    },
    dag=dag,
)

# Task 3: Silver layer (depends on bronze)
silver = DatabricksSubmitRunOperator(
    task_id='silver_layer',
    databricks_conn_id='databricks_default',
    notebook_task={
        'notebook_path': '/Users/masoomgupta0715@gmail.com/tesla_telemetry/03_silver',
    },
    dag=dag,
)

# Task 4: Gold layer (depends on silver)
gold = DatabricksSubmitRunOperator(
    task_id='gold_layer',
    databricks_conn_id='databricks_default',
    notebook_task={
        'notebook_path': '/Users/masoomgupta0715@gmail.com/tesla_telemetry/04_gold',
    },
    dag=dag,
)

# Task 5: Load to Snowflake (depends on gold)
load_snowflake = DatabricksSubmitRunOperator(
    task_id='load_snowflake',
    databricks_conn_id='databricks_default',
    notebook_task={
        'notebook_path': '/Users/masoomgupta0715@gmail.com/tesla_telemetry/05_load_snowflake',
    },
    dag=dag,
)

# Dependency chain (sequential — same as Databricks Job)
generate >> bronze >> silver >> gold >> load_snowflake
```

### Q47: Airflow mein failed task kaise handle karte ho? *(Accenture)*

```python
# Retry + email alert (Airflow default_args)
default_args = {
    'retries': 1,                    # 1 retry on failure
    'retry_delay': timedelta(minutes=5),  # 5-min cooldown
    'email_on_failure': True,        # Email alert
    'email': 'masoomgupta0715@gmail.com',
}

# Task-level retry with on_failure_callback
from airflow.operators.python import PythonOperator

def on_failure_callback(context):
    """Log failure + send custom alert"""
    task = context['task_instance']
    logging.error(f"Task {task.task_id} failed at {context['ts']}")
    # Send Slack/email alert

generate = PythonOperator(
    task_id='generate_telemetry',
    python_callable=generate_events,
    on_failure_callback=on_failure_callback,
    retries=2,
    retry_delay=timedelta(minutes=3),
    dag=dag,
)
```

### Q48: Backfilling in Airflow? *(Accenture)*

Backfill = past dates ke liye pipeline re-run karna. `catchup=True` set karo, start_date past mein do.

```python
dag = DAG(
    'tesla_backfill',
    start_date=datetime(2026, 9, 1),  # Past date
    catchup=True,                     # Backfill from Sep 1 to today
    schedule_interval='0 2 * * *',
)

# Or manual backfill
# airflow dags backfill tesla_backfill -s 2026-09-01 -e 2026-10-12
```

Tesla equivalent: checkpoint reset karke past runs re-process. left_anti join ensures no duplicates.

### Q49: Airflow vs Databricks Jobs — kab kaun use karo? *

**Databricks Jobs** (mera case): Databricks-native. Simpler for Databricks notebooks. depends_on chain. No external service.

**Airflow**: Language-agnostic. Better for multi-tool orchestration (Spark + S3 + Redshift + Snowflake). More complex but flexible.

Mera choice: Databricks Jobs + DABs + GitHub Actions = same functionality as Airflow, but simpler aur Databricks-integrated. Airflow use karta agar multiple systems (Spark + dbt + Snowflake + external APIs) orchestrate karne hote.

---

## I. Snowflake Questions *(Virtusa, PwC, Capgemini, HCL)*

### Q50: Snowflake architecture kya hai? *(Virtusa, PwC)*

Snowflake = cloud data warehouse. Storage aur compute separate hain:
- **Storage**: Cloud object storage (S3 on AWS). Micro-partitions.
- **Compute**: Virtual warehouses. Auto-scale, suspend/resume.
- **Cloud Services**: Metadata, optimization, access control.

Tesla mein Snowflake use kiya — Gold tables load kiye for cross-platform analytics.

### Q51: Snowflake mein kaunse tables hote hain? *(PwC, Capgemini)*

- **Permanent**: Standard tables. Persistent.
- **Transient**: No fail-safe. Cheaper.
- **Temporary**: Session-only. Gone after session.
- **External**: Data stays in S3. Snowflake reads externally.

Tesla: Permanent tables for Gold analytics. External tables possible for S3 Delta.

### Q52: Snowflake vs Redshift? *(PwC, HCL)*

| Feature | Snowflake | Redshift |
|---------|-----------|---------|
| Architecture | Separate storage + compute | MPP (coupled) |
| Scaling | Auto-scale warehouses | Manual resize cluster |
| Multi-cloud | Yes (AWS, Azure, GCP) | AWS only |
| Time Travel | Yes (up to 90 days) | Limited |
| Cost | Pay per use | Provisioned |

Tesla mein Snowflake — auto-scaling + time travel + multi-cloud flexibility.

### Q53: Snowpipe vs COPY INTO? *(PwC)*

- **Snowpipe**: Continuous, automated loading. Serverless. Micro-batch. Good for streaming.
- **COPY INTO**: Batch loading. Manual or scheduled. Good for bulk loads.

Tesla: COPY INTO use kiya — Gold tables batch load karte hain via PUT + COPY.

```python
# Tesla — Snowflake PUT + COPY pattern
import snowflake.connector

conn = snowflake.connector.connect(
    account='xy12345',
    user='tesla_svc',
    password=dbutils.secrets.get('snowflake_scope', 'password'),
    warehouse='ETL_WH',
    database='TESLA_ANALYTICS'
)

cursor = conn.cursor()

# PUT Parquet files to Snowflake internal stage
cursor.execute("PUT file:///tmp/gold_staging/telemetry_enriched.parquet @gold_stage")

# COPY INTO Snowflake table
cursor.execute("""
    COPY INTO telemetry_enriched
    FROM @gold_stage/telemetry_enriched.parquet
    FILE_FORMAT = (TYPE = PARQUET)
    ON_ERROR = 'CONTINUE'
""")

conn.close()
```

---

## J. Data Quality, Monitoring & Failures *(Deloitte, Capgemini L2, HCL, Cognizant)*

### Q54: Data quality kaise ensure karte ho? *(Deloitte, HCL)*

Tesla: 7 DQ rules Bronze mein: (1) required fields non-null, (2) VIN format, (3) timestamp range, (4) speed 0-300 km/h, (5) SOC 0-100%, (6) temp -40 to 120C, (7) event_type validation. Failed -> quarantine with rejection_reason.

FraudStream: Auto Loader bad data handling + Bronze DQ checks. Quarantine with reasons.

```python
# Tesla — DQ rules in Bronze
validated_df = raw_df.filter(
    (F.col("event_id").isNotNull()) &
    (F.col("vin").isNotNull()) &
    (F.col("speed_kmh") >= 0) & (F.col("speed_kmh") <= 300) &
    (F.col("battery_soc") >= 0) & (F.col("battery_soc") <= 100)
)

quarantine_df = raw_df.join(validated_df, ["event_id"], "left_anti")
quarantine_df.withColumn("rejection_reason", "DQ_RULE_FAILED")
```

### Q55: Pipeline monitor kaise karte ho? *(Deloitte, Capgemini L2)*

3 layers:
1. **Infrastructure**: Email alerts (task + job level). Auto-retry. `system.lakeflow.job_task_run_timeline` for run history.
2. **Business**: 3 SQL alerts every 15 min — battery temp >55C, SOC <10%, charging errors >100.
3. **Dashboards**: 4-widget dashboard — task duration, bottleneck trend, failed runs, avg duration.

### Q56: Failures aur retries kaise handle karte ho? *(Cognizant, HCL)*

**Tesla**: max_retries=1, 5-min cooldown. Checkpoint-based idempotency — crash pe checkpoint stale rehta hai, retry pe same run reprocess hota hai, left_anti join catches duplicates, 0 new writes. 3 layers crash-tested.

**FraudStream**: task_retry_mode ON_FAILURE. Streaming checkpoints — resume from last offset. safe_foreachbatch catches errors, logs with exc_info, does NOT re-raise.

### Q57: Schema evolution kaise handle karte ho? *(PwC, Annalect, Cognizant)*

```python
# Tesla — explicit schema, mergeSchema for Delta
raw_df = spark.read.schema(telemetry_schema).json(path)
# Delta with mergeSchema
raw_df.write.option("mergeSchema", "true").format("delta").save(bronze_path)

# FraudStream — Auto Loader schema evolution
bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(raw_path)
)
```

### Q58: Incremental loading kaise kiya? *(Capgemini L2, Deloitte)*

```python
# Tesla — checkpoint-based incremental
# Step 1: Read checkpoint
last_run_id = spark.read.delta(checkpoint_path).filter(
    F.col("pipeline_name") == "bronze"
).orderBy(F.col("checkpoint_updated_at").desc()).select("last_processed_run_id").first()[0]

# Step 2: Discover new runs > checkpoint
new_runs = [r for r in all_runs if r > last_run_id]

# Step 3: Process only new runs
# Step 4: Update checkpoint AFTER successful write
checkpoint_df.write.format("delta").mode("append").save(checkpoint_path)
```

---

## K. Behavioral & Scenario Questions *(All companies)*

### Q59: Sabse challenging part kya tha? *(HCL, Cognizant)*

**Tesla**: Idempotency ensure karna har layer mein. Original code mode(overwrite) use karta tha Gold mein — crash pe saara data lost. MERGE + idempotent append se replace kiya. Phir prove karna pada — har layer pe crash simulate kiya, 0 duplicates verify kiye. left_anti join + checkpoint = key insight.

**FraudStream**: Watermark tuning. 60s initially — legitimate late events drop ho rahe the. 10 min set kiya after understanding the tradeoff. Also, bare except in Gold silently errors swallow kar raha tha — invisible failures. Proper logging se fix.

### Q60: Ek failure batao aur kaise fix kiya. *(HCL, Cognizant)*

**Tesla**: .cache() serverless pe fail hua. Gold optimization ke liye .cache() add kiya, but Spark Connect (serverless) pe .cache() properly support nahi karta. Pipeline crash. Fix: .cache() remove kiya, sirf .distinct() removal rakha. Pipeline stable + optimized.

**Tesla**: Silver checkpoint bug — jab saare records duplicates hain (silver_new_count == 0), checkpoint update nahi hota tha. Pipeline same run ko infinitely reprocess karta. Fix: guard condition change kiya — `if bronze_df is None` instead of `if silver_new_count == 0`. Checkpoint always update hona chahiye.

**FraudStream**: Gold chargeback_summary ka bare except errors silently swallow kar raha tha. Table empty but no errors logged. Fix: `except Exception as e: logger.error(f"Failed: {e}", exc_info=True)`. Ab errors visible.

### Q61: Pipeline slow hai — kaise optimize karoge? *(Cognizant, Deloitte, HCL)*

1. **Spark UI** check — konsa stage slow, shuffle size
2. **Partition pruning** — filter early, reduce data scan
3. **Avoid .count()** for logging — use DeltaTable.isDeltaTable for existence checks
4. **Affected-key recompute** — only changed keys, not full table
5. **Broadcast join** for small tables
6. **File format** — Delta/Parquet, not CSV/JSON
7. **coalesce/repartition** — right partition count
8. **MERGE vs overwrite** — incremental upsert

Tesla real results: Gold 237s -> 206s. .count() logging remove, filtered anti-join, DeltaTable.isDeltaTable.

### Q62: Agar ye project dobara karo to kya improve karoge? *(All)*

**Tesla**:
1. Liquid Clustering instead of manual partitioning
2. Unity Catalog for table governance (path-based Delta currently)
3. Materialized Views for pre-computed Gold aggregates
4. Great Expectations for DQ framework
5. Kafka + Structured Streaming for real-time alerts

**FraudStream**:
1. Kafka instead of file-based Auto Loader
2. ML model for fraud scoring (currently rule-based)
3. Exactly-once sink with Delta MERGE in foreachBatch
4. Schema registry for evolving events
5. More edge case testing beyond 37 checks

---

## Quick Reference Table

| Metric | Tesla Telemetry | FraudStream V3 |
|--------|----------------|----------------|
| Architecture | Batch medallion | Real-time streaming |
| Notebooks | 9 | 7 |
| Job Mode | Scheduled (5 sequential) | Continuous (5 concurrent) |
| Data Storage | S3 (Delta) | UC Volume (Delta) |
| Incremental | Checkpoint-based (last_processed_run_id) | Auto Loader + watermark |
| Deduplication | Left-anti join on event_id | dropDuplicates + watermark (10 min) |
| DQ Rules | 7 rules + quarantine | Auto Loader DQ + quarantine |
| Gold Tables | 5 (MERGE + idempotent append) | 7 (full recompute, 60s trigger) |
| Gold Incremental | Affected-key recompute | N/A (full recompute) |
| Error Handling | Checkpoint retry-safe | safe_foreachbatch |
| Performance | Partition pruning, affected-key, MERGE | 2-day window, 60s trigger, single groupBy |
| Auto-Retry | max_retries 1, 5-min cooldown | task_retry_mode ON_FAILURE |
| SQL Alerts | 3 (temp, SOC, charging) | 1 (HIGH risk spike) |
| Dashboards | 4-widget pipeline monitoring | Real-time fraud monitoring |
| DABs | dev/prod, 2 jobs | dev/prod, parameterized |
| CI/CD | GitHub Actions | GitHub Actions |
| GitHub | github.com/guptamasoom/tesla-telemetry | github.com/guptamasoom/fraudstream-v3 |
| Git Files | 16 | 11 |
| Validation | 3 layers crash-tested | 37/37 checks passed |
| Scale Tested | 500K, 1M, 2M per run | Continuous (200/5s) |
| Compute | Serverless | Serverless/Interactive |
| External Sink | Snowflake (PUT/COPY) | N/A |

---

*Updated: October 12, 2026 — Hinglish, company-specific, Kafka + Airflow + code examples*