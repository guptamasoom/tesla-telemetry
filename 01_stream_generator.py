# Databricks notebook source
# DBTITLE 1,Stream Generator — Tesla Telemetry
# MAGIC %md
# MAGIC # 01_STREAM_GENERATOR — Tesla Telemetry
# MAGIC
# MAGIC Continuously generates synthetic Tesla vehicle telemetry events in micro-batches and writes them as JSON files to S3. Auto Loader in the Bronze layer picks these up in real-time.
# MAGIC
# MAGIC **Flow**: Generate batch → Write JSON to `stream/raw/` → Sleep `BATCH_INTERVAL_S` → Repeat
# MAGIC
# MAGIC **Usage**: Run this notebook first. It generates events for `GENERATOR_DURATION_S` seconds (default: infinite). Meanwhile, run the Bronze/Silver/Gold/Scoring notebooks in separate tabs.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
import random
import uuid
import json
import time
from datetime import datetime, timezone

print("Tesla Telemetry Stream Generator")
print(f"  Stream root  : {STREAM_RAW_PATH}")
print(f"  Batch size   : {BATCH_SIZE} events / {BATCH_INTERVAL_S}s")
print(f"  Duration     : {'infinite' if GENERATOR_DURATION_S == 0 else f'{GENERATOR_DURATION_S}s'}")

# COMMAND ----------

# DBTITLE 1,Event Generation Functions
# ============================================================
# EVENT GENERATORS
# ============================================================

def gen_telemetry_event(vehicle_number):
    """Generate a single telemetry event for a vehicle."""
    vin = f"WVW{vehicle_number:014d}"
    model = MODELS[vehicle_number % 4]
    city_name = list(CITIES.keys())[vehicle_number % 5]
    lat, lon = CITIES[city_name]
    sw_version = SOFTWARE_VERSIONS[vehicle_number % 5]
    battery_cap = BATTERY_CAPACITIES[vehicle_number % 5]

    # Event type distribution: 85% telemetry, 10% charge, 5% alert
    r = random.random()
    if r < 0.85:
        event_type = "telemetry"
    elif r < 0.95:
        event_type = "charge"
    else:
        event_type = "alert"

    # Speed
    if event_type == "charge":
        speed = 0.0
    else:
        speed = round(random.uniform(0, 130), 2)

    # Battery SOC
    soc = round(20 + random.random() * 80, 2)

    # Battery temp
    temp = round(20 + random.random() * 25, 2)

    # Odometer
    odometer = round(
        1000 + vehicle_number * 25 + random.uniform(0, 100000), 2
    )

    # Charging state
    charging = "Connected" if event_type == "charge" else "Disconnected"

    # Gear
    gear = "P" if speed < 1 else "D"

    # Autopilot
    autopilot = speed > 30 and random.random() < 0.25

    # Alert code
    alert_code = None
    if event_type == "alert":
        alert_code = random.choice(ALERT_CODES)

    now = datetime.now(timezone.utc)

    return {
        "event_id": f"evt_{uuid.uuid4().hex[:16]}",
        "event_ts": now.isoformat(),
        "ingest_ts": now.isoformat(),
        "vehicle_number": vehicle_number,
        "vin": vin,
        "model": model,
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
        "software_version": sw_version,
        "battery_capacity_kwh": battery_cap,
        "event_type": event_type,
        "speed_kmh": speed,
        "battery_soc": soc,
        "battery_temp_c": temp,
        "odometer_km": odometer,
        "charging_state": charging,
        "gear": gear,
        "autopilot_engaged": autopilot,
        "alert_code": alert_code,
    }


def gen_batch(n=BATCH_SIZE):
    """Generate a batch of telemetry events across random vehicles."""
    events = []
    for _ in range(n):
        vehicle_number = random.randint(1, 10000)
        events.append(gen_telemetry_event(vehicle_number))
    return events


print("Event generators defined.")

# COMMAND ----------

# DBTITLE 1,Batch Writer
# ============================================================
# BATCH WRITER
# ============================================================

def write_batch(events):
    """Write a batch of events as a single JSON file to S3."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    json_lines = "\n".join(json.dumps(e) for e in events)
    file_path = f"{STREAM_RAW_PATH}batch_{ts}.json"
    dbutils.fs.put(file_path, json_lines, overwrite=True)
    return len(events)


print("Batch writer defined.")

# COMMAND ----------

# DBTITLE 1,Start Real-Time Generation
# ============================================================
# START REAL-TIME GENERATION
# ============================================================

print("=" * 60)
print("Starting real-time telemetry generator...")
print("=" * 60)

start_time = time.time()
batch_num = 0
total_events = 0

try:
    while True:
        batch_num += 1
        events = gen_batch(BATCH_SIZE)
        written = write_batch(events)
        total_events += written
        elapsed = time.time() - start_time

        print(
            f"[Batch {batch_num:4d}] "
            f"{written:4d} events | "
            f"Total: {total_events:>8,} | "
            f"Elapsed: {elapsed:>6.0f}s"
        )

        if GENERATOR_DURATION_S > 0 and elapsed >= GENERATOR_DURATION_S:
            print(
                f"\nGenerator finished after {elapsed:.0f}s. "
                f"Total events: {total_events:,}"
            )
            break

        time.sleep(BATCH_INTERVAL_S)

except KeyboardInterrupt:
    print(f"\nGenerator stopped. Total events: {total_events:,}")