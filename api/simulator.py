"""
Demo traffic simulator for the hosted demo (Render + Neon).

In the full pipeline, Kafka -> Spark Streaming writes trips and per-minute
metrics into Postgres. Kafka and Spark only run locally, so in the hosted
demo nothing would ever write to the database. When the environment
variable SIMULATOR_ENABLED=true is set, this module stands in for them:

  1. On startup it backfills simulated trips and metrics_minute rows for any
     gap in the last 24 hours, so the dashboard is populated right after a
     cold start (Render's free tier sleeps when idle).
  2. While the API is awake it starts new trips, moves in-flight trips
     through requested -> accepted -> in_progress -> completed/cancelled,
     and upserts metrics_minute every few seconds, the same shape of data
     the Spark job writes.

Rows older than 48 hours are pruned to stay well inside free-tier storage.
Leave SIMULATOR_ENABLED unset when running the real Kafka/Spark pipeline.
"""

import logging
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_values

log = logging.getLogger("fleetline.simulator")

TICK_SECONDS = 5
LOCK_KEY = 72101           # advisory lock id: only one simulator writes at a time
BASE_TRIPS_PER_MIN = 0.55  # per city, before demand weight and time-of-day curve
KEEP_HOURS = 48
FINAL = ("completed", "cancelled")

# city: (lat, lng, UTC offset in hours, demand weight, fare multiplier)
CITIES = {
    "new_york":      (40.7128, -74.0060, -4, 1.6, 1.25),
    "san_francisco": (37.7749, -122.4194, -7, 1.2, 1.30),
    "chicago":       (41.8781, -87.6298, -5, 1.0, 1.00),
    "miami":         (25.7617, -80.1918, -4, 0.8, 0.95),
    "seattle":       (47.6062, -122.3321, -7, 0.7, 1.10),
    "austin":        (30.2672, -97.7431, -5, 0.6, 0.90),
}


# ---------------------------------------------------------------------------
# Demand model
# ---------------------------------------------------------------------------
def _bump(h, center, width):
    return math.exp(-((h - center) / width) ** 2)


def demand_curve(local_hour):
    """Relative demand by local hour: quiet at night, morning and evening peaks."""
    return (0.3 + 0.9 * _bump(local_hour, 8.5, 1.6) + 0.3 * _bump(local_hour, 13, 2.0)
            + 1.1 * _bump(local_hour, 18, 2.5) + 0.4 * _bump(local_hour, 22.5, 1.5))


def trips_per_minute(city, ts):
    _, _, offset, weight, _ = CITIES[city]
    local_hour = (ts.hour + ts.minute / 60 + offset) % 24
    return BASE_TRIPS_PER_MIN * weight * demand_curve(local_hour)


def poisson(lam):
    if lam <= 0:
        return 0
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= limit:
            return k
        k += 1


def make_fare(city, duration_s):
    mult = CITIES[city][4]
    fare = (2.5 + 1.25 * duration_s / 60 + random.uniform(0, 6)) * mult
    return round(min(max(fare, 6.5), 95.0), 2)


def jitter(lat, lng, spread=0.05):
    return (round(lat + random.uniform(-spread, spread), 6),
            round(lng + random.uniform(-spread, spread), 6))


def new_trip_row(city, requested_at):
    lat, lng = CITIES[city][:2]
    p_lat, p_lng = jitter(lat, lng)
    d_lat, d_lng = jitter(lat, lng)
    return {
        "trip_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1, 50000)}",
        "driver_id": None,
        "city": city,
        "status": "requested",
        "pickup_lat": p_lat, "pickup_lng": p_lng,
        "dropoff_lat": d_lat, "dropoff_lng": d_lng,
        "fare": None,
        "requested_at": requested_at,
        "completed_at": None,
        "updated_at": requested_at,
    }


def plan_lifecycle(city, requested_at):
    """Timestamps of each lifecycle event for one simulated trip."""
    accept = requested_at + timedelta(seconds=random.uniform(30, 150))
    eta_s = int(random.uniform(120, 420))  # driver's time to pickup
    start = accept + timedelta(seconds=eta_s)
    duration_s = random.uniform(6, 32) * 60
    events = [("requested", requested_at)]
    roll = random.random()
    if roll < 0.06:      # rider cancels before a driver accepts
        events.append(("cancelled", requested_at + timedelta(seconds=random.uniform(20, 140))))
    elif roll < 0.09:    # cancelled while the driver is on the way
        events += [("accepted", accept), ("cancelled", accept + timedelta(seconds=random.uniform(30, eta_s)))]
    else:
        events += [("accepted", accept), ("in_progress", start),
                   ("completed", start + timedelta(seconds=duration_s))]
    return events, eta_s, make_fare(city, duration_s)


# ---------------------------------------------------------------------------
# Metrics accumulation
# ---------------------------------------------------------------------------
def _new_bucket():
    return {"events": 0, "req": 0, "comp": 0, "canc": 0, "fare_sum": 0.0, "eta_sum": 0, "eta_n": 0}


def record(buckets, city, status, ts, fare=None, eta_s=None):
    minute = ts.replace(second=0, microsecond=0)
    b = buckets[(minute, city)]
    b["events"] += 1
    if status == "requested":
        b["req"] += 1
    elif status == "completed":
        b["comp"] += 1
        b["fare_sum"] += float(fare or 0)
    elif status == "cancelled":
        b["canc"] += 1
    if eta_s is not None:
        b["eta_sum"] += eta_s
        b["eta_n"] += 1


def flush_metrics(cur, buckets):
    if not buckets:
        return
    rows = []
    for (minute, city), b in buckets.items():
        avg_fare = round(b["fare_sum"] / b["comp"], 2) if b["comp"] else None
        avg_eta = int(b["eta_sum"] / b["eta_n"]) if b["eta_n"] else None
        rows.append((minute, minute + timedelta(minutes=1), city, b["events"], b["req"],
                     b["comp"], b["canc"], avg_fare, avg_eta))
    execute_values(cur, """
        INSERT INTO metrics_minute AS m
            (window_start, window_end, city, events_total, trips_requested,
             trips_completed, trips_cancelled, avg_fare, avg_eta_seconds)
        VALUES %s
        ON CONFLICT (window_start, city) DO UPDATE SET
            events_total    = m.events_total + EXCLUDED.events_total,
            trips_requested = m.trips_requested + EXCLUDED.trips_requested,
            trips_completed = m.trips_completed + EXCLUDED.trips_completed,
            trips_cancelled = m.trips_cancelled + EXCLUDED.trips_cancelled,
            avg_fare = CASE
                WHEN EXCLUDED.trips_completed > 0 THEN ROUND(
                    (COALESCE(m.avg_fare, 0) * m.trips_completed
                     + EXCLUDED.avg_fare * EXCLUDED.trips_completed)
                    / (m.trips_completed + EXCLUDED.trips_completed), 2)
                ELSE m.avg_fare END,
            avg_eta_seconds = COALESCE(EXCLUDED.avg_eta_seconds, m.avg_eta_seconds)
    """, rows, page_size=1000)


TRIP_COLUMNS = ("trip_id", "rider_id", "driver_id", "city", "status", "pickup_lat", "pickup_lng",
                "dropoff_lat", "dropoff_lng", "fare", "requested_at", "completed_at", "updated_at")


def insert_trips(cur, trips):
    if trips:
        execute_values(
            cur,
            f"INSERT INTO trips ({', '.join(TRIP_COLUMNS)}) VALUES %s ON CONFLICT (trip_id) DO NOTHING",
            [tuple(t[c] for c in TRIP_COLUMNS) for t in trips], page_size=1000)


def update_trips(cur, updates):
    """updates: list of (trip_id, status, driver_id, fare, completed_at, updated_at)."""
    if updates:
        execute_values(cur, """
            UPDATE trips AS t SET
                status = v.status,
                driver_id = COALESCE(v.driver_id, t.driver_id),
                fare = COALESCE(v.fare, t.fare),
                completed_at = COALESCE(v.completed_at, t.completed_at),
                updated_at = v.updated_at
            FROM (VALUES %s) AS v(trip_id, status, driver_id, fare, completed_at, updated_at)
            WHERE t.trip_id = v.trip_id::uuid
        """, updates, template="(%s, %s, %s, %s::numeric, %s::timestamptz, %s::timestamptz)")


def random_driver():
    return f"driver_{random.randint(1, 1999)}"


def upsert_health(cur, now):
    execute_values(cur, """
        INSERT INTO pipeline_health (component, status, last_heartbeat, uptime_pct_24h, details)
        VALUES %s
        ON CONFLICT (component) DO UPDATE SET
            status = EXCLUDED.status, last_heartbeat = EXCLUDED.last_heartbeat,
            details = EXCLUDED.details
    """, [
        ("api", "healthy", now, None, '{"host": "render"}'),
        ("postgres", "healthy", now, None, '{"host": "neon"}'),
        ("demo_simulator", "healthy", now, None, '{"note": "stands in for kafka + spark in the hosted demo"}'),
    ])


def prune(cur, now):
    cutoff = now - timedelta(hours=KEEP_HOURS)
    cur.execute("DELETE FROM trips WHERE requested_at < %s", (cutoff,))
    cur.execute("DELETE FROM metrics_minute WHERE window_start < %s", (cutoff,))
    cur.execute("DELETE FROM trip_events_log WHERE event_ts < %s", (cutoff,))


# ---------------------------------------------------------------------------
# Backfill (runs once per process start)
# ---------------------------------------------------------------------------
def backfill(conn):
    now = datetime.now(timezone.utc)
    buckets = defaultdict(_new_bucket)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            conn.rollback()
            return 0

        # 1. Trips left in flight when the service went to sleep: finish them
        #    at a plausible time in the past.
        cur.execute("""
            SELECT trip_id::text, city, status, requested_at FROM trips
            WHERE status NOT IN ('completed', 'cancelled') AND requested_at < %s
        """, (now - timedelta(minutes=60),))
        updates = []
        for trip_id, city, status, requested_at in cur.fetchall():
            if status == "requested":
                end = requested_at + timedelta(seconds=random.uniform(60, 300))
                updates.append((trip_id, "cancelled", None, None, None, end))
                record(buckets, city, "cancelled", end)
            else:
                duration = random.uniform(10, 40) * 60
                end = requested_at + timedelta(seconds=duration)
                fare = make_fare(city, duration)
                updates.append((trip_id, "completed", random_driver(), fare, end, end))
                record(buckets, city, "completed", end, fare=fare)
        update_trips(cur, updates)

        # 2. Generate trips for the gap since the newest trip (max 24h back).
        cur.execute("SELECT MAX(requested_at) FROM trips")
        last = cur.fetchone()[0]
        start = now - timedelta(hours=24)
        if last is not None and last > start:
            start = last
        trips = []
        minute = start.replace(second=0, microsecond=0)
        while minute <= now:
            for city in CITIES:
                for _ in range(poisson(trips_per_minute(city, minute))):
                    requested_at = minute + timedelta(seconds=random.uniform(0, 60))
                    if not (start < requested_at <= now):
                        continue
                    trip = new_trip_row(city, requested_at)
                    events, eta_s, fare = plan_lifecycle(city, requested_at)
                    for status, ts in events:
                        if ts > now:
                            break
                        trip["status"], trip["updated_at"] = status, ts
                        if status == "accepted":
                            trip["driver_id"] = random_driver()
                        if status == "completed":
                            trip["fare"], trip["completed_at"] = fare, ts
                        record(buckets, city, status, ts,
                               fare=fare if status == "completed" else None,
                               eta_s=eta_s if status == "accepted" else None)
                    trips.append(trip)
            minute += timedelta(minutes=1)

        insert_trips(cur, trips)
        flush_metrics(cur, buckets)
        upsert_health(cur, now)
        prune(cur, now)
    conn.commit()
    return len(trips)


# ---------------------------------------------------------------------------
# Live tick (every TICK_SECONDS while the API is awake)
# ---------------------------------------------------------------------------
def tick(conn):
    now = datetime.now(timezone.utc)
    buckets = defaultdict(_new_bucket)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            conn.rollback()
            return

        # Start new trips.
        new = []
        for city in CITIES:
            for _ in range(poisson(trips_per_minute(city, now) * TICK_SECONDS / 60)):
                trip = new_trip_row(city, now - timedelta(seconds=random.uniform(0, TICK_SECONDS)))
                new.append(trip)
                record(buckets, city, "requested", trip["requested_at"])
        insert_trips(cur, new)

        # Move in-flight trips along. Transitions are probabilistic per tick,
        # gated by trip age, so no extra state needs to be stored.
        cur.execute("""
            SELECT trip_id::text, city, status, requested_at FROM trips
            WHERE status NOT IN ('completed', 'cancelled')
        """)
        updates = []
        for trip_id, city, status, requested_at in cur.fetchall():
            age = (now - requested_at).total_seconds()
            new_status = None
            if status == "requested" and age > 45 and random.random() < 0.35:
                new_status = "cancelled" if random.random() < 0.07 else "accepted"
            elif status == "accepted" and age > 180 and random.random() < 0.3:
                new_status = "cancelled" if random.random() < 0.03 else "in_progress"
            elif status == "in_progress" and age > 480 and random.random() < 0.06:
                new_status = "completed"
            elif age > 3600:
                new_status = "cancelled" if status == "requested" else "completed"
            if not new_status:
                continue
            driver = random_driver() if new_status == "accepted" else None
            fare = make_fare(city, max(age - 300, 360)) if new_status == "completed" else None
            updates.append((trip_id, new_status, driver, fare,
                            now if new_status == "completed" else None, now))
            record(buckets, city, new_status, now, fare=fare,
                   eta_s=int(random.uniform(120, 420)) if new_status == "accepted" else None)
        update_trips(cur, updates)
        flush_metrics(cur, buckets)
        upsert_health(cur, now)
    conn.commit()


def _with_connection(engine, fn):
    conn = engine.raw_connection()
    try:
        return fn(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _run(engine):
    while True:
        try:
            n = _with_connection(engine, backfill)
            log.info("simulator backfill added %s trips", n)
            break
        except Exception:
            log.exception("simulator backfill failed, retrying in 30s")
            time.sleep(30)

    last_prune = time.monotonic()
    while True:
        try:
            _with_connection(engine, tick)
            if time.monotonic() - last_prune > 600:
                def _prune(conn):
                    with conn.cursor() as cur:
                        prune(cur, datetime.now(timezone.utc))
                    conn.commit()
                _with_connection(engine, _prune)
                last_prune = time.monotonic()
        except Exception:
            log.exception("simulator tick failed")
        time.sleep(TICK_SECONDS)


def start(engine):
    """Start the simulator thread if SIMULATOR_ENABLED is true."""
    if os.getenv("SIMULATOR_ENABLED", "").strip().lower() not in ("1", "true", "yes"):
        return False
    threading.Thread(target=_run, args=(engine,), daemon=True, name="fleetline-simulator").start()
    log.info("demo simulator started (tick every %ss)", TICK_SECONDS)
    return True
