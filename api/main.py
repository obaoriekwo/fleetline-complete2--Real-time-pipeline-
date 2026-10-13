"""
Read-only API serving the dashboard from Postgres.

Connects using the connection details in db/connection.py
(host=localhost, port=5432, database=rideshare, user=postgres by default).

Run with:
    uvicorn api.main:app --reload --port 8000
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from db.connection import get_engine  # noqa: E402
sys.path.append(os.path.dirname(__file__))
import simulator  # noqa: E402

logging.basicConfig(level=logging.INFO)

engine = get_engine()


@asynccontextmanager
async def lifespan(app):
    # Hosted demo only: SIMULATOR_ENABLED=true stands in for Kafka + Spark.
    simulator.start(engine)
    yield


app = FastAPI(title="Rideshare Pipeline Dashboard API", lifespan=lifespan)

# Dashboard is a static file opened via a local dev server / different origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"service": "fleetline-api", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/summary")
def summary():
    """High-level stats for the top stat row."""
    query = text("""
        SELECT
            COUNT(*) FILTER (WHERE requested_at >= NOW() - INTERVAL '24 hours') AS trips_24h,
            COUNT(*) FILTER (WHERE status = 'in_progress') AS trips_active,
            COUNT(*) FILTER (WHERE status = 'completed' AND requested_at >= NOW() - INTERVAL '24 hours') AS completed_24h,
            COUNT(*) FILTER (WHERE status = 'cancelled' AND requested_at >= NOW() - INTERVAL '24 hours') AS cancelled_24h,
            ROUND(AVG(fare) FILTER (WHERE status = 'completed' AND requested_at >= NOW() - INTERVAL '24 hours'), 2) AS avg_fare_24h
        FROM trips
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(query).mappings().first()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")

    if not row or row["trips_24h"] is None:
        return {
            "trips_24h": 0, "trips_active": 0, "completed_24h": 0,
            "cancelled_24h": 0, "avg_fare_24h": None,
        }
    return dict(row)


@app.get("/api/timeseries")
def timeseries(minutes: int = 60):
    """Events-per-minute series for the trend chart, last N minutes."""
    query = text("""
        SELECT window_start, SUM(events_total) AS events_total,
               SUM(trips_completed) AS trips_completed,
               SUM(trips_cancelled) AS trips_cancelled
        FROM metrics_minute
        WHERE window_start >= NOW() - (:minutes || ' minutes')::interval
          AND window_start < date_trunc('minute', NOW())  -- complete minutes only
        GROUP BY window_start
        ORDER BY window_start ASC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"minutes": minutes}).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    return [dict(r) for r in rows]


@app.get("/api/by-city")
def by_city():
    """Trip volume breakdown by city, last 24 hours."""
    query = text("""
        SELECT city, COUNT(*) AS trip_count
        FROM trips
        WHERE requested_at >= NOW() - INTERVAL '24 hours'
        GROUP BY city
        ORDER BY trip_count DESC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    return [dict(r) for r in rows]


@app.get("/api/status-breakdown")
def status_breakdown():
    query = text("""
        SELECT status, COUNT(*) AS count
        FROM trips
        WHERE requested_at >= NOW() - INTERVAL '24 hours'
        GROUP BY status
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    return [dict(r) for r in rows]


@app.get("/api/recent-events")
def recent_events(limit: int = 25):
    query = text("""
        SELECT trip_id, city, status, driver_id, fare, updated_at
        FROM trips
        ORDER BY updated_at DESC
        LIMIT :limit
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"limit": limit}).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    return [dict(r) for r in rows]


@app.get("/api/pipeline-health")
def pipeline_health():
    query = text("SELECT component, status, last_heartbeat, uptime_pct_24h FROM pipeline_health")
    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")
    return [dict(r) for r in rows]
