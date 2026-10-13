# Render service configs — Fleetline live demo

A Neon Postgres database plus two Render resources: a Web Service (API)
and a Static Site (frontend). Create them in that order, since the API
needs the database's connection details.

---

## 1. Postgres on Neon (create first)

The database runs on [Neon](https://neon.tech) (serverless Postgres, free
tier) rather than Render, because Render's free databases expire.

- Create a project in the **same region** as the Render API
  (AWS Europe Central 1 / Frankfurt here), Postgres 18.
- Copy the connection string from **Connect** in the Neon dashboard.
- Create the tables once, from your own machine:
```bash
psql "<neon-connection-string>" -f db/schema.sql
```

---

## 2. API — Web Service

New → Web Service → connect this repo

- **Name:** `fleetline-api`
- **Root Directory:** leave blank (repo root) — the Dockerfile's build
  context needs the whole repo, matching `docker-compose.yml`'s
  `context: .`
- **Environment:** Docker
- **Dockerfile Path:** `api/Dockerfile`
- **Region:** same as the Postgres database above
- **Instance Type:** Free or Starter

**Environment Variables** (Add Environment Variable, one per row) —
taken from the Neon connection string, matching what `db/connection.py`
expects:

| Key | Value |
|---|---|
| `POSTGRES_HOST` | the `ep-...neon.tech` host from the Neon connection string |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `neondb` |
| `POSTGRES_USER` | `neondb_owner` |
| `POSTGRES_PASSWORD` | from the Neon connection string |
| `PGSSLMODE` | `require` (Neon only accepts SSL; psycopg2 reads this automatically) |
| `SIMULATOR_ENABLED` | `true` (see below) |

**Demo simulator:** Kafka and Spark only run locally, so in the hosted
demo nothing else writes to the database. With `SIMULATOR_ENABLED=true`,
`api/simulator.py` stands in for them: on startup it backfills the last
24 hours of simulated trips and per-minute metrics, and while the API is
awake it moves trips through their lifecycle every 5 seconds, writing the
same tables the Spark job writes. Rows older than 48 hours are pruned.
Leave it unset when running the real pipeline locally.

Leave **Build Command** and **Start Command** blank — since this is a
Docker environment, Render just runs the Dockerfile's own `CMD`
(whatever `api/Dockerfile` already specifies, e.g. something like
`uvicorn main:app --host 0.0.0.0 --port 8000`). If the Dockerfile's
`CMD` binds to a fixed port other than what Render expects, add a
`PORT` env var matching it, or update the Dockerfile to read `$PORT`.

Click **Deploy Web Service**. Once live, copy its `.onrender.com` URL —
you'll need it for the frontend's API connection and possibly for
CORS (see note below).

**CORS note:** if `api/main.py` doesn't already allow cross-origin
requests, the dashboard (served from a different `.onrender.com`
domain) won't be able to call it from the browser. Add FastAPI's CORS
middleware if it's not already there:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-fleetline-frontend.onrender.com"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
```

---

## 3. Frontend — Static Site

New → Static Site → connect this repo

- **Name:** `fleetline-frontend`
- **Root Directory:** `frontend`
- **Build Command:** leave blank — no build step, per your README
- **Publish Directory:** `.` (relative to the Root Directory above,
  so this resolves to `frontend/`)

Click **Create Static Site**.

The dashboard connects to `https://fleetline-api.onrender.com` on load
(`DEFAULT_API` in `frontend/script.js`). While the free API wakes up
(up to ~50 seconds) it shows the demo stream and switches to live data
on the first successful response. Add `?api=<url>` to the dashboard URL
to point it at another API (e.g. `?api=http://localhost:8000`), or
`?demo=1` to stay on the demo stream.

---

## Summary of what's live vs. not

| Component | Where |
|---|---|
| Frontend | Render Static Site |
| API | Render Web Service (Docker) |
| Postgres | Neon (serverless Postgres, Frankfurt) |
| Live trip data | `api/simulator.py` inside the API (hosted demo only) |
| Kafka + Zookeeper | Local only (`docker-compose.yml`) |
| Spark Streaming job | Local only (`spark-submit`) |
| EMR / S3 / MSK | Documented, not deployed (see `aws/`) |
