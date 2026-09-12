# Render service configs — Fleetline live demo

Three Render resources total: a Postgres database, a Web Service (API),
and a Static Site (frontend). Create them in that order, since the API
needs the database's connection details.

---

## 1. Postgres (create first)

New → PostgreSQL

- **Name:** `fleetline-db` (or anything)
- **Database:** `rideshare`
- **User:** `postgres` (or Render's default — either is fine)
- **Region:** pick one, then use the same region for the API service
  below so they can talk over Render's private network
- **Plan:** Free or Starter — this is a demo, not production load

After it's created, open the database's page and note down:
- **Internal Database URL** (or the individual host/port/user/password
  fields — Render shows both)

Then run your schema against it once, from your own machine, using the
**External Database URL** Render gives you:
```bash
psql "<external-database-url-from-render>" -f db/schema.sql
```
Optionally load a seed snapshot of `trips` / `metrics_minute` data here
too, so the live dashboard shows real-looking numbers.

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
use the values from the Postgres service's **Internal Database URL**
fields, matching what `db/connection.py` expects:

| Key | Value |
|---|---|
| `POSTGRES_HOST` | internal hostname from the Render Postgres page |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `rideshare` |
| `POSTGRES_USER` | from the Render Postgres page |
| `POSTGRES_PASSWORD` | from the Render Postgres page |

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

Once both are live, open the frontend's URL, click **"Connect API"**
in the dashboard, and enter the API's `.onrender.com` URL (same as
your local `http://localhost:8000` step, just pointing at the deployed
one). If it doesn't connect, check the CORS setting above first.

---

## Summary of what's live vs. not

| Component | Where |
|---|---|
| Frontend | Render Static Site |
| API | Render Web Service (Docker) |
| Postgres | Render managed Postgres |
| Kafka + Zookeeper | Local only (`docker-compose.yml`) |
| Spark Streaming job | Local only (`spark-submit`) |
| EMR / S3 / MSK | Documented, not deployed (see `aws/`) |
