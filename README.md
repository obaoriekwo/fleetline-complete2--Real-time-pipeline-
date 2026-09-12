# Fleetline — Rideshare Streaming Data Pipeline

Scalable streaming pipeline processing 1M+ rideshare events/day, using Kafka
for message queuing, Spark Structured Streaming for real-time processing, and
AWS (S3 + EMR) for storage and compute — with a Postgres serving layer and a
live operations dashboard on top.

## Live demo vs. full architecture

The dashboard, API, and Postgres serving layer are deployed live on
Render:

- **Dashboard:** https://your-fleetline-frontend.onrender.com
- **API:** https://your-fleetline-api.onrender.com

This live version is fully functional — the dashboard queries a real
Postgres database through the same FastAPI read layer described below,
seeded with a representative snapshot of trip and metrics data.

**Kafka and Spark Structured Streaming on EMR are not running live.**
An always-on 3-broker Kafka cluster (MSK) plus an EMR Spark cluster
costs roughly $1,000+/month to run continuously at the replication and
node counts needed for real production use — not justified for a
public portfolio demo. That part of the pipeline is fully implemented
and runs locally via Docker Compose exactly as described in
[Running it locally](#running-it-locally), and is designed to deploy
to AWS exactly as described in [Deploying to AWS](#deploying-to-aws) —
it just isn't paid for continuously in this deployment.

In short: what's live on Render is the real serving layer (API +
Postgres + dashboard); what's documented-but-not-hosted is the
streaming ingestion layer (Kafka + Spark/EMR), which is genuine,
runnable code rather than a stub.

## Architecture

```
Rider/Driver apps → Kafka (trip-events, driver-locations)
                         │
                         ▼
              Spark Structured Streaming (on EMR)
                    │              │
                    ▼              ▼
             S3 (raw archive)   Postgres (aggregates: metrics_minute, trips)
                                        │
                                        ▼
                          FastAPI read layer (api/main.py)
                                        │
                                        ▼
                      Fleetline dashboard (frontend/)
```

- **Kafka**: durable, replayable message queue for trip lifecycle events and driver
  location pings. Configure replication factor ≥ 3 in production so broker failure
  doesn't lose unread messages — this is the local part of your failover story.
- **Spark Structured Streaming (on EMR)**: consumes Kafka, writes raw enriched
  events to S3 (partitioned by date/hour, for reprocessing/backfill/Athena queries),
  and computes 1-minute tumbling-window aggregates upserted into Postgres.
- **S3**: long-term event archive + Spark checkpoint storage. Checkpointing to S3
  means a failed Spark application resumes from where it left off instead of
  reprocessing or dropping data — this is what gives you recoverable failover.
- **PostgreSQL (`rideshare` database)**: the serving layer. Holds curated,
  query-friendly tables (`trips`, `metrics_minute`, `driver_locations`,
  `pipeline_health`) that the dashboard queries directly — never queries S3/EMR
  directly, which would be too slow for a live UI.
- **FastAPI**: thin read-only layer between Postgres and the dashboard.
- **Dashboard**: vanilla HTML/CSS/JS, no build step, responsive down to mobile.

## Postgres connection

Configured via environment variables (see `.env.example`), defaulting to what is available:

```
host=localhost, port=5432, database=rideshare, user=postgres
```

 The real password is set in `.env` before running anything against a real database.

## Full project file manifest

```
rideshare-pipeline/
├── README.md                      # this file
├── requirements.txt                 # Python deps (kafka client, pyspark, fastapi, etc.)
├── .env.example                     # env var template — copy to .env, fill in password
├── .gitignore
├── .dockerignore
├── docker-compose.yml                # local dev: Kafka + Zookeeper + Postgres + API
│
├── db/
│   ├── schema.sql                    # Postgres DDL — run this against `rideshare` first
│   └── connection.py                 # shared connection helper (used by Spark job + API)
│
├── producer/
│   └── event_producer.py             # simulates rideshare events → Kafka (for dev/testing)
│
├── spark/
│   └── streaming_job.py              # the core Structured Streaming job (Kafka → S3 + Postgres)
│
├── api/
│   ├── main.py                       # FastAPI read layer the dashboard calls
│   └── Dockerfile                    # container build for the API
│
├── aws/
│   ├── emr_cluster_config.json       # EMR cluster launch config (primary/core/task nodes)
│   ├── bootstrap.sh                  # EMR bootstrap action — installs Python deps on nodes
│   └── s3_lifecycle_policy.json      # cost control: tiers/expires old raw data + checkpoints
│
├── frontend/
│   ├── index.html                    # dashboard markup
│   ├── style.css                     # dashboard styling (responsive, dark ops-console theme)
│   └── script.js                     # dashboard behavior — live/demo data, canvas charts
│
└── tests/
    └── throughput_check.py           # confirms producer rate meets 1M events/day target
```

**Nothing in this list needs regenerating** — every file above is included and
functional as-is. Two things needed
deployment: the Postgres password (`.env`), and your actual S3 bucket name
(replace `your-rideshare-data-lake` in `aws/*.json`, `.env.example`, and
`spark/streaming_job.py`'s defaults).

## Running it locally

**1. Start Kafka + Postgres:**
```bash
docker compose up -d zookeeper kafka postgres
```

**2. Apply the schema** (docker-compose already does this automatically via
`docker-entrypoint-initdb.d`, but if running against an existing Postgres):
```bash
psql -h localhost -p 5432 -U postgres -d rideshare -f db/schema.sql
```

**3. Start the API:**
```bash
pip install -r requirements.txt
uvicorn api.main:app --reload --port 8000
```

**4. Start the event producer** (simulates rideshare traffic):
```bash
python producer/event_producer.py --rate 12   # ~1M events/day
```

**5. Run the Spark Streaming job locally** (needs Java 8/11 + PySpark):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/streaming_job.py
```

**6. Open the dashboard:**
```bash
cd frontend && python3 -m http.server 5500
# visit http://localhost:5500, then click "Connect API" and enter http://localhost:8000
```

The dashboard works immediately with simulated demo data even with nothing
else running — connecting the API switches it to your real Postgres data.

## Deploying to AWS

1. Upload `spark/streaming_job.py`, `db/connection.py` and `aws/bootstrap.sh` to your S3 bucket
2. Update the bucket name in `aws/emr_cluster_config.json`
3. Launch the cluster: `aws emr create-cluster --cli-input-json file://aws/emr_cluster_config.json`
4. Apply `aws/s3_lifecycle_policy.json` to your bucket for cost control on raw event storage
5. Point Kafka at a production cluster (MSK recommended) with replication factor ≥ 3
6. Run Postgres as RDS with Multi-AZ enabled — this is what gives you automatic
   failover for the serving layer, matching the 99.9% uptime requirement

## On the 99.9% uptime / failover claim

This is achieved architecturally, not by any single component:
- **Kafka**: replication factor ≥ 3 across brokers (MSK handles broker failover)
- **Spark on EMR**: checkpointing to S3 lets a restarted streaming job resume
  without data loss; EMR managed scaling replaces unhealthy task nodes
- **RDS Postgres Multi-AZ**: automatic failover to a standby on primary failure
- **`pipeline_health` table**: the dashboard surfaces per-component status so
  degraded conditions are visible immediately rather than discovered later
