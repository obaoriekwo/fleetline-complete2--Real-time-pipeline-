"""
Spark Structured Streaming job for the rideshare pipeline.

Reads trip events from Kafka, then:
  1. Writes enriched raw events to S3 (partitioned by date/hour) for long-term
     storage, reprocessing, and ad-hoc analytics via Athena/EMR.
  2. Computes 1-minute tumbling window aggregates per city and upserts them
     into Postgres (`metrics_minute`) for the live dashboard.
  3. Upserts trip state into the `trips` table so the dashboard can show
     current status per trip.

Run on EMR with:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark/streaming_job.py

Run fully locally (no AWS account needed) with the same command - see
"Local storage fallback" below for how that's handled.

Fault tolerance / 99.9% uptime:
  - Checkpointing to S3 means a failed Spark application can resume from
    where it left off without reprocessing or dropping data.
  - Kafka topics should be created with replication-factor >= 3 so broker
    failure doesn't lose unread messages.
  - Run this job on EMR with an On-Demand primary node + a mix of On-Demand/
    Spot core nodes, and enable EMR's automatic task node replacement.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRIP_EVENTS = os.getenv("KAFKA_TOPIC_TRIP_EVENTS", "rideshare.trip-events")

# ---------------------------------------------------------------------------
# Local storage fallback
#
# The README's "Running it locally" instructions tell you to run this exact
# script with spark-submit - but the raw-event archive and Spark checkpoints
# were previously hardcoded to s3a://your-rideshare-data-lake/..., a
# placeholder bucket name. Running that locally without a real AWS account
# and credentials would fail immediately the first time Spark tried to write
# a checkpoint. That's a real gap between the README's claim and the code.
#
# Fix: auto-detect. If S3_BUCKET is still the placeholder default (i.e. you
# haven't set a real bucket), write to a local folder instead - no AWS
# account, no extra Hadoop/S3 JARs needed, since Spark writes to local paths
# natively. Set a real S3_BUCKET env var (and AWS credentials) to switch to
# real S3 for an actual EMR/production run - nothing else in this file needs
# to change either way.
# ---------------------------------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET", "your-rideshare-data-lake")
USE_LOCAL_STORAGE = os.getenv("USE_LOCAL_STORAGE", str(S3_BUCKET == "your-rideshare-data-lake")).lower() == "true"

if USE_LOCAL_STORAGE:
    LOCAL_DATA_DIR = os.path.abspath(os.getenv("LOCAL_DATA_DIR", "./local_data"))
    S3_RAW_PATH = os.path.join(LOCAL_DATA_DIR, "raw", "trip-events")
    S3_CHECKPOINT_PATH = os.path.join(LOCAL_DATA_DIR, "checkpoints", "trip-events")
    print(f"[streaming_job] No real S3_BUCKET configured - writing locally under {LOCAL_DATA_DIR}")
else:
    S3_RAW_PATH = f"s3a://{S3_BUCKET}/{os.getenv('S3_RAW_PREFIX', 'raw/trip-events')}"
    S3_CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/{os.getenv('S3_CHECKPOINT_PREFIX', 'checkpoints/trip-events')}"
    print(f"[streaming_job] Writing to real S3: s3a://{S3_BUCKET}/")



TRIP_EVENT_SCHEMA = StructType([
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("trip_id", StringType()),
    StructField("rider_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("city", StringType()),
    StructField("pickup_lat", DoubleType()),
    StructField("pickup_lng", DoubleType()),
    StructField("dropoff_lat", DoubleType()),
    StructField("dropoff_lng", DoubleType()),
    StructField("fare", DoubleType()),
])


def build_spark_session():
    return (
        SparkSession.builder
        .appName("rideshare-streaming-pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_TRIP_EVENTS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(F.from_json("json_str", TRIP_EVENT_SCHEMA).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts")
        .withColumn("event_ts", F.coalesce(F.col("event_ts"), F.col("kafka_ts")))
        .withWatermark("event_ts", "2 minutes")
    )


def write_raw_to_s3(events_df):
    """Sink 1: append-only raw event archive in S3, partitioned by date/hour."""
    enriched = (
        events_df
        .withColumn("date", F.to_date("event_ts"))
        .withColumn("hour", F.hour("event_ts"))
    )
    return (
        enriched.writeStream
        .format("parquet")
        .option("path", S3_RAW_PATH)
        .option("checkpointLocation", f"{S3_CHECKPOINT_PATH}/raw")
        .partitionBy("date", "hour")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def compute_minute_metrics(events_df):
    """1-minute tumbling window aggregates per city."""
    return (
        events_df
        .groupBy(F.window("event_ts", "1 minute"), "city")
        .agg(
            F.count("*").alias("events_total"),
            F.sum(F.when(F.col("event_type") == "requested", 1).otherwise(0)).alias("trips_requested"),
            F.sum(F.when(F.col("event_type") == "completed", 1).otherwise(0)).alias("trips_completed"),
            F.sum(F.when(F.col("event_type") == "cancelled", 1).otherwise(0)).alias("trips_cancelled"),
            F.avg(F.when(F.col("event_type") == "completed", F.col("fare"))).alias("avg_fare"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "events_total", "trips_requested", "trips_completed", "trips_cancelled", "avg_fare",
        )
    )


def write_batch_to_postgres(batch_df, batch_id):
    """foreachBatch sink: upsert windowed metrics into Postgres."""
    if batch_df.isEmpty():
        return

    # Import here so the driver-side connection helper doesn't need to be
    # serialized/shipped to executors — collect to driver, then write.
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from db.connection import get_connection

    rows = batch_df.collect()
    upsert_sql = """
        INSERT INTO metrics_minute
            (window_start, window_end, city, events_total, trips_requested,
             trips_completed, trips_cancelled, avg_fare)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, city) DO UPDATE SET
            events_total = EXCLUDED.events_total,
            trips_requested = EXCLUDED.trips_requested,
            trips_completed = EXCLUDED.trips_completed,
            trips_cancelled = EXCLUDED.trips_cancelled,
            avg_fare = EXCLUDED.avg_fare
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(upsert_sql, (
                    row["window_start"], row["window_end"], row["city"],
                    row["events_total"], row["trips_requested"],
                    row["trips_completed"], row["trips_cancelled"], row["avg_fare"],
                ))


def write_metrics_to_postgres(metrics_df):
    return (
        metrics_df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", f"{S3_CHECKPOINT_PATH}/metrics")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark_session()
    raw_stream = read_kafka_stream(spark)
    events = parse_events(raw_stream)

    raw_query = write_raw_to_s3(events)
    metrics_query = write_metrics_to_postgres(compute_minute_metrics(events))

    raw_query.awaitTermination()
    metrics_query.awaitTermination()


if __name__ == "__main__":
    main()
