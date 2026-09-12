"""
Simulates a rideshare event stream and publishes to Kafka.

In production this role is played by your actual mobile app backends
(rider app, driver app) publishing real events. This script exists to
generate realistic traffic for developing/testing the Spark Streaming
job and dashboard without live production traffic.

Usage:
    python producer/event_producer.py --rate 15   # ~15 events/sec ≈ 1.3M/day
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker()

CITIES = ["san_francisco", "new_york", "austin", "seattle", "chicago", "miami"]
TRIP_STATUSES = ["requested", "accepted", "in_progress", "completed", "cancelled"]

# Rough city center coordinates for plausible pickup/dropoff points
CITY_CENTERS = {
    "san_francisco": (37.7749, -122.4194),
    "new_york": (40.7128, -74.0060),
    "austin": (30.2672, -97.7431),
    "seattle": (47.6062, -122.3321),
    "chicago": (41.8781, -87.6298),
    "miami": (25.7617, -80.1918),
}


def jitter(lat, lng, spread=0.05):
    return round(lat + random.uniform(-spread, spread), 6), round(lng + random.uniform(-spread, spread), 6)


def make_trip_event(trip_id, city, status, driver_id=None):
    lat, lng = CITY_CENTERS[city]
    pickup = jitter(lat, lng)
    dropoff = jitter(lat, lng)
    now = datetime.now(timezone.utc).isoformat()

    event = {
        "event_type": status,
        "event_ts": now,
        "trip_id": trip_id,
        "rider_id": f"rider_{random.randint(1, 50000)}",
        "driver_id": driver_id,
        "city": city,
        "pickup_lat": pickup[0],
        "pickup_lng": pickup[1],
        "dropoff_lat": dropoff[0],
        "dropoff_lng": dropoff[1],
    }
    if status == "completed":
        event["fare"] = round(random.uniform(6.5, 68.0), 2)
    return event


def make_driver_location_event(driver_id, city):
    lat, lng = CITY_CENTERS[city]
    dlat, dlng = jitter(lat, lng)
    return {
        "event_type": "driver_location",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "driver_id": driver_id,
        "city": city,
        "lat": dlat,
        "lng": dlng,
        "status": random.choice(["available", "en_route", "on_trip"]),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main(args):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_trips = os.getenv("KAFKA_TOPIC_TRIP_EVENTS", "rideshare.trip-events")
    topic_locations = os.getenv("KAFKA_TOPIC_DRIVER_LOCATIONS", "rideshare.driver-locations")

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    active_trips = {}  # trip_id -> (city, current_status_index)
    driver_pool = [f"driver_{i}" for i in range(1, 2000)]

    print(f"Producing to {bootstrap_servers} at ~{args.rate} events/sec. Ctrl+C to stop.")
    sent = 0
    try:
        while True:
            # Occasionally start a brand-new trip
            if random.random() < 0.3 or not active_trips:
                trip_id = str(uuid.uuid4())
                city = random.choice(CITIES)
                active_trips[trip_id] = [city, 0]

            # Advance a random in-flight trip through its lifecycle
            trip_id = random.choice(list(active_trips.keys()))
            city, status_idx = active_trips[trip_id]
            status = TRIP_STATUSES[min(status_idx, len(TRIP_STATUSES) - 1)]
            driver_id = random.choice(driver_pool) if status != "requested" else None

            event = make_trip_event(trip_id, city, status, driver_id)
            producer.produce(topic_trips, key=trip_id, value=json.dumps(event), callback=delivery_report)

            # Advance or complete the trip's lifecycle
            if status in ("completed", "cancelled"):
                del active_trips[trip_id]
            else:
                # ~15% chance of cancellation instead of progressing normally
                if random.random() < 0.05:
                    active_trips[trip_id][1] = len(TRIP_STATUSES) - 1  # jump to cancelled next tick
                    active_trips[trip_id] = [city, len(TRIP_STATUSES) - 1]
                else:
                    active_trips[trip_id][1] = status_idx + 1

            # Occasionally emit a driver location ping too
            if random.random() < 0.4:
                loc_event = make_driver_location_event(random.choice(driver_pool), random.choice(CITIES))
                producer.produce(topic_locations, key=loc_event["driver_id"], value=json.dumps(loc_event))

            producer.poll(0)
            sent += 1
            if sent % 500 == 0:
                print(f"Sent {sent} events...")

            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=12.0, help="Events per second to produce")
    args = parser.parse_args()
    main(args)
