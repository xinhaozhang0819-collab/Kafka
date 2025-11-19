import time
import json
import uuid
import random
from datetime import datetime

from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_trip():
    pickup_cities = ["New York", "Los Angeles", "Chicago", "Houston", "Seattle"]
    dropoff_cities = pickup_cities  
    statuses = ["Searching", "Ongoing", "Completed", "Cancelled"]
    payment_methods = ["Credit Card", "Debit Card", "Cash", "Apple Pay", "Google Pay"]

    pickup_city = random.choice(pickup_cities)
    dropoff_city = random.choice(dropoff_cities)

    distance_km = round(random.uniform(1, 30), 2)
    duration_min = round(random.uniform(5, 60), 1)

    base_fare = 3.0
    price = base_fare + distance_km * 1.5 + duration_min * 0.3
    price = round(price, 2)

    status = random.choices(
        population=statuses,
        weights=[0.1, 0.2, 0.6, 0.1], 
        k=1,
    )[0]

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "driver_id": str(uuid.uuid4())[:6],
        "rider_id": str(uuid.uuid4())[:6],
        "pickup_city": pickup_city,
        "dropoff_city": dropoff_city,
        "distance_km": distance_km,
        "duration_min": duration_min,
        "price": price,
        "status": status,
        "payment_method": random.choice(payment_methods),
        "timestamp": datetime.now().isoformat(),
    }


def run_producer():
    
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition "
                f"{record_metadata.partition} at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
