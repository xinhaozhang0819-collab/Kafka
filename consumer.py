import json

import psycopg2
from kafka import KafkaConsumer


def run_consumer():

    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id        VARCHAR(50) PRIMARY KEY,
                driver_id      VARCHAR(50),
                rider_id       VARCHAR(50),
                pickup_city    VARCHAR(100),
                dropoff_city   VARCHAR(100),
                distance_km    NUMERIC(6, 2),
                duration_min   NUMERIC(6, 2),
                price          NUMERIC(10, 2),
                status         VARCHAR(50),
                payment_method VARCHAR(50),
                timestamp      TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip = message.value

                insert_query = """
                    INSERT INTO trips (
                        trip_id,
                        driver_id,
                        rider_id,
                        pickup_city,
                        dropoff_city,
                        distance_km,
                        duration_min,
                        price,
                        status,
                        payment_method,
                        timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """

                cur.execute(
                    insert_query,
                    (
                        trip["trip_id"],
                        trip["driver_id"],
                        trip["rider_id"],
                        trip["pickup_city"],
                        trip["dropoff_city"],
                        trip["distance_km"],
                        trip["duration_min"],
                        trip["price"],
                        trip["status"],
                        trip["payment_method"],
                        trip["timestamp"],
                    ),
                )

                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip "
                    f"{trip['trip_id']} | {trip['pickup_city']} → {trip['dropoff_city']} "
                    f"| ${trip['price']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
