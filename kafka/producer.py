"""
BƯỚC 1: Kafka Producer
Đọc file CSV và publish từng record lên Kafka topic.
Chạy: python kafka/producer.py
"""

import csv
import json
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
BATCH_SIZE = 500          # Gửi 500 records/lần
SLEEP_BETWEEN_BATCH = 0.1 # 100ms giữa các batch (giả lập streaming)

def make_producer() -> KafkaProducer:
    """Tạo Kafka producer với JSON serializer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        # Đảm bảo message không bị mất khi broker chưa confirm
        acks="all",
        retries=3,
    )


def publish_csv(producer: KafkaProducer, filepath: str, topic: str, key_field: str):
    """
    Đọc CSV và publish lên Kafka.
    key_field: dùng làm Kafka message key (để Kafka partition đúng).
    """
    log.info(f"Publishing {filepath} → topic '{topic}'")
    sent = 0
    errors = 0

    with open(filepath, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch = []

        for row in reader:
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                for record in batch:
                    try:
                        producer.send(
                            topic=topic,
                            key=record.get(key_field, "unknown"),
                            value=record,
                        )
                        sent += 1
                    except Exception as e:
                        log.warning(f"Failed to send record: {e}")
                        errors += 1

                producer.flush()
                log.info(f"  Sent {sent:,} records to '{topic}' (errors: {errors})")
                batch.clear()
                time.sleep(SLEEP_BETWEEN_BATCH)

        # Gửi batch cuối còn dư
        for record in batch:
            producer.send(
                topic=topic,
                key=record.get(key_field, "unknown"),
                value=record,
            )
            sent += 1

    producer.flush()
    log.info(f"✅ Done! Total sent to '{topic}': {sent:,} records, errors: {errors}")
    return sent


if __name__ == "__main__":
    import os

    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

    producer = make_producer()

    try:
        # Publish Listings
        publish_csv(
            producer=producer,
            filepath=os.path.join(DATA_DIR, "Listings.csv"),
            topic="airbnb.listings.raw",
            key_field="listing_id",
        )

        # Publish Reviews
        publish_csv(
            producer=producer,
            filepath=os.path.join(DATA_DIR, "Reviews.csv"),
            topic="airbnb.reviews.raw",
            key_field="review_id",
        )

    finally:
        producer.close()
        log.info("Producer closed.")
