"""
BƯỚC 2: Kafka Consumer
Đọc message từ Kafka và ghi vào MinIO (Bronze zone) dạng JSON Lines.
Chạy: python kafka/consumer.py
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
from botocore.client import Config
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Kafka config ─────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["airbnb.listings.raw", "airbnb.reviews.raw"]
GROUP_ID = "bronze-writer-group"

# ── MinIO config ─────────────────────────────────────
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_BRONZE = "bronze"

# Ghi file mỗi N messages (micro-batch)
FLUSH_EVERY = 5_000


def get_s3_client():
    """Kết nối MinIO qua boto3 (S3-compatible)."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_to_bronze(s3, topic: str, records: list[dict], batch_num: int):
    """
    Upload một batch records lên MinIO bronze zone.
    Path: bronze/{topic}/year=YYYY/month=MM/day=DD/batch_{N}.jsonl
    """
    now = datetime.utcnow()
    # Partition theo ngày (Hive-style partitioning)
    s3_key = (
        f"{topic}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{batch_num:06d}.jsonl"
    )

    # Ghi JSON Lines (1 record/dòng)
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    s3.put_object(
        Bucket=BUCKET_BRONZE,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    log.info(f"  ✅ Uploaded {len(records):,} records → s3://{BUCKET_BRONZE}/{s3_key}")


def run_consumer():
    """Main consumer loop."""
    s3 = get_s3_client()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",   # đọc từ đầu nếu chưa có offset
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=30_000,      # dừng sau 30s không có message
    )

    log.info(f"Consumer started. Listening to topics: {TOPICS}")

    # Buffer riêng cho mỗi topic
    buffers: dict[str, list] = {t: [] for t in TOPICS}
    batch_counters: dict[str, int] = {t: 0 for t in TOPICS}

    try:
        for message in consumer:
            topic = message.topic
            buffers[topic].append(message.value)

            # Flush khi đủ batch
            if len(buffers[topic]) >= FLUSH_EVERY:
                batch_counters[topic] += 1
                upload_to_bronze(s3, topic, buffers[topic], batch_counters[topic])
                buffers[topic].clear()

    except Exception as e:
        log.error(f"Consumer error: {e}")

    finally:
        # Flush remaining records
        for topic, records in buffers.items():
            if records:
                batch_counters[topic] += 1
                upload_to_bronze(s3, topic, records, batch_counters[topic])
                log.info(f"Flushed remaining {len(records)} records for {topic}")

        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    run_consumer()
