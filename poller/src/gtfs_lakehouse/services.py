"""Local service clients. Credentials can be overridden through the environment."""

import os

import boto3
from botocore.config import Config
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    "gtfs.raw.snapshots",
    "gtfs.normalized.events",
    "gtfs.schedule.versions",
    "gtfs.route.metrics",
    "gtfs.dead_letter",
    "gtfs.late.events",
]


def kafka_address():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "lakehouse"),
        aws_secret_access_key=os.getenv(
            "AWS_SECRET_ACCESS_KEY", "local-lakehouse-secret"
        ),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def bootstrap():
    client = s3()
    existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
    for bucket in ("raw", "warehouse", "checkpoints"):
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
    admin = AdminClient({"bootstrap.servers": kafka_address()})
    existing_topics = admin.list_topics(timeout=20).topics
    requests = [
        NewTopic(
            topic,
            num_partitions=3,
            replication_factor=1,
            config={"cleanup.policy": "compact"}
            if topic == "gtfs.schedule.versions"
            else {},
        )
        for topic in TOPICS
        if topic not in existing_topics
    ]
    for future in admin.create_topics(requests).values() if requests else []:
        future.result(timeout=30)
