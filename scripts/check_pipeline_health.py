#!/usr/bin/env python3
"""Checks each stage of the pipeline actually moved data recently: producer -> Kafka -> bronze -> silver.

Checks GCS output timestamps directly rather than trusting checkpoint state --
a Spark checkpoint can report itself "caught up" while silently writing
nothing (this happened for real: a stale _spark_metadata sink log caused
consume_events.py to skip every batch without erroring).

Runnable standalone (python scripts/check_pipeline_health.py) or from Airflow.
Pass --stage {producers,kafka,bronze,silver} to check just one stage (used by
the pipeline_health_check DAG to run stages as separate, ordered tasks).
Exits 0 if every checked stage is healthy, 1 otherwise.
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import docker
from google.cloud import storage
from kafka import KafkaConsumer, TopicPartition

GCS_BUCKET = os.environ["GCS_BUCKET"]
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

POLL_INTERVAL_SECONDS = 900  # matches producer POLL_INTERVAL_SECONDS in docker-compose.yml
PRODUCER_FRESHNESS = timedelta(seconds=POLL_INTERVAL_SECONDS * 2)  # allow one missed cycle
GCS_FRESHNESS = timedelta(minutes=30)  # producer interval + trigger interval + buffer

TOPICS = ["weather-events", "air-pollution-events"]
PRODUCERS = {"weather-producer": "weather", "airpollution-producer": "air-pollution"}

results = []


def report(stage, ok, detail):
    results.append((stage, ok, detail))
    status = "OK" if ok else "FAIL"
    print(f"{status:5} {stage:28} {detail}")


def check_producers(client):
    for container_name, label in PRODUCERS.items():
        try:
            container = client.containers.get(container_name)
            container.reload()
            if container.status != "running":
                report(f"producer({label})", False, f"container status={container.status}")
                continue
            since = datetime.now(timezone.utc) - PRODUCER_FRESHNESS
            logs = container.logs(since=since, timestamps=False).decode("utf-8", errors="replace")
            if "Published" in logs:
                report(f"producer({label})", True, f"published within last {PRODUCER_FRESHNESS}")
            else:
                report(f"producer({label})", False, f"no publish log within last {PRODUCER_FRESHNESS}")
        except docker.errors.NotFound:
            report(f"producer({label})", False, "container not found")


def check_kafka():
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, consumer_timeout_ms=5000)
        topics = consumer.topics()
        for topic in TOPICS:
            if topic not in topics:
                report(f"kafka({topic})", False, "topic not found")
                continue
            partitions = consumer.partitions_for_topic(topic)
            end_offsets = consumer.end_offsets([TopicPartition(topic, p) for p in partitions])
            total = sum(end_offsets.values())
            report(f"kafka({topic})", total > 0, f"latest offset total={total}")
        consumer.close()
    except Exception as e:
        for topic in TOPICS:
            report(f"kafka({topic})", False, f"broker unreachable: {e}")


def check_spark_container(client, container_name, label):
    try:
        container = client.containers.get(container_name)
        container.reload()
        if container.status != "running":
            report(label, False, f"container status={container.status}")
            return False
        return True
    except docker.errors.NotFound:
        report(label, False, "container not found")
        return False


def check_gcs_freshness(gcs_client, prefix, stage):
    bucket = gcs_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)
    latest = None
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        if latest is None or blob.updated > latest:
            latest = blob.updated
    if latest is None:
        report(stage, False, f"no parquet files found under gs://{GCS_BUCKET}/{prefix}")
        return
    age = datetime.now(timezone.utc) - latest
    ok = age <= GCS_FRESHNESS
    report(stage, ok, f"latest file {latest.isoformat()} (age {age})")


def run_producers():
    check_producers(docker.from_env())


def run_kafka():
    check_kafka()


def run_bronze():
    docker_client = docker.from_env()
    gcs_client = storage.Client()
    if check_spark_container(docker_client, "spark-consumer", "bronze(container)"):
        check_gcs_freshness(gcs_client, "bronze/weather/", "bronze(weather)")
        check_gcs_freshness(gcs_client, "bronze/air-pollution/", "bronze(air-pollution)")


def run_silver():
    docker_client = docker.from_env()
    gcs_client = storage.Client()
    if check_spark_container(docker_client, "spark-transform", "silver(container)"):
        check_gcs_freshness(gcs_client, "silver/weather/", "silver(weather)")
        check_gcs_freshness(gcs_client, "silver/air-pollution/", "silver(air-pollution)")


STAGES = {
    "producers": run_producers,
    "kafka": run_kafka,
    "bronze": run_bronze,
    "silver": run_silver,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=STAGES.keys(), help="Check only this stage; default checks all.")
    args = parser.parse_args()

    if args.stage:
        STAGES[args.stage]()
    else:
        for stage_fn in STAGES.values():
            stage_fn()

    failures = [r for r in results if not r[1]]
    print()
    if failures:
        print(f"{len(failures)} stage(s) unhealthy.")
        sys.exit(1)
    print("All stages healthy.")
    sys.exit(0)


if __name__ == "__main__":
    main()
