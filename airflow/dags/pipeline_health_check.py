"""Runs scripts/check_pipeline_health.py on a schedule to catch silent pipeline failures.

One task per pipeline stage, chained in the order data actually flows
(producers -> Kafka -> bronze -> silver) so a failure points at exactly
where the pipeline broke instead of reporting one opaque pass/fail.

Detection only -- does not restart anything. A failed task surfaces in the
Airflow UI; investigate and fix manually, since automated remediation isn't
safe for every failure mode this catches (e.g. a stale _spark_metadata sink
log needs a scoped, verified deletion, not a blind retry).
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from notify import notify_telegram_failure

SCRIPT = "python /opt/airflow/scripts/check_pipeline_health.py --stage"

with DAG(
    dag_id="pipeline_health_check",
    schedule=timedelta(minutes=15),
    start_date=pendulum.datetime(2026, 7, 30, tz="UTC"),
    catchup=False,
    tags=["weather-pipeline"],
    default_args={"on_failure_callback": notify_telegram_failure},
) as dag:
    check_producers = BashOperator(task_id="check_producers", bash_command=f"{SCRIPT} producers")
    check_kafka = BashOperator(task_id="check_kafka", bash_command=f"{SCRIPT} kafka")
    check_bronze = BashOperator(task_id="check_bronze", bash_command=f"{SCRIPT} bronze")
    check_silver = BashOperator(task_id="check_silver", bash_command=f"{SCRIPT} silver")

    check_producers >> check_kafka >> check_bronze >> check_silver
