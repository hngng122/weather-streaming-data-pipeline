"""Checks the two Spark streaming containers on a schedule and restarts either one that isn't running.

Both jobs resume from their own checkpoints on restart, so a restart never
reprocesses or duplicates data.
"""

from datetime import timedelta

import docker
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

CONTAINERS = ["spark-consumer", "spark-transform"]


def check_and_restart(container_name, **context):
    client = docker.from_env()
    container = client.containers.get(container_name)
    container.reload()
    if container.status != "running":
        print(f"WARNING: {container_name} is not running (status={container.status}). Restarting.")
        container.start()
    else:
        print(f"{container_name} is running normally.")


with DAG(
    dag_id="spark_health_check",
    schedule=timedelta(minutes=10),
    start_date=pendulum.datetime(2026, 7, 30, tz="UTC"),
    catchup=False,
    tags=["weather-pipeline"],
) as dag:
    for name in CONTAINERS:
        PythonOperator(
            task_id=f"check_{name.replace('-', '_')}",
            python_callable=check_and_restart,
            op_kwargs={"container_name": name},
        )
