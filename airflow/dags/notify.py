"""Telegram alerting for Airflow task failures."""

import os

import requests

TELEGRAM_TOKEN_FILE = "/opt/credentials/telegram_bot_token.txt"


def notify_telegram_failure(context):
    with open(TELEGRAM_TOKEN_FILE) as f:
        token = f.read().strip()
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    task_instance = context["task_instance"]
    message = (
        f"Pipeline health check failed\n"
        f"DAG: {context['dag'].dag_id}\n"
        f"Task: {task_instance.task_id}\n"
        f"Execution time: {context['execution_date']}"
    )

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message},
        timeout=10,
    )
