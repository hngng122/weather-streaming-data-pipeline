"""Polls the OpenWeatherMap current-weather API and publishes each reading to Kafka."""

import json
import os
import time

import requests
from kafka import KafkaProducer

API_KEY = open("/opt/credentials/openweathermap_api_key.txt").read().strip()
LAT = os.environ["WEATHER_LAT"]
LON = os.environ["WEATHER_LON"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
POLL_INTERVAL_SECONDS = int(os.environ["POLL_INTERVAL_SECONDS"])

API_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather():
    response = requests.get(
        API_URL,
        params={"lat": LAT, "lon": LON, "appid": API_KEY},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        payload = fetch_weather()
        producer.send(KAFKA_TOPIC, value=payload)
        producer.flush()
        print(f"Published weather reading for lat={LAT}, lon={LON}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
