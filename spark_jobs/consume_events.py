"""Reads weather and air-pollution events from Kafka and writes each as Parquet to GCS."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather-events")
AIRPOLLUTION_TOPIC = os.environ.get("AIRPOLLUTION_TOPIC", "air-pollution-events")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GCS_BUCKET = os.environ["GCS_BUCKET"]

spark = SparkSession.builder.appName("weather-events-consumer").getOrCreate()


def read_topic(topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(col("topic"), col("timestamp"), col("value").cast("string").alias("json_str"))
    )


weather_df = read_topic(WEATHER_TOPIC)
airpollution_df = read_topic(AIRPOLLUTION_TOPIC)

weather_query = (
    weather_df.writeStream.format("parquet")
    .outputMode("append")
    .option("path", f"gs://{GCS_BUCKET}/bronze/weather/")
    .option("checkpointLocation", "/opt/spark_jobs/checkpoints/weather")
    .trigger(processingTime="5 minutes")
    .start()
)

airpollution_query = (
    airpollution_df.writeStream.format("parquet")
    .outputMode("append")
    .option("path", f"gs://{GCS_BUCKET}/bronze/air-pollution/")
    .option("checkpointLocation", "/opt/spark_jobs/checkpoints/airpollution")
    .trigger(processingTime="5 minutes")
    .start()
)

spark.streams.awaitAnyTermination()
