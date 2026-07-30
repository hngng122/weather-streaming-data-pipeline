"""Reads bronze layer Parquet from GCS, parses the raw JSON into typed columns, and writes the silver layer."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, timestamp_seconds, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GCS_BUCKET = os.environ["GCS_BUCKET"]
# Only pick up bronze rows with timestamp after the one-time initial_load.py
# batch job already backfilled silver, so this incremental job never
# reprocesses what that job already handled. Row-level filter, not a file
# source option, since `modifiedAfter` isn't supported for streaming sources.
CUTOFF_TIMESTAMP = os.environ["CUTOFF_TIMESTAMP"]

spark = SparkSession.builder.appName("bronze-to-silver-transform").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("json_str", StringType()),
    ]
)

WEATHER_JSON_SCHEMA = StructType(
    [
        StructField("coord", StructType([StructField("lon", DoubleType()), StructField("lat", DoubleType())])),
        StructField(
            "weather",
            ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType()),
                        StructField("main", StringType()),
                        StructField("description", StringType()),
                        StructField("icon", StringType()),
                    ]
                )
            ),
        ),
        StructField("base", StringType()),
        StructField(
            "main",
            StructType(
                [
                    StructField("temp", DoubleType()),
                    StructField("feels_like", DoubleType()),
                    StructField("temp_min", DoubleType()),
                    StructField("temp_max", DoubleType()),
                    StructField("pressure", IntegerType()),
                    StructField("humidity", IntegerType()),
                    StructField("sea_level", IntegerType()),
                    StructField("grnd_level", IntegerType()),
                ]
            ),
        ),
        StructField("visibility", IntegerType()),
        StructField(
            "wind",
            StructType(
                [
                    StructField("speed", DoubleType()),
                    StructField("deg", IntegerType()),
                    StructField("gust", DoubleType()),
                ]
            ),
        ),
        StructField("rain", StructType([StructField("1h", DoubleType())])),
        StructField("clouds", StructType([StructField("all", IntegerType())])),
        StructField("dt", LongType()),
        StructField(
            "sys",
            StructType(
                [
                    StructField("country", StringType()),
                    StructField("sunrise", LongType()),
                    StructField("sunset", LongType()),
                ]
            ),
        ),
        StructField("timezone", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("cod", IntegerType()),
    ]
)

AIRPOLLUTION_JSON_SCHEMA = StructType(
    [
        StructField("coord", StructType([StructField("lon", DoubleType()), StructField("lat", DoubleType())])),
        StructField(
            "list",
            ArrayType(
                StructType(
                    [
                        StructField("dt", LongType()),
                        StructField("main", StructType([StructField("aqi", IntegerType())])),
                        StructField(
                            "components",
                            StructType(
                                [
                                    StructField("co", DoubleType()),
                                    StructField("no", DoubleType()),
                                    StructField("no2", DoubleType()),
                                    StructField("o3", DoubleType()),
                                    StructField("so2", DoubleType()),
                                    StructField("pm2_5", DoubleType()),
                                    StructField("pm10", DoubleType()),
                                    StructField("nh3", DoubleType()),
                                ]
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)


def read_bronze(path):
    # Glob pattern, not the bare directory: avoids Spark's `_spark_metadata`
    # auto-detection, which was left in an inconsistent state by manual file
    # operations on this bucket and previously crashed both readers and the
    # bronze writer itself.
    # Row-level cutoff filter (not a file source option -- `modifiedAfter`
    # isn't supported for streaming sources) so this incremental job never
    # reprocesses what initial_load.py already backfilled into silver.
    return (
        spark.readStream.format("parquet")
        .schema(BRONZE_SCHEMA)
        .load(f"{path}*.parquet")
        .filter(col("timestamp") > to_timestamp(lit(CUTOFF_TIMESTAMP)))
    )


bronze_weather = read_bronze(f"gs://{GCS_BUCKET}/bronze/weather/")
bronze_airpollution = read_bronze(f"gs://{GCS_BUCKET}/bronze/air-pollution/")

silver_weather = (
    bronze_weather.withColumn("data", from_json(col("json_str"), WEATHER_JSON_SCHEMA))
    .select(
        col("timestamp"),
        timestamp_seconds(col("data.dt")).alias("event_time"),
        col("data.coord.lat").alias("lat"),
        col("data.coord.lon").alias("lon"),
        col("data.weather")[0]["id"].alias("condition_id"),
        col("data.weather")[0]["main"].alias("condition_main"),
        col("data.weather")[0]["description"].alias("condition_description"),
        col("data.weather")[0]["icon"].alias("condition_icon"),
        col("data.base").alias("base"),
        col("data.main.temp").alias("temp"),
        col("data.main.feels_like").alias("feels_like"),
        col("data.main.temp_min").alias("temp_min"),
        col("data.main.temp_max").alias("temp_max"),
        col("data.main.pressure").alias("pressure"),
        col("data.main.humidity").alias("humidity"),
        col("data.main.sea_level").alias("sea_level_pressure"),
        col("data.main.grnd_level").alias("ground_level_pressure"),
        col("data.visibility").alias("visibility"),
        col("data.wind.speed").alias("wind_speed"),
        col("data.wind.deg").alias("wind_deg"),
        col("data.wind.gust").alias("wind_gust"),
        col("data.rain.1h").alias("rain_1h"),
        col("data.clouds.all").alias("cloudiness"),
        col("data.sys.country").alias("country"),
        timestamp_seconds(col("data.sys.sunrise")).alias("sunrise"),
        timestamp_seconds(col("data.sys.sunset")).alias("sunset"),
        col("data.timezone").alias("timezone_offset_sec"),
        col("data.id").alias("city_id"),
        col("data.name").alias("city_name"),
        col("data.cod").alias("response_code"),
    )
)

silver_airpollution = (
    bronze_airpollution.withColumn("data", from_json(col("json_str"), AIRPOLLUTION_JSON_SCHEMA))
    .select(
        col("timestamp"),
        timestamp_seconds(col("data.list")[0]["dt"]).alias("event_time"),
        col("data.coord.lat").alias("lat"),
        col("data.coord.lon").alias("lon"),
        col("data.list")[0]["main"]["aqi"].alias("aqi"),
        col("data.list")[0]["components"]["co"].alias("co"),
        col("data.list")[0]["components"]["no"].alias("no"),
        col("data.list")[0]["components"]["no2"].alias("no2"),
        col("data.list")[0]["components"]["o3"].alias("o3"),
        col("data.list")[0]["components"]["so2"].alias("so2"),
        col("data.list")[0]["components"]["pm2_5"].alias("pm2_5"),
        col("data.list")[0]["components"]["pm10"].alias("pm10"),
        col("data.list")[0]["components"]["nh3"].alias("nh3"),
    )
)

weather_query = (
    silver_weather.writeStream.format("parquet")
    .outputMode("append")
    .option("path", f"gs://{GCS_BUCKET}/silver/weather/")
    .option("checkpointLocation", "/opt/spark_jobs/checkpoints/silver_weather")
    .trigger(processingTime="5 minutes")
    .start()
)

airpollution_query = (
    silver_airpollution.writeStream.format("parquet")
    .outputMode("append")
    .option("path", f"gs://{GCS_BUCKET}/silver/air-pollution/")
    .option("checkpointLocation", "/opt/spark_jobs/checkpoints/silver_airpollution")
    .trigger(processingTime="5 minutes")
    .start()
)

spark.streams.awaitAnyTermination()
