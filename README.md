# weather-streaming-data-pipeline

A personal learning project for practicing streaming data engineering: Kafka → Spark Structured Streaming → GCS (bronze/silver) → BigQuery, supervised by Airflow.

See [PIPELINE.md](PIPELINE.md) for the full architecture, schemas, and design decisions.

## Prerequisites

- Docker and Docker Compose
- A GCS bucket, with a service account key granting `roles/storage.objectAdmin` on that bucket, saved at `credentials/weather-pipeline-spark-key.json`
- An OpenWeatherMap API key, saved at `credentials/openweathermap_api_key.txt` (plain text, key only)

All credentials live under `credentials/` (gitignored) — mounted read-only into whichever container needs them, rather than passed as environment variables.

## Setup

1. Copy `.env.example` to `.env` — this holds non-secret config (lat/lon):
   ```bash
   cp .env.example .env
   ```
2. Create `credentials/openweathermap_api_key.txt` containing your OpenWeatherMap API key, and `credentials/weather-pipeline-spark-key.json` with your GCS service account key.
3. Update `GCS_BUCKET` in `docker-compose.yml` (both `spark` and `spark-transform` services) to your bucket name.

## Running

```bash
docker compose up -d
```

This starts:

| Service | Purpose | UI |
|---|---|---|
| `kafka` | Message broker | — |
| `kafka-ui` | Browse topics/messages | http://localhost:8080 |
| `producer`, `producer-airpollution` | Poll OpenWeatherMap, publish to Kafka | — |
| `spark` (bronze) | Kafka → raw Parquet in GCS | http://localhost:4040 |
| `spark-transform` (silver) | Bronze → typed Parquet in GCS | http://localhost:4041 |
| `airflow` | Health-checks and restarts the two Spark containers | http://localhost:8081 |

**One-time only**, before starting `spark-transform` for the first time (or after any bronze checkpoint reset): run `initial_load.py` to backfill and deduplicate bronze into silver. See the "Running it" section in [PIPELINE.md](PIPELINE.md) for the exact command, then set `CUTOFF_TIMESTAMP` in `docker-compose.yml` to the UTC time it finished.

## Querying

Once silver data exists in GCS, create BigQuery external tables over it (see [PIPELINE.md](PIPELINE.md#5-bigquery)), then query with:

```bash
bq query --use_legacy_sql=false 'SELECT * FROM `<project>.silver.weather_data` LIMIT 10'
```
