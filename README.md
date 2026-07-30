# weather-streaming-data-pipeline

A personal learning project for practicing streaming data engineering: Kafka → Spark Structured Streaming → GCS (bronze/silver) → BigQuery, with Airflow verifying data actually flows end-to-end.

See [PIPELINE.md](PIPELINE.md) for the full architecture, schemas, and design decisions.

## Prerequisites

- Docker and Docker Compose
- A GCP project with a GCS bucket, and a service account key granting `roles/storage.objectAdmin` on that bucket, saved at `credentials/gcs-service-account-key.json`
- An OpenWeatherMap API key, saved at `credentials/openweathermap_api_key.txt` (plain text, key only)
- The `bq` CLI (part of the Google Cloud SDK), authenticated against your GCP project, if you want the BigQuery querying layer

All credentials live under `credentials/` (gitignored) — mounted read-only into whichever container needs them, rather than passed as environment variables.

## Setup

1. Copy `.env.example` to `.env` and fill in your own values:
   ```bash
   cp .env.example .env
   ```
   - `GCP_PROJECT_ID` — your GCP project ID
   - `GCS_BUCKET` — your GCS bucket name
   - `WEATHER_LAT` / `WEATHER_LON` — the coordinates you want to track (no default — the pipeline won't start without these set)
2. Create `credentials/openweathermap_api_key.txt` containing your OpenWeatherMap API key, and `credentials/gcs-service-account-key.json` with your GCS service account key.

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
| `airflow` | Verifies producers/Kafka/bronze/silver are actually moving data | http://localhost:8081 |

**One-time only**, before starting `spark-transform` for the first time (or after any bronze checkpoint reset): run `initial_load.py` to backfill and deduplicate bronze into silver. See the "Running it" section in [PIPELINE.md](PIPELINE.md) for the exact command, then set `CUTOFF_TIMESTAMP` in `docker-compose.yml` to the UTC time it finished.

## Querying

Once silver data exists in GCS, create the BigQuery dataset and external tables:

```bash
./scripts/setup_bigquery.sh
```

Then query with:

```bash
bq query --use_legacy_sql=false 'SELECT * FROM `'"$GCP_PROJECT_ID"'.silver.weather_data` LIMIT 10'
```
