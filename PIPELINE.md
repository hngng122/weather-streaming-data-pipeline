# Weather Data Pipeline

A personal learning project for practicing streaming data engineering: two producers publish weather and air-quality readings to Kafka, Spark Structured Streaming ingests and transforms them into a GCS-based bronze/silver medallion layout, BigQuery exposes the silver layer as queryable external tables, and Airflow supervises the two long-running Spark jobs.

## Architecture

```
OpenWeatherMap current-weather API ──▶ produce_weather.py ──┐
                                                              ├──▶ Kafka (weather-events, air-pollution-events)
OpenWeatherMap air-pollution API   ──▶ produce_airpollution.py ┘
                                                              │
                                                              ▼
                                              consume_events.py (bronze, streaming)
                                                              │
                                     gs://<bucket>/bronze/weather/, bronze/air-pollution/  (raw JSON, Parquet-wrapped)
                                                              │
                          ┌───────────────────────────────────┴───────────────────────────────────┐
                          ▼ (one-time)                                                             ▼ (ongoing)
                  initial_load.py                                                        transform_events.py
              (batch backfill + dedup)                                              (streaming, cutoff-filtered)
                          │                                                                        │
                          └───────────────────────────────────┬────────────────────────────────────┘
                                                              ▼
                                gs://<bucket>/silver/weather/, silver/air-pollution/  (typed, parsed columns)
                                                              │
                                                              ▼
                        BigQuery external tables: silver.weather_data, silver.air_pollution_data
                                                              │
                                                              ▼
                                                       SQL queries (bq / console)

Airflow (spark_health_check DAG, every 10 min) supervises consume_events.py and
transform_events.py, restarting either container if it's found stopped.
```

## Components

### 1. Producers (`producer/`)

Two long-running Python scripts, each in its own container (built from the same `producer/Dockerfile`, only the `command`/env differ):

| Script | Source API | Kafka topic |
|---|---|---|
| `produce_weather.py` | `api.openweathermap.org/data/2.5/weather` | `weather-events` |
| `produce_airpollution.py` | `api.openweathermap.org/data/2.5/air_pollution` | `air-pollution-events` |

Each polls its API every `POLL_INTERVAL_SECONDS` (currently 900s / 15 min), publishes the raw JSON response as-is to its topic via `kafka-python`, and loops forever. All config (API key, lat/lon, Kafka address, topic, poll interval) comes from environment variables set in `docker-compose.yml` — no defaults in code, since these only ever run inside Docker.

### 2. Kafka

Single-broker KRaft-mode cluster (`apache/kafka:3.8.0`, no Zookeeper), with dual listeners: `kafka:9092` for other containers, `localhost:29092` for host access. Two topics, default retention (`cleanup.policy=delete`, `retention.ms=604800000` — 7 days), kept deliberately at this default rather than tuned. **`kafka-ui`** (port 8080) gives a browser view of topics/messages for inspection.

**Retention decision:** the risk retention protects against here is the whole laptop/Docker stack being off for a stretch (Airflow's health-check only restarts a crashed *container* while Docker keeps running — it can't help if the host itself is down). Message volume is tiny (2 topics, one message each per 15 min), so retention length costs almost nothing in disk regardless of value. 7 days already covers a full week of downtime, comfortably more than a realistic gap for this project, so it was kept as-is rather than tuned.

### 3. Bronze layer — `spark_jobs/consume_events.py`

A single Spark Structured Streaming job (container `spark-consumer`) that reads **both** topics, each via its own independent `readStream` (reading them via one combined subscribe + `.filter()` split was tried and rejected — it caused empty output files because Spark inherits partitions from both topics into both filtered streams). Each stream writes raw `(topic, timestamp, json_str)` rows straight to Parquet in GCS, unparsed, with its own checkpoint:

- `gs://<bucket>/bronze/weather/` (checkpoint: `checkpoints/weather`)
- `gs://<bucket>/bronze/air-pollution/` (checkpoint: `checkpoints/airpollution`)

`timestamp` here is Kafka's own message timestamp (set at publish time) — this becomes the pipeline's dedup key downstream. Trigger: `processingTime="5 minutes"` (a deliberate choice to practice micro-batch streaming rather than batch/`AvailableNow` semantics).

### 4. Silver layer — parsing bronze JSON into typed tables

Two jobs share the same parsing logic (explicit `StructType` schemas for both APIs' JSON shapes) but serve different purposes:

**`initial_load.py`** — one-time **batch** job (`spark.read`, not streaming). Reads all of bronze, parses, applies `.dropDuplicates(["timestamp"])`, `coalesce()`s output, writes to silver with `mode("overwrite")`. Run once manually via a one-off `docker run` (not in `docker-compose.yml`) — never re-run unless bronze checkpoints are ever reset and reprocessed.

**`transform_events.py`** — the ongoing **streaming** job (container `spark-transform`). Reads bronze the same way, but filters to only rows with `timestamp` after a fixed `CUTOFF_TIMESTAMP` env var (set to the moment `initial_load.py` finished), and does **no deduplication** — bronze going forward is clean, so none is needed. Writes to silver with its own checkpoints (`checkpoints/silver_weather`, `checkpoints/silver_airpollution`), 5-minute trigger.

Why split this way: `.dropDuplicates()` requires a shuffle, and Spark's default `spark.sql.shuffle.partitions=200` blew up a ~93-row batch into 78 mostly-empty output files. Doing the expensive dedup once in a batch job (where shuffle cost doesn't matter) and keeping the continuous streaming job shuffle-free (cutoff filter instead) avoids that entirely.

Both silver outputs:
- `gs://<bucket>/silver/weather/` — see [schema](#silverweather_data-schema) below
- `gs://<bucket>/silver/air-pollution/` — see [schema](#silverair_pollution_data-schema) below

Both read bronze via a glob (`f"{path}*.parquet"`), not the bare directory — this bypasses Spark's `_spark_metadata`-based consistency checks, which were left in an inconsistent state by an earlier manual file cleanup and had previously crashed both a reader and the bronze writer itself.

#### `silver.weather_data` schema

| Column | Type | Source JSON path |
|---|---|---|
| timestamp | timestamp | Kafka ingestion time |
| event_time | timestamp | `dt` |
| lat, lon | double | `coord.lat`, `coord.lon` |
| condition_id | int | `weather[0].id` |
| condition_main | string | `weather[0].main` |
| condition_description | string | `weather[0].description` |
| condition_icon | string | `weather[0].icon` |
| base | string | `base` |
| temp, feels_like, temp_min, temp_max | double | `main.*` |
| pressure, humidity | int | `main.*` |
| sea_level_pressure, ground_level_pressure | int | `main.sea_level`, `main.grnd_level` |
| visibility | int | `visibility` |
| wind_speed, wind_gust | double | `wind.*` |
| wind_deg | int | `wind.deg` |
| rain_1h | double, nullable | `rain.1h` (only present when raining) |
| cloudiness | int | `clouds.all` |
| country | string | `sys.country` |
| sunrise, sunset | timestamp | `sys.sunrise`, `sys.sunset` |
| timezone_offset_sec | int | `timezone` |
| city_id | int | `id` |
| city_name | string | `name` |
| response_code | int | `cod` |

#### `silver.air_pollution_data` schema

| Column | Type | Source JSON path |
|---|---|---|
| timestamp | timestamp | Kafka ingestion time |
| event_time | timestamp | `list[0].dt` |
| lat, lon | double | `coord.lat`, `coord.lon` |
| aqi | int | `list[0].main.aqi` |
| co, no, no2, o3, so2, pm2_5, pm10, nh3 | double | `list[0].components.*` |

`timestamp` vs `event_time`: `timestamp` is when the pipeline ingested the reading (Kafka's message timestamp, stable across any reprocessing — hence used as the dedup key); `event_time` is when OpenWeatherMap says the observation itself was taken. They're usually seconds apart given how frequently we poll.

Two separate tables (not one combined wide table) by design — no column-prefix ambiguity needed since each is topic-specific.

### 5. BigQuery

Pure SQL query engine, no data actually stored in BigQuery — `silver.weather_data` and `silver.air_pollution_data` are **external tables** defined directly over the GCS Parquet files. Created via `scripts/setup_bigquery.sh` (reads `GCP_PROJECT_ID`/`GCS_BUCKET` from `.env`), not by hand — see [README.md](README.md#querying). Querying them reads straight from GCS at query time (schema-on-read); no separate load/ETL step into BigQuery storage.

### 6. Airflow — operational supervision

Airflow runs in standalone mode (single container, SQLite metadata DB — appropriate for a personal project, not production) with one DAG: `spark_health_check` (`airflow/dags/spark_health_check.py`), scheduled every 10 minutes.

It does **not** orchestrate the streaming logic itself — `consume_events.py` and `transform_events.py` are long-running streaming queries that run continuously on their own via `docker-compose up`, with no scheduler needed for them to function. Airflow's job is purely supervisory: each run checks whether the `spark-consumer` and `spark-transform` containers are `running` via the Docker SDK (container access via a mounted `/var/run/docker.sock`), and restarts either one it finds stopped. Because both jobs resume from their own checkpoints, a restart never reprocesses or duplicates data.

UI: `localhost:8081`. DAGs are paused by default when first deployed — must be explicitly unpaused for the schedule to run.

## Deduplication — design history

A real duplication incident (checkpoint reset caused ~72–96 events to be reprocessed from `earliest`) drove the final design:

1. Considered watermark-based dedup — rejected as unnecessarily complex for this data volume.
2. Considered dedup-on-every-batch inside the streaming job — worked, but the shuffle cost exploded file counts (see above).
3. **Final design ("Option B")**: dedup once, in a batch job (`initial_load.py`), then rely on `transform_events.py`'s cutoff-timestamp filter to guarantee the ongoing stream never touches already-deduped data. No dedup logic needed in the streaming path at all.

Gotcha hit along the way: `.option("modifiedAfter", ...)` — the natural way to express "only files newer than X" — is a **batch-only** file source option, not supported for `readStream`. The cutoff had to be a row-level `.filter(col("timestamp") > to_timestamp(lit(CUTOFF_TIMESTAMP)))` instead.

## Running it

```bash
docker compose up -d              # everything except initial_load.py (which is one-off, see below)
```

Services: `kafka`, `producer`, `producer-airpollution`, `spark` (bronze), `spark-transform` (silver), `kafka-ui` (:8080), `airflow` (:8081).

`initial_load.py` is intentionally **not** a compose service — run it manually, once, after bronze has some data and before starting `spark-transform` for the first time (or after any bronze checkpoint reset). Export `.env` into your shell first so `GCS_BUCKET` is available to `docker run`:

```bash
set -a && source .env && set +a
```

```bash
docker run --rm --user root \
  -v "$(pwd)/spark_jobs:/opt/spark_jobs" \
  -v "$(pwd)/credentials:/opt/credentials:ro" \
  -e GCS_BUCKET \
  apache/spark-py:v3.4.0 \
  /opt/spark/bin/spark-submit \
  --jars /opt/spark_jobs/jars/gcs-connector-hadoop3-latest.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/credentials/gcs-service-account-key.json \
  /opt/spark_jobs/initial_load.py
```

After it finishes, set `CUTOFF_TIMESTAMP` in `docker-compose.yml` (the `spark-transform` service) to the UTC time it completed, then start `spark-transform`.
