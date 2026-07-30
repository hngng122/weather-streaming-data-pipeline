#!/usr/bin/env bash
# Creates the BigQuery silver dataset and external tables over the GCS silver layer.
# Reads GCP_PROJECT_ID and GCS_BUCKET from .env (in the repo root) unless already exported.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  source "${REPO_ROOT}/.env"
  set +a
fi

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID in .env or the environment}"
: "${GCS_BUCKET:?Set GCS_BUCKET in .env or the environment}"

bq mk --dataset --location=US "${GCP_PROJECT_ID}:silver" 2>/dev/null || echo "Dataset silver already exists, skipping."

bq mk --external_table_definition="PARQUET=gs://${GCS_BUCKET}/silver/weather/*.parquet" \
  "${GCP_PROJECT_ID}:silver.weather_data"

bq mk --external_table_definition="PARQUET=gs://${GCS_BUCKET}/silver/air-pollution/*.parquet" \
  "${GCP_PROJECT_ID}:silver.air_pollution_data"

echo "Done. Query with: bq query --use_legacy_sql=false 'SELECT * FROM \`${GCP_PROJECT_ID}.silver.weather_data\` LIMIT 10'"
