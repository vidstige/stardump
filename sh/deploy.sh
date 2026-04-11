#!/usr/bin/env bash
set -euo pipefail

command -v gcloud >/dev/null 2>&1 || {
  echo "missing required command: gcloud" >&2
  exit 1
}
command -v git >/dev/null 2>&1 || {
  echo "missing required command: git" >&2
  exit 1
}

project_id="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${project_id}" || "${project_id}" == "(unset)" ]]; then
  echo "gcloud project is not set; run 'gcloud config set project <project-id>' first" >&2
  exit 1
fi

export IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
export IMAGE_URI="${IMAGE_URI:-gcr.io/${project_id}/star-dump:${IMAGE_TAG}}"

if [[ -z "${DATA_ROOT:-}" ]]; then
  echo "DATA_ROOT is required, for example DATA_ROOT=/mnt/gcs/<run-name>" >&2
  exit 1
fi

if [[ -z "${INGEST_URL:-${DEFAULT_INGEST_URL:-}}" ]]; then
  echo "INGEST_URL is required" >&2
  exit 1
fi

./sh/build-image.sh
./sh/deploy-service.sh
./sh/deploy-ingest-job.sh
./sh/deploy-build-index-job.sh
