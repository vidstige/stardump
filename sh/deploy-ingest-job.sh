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

project_number="$(gcloud projects describe "${project_id}" --format='value(projectNumber)')"
image_tag="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
image_uri="${IMAGE_URI:-gcr.io/${project_id}/star-dump:${image_tag}}"
bucket_name="${BUCKET_NAME:-star-dump-data-${project_number}}"
mount_root="${MOUNT_ROOT:-/mnt/gcs}"
job_name="${INGEST_JOB_NAME:-star-dump-ingest}"
service_account_name="${SERVICE_ACCOUNT_NAME:-star-dump-run}"
service_account_email="${SERVICE_ACCOUNT_EMAIL:-${service_account_name}@${project_id}.iam.gserviceaccount.com}"
ingest_url="${INGEST_URL:-${DEFAULT_INGEST_URL:-}}"
data_root="${DATA_ROOT:-}"

if [[ -z "${ingest_url}" ]]; then
  echo "INGEST_URL is required" >&2
  exit 1
fi

if [[ -z "${data_root}" ]]; then
  echo "DATA_ROOT is required, for example DATA_ROOT=${mount_root}/<run-name>" >&2
  exit 1
fi

join_with_commas() {
  local IFS=,
  printf '%s' "$*"
}

if gcloud beta run jobs describe "${job_name}" >/dev/null 2>&1; then
  deploy_command=update
else
  deploy_command=create
fi

args=(
  --input "${ingest_url}"
  --output-root "${data_root}"
)

gcloud beta run jobs "${deploy_command}" "${job_name}" \
  --image "${image_uri}" \
  --service-account "${service_account_email}" \
  --memory 1Gi \
  --task-timeout=3600 \
  --max-retries=0 \
  --add-volume "name=gcs,type=cloud-storage,bucket=${bucket_name},readonly=false" \
  --add-volume-mount "volume=gcs,mount-path=${mount_root}" \
  --command /usr/local/bin/ingest \
  --args="$(join_with_commas "${args[@]}")"

echo "image: ${image_uri}"
echo "ingest_job: ${job_name}"
