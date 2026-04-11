#!/usr/bin/env bash
set -euo pipefail

command -v gcloud >/dev/null 2>&1 || {
  echo "missing required command: gcloud" >&2
  exit 1
}

project_id="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${project_id}" || "${project_id}" == "(unset)" ]]; then
  echo "gcloud project is not set; run 'gcloud config set project <project-id>' first" >&2
  exit 1
fi

region="${REGION:-$(gcloud config get-value run/region 2>/dev/null)}"
if [[ -z "${region}" || "${region}" == "(unset)" ]]; then
  echo "gcloud run/region is not set; run 'gcloud config set run/region <region>' first" >&2
  exit 1
fi

project_number="$(gcloud projects describe "${project_id}" --format='value(projectNumber)')"
bucket_name="${BUCKET_NAME:-star-dump-data-${project_number}}"
bucket_uri="gs://${bucket_name}"
service_account_name="${SERVICE_ACCOUNT_NAME:-star-dump-run}"
service_account_email="${SERVICE_ACCOUNT_EMAIL:-${service_account_name}@${project_id}.iam.gserviceaccount.com}"

gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com \
  storage.googleapis.com

if ! gcloud storage buckets describe "${bucket_uri}" >/dev/null 2>&1; then
  gcloud storage buckets create "${bucket_uri}" \
    --location "${region}" \
    --uniform-bucket-level-access
fi

if ! gcloud iam service-accounts describe "${service_account_email}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${service_account_name}" \
    --display-name "StarDump Cloud Run"
fi

gcloud projects add-iam-policy-binding "${project_id}" \
  --member "serviceAccount:${service_account_email}" \
  --role roles/storage.objectAdmin

echo "project: ${project_id}"
echo "region: ${region}"
echo "bucket: ${bucket_uri}"
echo "service_account: ${service_account_email}"
