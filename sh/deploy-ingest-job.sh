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
bucket_uri="${BUCKET_URI:-gs://star-dump-data-${project_number}}"
data_root="${DATA_ROOT:-${bucket_uri}/runs/gaia-source-786097-786431}"
job_name="${INGEST_JOB_NAME:-star-dump-ingest}"
service_account_name="${SERVICE_ACCOUNT_NAME:-star-dump-run}"
service_account_email="${SERVICE_ACCOUNT_EMAIL:-${service_account_name}@${project_id}.iam.gserviceaccount.com}"
default_ingest_url="${DEFAULT_INGEST_URL:-https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_786097-786431.csv.gz}"
parallax_filter_mas="${PARALLAX_FILTER_MAS:-10}"

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
  --input "${default_ingest_url}"
  --output-root "${data_root}"
  --parallax-filter-mas "${parallax_filter_mas}"
)

gcloud beta run jobs "${deploy_command}" "${job_name}" \
  --image "${image_uri}" \
  --service-account "${service_account_email}" \
  --memory 1Gi \
  --task-timeout=3600 \
  --max-retries=0 \
  --command /usr/local/bin/ingest \
  --args="$(join_with_commas "${args[@]}")"

echo "image: ${image_uri}"
echo "ingest_job: ${job_name}"
