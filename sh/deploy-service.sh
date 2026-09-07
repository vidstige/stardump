#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

project_number="$(gcloud projects describe "${project_id}" --format='value(projectNumber)')"
image_uri="${IMAGE_URI:-gcr.io/${project_id}/star-dump:latest}"
bucket_name="${BUCKET_NAME:-star-dump-data-${project_number}}"
mount_root="${MOUNT_ROOT:-/mnt/gcs}"
service_name="${SERVICE_NAME:-star-dump-query-api}"
service_account_name="${SERVICE_ACCOUNT_NAME:-star-dump-run}"
service_account_email="${SERVICE_ACCOUNT_EMAIL:-${service_account_name}@${project_id}.iam.gserviceaccount.com}"

gcloud run deploy "${service_name}" \
  --platform managed \
  --image "${image_uri}" \
  --service-account "${service_account_email}" \
  --port 8080 \
  --memory 4Gi \
  --cpu 2 \
  --add-volume "name=gcs,type=cloud-storage,bucket=${bucket_name},readonly=true" \
  --add-volume-mount "volume=gcs,mount-path=${mount_root}" \
  --command /usr/local/bin/query-api \
  --args="--data-root,${mount_root},--bind,0.0.0.0:8080,--viewer-root,/usr/local/share/star-dump/viewer" \
  --allow-unauthenticated

echo "image: ${image_uri}"
echo "service: ${service_name}"
