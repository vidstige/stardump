#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

image_tag="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
image_sha_uri="gcr.io/${project_id}/star-dump:${image_tag}"
image_latest_uri="gcr.io/${project_id}/star-dump:latest"

gcloud builds submit --tag "${image_sha_uri}"
gcloud container images add-tag --quiet "${image_sha_uri}" "${image_latest_uri}"
echo "${image_sha_uri}"
echo "${image_latest_uri}"
