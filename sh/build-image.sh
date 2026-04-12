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

image_tag="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
image_sha_uri="gcr.io/${project_id}/star-dump:${image_tag}"
image_latest_uri="gcr.io/${project_id}/star-dump:latest"

gcloud builds submit --tag "${image_sha_uri}"
gcloud container images add-tag --quiet "${image_sha_uri}" "${image_latest_uri}"
echo "${image_sha_uri}"
echo "${image_latest_uri}"
