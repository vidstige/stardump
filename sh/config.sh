#!/usr/bin/env bash
# Shared setup for the deploy scripts, meant to be sourced.
#
# Deployment settings that should not be committed go in `.env` at the repo
# root, which is gitignored. Set the target project there:
#
#   CLOUDSDK_CORE_PROJECT=<project-id>

for required in gcloud git; do
  command -v "${required}" >/dev/null 2>&1 || {
    echo "missing required command: ${required}" >&2
    exit 1
  }
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "${repo_root}/.env" ]]; then
  set -a
  source "${repo_root}/.env"
  set +a
fi

project_id="$(gcloud config get-value project 2>/dev/null)"
if [[ -z "${project_id}" || "${project_id}" == "(unset)" ]]; then
  echo "no project set; put CLOUDSDK_CORE_PROJECT=<project-id> in .env" >&2
  exit 1
fi
# Exported so nested gcloud calls and child scripts agree on the target.
export CLOUDSDK_CORE_PROJECT="${project_id}"
