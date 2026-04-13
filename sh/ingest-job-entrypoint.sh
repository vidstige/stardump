#!/usr/bin/env bash
set -euo pipefail

config_path="/mnt/gcs/.stardump/ingest-config.txt"
input_manifest=""
output_root=""

while IFS='=' read -r key value; do
  case "${key}" in
    input_manifest) input_manifest="${value}" ;;
    output_root) output_root="${value}" ;;
  esac
done < "${config_path}"

if [[ -z "${input_manifest}" || -z "${output_root}" ]]; then
  echo "invalid ingest config: ${config_path}" >&2
  exit 1
fi

exec /usr/local/bin/ingest \
  "--input-manifest=${input_manifest}" \
  "--output-root=${output_root}"
