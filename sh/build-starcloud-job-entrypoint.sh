#!/usr/bin/env bash
set -euo pipefail

config_path="/mnt/gcs/.stardump/build-index-config.txt"
data_root=""
octree_depth=""
sample_budget=""
quality_threshold=""

while IFS='=' read -r key value; do
  case "${key}" in
    data_root) data_root="${value}" ;;
    octree_depth) octree_depth="${value}" ;;
    sample_budget) sample_budget="${value}" ;;
    quality_threshold) quality_threshold="${value}" ;;
  esac
done < "${config_path}"

if [[ -z "${data_root}" || -z "${octree_depth}" ]]; then
  echo "invalid build-starcloud config: ${config_path}" >&2
  exit 1
fi

tmp_root="$(mktemp -d /tmp/build-starcloud.XXXXXX)"
trap 'rm -rf "${tmp_root}"' EXIT

output_root="${tmp_root}/dataset"
echo "build-starcloud-job: starting local build data_root=${data_root} octree_depth=${octree_depth} output_root=${output_root}"
args=(
  "--data-root=${data_root}"
  "--output-root=${output_root}"
  "--octree-depth=${octree_depth}"
)
if [[ -n "${sample_budget}" ]]; then
  args+=("--sample-budget=${sample_budget}")
fi
if [[ -n "${quality_threshold}" ]]; then
  args+=("--quality-threshold=${quality_threshold}")
fi
/usr/local/bin/build-starcloud "${args[@]}"
echo "build-starcloud-job: local build finished"

rm -f "${data_root}/starcloud.bin"
cat "${output_root}/starcloud.bin" > "${data_root}/starcloud.bin"
echo "build-starcloud-job: publish finished"
