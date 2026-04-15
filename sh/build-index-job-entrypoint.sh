#!/usr/bin/env bash
set -euo pipefail

config_path="/mnt/gcs/.stardump/build-index-config.txt"
data_root=""
octree_depth=""

while IFS='=' read -r key value; do
  case "${key}" in
    data_root) data_root="${value}" ;;
    octree_depth) octree_depth="${value}" ;;
  esac
done < "${config_path}"

if [[ -z "${data_root}" || -z "${octree_depth}" ]]; then
  echo "invalid build-index config: ${config_path}" >&2
  exit 1
fi

tmp_root="$(mktemp -d /tmp/build-index.XXXXXX)"
trap 'rm -rf "${tmp_root}"' EXIT

output_root="${tmp_root}/dataset"
echo "build-index-job: starting local build data_root=${data_root} octree_depth=${octree_depth} output_root=${output_root}"
/usr/local/bin/build-index \
  "--data-root=${data_root}" \
  "--octree-depth=${octree_depth}" \
  "--output-root=${output_root}"
echo "build-index-job: local build finished"

rm -f "${data_root}/index.octree"
cat "${output_root}/index.octree" > "${data_root}/index.octree"
echo "build-index-job: publish finished"
