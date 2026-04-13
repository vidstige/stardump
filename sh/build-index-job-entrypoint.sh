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
/usr/local/bin/build-index \
  "--data-root=${data_root}" \
  "--octree-depth=${octree_depth}" \
  "--output-root=${output_root}"

rm -rf "${data_root}/indices"
mkdir -p "${data_root}/indices"

find "${output_root}/indices" -type d | while read -r source_dir; do
  relative_dir="${source_dir#"${output_root}/"}"
  mkdir -p "${data_root}/${relative_dir}"
done

find "${output_root}/indices" -type f | while read -r source_file; do
  relative_file="${source_file#"${output_root}/"}"
  cat "${source_file}" > "${data_root}/${relative_file}"
done

rm -f "${data_root}/index.octree"
cat "${output_root}/index.octree" > "${data_root}/index.octree"
