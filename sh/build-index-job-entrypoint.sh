#!/usr/bin/env bash
set -euo pipefail

data_root=""
passthrough=()

while (($# > 0)); do
  case "$1" in
    --data-root)
      data_root="$2"
      passthrough+=("$1" "$2")
      shift 2
      ;;
    --data-root=*)
      data_root="${1#*=}"
      passthrough+=("$1")
      shift
      ;;
    *)
      passthrough+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${data_root}" ]]; then
  echo "missing required argument: --data-root" >&2
  exit 1
fi

tmp_root="$(mktemp -d /tmp/build-index.XXXXXX)"
trap 'rm -rf "${tmp_root}"' EXIT

output_root="${tmp_root}/dataset"
/usr/local/bin/build-index "${passthrough[@]}" "--output-root=${output_root}"

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
