#!/usr/bin/env bash
# Run an offline renderer and write an image.
# Usage:
#   sh/render.sh [--mode fast|exact] [--output FILE.png] [renderer args...]
# Defaults: --mode fast, --output renders/still.png,
#           --dataset <first local dataset>.
#
#   fast  — the shared renderer: the viewer's octree, level-of-detail cut and
#           shaders on the GPU, reading a local index or, with --url, a running
#           query API. This is what the video renders with.
#   exact — the CPU reference: every leaf, no level of detail, no GPU. Slow,
#           and the thing the fast path is measured against.
#
# Extra flags are forwarded to the selected renderer.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

output="renders/still.png"
mode="fast"
dataset=""
forward_args=()

while (( $# > 0 )); do
  case "$1" in
    --output)  output="$2"; shift 2 ;;
    --mode)    mode="$2"; shift 2 ;;
    --dataset) dataset="$2"; shift 2 ;;
    *)         forward_args+=("$1"); shift ;;
  esac
done

if [[ -z "$dataset" ]]; then
  dataset="$(ls "$repo_root/data" 2>/dev/null | head -n1 || true)"
  if [[ -z "$dataset" ]]; then
    echo "no dataset in $repo_root/data; pass --dataset" >&2
    exit 1
  fi
fi

mkdir -p "$(dirname "$output")"

case "$mode" in
  fast)
    npx tsx "$repo_root/offline/still.ts" \
      --dataset "$dataset" "${forward_args[@]}" --output "$output"
    ;;
  exact)
    npx tsx "$repo_root/render-check/render-exact.ts" \
      --starcloud "$repo_root/data/$dataset/starcloud.bin" \
      "${forward_args[@]}" --output "$output"
    ;;
  *)
    echo "unknown --mode '$mode' (expected fast|exact)" >&2; exit 1 ;;
esac
