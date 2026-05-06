#!/usr/bin/env bash
# Run the offline renderer and write a PNG.
# Usage:
#   sh/render.sh [--mode exact|fast] [--output FILE.png] [renderer args...]
# Defaults: --mode exact, --output /tmp/stars.png,
#           --dataset <first local dataset>.
# Extra flags are forwarded to the selected renderer.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

output_png="/tmp/stars.png"
mode="exact"
dataset=""
have_dataset=0
url="http://127.0.0.1:3000"
forward_args=()

while (( $# > 0 )); do
  case "$1" in
    --output)  output_png="$2"; shift 2 ;;
    --mode)    mode="$2"; shift 2 ;;
    --dataset) dataset="$2"; have_dataset=1; shift 2 ;;
    --url)     url="$2"; shift 2 ;;
    *)         forward_args+=("$1"); shift ;;
  esac
done

tsx="$repo_root/render-check/node_modules/.bin/tsx"

if (( ! have_dataset )); then
  dataset="$(ls "$repo_root/data" 2>/dev/null | head -n1 || true)"
  if [[ -z "$dataset" ]]; then
    echo "no dataset in $repo_root/data; pass --dataset" >&2
    exit 1
  fi
fi

case "$mode" in
  exact)
    "$tsx" "$repo_root/render-check/render-exact.ts" \
      --starcloud "$repo_root/data/$dataset/starcloud.bin" \
      "${forward_args[@]}" --output "$output_png"
    ;;
  fast)
    "$tsx" "$repo_root/render-check/render-fast.ts" \
      --url "$url" --dataset "$dataset" \
      "${forward_args[@]}" --output "$output_png"
    ;;
  *)
    echo "unknown --mode '$mode' (expected exact|fast)" >&2; exit 1 ;;
esac

echo "Saved $output_png"
