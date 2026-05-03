#!/usr/bin/env bash
# Run the offline render-check and convert to PNG.
# Usage:
#   sh/render.sh [--mode exact|fast] [--output FILE.png] [renderer args...]
# Defaults: --mode exact, --output /tmp/stars.png,
#           --dataset <first local dataset>.
# Extra flags are forwarded to the selected renderer.

set -euo pipefail

command -v sips >/dev/null 2>&1 || { echo "missing sips" >&2; exit 1; }
command -v npx  >/dev/null 2>&1 || { echo "missing npx"  >&2; exit 1; }

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root/render-check"

output_png="/tmp/stars.png"
mode="exact"
dataset=""
have_dataset=0
have_url=0
url="http://127.0.0.1:3000"
forward_args=()

while (( $# > 0 )); do
  case "$1" in
    --output)  output_png="$2"; shift 2 ;;
    --mode)    mode="$2"; shift 2 ;;
    --dataset) dataset="$2"; have_dataset=1; shift 2 ;;
    --url)     url="$2"; have_url=1; shift 2 ;;
    *)         forward_args+=("$1"); shift ;;
  esac
done

if (( ! have_dataset )); then
  dataset="$(ls "$repo_root/data" 2>/dev/null | head -n1 || true)"
  if [[ -z "$dataset" ]]; then
    echo "no dataset in $repo_root/data; pass --dataset" >&2
    exit 1
  fi
fi

case "$mode" in
  exact)   src="render-exact.ts" ;;
  fast)    src="render-fast.ts" ;;
  *) echo "unknown --mode '$mode' (expected exact|fast)" >&2; exit 1 ;;
esac

ppm="$(mktemp -t stardump-render).ppm"
trap 'rm -f "$ppm"' EXIT

case "$mode" in
  exact)
    npx tsx "$src" \
      --starcloud "$repo_root/data/$dataset/starcloud.bin" \
      "${forward_args[@]}" --output "$ppm"
    ;;
  fast)
    npx tsx "$src" \
      --url "$url" --dataset "$dataset" \
      "${forward_args[@]}" --output "$ppm"
    ;;
esac

sh "$repo_root/sh/convert.sh" "$ppm" "$output_png"
echo "Saved $output_png"
