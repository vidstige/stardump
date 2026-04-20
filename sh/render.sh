#!/usr/bin/env bash
# Run the offline render-check against the local query-api and convert to PNG.
# Usage:
#   sh/render.sh [--mode fast|exact] [--output FILE.png] [renderer args...]
# Defaults: --mode fast, --url http://127.0.0.1:3000,
#           --dataset <first local dataset>, --output /tmp/stars.png.
# Extra flags are forwarded to the selected renderer.

set -euo pipefail

command -v node >/dev/null 2>&1 || { echo "missing node" >&2; exit 1; }
command -v sips >/dev/null 2>&1 || { echo "missing sips" >&2; exit 1; }
command -v npx  >/dev/null 2>&1 || { echo "missing npx"  >&2; exit 1; }

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root/render-check"

output_png="/tmp/stars.png"
mode="fast"
forward_args=()
have_url=0
have_dataset=0

while (( $# > 0 )); do
  case "$1" in
    --output)
      output_png="$2"; shift 2 ;;
    --mode)
      mode="$2"; shift 2 ;;
    --url)
      have_url=1; forward_args+=("$1" "$2"); shift 2 ;;
    --dataset)
      have_dataset=1; forward_args+=("$1" "$2"); shift 2 ;;
    *)
      forward_args+=("$1"); shift ;;
  esac
done

case "$mode" in
  fast)    src="render.ts";       js="render.js" ;;
  fast-v2) src="render-fast.ts";  js="render-fast.js" ;;
  exact)   src="render-exact.ts"; js="render-exact.js" ;;
  *) echo "unknown --mode '$mode' (expected fast|fast-v2|exact)" >&2; exit 1 ;;
esac

# Rebuild JS if source is newer (or JS is missing).
if [[ ! -f "$js" || "$src" -nt "$js" ]]; then
  npx esbuild "$src" --bundle --platform=node --outfile="$js" >/dev/null
fi

if (( ! have_url )); then
  forward_args=(--url http://127.0.0.1:3000 "${forward_args[@]}")
fi

if (( ! have_dataset )); then
  dataset="$(ls "$repo_root/data" 2>/dev/null | head -n1 || true)"
  if [[ -z "$dataset" ]]; then
    echo "no dataset in $repo_root/data; pass --dataset" >&2
    exit 1
  fi
  forward_args=(--dataset "$dataset" "${forward_args[@]}")
fi

ppm="$(mktemp -t stardump-render).ppm"
trap 'rm -f "$ppm"' EXIT

node "$js" "${forward_args[@]}" --output "$ppm"
sips -s format png "$ppm" --out "$output_png" >/dev/null
echo "Saved $output_png"
