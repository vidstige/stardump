#!/bin/sh
# Usage: convert.sh input.ppm [output.png]
INPUT="$1"
OUTPUT="${2:-${INPUT%.ppm}.png}"
convert "$INPUT" "$OUTPUT"
echo "$OUTPUT"
