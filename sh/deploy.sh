#!/usr/bin/env bash
set -euo pipefail

./sh/build-image.sh
./sh/push-image.sh
./sh/deploy-service.sh
./sh/deploy-ingest-job.sh
./sh/deploy-build-index-job.sh
