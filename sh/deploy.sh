#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

export IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
export IMAGE_URI="${IMAGE_URI:-gcr.io/${project_id}/star-dump:latest}"

./sh/build-image.sh
./sh/deploy-service.sh
./sh/deploy-ingest-job.sh
./sh/deploy-build-starcloud-job.sh
