#!/bin/bash

set -euo pipefail

./docker-env.sh
DOCKER_BUILDKIT=1 docker compose up dev
