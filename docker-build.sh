#!/bin/bash

./docker-env.sh

export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker pull --platform linux/amd64 rocker/tidyverse:4.4.2
DOCKER_BUILDKIT=1 docker compose build dev --ssh default=/Users/philiprobinson/.ssh/id_ed25519
