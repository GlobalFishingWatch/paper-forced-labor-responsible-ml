#!/bin/bash

set -euo pipefail

rm -f .env
cat <<EOF > .env
UID=$(id -u)
GID=$(id -g)
USERNAME=$(whoami)
HOME=$(echo $HOME)
CWD=$(realpath $(dirname $0))
GITHUB_PAT=$(cat ./untracked/gittoken.json | jq ".token")
SSH_AUTH_SOCK=$(echo $SSH_AUTH_SOCK)
EOF
chmod 600 .env
