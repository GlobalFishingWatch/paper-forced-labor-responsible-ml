#!/bin/bash

cat <<EOF > .env
UID=$(id -u)
GID=$(id -g)
USERNAME=$(whoami)
HOME=$(echo $HOME)
CWD=$(realpath $(dirname $0))
GITHUB_PAT=$(cat ./untracked/gittoken.json | jq ".token")
EOF
