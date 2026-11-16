#!/usr/bin/env bash
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
exec "${script_dir}/../linux/docker-build.sh" amd64 "$@"
