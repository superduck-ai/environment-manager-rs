#!/usr/bin/env bash
# 在 Linux ARM64 容器中编译，源码和产物留在当前工作区。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly workspace="$(cd "${script_dir}/../.." && pwd -P)"
readonly image="${RUST_BUILD_IMAGE:-environment-manager-rust:1.97.0-arm64}"

docker build --platform linux/arm64 --tag "${image}" "${script_dir}"
docker run --rm --platform linux/arm64 \
  --volume "${workspace}:/workspace" \
  --volume environment-manager-arm64-registry:/root/.cargo/registry \
  --volume environment-manager-arm64-git:/root/.cargo/git \
  --env CARGO_TARGET_DIR=/workspace/target/docker-linux-arm64 \
  --workdir /workspace \
  "${image}" \
  cargo build --release --locked --target aarch64-unknown-linux-gnu "$@"
