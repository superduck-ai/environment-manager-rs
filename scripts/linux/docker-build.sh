#!/usr/bin/env bash
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly workspace="$(cd "${script_dir}/../.." && pwd -P)"
readonly architecture="${1:-}"

source "${script_dir}/../linux-arm64/container-images.env"

case "${architecture}" in
  arm64)
    rust_target=aarch64-unknown-linux-gnu
    rust_machine=aarch64
    base_image="${DEFAULT_RUST_RELEASE_IMAGE}"
    ;;
  amd64)
    rust_target=x86_64-unknown-linux-gnu
    rust_machine=x86_64
    base_image=docker.io/library/almalinux@sha256:465a5007813f19b5ac8a88cd161f3d8dcc6b1958e0c03c31f7c9a1b079d0a7da
    ;;
  *)
    printf 'Usage: %s arm64|amd64 [cargo build arguments...]\n' "$0" >&2
    exit 2
    ;;
esac
shift

readonly image="${RUST_BUILD_IMAGE:-environment-manager-rust:1.97.0-${architecture}}"
readonly build_jobs="${RUST_BUILD_JOBS:-6}"
if [[ ! "${build_jobs}" =~ ^[1-9][0-9]*$ ]]; then
  printf 'RUST_BUILD_JOBS must be a positive integer\n' >&2
  exit 2
fi

manager_build_hash="${BUILD_GIT_HASH:-}"
if [[ -z "${manager_build_hash}" ]]; then
  manager_build_hash="$(git -C "${workspace}" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)"
  if [[ -n "$(git -C "${workspace}" status --porcelain 2>/dev/null || true)" ]]; then
    manager_build_hash="${manager_build_hash}-dirty"
  fi
fi

docker build --platform "linux/${architecture}" --tag "${image}" \
  --build-arg "ALMALINUX_IMAGE=${base_image}" \
  --build-arg "RUST_TARGET=${rust_target}" \
  --build-arg "RUST_MACHINE=${rust_machine}" \
  "${script_dir}/../linux-arm64"

docker run --rm --platform "linux/${architecture}" \
  --volume "${workspace}:/workspace" \
  --volume environment-manager-arm64-registry:/root/.cargo/registry \
  --volume environment-manager-arm64-git:/root/.cargo/git \
  --env "CARGO_TARGET_DIR=/workspace/target/docker-linux-${architecture}" \
  --env "CARGO_BUILD_JOBS=${build_jobs}" \
  --env "BUILD_GIT_HASH=${manager_build_hash}" \
  --env BUILD_TIME \
  --workdir /workspace \
  "${image}" bash -euc '
    rust_target="$1"
    build_architecture="$2"
    shift 2
    cargo build --release --locked --target "${rust_target}" \
      --package environment-manager --bin environment-manager "$@"
    binary="${CARGO_TARGET_DIR}/${rust_target}/release/environment-manager"
    "${binary}" --version
    mkdir -p dist/local
    artifact="environment-manager_linux_${build_architecture}"
    staging="$(mktemp "dist/local/.${artifact}.XXXXXX")"
    install -m 0755 "${binary}" "${staging}"
    mv "${staging}" "dist/local/${artifact}"
    cd dist/local
    checksum_staging="$(mktemp ".${artifact}.sha256.XXXXXX")"
    sha256sum "${artifact}" > "${checksum_staging}"
    mv "${checksum_staging}" "${artifact}.sha256"
    cat "${artifact}.sha256"
  ' bash "${rust_target}" "${architecture}" "$@"
printf 'Artifact: %s/dist/local/environment-manager_linux_%s\n' "${workspace}" "${architecture}"
