#!/usr/bin/env bash
# 构建、导入、发布多架构 environment-manager OCI 制品。
#
# 构建器始终原生运行；非宿主架构由 Rust + Zig 交叉编译，不在 QEMU 中运行
# 编译器。publish 只组装已经通过验证的单架构镜像，不重新构建。
# 不使用 Makefile/just 编排——本脚本就是唯一入口，
# 直接对着 Dockerfile 操作。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly repository_root="$(cd "${script_dir}/.." && pwd -P)"

# 两种架构共用的不可变多平台基础镜像和版本号。
readonly ALMALINUX_IMAGE='docker.io/library/almalinux@sha256:d2515c769e7b73f95c4fde38c0a505336ff38f14990c0b7253b77060a049a743'
readonly RUST_VERSION='1.97.0'
readonly CARGO_ZIGBUILD_VERSION='0.23.3'
readonly ZIG_VERSION='0.14.1'
readonly LOCAL_IMAGE_REPOSITORY='oma-local/environment-manager-rs'
# artifact stage 内二进制的路径，import/publish 都要从容器里把它复制出来做校验。
readonly ARTIFACT_BINARY_PATH='/usr/local/bin/environment-manager'

usage() {
  cat >&2 <<'USAGE'
usage: scripts/build-image.sh <command> [argument ...]

commands:
  build [amd64|arm64|all]                        build and verify one or all platforms (default: all)
  import <amd64|arm64> <image@sha256:digest> <binary-sha256>
                                                  import an already published, independently verified platform
  publish <registry/repository> [tag]            promote verified local images into one OCI index
                                                  (tag defaults to <commit-date>-<short-sha>)
  smoke                                           exercise publish against a disposable local registry
USAGE
}

require_tools() {
  command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
  docker buildx version >/dev/null
}

git_revision() {
  git -C "$repository_root" rev-parse HEAD
}

git_dirty() {
  [[ -n "$(git -C "$repository_root" status --porcelain)" ]]
}

# 构建和发布 tag 格式：<构建日期>-<7 位 short sha>，例如 20260831-92ac1bf。
default_release_tag() {
  local revision build_date
  revision="$(git_revision)"
  build_date="$(date +%Y%m%d)"
  printf '%s-%s\n' "$build_date" "${revision:0:7}"
}

# 写入镜像 revision label 的值。工作树脏时打上 -dirty 后缀，
# 这样 publish 才能识别出"这个本地镜像不是从干净提交构建的"并拒绝正式发布。
build_git_hash() {
  local revision
  revision="$(git_revision)"
  if git_dirty; then
    printf '%s-dirty\n' "$revision"
  else
    printf '%s\n' "$revision"
  fi
}

# 本地单架构标签、OCI version label 和最终发布标签使用同一个版本号。
image_version() {
  default_release_tag
}

# 本地单架构镜像标签，build/import/publish 三个命令共用同一套命名规则，
# 保证 publish 总能找到 build 或 import 刚刚产出的镜像。
local_image_tag() {
  printf '%s:%s-%s\n' "$LOCAL_IMAGE_REPOSITORY" "$(image_version)" "$1"
}

# 用来跑二进制探针的裸系统镜像标签（只含 OS，不含 Rust 工具链），
# 与 artifact 镜像分开构建，避免验证时误用带完整构建环境的镜像。
verification_image_tag() {
  printf 'oma-local/environment-manager-verification:%s-%s\n' "$(image_version)" "$1"
}

# ── build：原生构建并验证一个或多个架构 ──────────────────────
#
# 每个架构依次跑三个 target：
#   verify              - fmt/clippy/单元测试/集成测试，只跑不产出镜像
#   artifact             - scratch 基础上的纯二进制制品，发布出去的就是它
#   verification-runtime  - 裸 AlmaLinux，用来跑 artifact 里提取出的二进制
cmd_build() {
  local requested_arch="${1:-all}" arches

  case "$requested_arch" in
    amd64|arm64) arches=("$requested_arch") ;;
    all) arches=(amd64 arm64) ;;
    *) usage; exit 2 ;;
  esac

  require_tools

  local version build_time hash
  version="$(image_version)"
  build_time="$(git -C "$repository_root" show -s --format=%cI HEAD)"
  hash="$(build_git_hash)"

  local arch platform local_image verification_image common_args
  for arch in "${arches[@]}"; do
    platform="linux/${arch}"
    local_image="$(local_image_tag "$arch")"
    verification_image="$(verification_image_tag "$arch")"
    common_args=(
      --platform "$platform"
      --build-arg "ALMALINUX_IMAGE=${ALMALINUX_IMAGE}"
      --build-arg "RUST_VERSION=${RUST_VERSION}"
      --build-arg "CARGO_ZIGBUILD_VERSION=${CARGO_ZIGBUILD_VERSION}"
      --build-arg "ZIG_VERSION=${ZIG_VERSION}"
      --build-arg "ENVIRONMENT_MANAGER_VERSION=${version}"
      --build-arg "BUILD_GIT_HASH=${hash}"
      --build-arg "BUILD_TIME=${build_time}"
    )

    docker buildx build "${common_args[@]}" --target verify "$repository_root"
    docker buildx build "${common_args[@]}" --target artifact \
      --provenance=false --tag "$local_image" --load "$repository_root"
    docker buildx build "${common_args[@]}" --target verification-runtime \
      --provenance=false --tag "$verification_image" --load "$repository_root"

    local image_arch
    image_arch="$(docker image inspect "$local_image" --format '{{.Architecture}}')"
    [[ "$image_arch" == "$arch" ]] || {
      echo "image architecture mismatch: expected $arch, got $image_arch" >&2
      exit 1
    }

    local work_dir binary_output container_id
    work_dir="$(mktemp -d)"
    binary_output="${work_dir}/environment-manager"
    container_id="$(docker create --platform "$platform" \
      --entrypoint "$ARTIFACT_BINARY_PATH" "$local_image" --help)"
    docker cp "${container_id}:${ARTIFACT_BINARY_PATH}" "$binary_output"
    docker rm "$container_id" >/dev/null
    chmod 0755 "$binary_output"
    docker run --rm --platform "$platform" \
      --volume "${binary_output}:/candidate/environment-manager:ro" \
      --entrypoint /candidate/environment-manager \
      "$verification_image" --help >/dev/null
    docker run --rm --platform "$platform" \
      --volume "${binary_output}:/candidate/environment-manager:ro" \
      --entrypoint /candidate/environment-manager \
      "$verification_image" --version >/dev/null
    rm -rf "$work_dir"
    echo "built and verified $platform: $local_image"
  done
}

# ── import：直接复用已发布、已独立验证过的某架构制品 ──────────
#
# 典型场景：ARM Mac 上 QEMU 模拟 amd64 编译会崩溃，于是 amd64 侧不在本机重新
# 构建，而是把已经在 CI/其他机器上构建并验证过的镜像拉下来、校验二进制字节，
# 当作等价于本机 build 的产物纳入后续 publish 流程。
cmd_import() (
  local arch="${1:-}" source_ref="${2:-}" expected_binary_sha256="${3:-}"

  [[ "$arch" == amd64 || "$arch" == arm64 ]] \
    && [[ "$source_ref" =~ @sha256:[0-9a-f]{64}$ ]] \
    && [[ "$expected_binary_sha256" =~ ^[0-9a-f]{64}$ ]] || {
    echo "usage: scripts/build-image.sh import <amd64|arm64> <image@sha256:digest> <binary-sha256>" >&2
    exit 2
  }

  require_tools

  local platform local_image verification_image work_dir binary_output
  platform="linux/${arch}"
  local_image="$(local_image_tag "$arch")"
  verification_image="$(verification_image_tag "$arch")"
  work_dir="$(mktemp -d)"
  binary_output="${work_dir}/environment-manager"
  trap 'rm -rf "$work_dir"' EXIT

  docker image pull --platform "$platform" "$source_ref"
  docker image tag "$source_ref" "$local_image"
  local image_arch
  image_arch="$(docker image inspect "$local_image" --format '{{.Architecture}}')"
  [[ "$image_arch" == "$arch" ]] || {
    echo "image architecture mismatch: expected $arch, got $image_arch" >&2
    exit 1
  }

  # 从镜像里提取真实二进制字节，核对调用方声明的 SHA-256——
  # 防止 import 命令把一个错误或被篡改的镜像当成"已验证"接受进来。
  local container_id
  container_id="$(docker create --platform "$platform" \
    --entrypoint "$ARTIFACT_BINARY_PATH" "$local_image" --help)"
  docker cp "${container_id}:${ARTIFACT_BINARY_PATH}" "$binary_output"
  docker rm "$container_id" >/dev/null
  chmod 0755 "$binary_output"

  local actual_binary_sha256
  actual_binary_sha256="$(sha256sum "$binary_output" | awk '{print $1}')"
  [[ "$actual_binary_sha256" == "$expected_binary_sha256" ]] || {
    echo "binary SHA-256 mismatch: expected $expected_binary_sha256, got $actual_binary_sha256" >&2
    exit 1
  }

  # 本地临时构建裸系统验证镜像，跑一次 --version 确认二进制真的能在
  # 目标架构上执行（而不仅仅是字节匹配）。
  docker buildx build \
    --platform "$platform" \
    --build-arg "ALMALINUX_IMAGE=${ALMALINUX_IMAGE}" \
    --target verification-runtime --provenance=false \
    --tag "$verification_image" --load "$repository_root"
  docker run --rm --platform "$platform" \
    --volume "${binary_output}:/candidate/environment-manager:ro" \
    --entrypoint /candidate/environment-manager \
    "$verification_image" --version >/dev/null

  echo "imported $platform: image=$source_ref binary_sha256=$actual_binary_sha256"
)

# ── publish：把已验证的本地镜像组装成一个多架构 OCI 索引 ──────
#
# 不重新构建、不重新验证——只信任 build/import 已经做过的检查结果，
# 逐架构推送单架构镜像、再用 imagetools create 合成统一标签。
cmd_publish() (
  local image_repository="${1:-}" release_tag="${2:-}"

  [[ -n "$image_repository" ]] || {
    echo "usage: scripts/build-image.sh publish <registry/repository> [tag]" >&2
    exit 2
  }
  [[ -n "$release_tag" ]] || release_tag="$(default_release_tag)"

  require_tools
  command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

  local revision
  revision="$(git_revision)"
  # 默认拒绝发布脏工作树；只有本地一次性冒烟测试（cmd_smoke）会显式放行。
  if git_dirty && [[ "${ALLOW_DIRTY_PUBLISH:-0}" != 1 ]]; then
    echo "refusing to publish a dirty worktree; set ALLOW_DIRTY_PUBLISH=1 only for a disposable local registry smoke test" >&2
    exit 1
  fi

  local -a child_refs=() summary_lines=()
  local work_dir
  work_dir="$(mktemp -d)"
  trap 'rm -rf "$work_dir"' EXIT

  local arch platform local_image verification_image binary_output
  local image_arch image_revision container_id binary_sha256 platform_tag digest
  for arch in amd64 arm64; do
    platform="linux/${arch}"
    local_image="$(local_image_tag "$arch")"
    verification_image="$(verification_image_tag "$arch")"
    binary_output="${work_dir}/environment-manager-${arch}"

    image_arch="$(docker image inspect "$local_image" --format '{{.Architecture}}')"
    [[ "$image_arch" == "$arch" ]] || {
      echo "image architecture mismatch: expected $arch, got $image_arch" >&2
      exit 1
    }
    # 核对镜像 label 里记录的 revision，确保发布的是当前 HEAD 构建出的镜像，
    # 而不是本地缓存里某个更早提交的产物。
    image_revision="$(docker image inspect "$local_image" \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
    if [[ "${ALLOW_DIRTY_PUBLISH:-0}" == 1 ]]; then
      [[ "${image_revision%-dirty}" == "$revision" ]] || {
        echo "image revision mismatch: $local_image" >&2; exit 1;
      }
    elif [[ "$image_revision" != "$revision" ]]; then
      echo "image was not built from the clean current revision: $local_image" >&2
      exit 1
    fi

    # 提取二进制、算 SHA-256、在裸系统验证镜像里跑一次，
    # 与 import 命令使用同样的校验逻辑。
    container_id="$(docker create --platform "$platform" \
      --entrypoint "$ARTIFACT_BINARY_PATH" "$local_image" --help)"
    docker cp "${container_id}:${ARTIFACT_BINARY_PATH}" "$binary_output"
    docker rm "$container_id" >/dev/null
    chmod 0755 "$binary_output"
    binary_sha256="$(sha256sum "$binary_output" | awk '{print $1}')"
    docker run --rm --platform "$platform" \
      --volume "${binary_output}:/candidate/environment-manager:ro" \
      --entrypoint /candidate/environment-manager \
      "$verification_image" --version >/dev/null

    # 推送单架构子标签，记录推送后 Registry 返回的不可变 digest——
    # 后面组装索引只使用这个 digest，不使用可变的 <tag>-<arch> 标签本身。
    platform_tag="${image_repository}:${release_tag}-${arch}"
    docker image tag "$local_image" "$platform_tag"
    docker image push "$platform_tag"
    digest="$(docker buildx imagetools inspect "$platform_tag" | sed -n 's/^Digest:[[:space:]]*//p' | head -n 1)"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "unable to resolve digest for $platform_tag" >&2; exit 1; }
    child_refs+=("${image_repository}@${digest}")
    summary_lines+=("linux/${arch}: image=${image_repository}@${digest} binary_sha256=${binary_sha256}")
  done

  # 用不可变 digest 组装统一标签，再读回来确认平台集合恰好是 amd64+arm64。
  local index_ref index_raw actual_platforms
  index_ref="${image_repository}:${release_tag}"
  docker buildx imagetools create --tag "$index_ref" "${child_refs[@]}"
  index_raw="$(docker buildx imagetools inspect --raw "$index_ref")"
  actual_platforms="$(jq -r '[.manifests[] | select(.platform.os == "linux") | .platform.architecture] | sort | join(" ")' <<<"$index_raw")"
  [[ "$actual_platforms" == 'amd64 arm64' ]] || {
    echo "published index has unexpected platforms: $actual_platforms" >&2
    exit 1
  }

  echo "published multi-architecture index ${index_ref}"
  printf '  %s\n' "${summary_lines[@]}"
)

# ── smoke：对着一次性本地 Registry 走一遍真实的 publish 流程 ──
#
# 用来验证 push + imagetools create 这条路径本身没问题，
# 不需要连正式 Registry，容器退出时临时 registry 自动清理。
cmd_smoke() (
  local registry_name="oma-environment-manager-registry-$$"
  cleanup() {
    docker rm -f "$registry_name" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT

  docker run --detach --rm --publish 127.0.0.1::5000 --name "$registry_name" registry:2 >/dev/null
  local registry_port
  registry_port="$(docker port "$registry_name" 5000/tcp | sed -n 's/.*://p' | head -n 1)"
  [[ "$registry_port" =~ ^[0-9]+$ ]] || { echo "unable to resolve local registry port" >&2; exit 1; }

  ALLOW_DIRTY_PUBLISH=1 cmd_publish "127.0.0.1:${registry_port}/oma/environment-manager-rs" local-smoke
)

main() {
  local command="${1:-}"
  shift || true

  case "$command" in
    build) cmd_build "$@" ;;
    import) cmd_import "$@" ;;
    publish) cmd_publish "$@" ;;
    smoke) cmd_smoke "$@" ;;
    *) usage; exit 2 ;;
  esac
}

main "$@"
