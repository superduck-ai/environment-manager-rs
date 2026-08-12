#!/usr/bin/env bash
# 在固定 AlmaLinux 9 ARM64 digest 中构建可供 staging E2E 验证的唯一候选产物。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=container-images.env
source "${script_dir}/container-images.env"
readonly machine="${ORB_MACHINE:-ubuntu-2404}"
readonly user="${ORB_USER:-arthur}"
readonly target="/home/${user}/environment-manager"
readonly go_root="${VM_GO_SOURCE_ROOT:-/mnt/mac/Users/${user}/GolandProjects/environment-manager}"
readonly image="${RUST_RELEASE_IMAGE:-${DEFAULT_RUST_RELEASE_IMAGE}}"
readonly output_directory="dist/preflight"
readonly almalinux_mirror="${ALMALINUX_MIRROR:-https://mirrors.aliyun.com/almalinux}"
build_git_hash="$(git -C "${script_dir}/../.." rev-parse --short=12 HEAD)"
if [[ -n "$(git -C "${script_dir}/../.." status --porcelain)" ]]; then
  build_git_hash="${build_git_hash}-dirty"
fi
readonly build_git_hash
readonly build_time="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

# 正式发布不再调用本脚本，也不允许通过位置参数把候选直接写入 dist 根目录。
# 这样 staging E2E 验证的 SHA 始终对应一个明确的 preflight 候选。
if (($# != 0)); then
  echo "build-release-artifact.sh 不接受输出目录参数；候选固定写入 dist/preflight" >&2
  exit 1
fi
if ! is_audited_container_image rust "${image}"; then
  echo "RUST_RELEASE_IMAGE 不在审核清单 ${CONTAINER_IMAGE_ALLOWLIST_VERSION} 中" >&2
  echo "拒绝镜像: ${image}" >&2
  exit 1
fi
if [[ ! "${almalinux_mirror}" =~ ^https://[^[:space:]]+$ ]]; then
  echo "ALMALINUX_MIRROR 必须是无空白的 HTTPS URL: ${almalinux_mirror}" >&2
  exit 1
fi

"${script_dir}/sync.sh"

orb -m "${machine}" -u "${user}" -w "${target}" bash -s -- \
  "${image}" "${target}" "${go_root}" "${output_directory}" "${almalinux_mirror}" \
  "${CONTAINER_IMAGE_ALLOWLIST_VERSION}" "${build_git_hash}" "${build_time}" <<'REMOTE'
set -euo pipefail
image="$1"
target="$2"
go_root="$3"
output_directory="$4"
almalinux_mirror="$5"
image_allowlist_version="$6"
build_git_hash="$7"
build_time="$8"
artifact_name="environment-manager_linux_arm64"
cache_root="${XDG_CACHE_HOME:-${HOME}/.cache}/environment-manager"
mkdir -p "${cache_root}"
staging="$(mktemp -d "${cache_root}/release.XXXXXX")"
candidate_directory="${target}/${output_directory}"
dist_directory="${target}/dist"
publication=""
obsolete_root=""
owns_build_lock=false

# 一旦取得 preflight 独占锁，任何失败都删除 canonical 候选路径。这样旧候选不会
# 因新构建失败而继续伪装成“本次成功”的四件套；隐藏的临时目录也始终由 trap 清理。
cleanup() {
  local status=$?
  trap - EXIT
  if ((status != 0)) && [[ "${owns_build_lock}" == true ]]; then
    rm -rf -- "${candidate_directory}" || true
  fi
  if [[ -n "${publication}" ]]; then
    rm -rf -- "${publication}" || true
  fi
  if [[ -n "${obsolete_root}" ]]; then
    rm -rf -- "${obsolete_root}" || true
  fi
  rm -rf -- "${staging}" || true
  exit "${status}"
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "候选构建宿主必须是 Linux ARM64" >&2
  exit 1
fi
if [[ "${output_directory}" != "dist/preflight" \
  || "$(realpath -e "${target}")" != "/home/$(id -un)/environment-manager" ]]; then
  echo "候选输出目录越界: ${output_directory}" >&2
  exit 1
fi
if [[ -L "${dist_directory}" \
  || (-e "${dist_directory}" && ! -d "${dist_directory}") ]]; then
  echo "dist 必须是工作区内的真实目录，不能是符号链接或其他文件" >&2
  exit 1
fi
mkdir -p "${dist_directory}"

# build-release 可能被误并发调用。锁文件固定留在被 source manifest 排除的 dist，
# 不删除 inode，避免清理锁文件时另一进程绕过仍持有的 flock。
exec 9>"${dist_directory}/.preflight-build.lock"
if ! flock -n 9; then
  echo "已有另一个 preflight 构建正在运行" >&2
  exit 1
fi
owns_build_lock=true

# 通过同一文件系统内的目录 rename 原子移走旧 preflight。构建随后失败时 canonical
# 路径保持不存在；即使进程被 SIGKILL，旧证据也只会留在隐藏目录，不能被当成候选。
if [[ -e "${candidate_directory}" || -L "${candidate_directory}" ]]; then
  obsolete_root="$(mktemp -d "${dist_directory}/.preflight-obsolete.XXXXXX")"
  if ! mv -T -- "${candidate_directory}" "${obsolete_root}/preflight"; then
    echo "无法使旧 preflight 候选失效" >&2
    exit 1
  fi
fi
publication="$(mktemp -d "${dist_directory}/.preflight-publish.XXXXXX")"
# mktemp 默认创建 0700 目录；最终 preflight 是发布证据目录，提交前固定为 0755，
# 与其中 0755 binary 和 0644 manifest 的既有读取语义保持一致。
chmod 0755 "${publication}"

# 候选构建可以被单独调用，因此仍需自行验证 Go 基线和 Cargo 依赖。
scripts/linux-arm64/verify-go-baseline.sh "${go_root}"
scripts/linux-arm64/verify-cargo-dependencies.sh "${target}"

# 在构建前记录完整源码状态。清单无时间戳，后续 release 可以逐字节复算；构建
# 完成后还会再算一次，避免并发同步造成“二进制与清单来自两个源码状态”。
scripts/linux-arm64/source-manifest.sh \
  "${target}" "${staging}/${artifact_name}.source-manifest" >/dev/null

# digest 引用已由版本化 allowlist 精确约束；容器内再验证发行版、架构和 glibc，
# 防止 registry 返回意外平台。CARGO_TARGET_DIR 位于容器临时目录，绝不复用
# Ubuntu 24.04 宿主产生的 build script 或链接产物。
podman run --rm --pull=always --arch arm64 \
  --volume "${target}:/workspace:ro" \
  --volume "${staging}:/out" \
  --env "ALMALINUX_MIRROR=${almalinux_mirror}" \
  --env "BUILD_GIT_HASH=${build_git_hash}" \
  --env "BUILD_TIME=${build_time}" \
  --workdir /workspace \
  "${image}" \
  bash -euc '
    source /etc/os-release
    test "${ID}" = "almalinux"
    [[ "${VERSION_ID}" == 9* ]]
    test "$(uname -m)" = "aarch64"
    test "$(getconf GNU_LIBC_VERSION)" = "glibc 2.34"

    # 固定镜像内的默认 mirrorlist 在当前验证网络极慢；仅替换仓库 baseurl，
    # AlmaLinux 包签名和 GPG key 校验仍由 DNF 执行。
    sed -ri "s|^mirrorlist=|# mirrorlist=|" /etc/yum.repos.d/almalinux-*.repo
    sed -ri \
      "s|^# baseurl=https://repo.almalinux.org/almalinux|baseurl=${ALMALINUX_MIRROR}|" \
      /etc/yum.repos.d/almalinux-*.repo

    # 基础镜像已有 curl-minimal；安装完整 curl 会与其互斥，这里只安装链接和
    # 产物审计所需的软件包。
    dnf -y --setopt=install_weak_deps=False install \
      binutils ca-certificates file gcc gcc-c++ git gzip make \
      pkgconf-pkg-config tar xz
    curl --proto "=https" --tlsv1.2 --fail --silent --show-error \
      https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path
    source /root/.cargo/env
    rustup toolchain install 1.97.0 --profile minimal \
      --target aarch64-unknown-linux-gnu
    test "$(rustc +1.97.0 --version | cut -d" " -f2)" = "1.97.0"
    test "$(rustc +1.97.0 -vV | sed -n "s/^host: //p")" \
      = "aarch64-unknown-linux-gnu"

    export CARGO_TARGET_DIR=/tmp/environment-manager-target
    cargo +1.97.0 build --release --locked \
      --target aarch64-unknown-linux-gnu
    install -m 0755 \
      "${CARGO_TARGET_DIR}/aarch64-unknown-linux-gnu/release/environment-manager" \
      /out/environment-manager_linux_arm64
  '

scripts/linux-arm64/verify-release-artifact.sh \
  "${staging}/${artifact_name}"
scripts/linux-arm64/source-manifest.sh \
  "${target}" "${staging}/${artifact_name}.source-manifest.after" >/dev/null
if ! cmp -s "${staging}/${artifact_name}.source-manifest" \
  "${staging}/${artifact_name}.source-manifest.after"; then
  echo "构建期间源码发生变化；拒绝生成身份不明确的候选" >&2
  exit 1
fi
rm -f "${staging}/${artifact_name}.source-manifest.after"

artifact_sha256="$(sha256sum "${staging}/${artifact_name}" | awk '{print $1}')"
source_sha256="$(sha256sum \
  "${staging}/${artifact_name}.source-manifest" | awk '{print $1}')"
printf '%s  %s\n' "${artifact_sha256}" "${artifact_name}" \
  >"${staging}/${artifact_name}.sha256"

# manifest 不记录时间或主机名；相同源码、镜像和产物字节会得到相同内容。发布
# 脚本将逐字段校验它，而不是信任一个可以独立修改的 .sha256 文件。
{
  printf 'schema=environment-manager-release-candidate-v1\n'
  printf 'artifact_name=%s\n' "${artifact_name}"
  printf 'artifact_sha256=%s\n' "${artifact_sha256}"
  printf 'source_manifest=%s.source-manifest\n' "${artifact_name}"
  printf 'source_sha256=%s\n' "${source_sha256}"
  printf 'target=aarch64-unknown-linux-gnu\n'
  printf 'rust_toolchain=1.97.0\n'
  printf 'release_image_allowlist=%s\n' "${image_allowlist_version}"
  printf 'release_image=%s\n' "${image}"
} >"${staging}/${artifact_name}.manifest"

# 四份证据先完整写入 dist 内的隐藏目录；它与最终路径处于同一文件系统，最后一次
# `mv -T` 是目录级原子提交，观察者不会看到只有 binary 或只有 manifest 的中间态。
install -m 0755 "${staging}/${artifact_name}" \
  "${publication}/${artifact_name}"
install -m 0644 \
  "${staging}/${artifact_name}.sha256" \
  "${staging}/${artifact_name}.source-manifest" \
  "${staging}/${artifact_name}.manifest" \
  "${publication}/"

for required_file in \
  "${artifact_name}" \
  "${artifact_name}.sha256" \
  "${artifact_name}.source-manifest" \
  "${artifact_name}.manifest"; do
  if [[ ! -f "${publication}/${required_file}" \
    || -L "${publication}/${required_file}" ]]; then
    echo "preflight 发布目录缺少普通证据文件: ${required_file}" >&2
    exit 1
  fi
done
if [[ "$(find "${publication}" -mindepth 1 -maxdepth 1 -print | wc -l)" -ne 4 ]]; then
  echo "preflight 发布目录必须恰好包含四份证据" >&2
  exit 1
fi
if [[ -e "${candidate_directory}" || -L "${candidate_directory}" ]]; then
  echo "preflight canonical 路径在构建期间被其他进程创建" >&2
  exit 1
fi
mv -T -- "${publication}" "${candidate_directory}"
publication=""

echo "候选产物: ${target}/${output_directory}/${artifact_name}"
echo "候选 SHA-256: ${artifact_sha256}"
echo "源码清单 SHA-256: ${source_sha256}"
REMOTE
