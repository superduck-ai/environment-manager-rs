#!/usr/bin/env bash
# 为 OrbStack Ubuntu ARM64 验证机幂等安装构建、测试和产物检查依赖。
set -euo pipefail

readonly expected_kernel="Linux"
readonly expected_arch="aarch64"
readonly rust_toolchain="1.97.0"
readonly rust_target="aarch64-unknown-linux-gnu"
readonly marker="${HOME}/.cache/environment-manager/bootstrap-v3"
readonly apt_packages=(
  binutils
  build-essential
  ca-certificates
  curl
  file
  git
  pkg-config
  podman
  python3
  rsync
  slirp4netns
  uidmap
  util-linux
)

if [[ "$(uname -s)" != "${expected_kernel}" || "$(uname -m)" != "${expected_arch}" ]]; then
  echo "bootstrap 只允许在 Linux ARM64（aarch64）上运行" >&2
  exit 1
fi
if [[ ! -r /etc/os-release ]]; then
  echo "无法读取 /etc/os-release，不能确认权威 Ubuntu 版本" >&2
  exit 1
fi
# 包管理器、glibc 和系统工具均属于测试环境的一部分，因此不接受“任意 ARM64
# Linux”。当前唯一权威宿主是 Ubuntu 24.04；升级发行版必须重新审核全部门禁。
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
  echo "bootstrap 只支持 Ubuntu 24.04 ARM64，实际 ${ID:-unknown} ${VERSION_ID:-unknown}" >&2
  exit 1
fi

# 仅在缺包时刷新 apt 索引，既保持脚本幂等，也避免每次验证都产生一次网络请求。
missing_packages=()
for package in "${apt_packages[@]}"; do
  if ! dpkg-query -W -f='${db:Status-Status}' "${package}" 2>/dev/null \
    | grep -qx 'installed'; then
    missing_packages+=("${package}")
  fi
done

if ((EUID == 0)); then
  privilege=()
elif command -v sudo >/dev/null 2>&1; then
  privilege=(sudo)
else
  privilege=()
fi

if ((${#missing_packages[@]} > 0)); then
  if ((EUID != 0)) && ((${#privilege[@]} == 0)); then
    echo "缺少软件包且当前用户不是 root，同时系统没有 sudo: ${missing_packages[*]}" >&2
    exit 1
  fi

  "${privilege[@]}" env DEBIAN_FRONTEND=noninteractive apt-get update
  "${privilege[@]}" env DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends "${apt_packages[@]}"
fi

# OrbStack 创建的低 UID 用户可能没有 subordinate UID/GID，rootless Podman 因而
# 无法解包镜像中属于其他系统账号的文件。只在条目缺失时分配专用范围，并让
# Podman 迁移现有 namespace；已配置的验证机不会重复修改系统文件。
podman_mapping_changed=false
current_user="$(id -un)"
if ! grep -q "^${current_user}:" /etc/subuid; then
  if ((EUID != 0)) && ((${#privilege[@]} == 0)); then
    echo "缺少 /etc/subuid 映射且无法获取 root 权限" >&2
    exit 1
  fi
  "${privilege[@]}" usermod --add-subuids 100000-165535 "${current_user}"
  podman_mapping_changed=true
fi
if ! grep -q "^${current_user}:" /etc/subgid; then
  if ((EUID != 0)) && ((${#privilege[@]} == 0)); then
    echo "缺少 /etc/subgid 映射且无法获取 root 权限" >&2
    exit 1
  fi
  "${privilege[@]}" usermod --add-subgids 100000-165535 "${current_user}"
  podman_mapping_changed=true
fi
if [[ "${podman_mapping_changed}" == true ]]; then
  podman system migrate
fi

# OrbStack 镜像通常已经安装 rustup；裸 Ubuntu 没有时才从 rustup 官方安装器引导。
# 工具链本身仍固定为 1.97.0，后续会对 rustc 版本和目标列表进行精确校验。
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
    https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --no-modify-path
fi

export PATH="${CARGO_HOME:-${HOME}/.cargo}/bin:${PATH}"
if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup 安装后仍不可用" >&2
  exit 1
fi

toolchain_ready=false
if actual_version="$(rustc +"${rust_toolchain}" --version 2>/dev/null)" \
  && [[ "${actual_version}" =~ ^rustc\ 1\.97\.0\  ]] \
  && rustup component list --toolchain "${rust_toolchain}" --installed \
    | grep -Eq '^clippy-' \
  && rustup component list --toolchain "${rust_toolchain}" --installed \
    | grep -Eq '^rustfmt-' \
  && rustup target list --toolchain "${rust_toolchain}" --installed \
    | grep -qx "${rust_target}"; then
  toolchain_ready=true
fi

if [[ "${toolchain_ready}" != true ]]; then
  rustup toolchain install "${rust_toolchain}" \
    --profile minimal \
    --component clippy,rustfmt \
    --target "${rust_target}"
fi

actual_rustc="$(rustc +"${rust_toolchain}" --version)"
if [[ ! "${actual_rustc}" =~ ^rustc\ 1\.97\.0\  ]]; then
  echo "Rust 工具链版本不匹配: ${actual_rustc}" >&2
  exit 1
fi
actual_rustc_host="$(rustc +"${rust_toolchain}" -vV | sed -n 's/^host: //p')"
if [[ "${actual_rustc_host}" != "${rust_target}" ]]; then
  echo "rustc host triple 不匹配: 期望 ${rust_target}，实际 ${actual_rustc_host}" >&2
  exit 1
fi
if ! rustup target list --toolchain "${rust_toolchain}" --installed \
  | grep -qx "${rust_target}"; then
  echo "Rust 目标未安装: ${rust_target}" >&2
  exit 1
fi

# marker 只作为诊断记录，是否就绪始终以上面的真实包和工具链检查为准。
mkdir -p "$(dirname "${marker}")"
temporary_marker="${marker}.tmp.$$"
trap 'rm -f "${temporary_marker}"' EXIT
{
  printf 'schema=3\n'
  printf 'distribution=ubuntu-24.04\n'
  printf 'architecture=%s\n' "${expected_arch}"
  printf 'rustc=%s\n' "${actual_rustc}"
  printf 'rustc_host=%s\n' "${actual_rustc_host}"
  printf 'target=%s\n' "${rust_target}"
} >"${temporary_marker}"
chmod 0600 "${temporary_marker}"
mv -f "${temporary_marker}" "${marker}"
trap - EXIT
