#!/usr/bin/env bash
# 为 Linux ARM64 候选产物生成可复算、无时间戳的完整源码清单。
set -euo pipefail

readonly source_root="${1:-}"
readonly output="${2:-}"
readonly manifest_schema="environment-manager-source-manifest-v1"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "源码摘要只能在 Linux ARM64 上生成" >&2
  exit 1
fi
if [[ -z "${source_root}" || ! -d "${source_root}" ]]; then
  echo "源码目录不存在: ${source_root:-<empty>}" >&2
  exit 1
fi
if [[ -z "${output}" ]]; then
  echo "用法: $0 <source-root> <output-manifest>" >&2
  exit 1
fi

root="$(realpath -e "${source_root}")"
if [[ ! -f "${root}/Cargo.toml" || ! -f "${root}/Cargo.lock" ]]; then
  echo "源码目录不是完整的 Rust 工作区: ${root}" >&2
  exit 1
fi

# 输出文件通常位于被排除的 dist 目录；仍显式禁止把清单写回其他源码路径，
# 避免清单把自己纳入下一次计算并导致摘要永远漂移。
output_parent="$(realpath -m "$(dirname "${output}")")"
case "${output_parent}/" in
  "${root}/dist/"*|"${root}/target/"*) ;;
  /tmp/*|"${HOME}/.cache/"*) ;;
  *)
    echo "源码清单只能写入 dist、target、/tmp 或用户缓存目录: ${output}" >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "${output}")"
temporary="${output}.tmp.$$"
paths="$(mktemp)"
trap 'rm -f "${temporary}" "${paths}"' EXIT

# `.git` 不会同步到权威验证目录；target/dist 是派生产物；IDE 元数据、macOS
# Finder 文件、Python 字节码缓存和同步 marker 也不参与构建。Python 比较器会按
# 解释器小版本生成不同的 `__pycache__`，若纳入清单会把同一源码误判成漂移。除
# 这些明确派生项外，所有普通文件与符号链接都进入清单，使代码、测试、fixture、
# 门禁脚本或配置发生变化时都必须重建候选并重新做 staging E2E。
(
  cd "${root}"
  LC_ALL=C find . \
    \( -path './.git' -o -path './.git/*' \
      -o -path './.idea' -o -path './.idea/*' \
      -o -path './target' -o -path './target/*' \
      -o -path './dist' -o -path './dist/*' \
      -o -name '__pycache__' \) -prune \
    -o \( -type f -o -type l \) \
      ! -name '.DS_Store' \
      ! -name '*.pyc' \
      ! -name '*.pyo' \
      ! -name '.environment-manager-sync-root' \
      -print0 \
    | LC_ALL=C sort -z >"${paths}"
)

{
  printf 'schema=%s\n' "${manifest_schema}"
  printf 'target=aarch64-unknown-linux-gnu\n'
  printf 'rust_toolchain=1.97.0\n'

  while IFS= read -r -d '' relative_path; do
    # 文本 manifest 使用 TAB 和换行作为结构字符。拒绝极端路径名比生成一个
    # 无法无歧义复算的“看似成功”摘要更安全。
    if [[ "${relative_path}" == *$'\n'* || "${relative_path}" == *$'\t'* ]]; then
      echo "源码路径包含 manifest 保留字符: ${relative_path@Q}" >&2
      exit 1
    fi

    absolute_path="${root}/${relative_path#./}"
    mode="$(stat -c '%a' -- "${absolute_path}")"
    if [[ -L "${absolute_path}" ]]; then
      link_target="$(readlink -- "${absolute_path}")"
      if [[ "${link_target}" == *$'\n'* || "${link_target}" == *$'\t'* ]]; then
        echo "符号链接目标包含 manifest 保留字符: ${relative_path}" >&2
        exit 1
      fi
      content_hash="$(printf '%s' "${link_target}" | sha256sum | awk '{print $1}')"
      printf 'link\t%s\t%s\t%s\t%s\n' \
        "${mode}" "${#link_target}" "${content_hash}" "${relative_path#./}"
    elif [[ -f "${absolute_path}" ]]; then
      size="$(stat -c '%s' -- "${absolute_path}")"
      content_hash="$(sha256sum -- "${absolute_path}" | awk '{print $1}')"
      printf 'file\t%s\t%s\t%s\t%s\n' \
        "${mode}" "${size}" "${content_hash}" "${relative_path#./}"
    else
      # find 与读取之间若文件类型发生变化，不能静默生成混合状态的清单。
      echo "生成清单期间源码项发生变化: ${relative_path}" >&2
      exit 1
    fi
  done <"${paths}"
} >"${temporary}"

chmod 0644 "${temporary}"
mv -f "${temporary}" "${output}"
trap - EXIT
rm -f "${paths}"

sha256sum "${output}" | awk '{print $1}'
