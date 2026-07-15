#!/usr/bin/env bash
# 在不修改 Go 工作树的前提下验证迁移 oracle 仍与固化基线完全一致。
set -euo pipefail

readonly go_root="${1:-${GO_SOURCE_ROOT:-}}"
readonly expected_head="0706a81d488857f87cf13b68f5c8d2bb54b61834"
readonly expected_diff="fa90df15874d704b257f68c16ca0983c24af705e82d980ef3e762d852bae87ca"
readonly rust_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
readonly expected_untracked="${rust_root}/compat/go-untracked-paths.txt"
readonly expected_build_inputs="${rust_root}/compat/go-untracked-build-inputs.sha256"

if [[ -z "${go_root}" || ! -d "${go_root}/.git" ]]; then
  echo "Go 基线目录不存在或不是 Git 工作树: ${go_root:-<empty>}" >&2
  exit 1
fi

actual_head="$(git -C "${go_root}" rev-parse HEAD)"
if [[ "${actual_head}" != "${expected_head}" ]]; then
  echo "Go HEAD 已变化: 期望 ${expected_head}，实际 ${actual_head}" >&2
  exit 1
fi

# `LC_ALL=C` 固定 Git 文本输出；`--binary` 确保二进制补丁也进入摘要。
actual_diff="$(LC_ALL=C git -C "${go_root}" diff --binary HEAD \
  | sha256sum | awk '{print $1}')"
if [[ "${actual_diff}" != "${expected_diff}" ]]; then
  echo "Go 工作树 diff 已变化: 期望 ${expected_diff}，实际 ${actual_diff}" >&2
  exit 1
fi

if [[ ! -f "${expected_untracked}" || ! -f "${expected_build_inputs}" ]]; then
  echo "Go 未跟踪输入基线文件缺失" >&2
  exit 1
fi

actual_untracked="$(mktemp)"
new_untracked="$(mktemp)"
actual_build_inputs="$(mktemp)"
trap 'rm -f "${actual_untracked}" "${new_untracked}" "${actual_build_inputs}"' EXIT

# 先固化所有已知未跟踪路径并拒绝任何新增项。Go 会自动编译包目录中新出现的
# .go/.s/.syso 等文件，因此仅核对 tracked diff 不足以保证 oracle 输入不变；
# 对全部新增路径采取 fail-closed 也覆盖未来可能被 //go:embed 引用的资源。
LC_ALL=C git -c core.quotepath=false -C "${go_root}" \
  ls-files --others --exclude-standard \
  | LC_ALL=C sort >"${actual_untracked}"
LC_ALL=C comm -13 "${expected_untracked}" "${actual_untracked}" >"${new_untracked}"
if [[ -s "${new_untracked}" ]]; then
  echo "Go 工作树出现未审核的未跟踪文件:" >&2
  sed 's/^/  - /' "${new_untracked}" >&2
  exit 1
fi

# 当前未跟踪构建输入只有根 Makefile；分类逻辑也覆盖未来经审核加入的 Go、Proto、
# vendor 与包内 native 文件。内容摘要要求已知构建输入既不能被删除也不能被修改。
is_untracked_go_build_input() {
  local relative_path="$1"
  local directory
  case "${relative_path}" in
    Makefile|GNUmakefile|go.mod|go.sum|go.work|go.work.sum|*.go|*.proto|vendor/*)
      return 0
      ;;
    *.c|*.cc|*.cpp|*.cxx|*.h|*.hh|*.hpp|*.s|*.S|*.syso|*.swig|*.swigcxx)
      directory="$(dirname "${relative_path}")"
      [[ -n "$(find "${go_root}/${directory}" -maxdepth 1 -type f \
        -name '*.go' -print -quit 2>/dev/null)" ]]
      return
      ;;
    *) return 1 ;;
  esac
}

while IFS= read -r relative_path; do
  if is_untracked_go_build_input "${relative_path}"; then
    if [[ ! -f "${go_root}/${relative_path}" ]]; then
      echo "Go 未跟踪构建输入不是普通文件: ${relative_path}" >&2
      exit 1
    fi
    printf '%s  %s\n' \
      "$(sha256sum "${go_root}/${relative_path}" | awk '{print $1}')" \
      "${relative_path}"
  fi
done <"${actual_untracked}" | LC_ALL=C sort -k2 >"${actual_build_inputs}"

if ! cmp -s "${expected_build_inputs}" "${actual_build_inputs}"; then
  echo "Go 未跟踪构建输入清单或内容发生变化:" >&2
  diff -u "${expected_build_inputs}" "${actual_build_inputs}" >&2 || true
  exit 1
fi

check_file_hash() {
  local relative_path="$1"
  local expected_hash="$2"
  local file="${go_root}/${relative_path}"
  if [[ ! -f "${file}" ]]; then
    echo "Go 基线文件缺失: ${relative_path}" >&2
    return 1
  fi
  local actual_hash
  actual_hash="$(sha256sum "${file}" | awk '{print $1}')"
  if [[ "${actual_hash}" != "${expected_hash}" ]]; then
    echo "Go 基线文件已变化: ${relative_path}" >&2
    echo "期望 ${expected_hash}，实际 ${actual_hash}" >&2
    return 1
  fi
}

check_file_hash \
  "internal/claude/claude_code_executor.go" \
  "26a70bad574108d8b3344003b965e8edc79085f2087bf362456c01ca3791628b"
check_file_hash \
  "internal/claude/claude_code_executor_test.go" \
  "4ce510a82b4b1c73d31526a30368d3d39a2a55e3e55234237190824fee237e1b"
check_file_hash \
  "Makefile" \
  "cc5f4b15b15901813f7b0ede9b72b0d91cc8b78f8e06989f0e8a143a5b057311"
check_file_hash \
  "environment-manager-06-25" \
  "1c6a4f260f77ad12fa19186ad1fbe0b85f5a2f904bafbd5b11039f49be5f58c0"

if ! grep -Eq '^go 1\.25$' "${go_root}/go.mod"; then
  echo "Go 基线的 go.mod 不再声明 Go 1.25" >&2
  exit 1
fi

echo "Go 迁移基线校验通过: ${actual_head}"
