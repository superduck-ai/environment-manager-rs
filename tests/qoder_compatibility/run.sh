#!/usr/bin/env bash
# 在相同 Linux 镜像中构建参考服务和 Rust 服务，再运行原始动态语料。
set -euo pipefail
project="$(cd "$(dirname "$0")/../.." && pwd)"
source_project="${QODER_GO_SOURCE:-/Users/arthur/GolandProjects/qoder-mcp-server}"
export QODER_COMPAT_ROOT="${project}/target/qoder-compatibility"
export QODER_COMPAT_IMAGE="${QODER_COMPAT_IMAGE:-environment-manager-qoder-oracle}"
mkdir -p "${QODER_COMPAT_ROOT}/recovery/evidence" "${QODER_COMPAT_ROOT}/recovery/tools"
docker build -q -t "${QODER_COMPAT_IMAGE}" "${project}/tests/qoder_compatibility"
docker run --rm --entrypoint bash -v "${project}:/workspace" -v qoder-rust-cargo-cache:/home/user/.cargo -w /workspace "${QODER_COMPAT_IMAGE}" -lc 'CARGO_TARGET_DIR=/workspace/target/linux cargo build --locked -p qoder-mcp-server'
docker run --rm --entrypoint bash -v "${source_project}:/oracle:ro" -v "${project}/tests/qoder_compatibility/oracle_tools:/probes:ro" -v qoder-go-cache:/root/go -v "${QODER_COMPAT_ROOT}:/out" -w /oracle "${QODER_COMPAT_IMAGE}" -lc 'set -e; go build -o /out/mcp-server ./cmd/mcp-server; go run /probes/images.go /out/recovery/images; go run /probes/image_formats.go /out/recovery/image-formats-inputs "$(go env GOMODCACHE)/golang.org/x/image@v0.16.0/testdata"'
cp "${project}/target/linux/debug/mcp-server" "${QODER_COMPAT_ROOT}/recovery/rebuilt-mcp-server"
cp "${QODER_COMPAT_ROOT}/recovery/rebuilt-mcp-server" "${QODER_COMPAT_ROOT}/recovery/file-rebuilt"
cp "${QODER_COMPAT_ROOT}/recovery/rebuilt-mcp-server" "${QODER_COMPAT_ROOT}/recovery/search-rebuilt"
cp "${QODER_COMPAT_ROOT}/recovery/rebuilt-mcp-server" "${QODER_COMPAT_ROOT}/recovery/process-rebuilt"
cp "${QODER_COMPAT_ROOT}/recovery/rebuilt-mcp-server" "${QODER_COMPAT_ROOT}/recovery/protocol-rebuilt"
cp "${project}/tests/qoder_compatibility/oracle_tools/"* "${QODER_COMPAT_ROOT}/recovery/tools/"
python3 - "${QODER_COMPAT_ROOT}" "${QODER_COMPAT_IMAGE}" <<'PY'
import pathlib,sys
for p in (pathlib.Path(sys.argv[1])/'recovery/tools').glob('*.py'):
 s=p.read_text().replace("'mcp-recovery-oracle'",repr(sys.argv[2])).replace("'--platform','linux/amd64',",'')
 p.write_text(s)
PY
if (( $# )); then
 suites=("$@")
else
 suites=(differential glob_corpus search_alignment protocol_alignment file_alignment process_alignment adversarial_literals extended transports image_formats)
fi
for suite in "${suites[@]}"; do
 python3 "${QODER_COMPAT_ROOT}/recovery/tools/${suite}.py"
done
