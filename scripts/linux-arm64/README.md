# Linux ARM64 验证与发布

这里的脚本是自包含的 Rust 构建、验证和发布流程。

## 权威验证

`verify.sh` 先将源码同步到 OrbStack 的 Ubuntu 24.04 ARM64 虚拟机，再依次执行：

- 校验 Ubuntu 24.04、AArch64 和 Rust 1.97.0 GNU host；
- 审计 `Cargo.lock` 中禁止的依赖；
- `cargo fmt --all --check`；
- 中文注释与 rustdoc 链接审计；
- `cargo clippy --all-targets --locked -- -D warnings`；
- `cargo test --locked`。

任一门禁失败都会汇总并返回非零状态。

## 候选构建

`build-release-artifact.sh` 在审核过的 AlmaLinux 9 ARM64 镜像中构建 Rust
release，并将唯一候选写入 `dist/preflight`。镜像必须与
`container-images.env` 中固定的 digest 完全一致。

候选目录恰好包含四份证据：

- `environment-manager_linux_arm64`；
- `environment-manager_linux_arm64.sha256`；
- `environment-manager_linux_arm64.source-manifest`；
- `environment-manager_linux_arm64.manifest`。

源码清单不含时间戳，可在正式提升前逐字节复算。构建期间源码发生变化时，候选
会被拒绝。

## 正式提升与回滚

完成 staging E2E 后，以候选的精确 SHA 执行：

```bash
STAGING_E2E_APPROVED=1 \
STAGING_E2E_ARTIFACT_SHA256=<sha256> \
scripts/linux-arm64/release.sh
```

`release.sh` 会重新执行权威验证、复算源码清单，并把 staging 已验证的同一 Rust
二进制提升到 `dist`；它不会重新构建。

回滚使用上一版已经发布并留存的 Rust artifact；当前发布流程只处理 Rust 产物。
