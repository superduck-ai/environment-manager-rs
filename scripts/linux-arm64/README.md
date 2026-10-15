# Linux ARM64 验证与发布

这里的脚本是自包含的 Rust 构建、验证和发布流程。

## Docker 编译环境

在仓库根目录执行：

```bash
just build
# 或直接调用脚本
scripts/linux-arm64/docker-build.sh
```

脚本构建本地镜像 `environment-manager-rust:1.97.0-arm64`，再编译当前仓库。
可追加 Cargo build 参数，例如 `scripts/linux-arm64/docker-build.sh --features <feature>`。
可执行文件写入：

```text
target/docker-linux-arm64/aarch64-unknown-linux-gnu/release/environment-manager
```

镜像使用 AlmaLinux 9 ARM64 和 Rust 1.97.0，目标是
`aarch64-unknown-linux-gnu`，链接 glibc。运行目标系统需提供 glibc 2.34
或更新版本及产物所需的动态库。这个环境不生成 musl 静态可执行文件。
Docker 必须支持 `linux/arm64`；ARM64 宿主可直接运行，x86_64 宿主需先配置
ARM64 的 QEMU/binfmt 模拟，Docker Desktop 通常已提供该能力。

镜像内已设置：

```bash
export RUSTUP_DIST_SERVER="https://rsproxy.cn"
export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
```

`config.toml` 安装到容器的 `/root/.cargo/config.toml`，使用 Cargo 推荐的
配置文件名，与 `~/.cargo/config` 作用相同。默认使用 rsproxy sparse 索引，
同时保留 Git 索引配置，启用 `git-fetch-with-cli`。
依赖下载缓存保存在 Docker 的 `environment-manager-arm64-registry` 和
`environment-manager-arm64-git` 卷中，编译缓存位于 `target/docker-linux-arm64`。
Linux 宿主上，容器默认以 root 写入挂载目录，产物可能归 root 所有。

也可以单独创建镜像，并进入交互式编译环境：

```bash
docker build --platform linux/arm64 \
  -t environment-manager-rust:1.97.0-arm64 scripts/linux-arm64
docker run --rm -it --platform linux/arm64 \
  -v "$PWD:/workspace" \
  -e CARGO_TARGET_DIR=/workspace/target/docker-linux-arm64 \
  environment-manager-rust:1.97.0-arm64
# 在容器中执行
cargo build --release --locked --target aarch64-unknown-linux-gnu
```

构建镜像时可通过 `--build-arg RUST_VERSION=<版本>` 选择其他 Rust 版本，
但当前仓库要求 Rust 1.97.0。这个入口用于日常编译；正式发布仍使用下面的候选
构建和提升流程。

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
