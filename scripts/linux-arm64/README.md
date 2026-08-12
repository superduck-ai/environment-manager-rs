# Linux ARM64 验证与发布

这些脚本唯一支持 `aarch64-unknown-linux-gnu`。macOS 只负责编辑源码和通过
OrbStack 发起命令；构建、测试、差分、产物执行与发布提升都发生在
`ubuntu-2404` ARM64 虚拟机。验证脚本同时检查 `/etc/os-release`、`uname -m`、
Debian architecture 和 `rustc -vV` 的 host triple，其他 Linux 发行版或 Rust host
均不构成发布证据。

## 镜像审核清单

[`container-images.env`](container-images.env) 是版本化镜像 allowlist。目前只允许：

- Go 1.25.7 ARM64：
  `docker.io/library/golang@sha256:f630c6aaf1842769464a1ddec76b2f5aff68783fe4479d4299ead44d8b488eb1`
- AlmaLinux 9 ARM64：
  `docker.io/library/almalinux@sha256:2c999b3bd705fad8b115741d9036ae2499148ba162752f09f2f4ab62b0c07320`

`GO_ORACLE_IMAGE` 和 `RUST_RELEASE_IMAGE` 仍可显式传入，但值必须与当前版本的
allowlist 某一项逐字相等。仅仅带有 `@sha256:` 并不足以通过。增加或替换镜像时要
提升 allowlist schema、审核实际平台与工具链，并重新完成全部本地门禁和 staging
E2E。

## 本地门禁

```bash
scripts/linux-arm64/verify.sh
```

脚本同步源码到 `/home/arthur/environment-manager` 后，依次检查 Ubuntu 24.04 ARM64、
Rust 1.97.0、原生 GNU host triple、不可变 Go 基线、格式、中文注释、rustdoc 链接、
Cargo.lock 禁止依赖、完整符号映射、100% Go logger source oracle 映射、Clippy、全部
Rust 测试、logger source 严格 transcript，以及 Go/Rust 协议差分。

`verify-cargo-dependencies.sh` 使用 Python `tomllib` 解析 Cargo 实际 lockfile，而不是
扫描 Cargo.toml 文本。它拒绝 `tonic`/`tonic-*`、`git2`/`libgit2-sys`、
`openssl`/`openssl-sys`、`native-tls`、`hyper-tls` 和 `tokio-native-tls`；
`openssl-probe` 仅用于 rustls 的系统证书路径发现，不链接 OpenSSL，因此明确允许。
该检查也由候选构建脚本独立执行，不能通过跳过 `verify.sh` 把禁止依赖带入产物。

差分 runner 唯一固定为 `scripts/compat/run_differential.sh`，默认且无条件执行。
设置 `DIFFERENTIAL_RUNNER` 会立即失败；`REQUIRE_DIFFERENTIAL=0` 也不能跳过。
runner 同时收到值完全相同的 `GO_SOURCE_ROOT` 和 `GO_SOURCE_DIR`，所以二进制 oracle
与 overlay protocol probe 使用同一棵只读 Go 工作树。

当前 runner 比较协议 transcript，不再将 Rust CLI 与 Go/Cobra 文本和参数行为
做逐字节差分。协议范围包括
HTTP API、V0/V1 input、MCP Streamable HTTP 以及 Tunnel HTTP/WebSocket ProtoJSON。
请求/响应原文不会在被测实现中归一化；input 的不可预测解析耗时保留原始整数，
仅由比较器按 manifest 中唯一审核规则验证后处理。

logger source 使用独立的 `scripts/compat/run_logger_source_differential.sh`。它将 catalog
probe 复制到容器临时目录，并从固定 Go 1.25.7 `-trimpath` ARM64 ELF 提取生产 slog
调用表达式内的 PC；随后由同一个 ELF 用 `runtime.CallersFrames` 解析
`function/file/line`。对于 canonical CLI 已证明候选 PC 不等于真实 `slog.Record.PC` 的
多行调用，生成器只接受 `compat/go-log-source-canonical-overrides.tsv` 中钉住 probe
旧值、canonical 新值和证据用例的逐项覆盖，再重建 `compat/go-log-source-oracle.tsv`。
函数、文件、调用表达式范围或 probe 原值漂移都会失败。probe 只通过不可达环境变量
分支保留原程序链接器会裁掉的 `UpdateRemoteURLs` 调用点，不执行该业务路径，也不修改
只读 Go 工作树。脚本逐字节核对仓库 oracle，再运行 Go/Rust `LogWriter` 完整行与截断
行 probe。该 transcript 不包含 time；`function`、`file`、`line` 从不归一化。

`compat/go-rust-log-sources.tsv` 的发布规则是双向且 fail-closed：每个 Rust `Logger`
调用必须解析成一个 `mapped-oracle` 规则，同时 683 个 Go 生产 slog 调用点中的每一个
必须恰好由一个 Rust runtime callsite 消费。未消费、被多个 Rust callsite 消费、source
identity 冲突、无 Go 对应的 runtime 规则、动态消息以及 Rust-only 事件都会阻断门禁。
生产源码中的 `tracing::{trace,debug,info,warn,error}!` 由独立扫描器全部标记为
`tracing-event-not-source-compatible`，不能冒充带精确 Go caller 的 `Logger` 事件；runtime
loader 对未映射调用输出显式 sentinel，也不会退回 Rust caller。

## Go 基线和两种 oracle 构建

`verify-go-baseline.sh` 除 HEAD、tracked diff 和关键文件 SHA 外，还读取：

- `compat/go-untracked-paths.txt`：已审核的全部未跟踪路径；任何新增路径均失败。
- `compat/go-untracked-build-inputs.sha256`：未跟踪 Go 构建输入及其内容摘要；删除或
  修改均失败。

检查过程只调用 Git、find 和 sha256sum 读取 Go 工作树，不写入任何文件。

正式 rollback 的唯一构建定义是：

```bash
scripts/linux-arm64/build-go-oracle.sh \
  /mnt/mac/Users/arthur/GolandProjects/environment-manager \
  /tmp/environment-manager_go_linux_arm64
```

它与未跟踪 Makefile 的 `linux-arm64` 定义一致：`CGO_ENABLED=0`、`GOOS=linux`、
`GOARCH=arm64`、`-trimpath`、`-s -w`，版本来自 `git describe`；额外关闭 VCS 自动
注入，使构建信息不受容器 Git 探测差异影响。helper 在安装前后都验证 rollback 是
静态 AArch64 ELF、没有 `INTERP` 和动态 `NEEDED`，并在 Ubuntu ARM64 宿主执行一次
限时 `--help` 烟测；release 对即将提升的最终路径再独立复核一次。

logger source 差分仍由 `scripts/compat/build_go_oracle.sh` 构建行为 oracle。它为了测试依赖
只读与复用缓存，使用 `CGO_ENABLED=1`、`-mod=readonly`、不剥离符号，并默认注入
测试版本 `0.1.0`。因此这个二进制只用于对比外部行为，不承诺与正式 rollback
逐字节相同，也不会被 release 保存。协议差分另外用 `go test -overlay` 注入 probe；
overlay 不进入 Go 工作树，也不属于 rollback 定义。

## 构建 staging 候选

```bash
scripts/linux-arm64/build-release-artifact.sh
```

脚本不接受输出目录参数。取得独占 flock 后，它先通过目录 rename 原子使旧
`dist/preflight` 失效；此后任何失败都会删除 canonical 候选路径，不能继续误用旧 SHA。
新 binary、SHA、source manifest 和 manifest 先完整写入 `dist` 内的隐藏目录，最后用
同文件系统目录 rename 一次性发布四件套。它在 allowlist 中的
AlmaLinux 9 ARM64 镜像内使用 Rust 1.97.0 构建，然后用 `file`/`readelf` 验证
AArch64 GNU ELF、动态加载器、libc、无 OpenSSL 依赖及最高 `GLIBC_2.34`。
即使不先运行 `verify.sh`，候选脚本也会重新检查只读 Go 基线，并执行 logger source
映射的 `--check --require-complete`；因此单独调用 preflight 不能把静态近似映射装进
候选产物。

候选目录包含四份互相绑定的证据：

- `environment-manager_linux_arm64`
- `environment-manager_linux_arm64.sha256`
- `environment-manager_linux_arm64.source-manifest`
- `environment-manager_linux_arm64.manifest`

源码清单确定性记录除 `.git`、`.idea`、`target`、`dist`、Finder 文件和同步 marker
外的所有文件/符号链接、mode、size 与内容 SHA。它覆盖生产代码、测试、fixture、
门禁脚本和配置。构建前后清单必须完全一致，候选 manifest 再绑定源码 SHA、产物
SHA、目标 triple、Rust 版本、allowlist 版本和 AlmaLinux 镜像 digest。manifest
不包含时间戳或主机名。

## staging E2E 后正式提升

把候选 SHA 记录到 staging E2E 结果中；全部真实后端流程通过后，以同一个 SHA
执行：

```bash
STAGING_E2E_APPROVED=1 \
STAGING_E2E_ARTIFACT_SHA256=<64位小写候选SHA> \
scripts/linux-arm64/release.sh
```

`release.sh` 会重新执行完整 `verify.sh`，随后逐字段检查 preflight manifest、候选
SHA 和审批 SHA，并复算当前源码清单。任何源码、测试、门禁或配置漂移都会要求重建
候选并重跑 staging E2E。

正式提升不会调用 Cargo，也不会调用 `build-release-artifact.sh`。它通过 `cp`、
`cmp` 和 SHA 校验把已验证候选的原字节提升为
`dist/environment-manager_linux_arm64`，同时保留其 manifest 与源码清单。

在移动 Rust 正式产物前，release 必须用统一 helper 构建并验证 Go ARM64 rollback：

- `dist/rollback/environment-manager_go_linux_arm64`
- `dist/rollback/environment-manager_go_linux_arm64.sha256`
- `dist/rollback/environment-manager_go_linux_arm64.manifest`

rollback manifest 固定记录 Go HEAD、tracked diff SHA、未跟踪基线摘要、Go 1.25.7、
构建版本、目标、allowlist 版本、镜像 digest 和产物 SHA。rollback 构建或二次 Go
基线校验失败时，Rust 候选不会被提升。

`ALMALINUX_MIRROR` 仅控制 AlmaLinux 签名软件包的 HTTPS 下载源；它不改变基础镜像
digest，DNF 仍执行发行版 GPG 校验。
