# Go 迁移基线

本文件固定 Rust 迁移使用的 Go 行为来源。迁移期间 Go 工作区保持只读；如果 Go
工作树发生变化，必须重新审核并显式更新本文件、补丁和兼容性测试。

## 版本

- 仓库：`/Users/arthur/GolandProjects/environment-manager`
- Git HEAD：`65e3c65ce3f0f3b69819775d35ac448d9cd1b4fb`
- Git describe：`65e3c65-dirty`
- `go.mod`：Go 1.25
- 参考 ELF Go 版本：Go 1.25.7
- 原始工作树 diff SHA-256：`d0606591baf87fc0c16ea1d0d8dc7b7934fcd67dcc1d5a619c6e390c799de775`
- 捕获的 diff：`compat/go-baseline.patch`

## 当前 HEAD 新增行为

- `c88632d6e3c139417ec09eb4a10ec800ba4859e0`：CCR ingress token 文件逻辑。
- `4a0b1c8818b3bf70a4b7320b4ebd80e8f62fe855`：一次性 package provisioning 命令。
- `65e3c65ce3f0f3b69819775d35ac448d9cd1b4fb`：移除 legacy skill preparation flow。

关键基线文件：

- `internal/claude/claude_code_executor.go`
  - SHA-256：`9b8541e81f706e243609cbce69371c8936753f00d089b3ad5cdd4d746d97f7f7`
- `internal/claude/claude_code_executor_test.go`
  - SHA-256：`4ce510a82b4b1c73d31526a30368d3d39a2a55e3e55234237190824fee237e1b`

当前工作树另将 `FUNCTION_INDEX.md` 与 `index.html` 暂存为删除；二者不参与 Go
构建，但仍进入原始 diff 摘要和 `compat/go-baseline.patch`，防止迁移期间静默漂移。

## 相关未跟踪输入

- `Makefile`，SHA-256：`cc5f4b15b15901813f7b0ede9b72b0d91cc8b78f8e06989f0e8a143a5b057311`
- `environment-manager-06-25`（由 `.gitignore` 排除），SHA-256：`1c6a4f260f77ad12fa19186ad1fbe0b85f5a2f904bafbd5b11039f49be5f58c0`

`.idea/**`、编译产物和纯反汇编临时错误输出不作为 Rust 代码输入；ASM 对照报告及
参考 ELF 只在 Go 源码或测试无法确定行为时作为补充 oracle。

全部已知未跟踪路径固化在 `compat/go-untracked-paths.txt`；Linux ARM64 门禁拒绝
出现任何清单外的新路径。这是 fail-closed 约束，因为新 `.go`、包内汇编、syso 或
未来 `//go:embed` 资源可能绕过 tracked diff 并改变 Go oracle。

当前 `git ls-files --others --exclude-standard` 唯一返回项也是唯一未跟踪构建输入：
根目录 `Makefile`。其内容摘要固化在
`compat/go-untracked-build-inputs.sha256`。门禁会重新分类并计算未跟踪 Go/Proto、
vendor、Makefile 以及 Go 包目录内 native 文件；构建输入被删除、修改或新增都会
失败。整个检查只读 Go 工作树。

## 判定优先级

1. 本基线对应的当前 Go 工作树。
2. Go 测试、fixture 和可重复的黑盒 transcript。
3. 参考 ELF 与 ASM 对照资料。
4. Linux staging 的真实后端行为，用于确认前述材料无法确定的协议细节。
