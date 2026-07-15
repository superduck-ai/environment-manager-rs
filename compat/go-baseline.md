# Go 迁移基线

本文件固定 Rust 迁移使用的 Go 行为来源。迁移期间 Go 工作区保持只读；如果 Go
工作树发生变化，必须重新审核并显式更新本文件、补丁和兼容性测试。

## 版本

- 仓库：`/Users/arthur/GolandProjects/environment-manager`
- Git HEAD：`0706a81d488857f87cf13b68f5c8d2bb54b61834`
- Git describe：`0706a81-dirty`
- `go.mod`：Go 1.25
- 参考 ELF Go 版本：Go 1.25.7
- 原始工作树 diff SHA-256：`fa90df15874d704b257f68c16ca0983c24af705e82d980ef3e762d852bae87ca`
- 捕获的 diff：`compat/go-baseline.patch`

## 已修改文件

- `.gitignore`
- `internal/claude/claude_code_executor.go`
  - SHA-256：`26a70bad574108d8b3344003b965e8edc79085f2087bf362456c01ca3791628b`
- `internal/claude/claude_code_executor_test.go`
  - SHA-256：`4ce510a82b4b1c73d31526a30368d3d39a2a55e3e55234237190824fee237e1b`

CCR upstream proxy token 的未提交实现属于正式迁移基线，不得遗漏。

## 相关未跟踪输入

- `Makefile`，SHA-256：`cc5f4b15b15901813f7b0ede9b72b0d91cc8b78f8e06989f0e8a143a5b057311`
- `asm-analysis/**`
- `docs/plans/2026-06-30-asm-vs-source-comparison.md`
- `environment-manager-06-25`，SHA-256：`1c6a4f260f77ad12fa19186ad1fbe0b85f5a2f904bafbd5b11039f49be5f58c0`

`.idea/**`、编译产物和纯反汇编临时错误输出不作为 Rust 代码输入；ASM 对照报告及
参考 ELF 只在 Go 源码或测试无法确定行为时作为补充 oracle。

全部已知未跟踪路径固化在 `compat/go-untracked-paths.txt`；Linux ARM64 门禁拒绝
出现任何清单外的新路径。这是 fail-closed 约束，因为新 `.go`、包内汇编、syso 或
未来 `//go:embed` 资源可能绕过 tracked diff 并改变 Go oracle。

当前唯一未跟踪构建输入是根目录 `Makefile`，其内容摘要固化在
`compat/go-untracked-build-inputs.sha256`。门禁会重新分类并计算未跟踪 Go/Proto、
vendor、Makefile 以及 Go 包目录内 native 文件；构建输入被删除、修改或新增都会
失败。整个检查只读 Go 工作树。

## 判定优先级

1. 本基线对应的当前 Go 工作树。
2. Go 测试、fixture 和可重复的黑盒 transcript。
3. 参考 ELF 与 ASM 对照资料。
4. Linux staging 的真实后端行为，用于确认前述材料无法确定的协议细节。
