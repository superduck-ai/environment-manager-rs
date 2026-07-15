# Go/Rust 符号映射门禁

`go-rust-symbols.csv` 由 `scripts/compat/generate_symbol_map.py` 从只读 Go 基线和当前
Rust 源码重新生成。CSV 将机械期望路径与实际命中的 Rust 路径、行号和符号分开，
因此文件拆分或方法 owner 变化不会被一个模糊的同名函数掩盖。

## 状态

- `file-present`：机械期望的 Rust 文件存在；它不代表文件内所有符号已经迁移。
- `file-split`：机械期望文件不存在，但其中至少一个符号在同 package 的其他 Rust
  文件中得到严格匹配。
- `migrated`：在同 package 范围内找到唯一 Rust 声明；方法还必须匹配 receiver/impl
  owner。严格的 Go `NewFoo` 还可映射到 owner 精确匹配的 Rust `Foo::new`；其他
  非机械映射必须由紧邻 Rust 声明的 doc code span 明确引用 Go 符号。
  `rust_path`、`rust_line` 和 `rust_symbol` 均来自实际源码。
- `symbol-missing`：Rust package 已存在，但没有匹配声明。
- `pending`：Rust 文件/package 尚不存在，或缺失文件中没有任何严格符号匹配。
- `ambiguous`：存在多个可能声明，生成器拒绝猜测。
- `name-only-match`：只找到唯一同名声明，但自由函数/方法形态或 owner 不符；它只是
  人工复核线索，不算迁移完成。
- `generated-wrapper`：仓库内存在 Rust 生成代码 wrapper。
- `generated-exempt`：仅用于带标准 `protoc-gen-go` header 的 `gen/proto/**/*.pb.go`。
  对应 Rust API 由 prost/pbjson 写入 `OUT_DIR`，所以清单不伪造逐个 Rust 方法名。
- `generated-wrapper-missing`：Go 文件虽可识别为协议生成物，但 Rust wrapper 缺失，
  仍属于未完成状态。

## 使用

```bash
python3 scripts/compat/generate_symbol_map.py --summary
python3 scripts/compat/generate_symbol_map.py --check
python3 scripts/compat/generate_symbol_map.py --check --require-complete \
  --expected-go-tests 278 --summary
```

`--check` 验证 CSV 与两棵源码树一致；`--require-complete` 还会拒绝 `pending`、
`symbol-missing`、`ambiguous`、`name-only-match` 和缺少 wrapper 的生成文件。明确的
`generated-exempt` 是审计豁免，不会被报告成 `migrated`。

Linux ARM64 发布门禁额外传入 `--expected-go-tests 278`。它同时校验当前只读 Go
基线仍精确包含 278 个顶层 `Test*` 场景，以及这 278 项在 CSV 中全部处于
`migrated`；因此仅补普通 helper 或减少测试入口都不能让门禁误通过。

## Go 工作树输入边界

`go-baseline.md`、`go-baseline.patch`、`go-untracked-paths.txt` 和
`go-untracked-build-inputs.sha256` 共同定义只读 Go oracle。Linux ARM64 门禁既校验
HEAD/tracked diff，也拒绝出现未审核的未跟踪路径，并对未跟踪 Go 构建输入逐文件
核对 SHA。这样新出现但不进入 `git diff` 的 `.go`、包内 native 文件或 embed 资源
不能静默改变 oracle。

Rust preflight 的 `*.source-manifest` 是另一层边界：它绑定当前 Rust 代码、测试、
fixture、配置与门禁脚本。正式 release 只有在重新执行 verify 后复算出同一清单，
且 staging E2E 提供的候选 SHA 精确匹配时，才会原字节提升该候选。
