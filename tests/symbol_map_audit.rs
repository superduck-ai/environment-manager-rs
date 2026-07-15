//! Go/Rust 符号映射清单的新鲜度审计。
//!
//! 该测试对应迁移门禁中的 `compat/go-rust-symbols.csv` 校验：每次 Go 基线、Rust
//! 文件或机械命名规则变化后必须重新生成清单，避免提交一份已经过期的状态快照。

use std::{path::PathBuf, process::Command};

/// 验证当前 CSV 与只读 Go 基线和 Rust 工作树重新扫描的结果完全一致。
///
/// 迁移实现已经收口，因此本测试同时要求映射完整以及 Go 顶层测试精确为 278/278；
/// 任何 `pending`/`symbol-missing`、漏掉的测试入口或基线测试数量变化都会失败。
#[test]
fn go_rust_symbol_map_is_current() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let go_root = std::env::var_os("GO_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/arthur/GolandProjects/environment-manager"));
    if !go_root.exists() {
        // Linux ARM64 测试机通过 OrbStack 共享目录读取只读 Go oracle；若调用方没有
        // 提供该目录，则明确跳过新鲜度重算，发布脚本仍会强制传入 GO_SOURCE_ROOT。
        eprintln!("skip symbol map freshness audit: Go baseline is unavailable");
        return;
    }

    let status = Command::new("python3")
        .arg(manifest_dir.join("scripts/compat/generate_symbol_map.py"))
        .arg("--go-root")
        .arg(go_root)
        .arg("--rust-root")
        .arg(&manifest_dir)
        .arg("--check")
        .arg("--require-complete")
        .arg("--expected-go-tests")
        .arg("278")
        .status()
        .expect("Linux ARM64 bootstrap 必须安装 python3");
    assert!(
        status.success(),
        "compat/go-rust-symbols.csv 已过期或不完整，请修复映射后重新生成"
    );
}
