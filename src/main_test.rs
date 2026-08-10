//! `provision-packages` 完整二进制的敏感输入防泄露测试。

use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

use assert_cmd::cargo::CommandCargoExt as _;

/// 对应 Go `TestProvisionPackagesBinaryDoesNotLeakRejectedSpecs`。
#[test]
fn test_provision_packages_binary_does_not_leak_rejected_specs() {
    let binary_path = build_environment_manager_binary();
    let cases = [
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["--codex-sensitive-option-marker"]}}"#,
            &["--codex-sensitive-option-marker"][..],
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["codex-sensitive-package @ https://codex-sensitive-user:codex-sensitive-password@example.test/codex-sensitive-package.whl"]}}"#,
            &[
                "codex-sensitive-package",
                "codex-sensitive-user",
                "codex-sensitive-password",
            ][..],
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["codex-sensitive-package @ https://codex-sensitive-user:%zz@example.test/codex-sensitive-package.whl"]}}"#,
            &["codex-sensitive-package", "codex-sensitive-user", "%zz"][..],
        ),
    ];

    for (input, forbidden) in cases {
        let mut child = Command::new(&binary_path)
            .args(["provision-packages", "--protocol", "v1", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write as _;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(
            output.stdout.iter().filter(|byte| **byte == b'\n').count(),
            1
        );
        assert!(output.stderr.len() <= 256);
        assert_eq!(
            output.stderr.iter().filter(|byte| **byte == b'\n').count(),
            1
        );
        let combined = [output.stdout, output.stderr].concat();
        let combined = String::from_utf8_lossy(&combined);
        for value in forbidden {
            assert!(!combined.contains(value), "process output leaked {value:?}");
        }
    }
}

/// 对应 Go `buildEnvironmentManagerBinary`，定位 Cargo 为当前测试构建的真实二进制。
///
/// Rust 测试 harness 已在运行测试前完成构建，因此无需像 Go helper 那样再次调用编译器；
/// 返回值仍是独立子进程测试使用的绝对或 Cargo 可解析路径，不修改工作树。
#[must_use]
fn build_environment_manager_binary() -> PathBuf {
    Command::cargo_bin("environment-manager")
        .expect("Cargo 应向集成测试提供 environment-manager 二进制")
        .get_program()
        .into()
}
