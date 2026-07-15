//! `internal/manager/skill_symlink_test.go` 对应的目录 symlink 测试。

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::process::Command;

use tempfile::TempDir;

use super::skill_symlink::link_workspace_skills_dir_to_claude_dir_under;
use crate::internal::logger::default_slog_logger;

const ZERO_UMASK_CHILD_ROOT: &str = "ENV_MANAGER_SKILL_SYMLINK_ZERO_UMASK_ROOT";

/// 验证首次调用创建 source、Claude 目录和精确目标 symlink。
#[test]
fn test_link_workspace_skills_dir_to_claude_dir_creates_directory_symlink() {
    let source_root = TempDir::new().unwrap();
    let home_root = TempDir::new().unwrap();
    let source = source_root.path().join("workspace/skills");
    let home = home_root.path().join("home");
    link_workspace_skills_dir_to_claude_dir_under(default_slog_logger(), &source, &home).unwrap();
    let link = home.join(".claude/skills");
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read_link(link).unwrap(), source);
}

/// 验证空 skills 目录会被安全替换为链接。
#[test]
fn test_link_workspace_skills_dir_to_claude_dir_replaces_empty_skills_dir() {
    let source_root = TempDir::new().unwrap();
    let home_root = TempDir::new().unwrap();
    let source = source_root.path().join("workspace/skills");
    let home = home_root.path().join("home");
    let target = home.join(".claude/skills");
    fs::create_dir_all(&target).unwrap();
    link_workspace_skills_dir_to_claude_dir_under(default_slog_logger(), &source, &home).unwrap();
    assert!(
        fs::symlink_metadata(target)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

/// 验证非空用户 skills 目录不会被删除或替换。
#[test]
fn test_link_workspace_skills_dir_to_claude_dir_preserves_non_empty_skills_dir() {
    let source_root = TempDir::new().unwrap();
    let home_root = TempDir::new().unwrap();
    let source = source_root.path().join("workspace/skills");
    let home = home_root.path().join("home");
    let target = home.join(".claude/skills");
    fs::create_dir_all(target.join("session-start-hook")).unwrap();
    assert!(
        link_workspace_skills_dir_to_claude_dir_under(default_slog_logger(), &source, &home)
            .is_err()
    );
    assert!(fs::symlink_metadata(target).unwrap().is_dir());
}

/// 在独立进程的 `umask=000` 环境验证 source 与 Claude 目录都保持 Go 的 `0755`。
#[test]
fn test_skill_symlink_directories_use_0755_under_zero_umask() {
    if let Some(root) = std::env::var_os(ZERO_UMASK_CHILD_ROOT) {
        // SAFETY: 子进程只执行本精确测试，修改 umask 后不会运行其他并行测试。
        unsafe { libc::umask(0) };
        let root = std::path::PathBuf::from(root);
        link_workspace_skills_dir_to_claude_dir_under(
            default_slog_logger(),
            &root.join("workspace/skills"),
            &root.join("home"),
        )
        .expect("在零 umask 下准备 skills 链接");
        return;
    }

    let root = TempDir::new().expect("创建父进程临时目录");
    let target = root.path().join("fixture");
    let status = Command::new(std::env::current_exe().expect("定位当前测试二进制"))
        .args([
            "--exact",
            "internal::manager::skill_symlink_test::test_skill_symlink_directories_use_0755_under_zero_umask",
            "--nocapture",
        ])
        .env(ZERO_UMASK_CHILD_ROOT, &target)
        .status()
        .expect("启动零 umask 子测试");
    assert!(status.success(), "零 umask 子测试失败: {status}");
    for path in [target.join("workspace/skills"), target.join("home/.claude")] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
