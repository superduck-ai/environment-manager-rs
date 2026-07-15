//! `internal/manager/skill_extraction_test.go` 对应的 ZIP 解包测试。

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;
use zip::ZipArchive;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use super::skill_extraction::{
    MAX_ZIP_ENTRY_SIZE, MAX_ZIP_TOTAL_SIZE, create_dir_all_0755, extract_zip, extract_zip_entry,
    sanitize_zip_entry_path,
};

const ZERO_UMASK_CHILD_DIRECTORY: &str = "ENV_MANAGER_SKILL_EXTRACTION_ZERO_UMASK_DIR";

/// 测试 ZIP 中的目录、普通文件或 symlink。
enum TestZipEntry<'a> {
    Directory(&'a str, u32),
    File(&'a str, u32, &'a [u8]),
    Symlink(&'a str, &'a str),
}

/// 创建含指定 entries 的 ZIP，并返回由 TempDir 持有的路径。
fn write_test_zip(directory: &TempDir, entries: &[TestZipEntry<'_>]) -> PathBuf {
    let zip_path = directory.path().join("skills.zip");
    let file = File::create(&zip_path).unwrap();
    let mut writer = ZipWriter::new(file);
    for entry in entries {
        match entry {
            TestZipEntry::Directory(name, mode) => writer
                .add_directory(*name, SimpleFileOptions::default().unix_permissions(*mode))
                .unwrap(),
            TestZipEntry::File(name, mode, data) => {
                writer
                    .start_file(*name, SimpleFileOptions::default().unix_permissions(*mode))
                    .unwrap();
                writer.write_all(data).unwrap();
            }
            TestZipEntry::Symlink(name, target) => writer
                .add_symlink(*name, *target, SimpleFileOptions::default())
                .unwrap(),
        }
    }
    writer.finish().unwrap();
    zip_path
}

/// 把 ZIP 的 local/central header 声明解压大小改为指定值。
///
/// 测试只修改 size 元数据而不分配对应内容，从而以很小的 fixture 真实抵达生产
/// `extract_zip_entry` 的预解压容量检查。CRC 与压缩数据不会被读取，因为限制先返回。
fn patch_zip_uncompressed_size(zip_path: &Path, size: u64) {
    let size = u32::try_from(size).expect("test size must fit classic ZIP header");
    let mut archive = fs::read(zip_path).unwrap();
    let local = archive
        .windows(4)
        .position(|window| window == b"PK\x03\x04")
        .expect("local ZIP header must be present");
    let central = archive
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .expect("central ZIP header must be present");
    archive[local + 22..local + 26].copy_from_slice(&size.to_le_bytes());
    archive[central + 24..central + 28].copy_from_slice(&size.to_le_bytes());
    fs::write(zip_path, archive).unwrap();
}

/// 验证目录、内容和 Unix mode 均按 ZIP entry 恢复。
#[test]
fn test_extract_zip_extracts_files_and_directories() {
    let archive_dir = TempDir::new().unwrap();
    let zip_path = write_test_zip(
        &archive_dir,
        &[
            TestZipEntry::Directory("session-start-hook/", 0o755),
            TestZipEntry::File("session-start-hook/SKILL.md", 0o640, b"skill body"),
        ],
    );
    let destination_root = TempDir::new().unwrap();
    let destination = destination_root.path().join("skills");
    extract_zip(&zip_path, &destination).unwrap();
    let skill = destination.join("session-start-hook/SKILL.md");
    assert_eq!(fs::read(&skill).unwrap(), b"skill body");
    assert_eq!(
        fs::metadata(skill).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

/// 验证 `..` 无法逃出 destination，且失败会清理本次新建目录。
#[test]
fn test_extract_zip_rejects_path_traversal() {
    let archive_dir = TempDir::new().unwrap();
    let zip_path = write_test_zip(
        &archive_dir,
        &[TestZipEntry::File("../outside.txt", 0o644, b"escape")],
    );
    let root = TempDir::new().unwrap();
    let destination = root.path().join("skills");
    let error = extract_zip(zip_path, &destination).unwrap_err();
    assert!(error.to_string().contains("resolves outside destination"));
    assert!(!root.path().join("outside.txt").exists());
    assert!(!destination.exists());
}

/// 验证 archive symlink 在创建任何链接前被拒绝。
#[test]
fn test_extract_zip_rejects_symlink() {
    let archive_dir = TempDir::new().unwrap();
    let zip_path = write_test_zip(&archive_dir, &[TestZipEntry::Symlink("link", "target")]);
    let root = TempDir::new().unwrap();
    let error = extract_zip(zip_path, root.path().join("skills")).unwrap_err();
    assert!(error.to_string().contains("zip contains symlink"));
}

/// 验证声明的单 entry 与累计容量限制真实拒绝越界 entry。
#[test]
fn test_extract_zip_entry_rejects_size_limits() {
    assert_eq!(MAX_ZIP_ENTRY_SIZE, 100 * 1024 * 1024);
    assert_eq!(MAX_ZIP_TOTAL_SIZE, 500 * 1024 * 1024);

    let oversized_archive_dir = TempDir::new().unwrap();
    let oversized_zip = write_test_zip(
        &oversized_archive_dir,
        &[TestZipEntry::File("large.bin", 0o600, b"x")],
    );
    patch_zip_uncompressed_size(&oversized_zip, MAX_ZIP_ENTRY_SIZE + 1);
    let oversized_file = File::open(&oversized_zip).unwrap();
    let mut oversized_archive = ZipArchive::new(oversized_file).unwrap();
    let mut oversized_entry = oversized_archive.by_index(0).unwrap();
    let oversized_destination = TempDir::new().unwrap();
    let mut total = 0;
    let error = extract_zip_entry(
        &mut oversized_entry,
        oversized_destination.path(),
        &mut total,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeds max file size"));

    let cumulative_archive_dir = TempDir::new().unwrap();
    let cumulative_zip = write_test_zip(
        &cumulative_archive_dir,
        &[TestZipEntry::File("small.bin", 0o600, b"ok")],
    );
    let cumulative_file = File::open(cumulative_zip).unwrap();
    let mut cumulative_archive = ZipArchive::new(cumulative_file).unwrap();
    let mut cumulative_entry = cumulative_archive.by_index(0).unwrap();
    let cumulative_destination = TempDir::new().unwrap();
    let mut total = MAX_ZIP_TOTAL_SIZE - 1;
    let error = extract_zip_entry(
        &mut cumulative_entry,
        cumulative_destination.path(),
        &mut total,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("total extracted size exceeds limit")
    );
}

/// 验证合法嵌套路径和 traversal 的词法处理。
#[test]
fn test_sanitize_zip_entry_path() {
    let root = TempDir::new().unwrap();
    let destination = root.path().join("skills");
    assert_eq!(
        sanitize_zip_entry_path(&destination, "nested/SKILL.md").unwrap(),
        destination.join("nested/SKILL.md")
    );
    assert!(sanitize_zip_entry_path(&destination, "../outside.txt").is_err());
    assert_eq!(
        sanitize_zip_entry_path(&destination, "/outside.txt").unwrap(),
        destination.join("outside.txt")
    );
}

/// 保留 Path 类型在测试签名中的 Linux 组件语义，避免误引入 Windows separator 分支。
#[test]
fn destination_is_linux_path() {
    assert!(Path::new("/workspace/skills").is_absolute());
}

/// 在独立测试进程把 umask 设为 `000`，证明目录仍按 Go 的 `0755` 请求创建。
///
/// umask 是进程全局状态，不能在并行测试线程中安全切换；父测试只负责启动当前
/// test harness 的单一精确用例，子进程完成全局修改与创建后立即退出。
#[test]
fn test_skill_extraction_directories_use_0755_under_zero_umask() {
    if let Some(path) = std::env::var_os(ZERO_UMASK_CHILD_DIRECTORY) {
        // SAFETY: 该分支只在专用子进程的唯一测试中运行，进程退出前不会创建并行线程
        // 或执行其他用例，因此修改进程级 umask 不会与同进程文件操作竞争。
        unsafe { libc::umask(0) };
        create_dir_all_0755(PathBuf::from(path).join("first/second"))
            .expect("以 Go mode 创建目录树");
        return;
    }

    let root = TempDir::new().expect("创建父进程临时目录");
    let target = root.path().join("skills");
    let status = Command::new(std::env::current_exe().expect("定位当前测试二进制"))
        .args([
            "--exact",
            "internal::manager::skill_extraction_test::test_skill_extraction_directories_use_0755_under_zero_umask",
            "--nocapture",
        ])
        .env(ZERO_UMASK_CHILD_DIRECTORY, &target)
        .status()
        .expect("启动零 umask 子测试");
    assert!(status.success(), "零 umask 子测试失败: {status}");
    for path in [target.join("first"), target.join("first/second")] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
