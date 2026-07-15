//! Go `internal/manager/skill_extraction.go` 对应的技能 ZIP 安全解包。
//!
//! 该模块在 manager 启动早期扫描 `/mnt/skills`，把每个 ZIP 依次解压到
//! `/workspace/skills`。路径、文件 mode、失败后的条件清理和单文件/总容量限制均保留
//! Go 行为；这里仍只做词法路径约束，不额外改变 Go 对目标目录内既有 symlink 的处理。

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Result, anyhow};
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::internal::logger::{LogAttributes, Logger};

/// 技能 ZIP 的固定挂载目录。
const SKILL_ZIP_SOURCE_DIR: &str = "/mnt/skills";
/// 技能解压后的固定 workspace 目录。
pub(super) const SKILL_ZIP_DEST_DIR: &str = "/workspace/skills";
/// 单个 ZIP entry 允许解压的最大字节数（100 MiB）。
pub(super) const MAX_ZIP_ENTRY_SIZE: u64 = 100 * 1024 * 1024;
/// 单个 ZIP 所有普通文件声明大小的累计上限（500 MiB）。
pub(super) const MAX_ZIP_TOTAL_SIZE: u64 = 500 * 1024 * 1024;

/// 对应 Go `extractSkillZips`。
///
/// 扫描固定 source 目录并逐个解压 `.zip` 文件。source 不存在、没有 ZIP、无法创建
/// destination 或单个 ZIP 失败都只记录日志，不阻止 manager 启动；成功数会在结束时记录。
///
/// # 参数
/// - `logger`：manager 的具体 session logger；对应 Go 中 nil 已回退后的 `*slog.Logger`。
///
/// # 副作用
/// 创建 `/workspace/skills` 及 ZIP 中的目录/文件，可能覆盖同名普通文件。ZIP 处理顺序
/// 沿用 Linux `read_dir` 的返回顺序，和 Go `os.ReadDir` 的已排序结果差异由显式排序补齐。
pub fn extract_skill_zips(logger: &Logger) {
    let mut zip_paths = match read_skill_zip_paths(SKILL_ZIP_SOURCE_DIR) {
        Ok(paths) => paths,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            logger.info(
                "skill_extraction_skipped",
                log_attributes([
                    ("source_dir", serde_json::json!(SKILL_ZIP_SOURCE_DIR)),
                    (
                        "reason",
                        serde_json::json!("source directory does not exist"),
                    ),
                ]),
            );
            return;
        }
        Err(error) => {
            logger.warn(
                "skill_extraction_readdir_failed",
                log_attributes([
                    ("source_dir", serde_json::json!(SKILL_ZIP_SOURCE_DIR)),
                    ("error", serde_json::json!(error.to_string())),
                ]),
            );
            return;
        }
    };
    // Go `os.ReadDir` 按 filename 排序；显式排序让覆盖顺序和最终文件字节保持一致。
    zip_paths.sort();

    if zip_paths.is_empty() {
        logger.info(
            "skill_extraction_skipped",
            log_attributes([
                ("source_dir", serde_json::json!(SKILL_ZIP_SOURCE_DIR)),
                ("reason", serde_json::json!("no zip files found")),
            ]),
        );
        return;
    }

    logger.info(
        "skill_extraction_started",
        log_attributes([
            ("zip_count", serde_json::json!(zip_paths.len())),
            ("dest_dir", serde_json::json!(SKILL_ZIP_DEST_DIR)),
        ]),
    );
    if let Err(error) = create_dir_all_0755(SKILL_ZIP_DEST_DIR) {
        logger.warn(
            "skill_extraction_mkdir_failed",
            log_attributes([
                ("dest_dir", serde_json::json!(SKILL_ZIP_DEST_DIR)),
                ("error", serde_json::json!(error.to_string())),
            ]),
        );
        return;
    }

    let mut extracted = 0usize;
    for zip_path in &zip_paths {
        if let Err(error) = extract_zip(zip_path, SKILL_ZIP_DEST_DIR) {
            logger.warn(
                "skill_extraction_failed",
                log_attributes([
                    ("zip_path", serde_json::json!(zip_path.to_string_lossy())),
                    ("dest_dir", serde_json::json!(SKILL_ZIP_DEST_DIR)),
                    ("error", serde_json::json!(error.to_string())),
                ]),
            );
            continue;
        }
        extracted += 1;
    }
    logger.info(
        "skill_extraction_completed",
        log_attributes([
            ("zip_count", serde_json::json!(zip_paths.len())),
            ("extracted", serde_json::json!(extracted)),
        ]),
    );
}

/// 把 Rust 惰性 `read_dir` 收敛为与 Go `os.ReadDir` 一致的单次结果。
///
/// Go 只在 `os.ReadDir` 调用点处处理一次错误，不会为迭代阶段或 entry type 查询
/// 分别记录。该 helper 因此把所有目录读取错误原样上传，由调用方的唯一
/// `skill_extraction_readdir_failed` 日志点处理。
///
/// # 参数
/// - `source_dir`：只读扫描的技能 ZIP 目录。
///
/// # 返回
/// 成功时返回非目录且名称以 `.zip` 结尾的路径；打开目录、读取 entry
/// 或查询文件类型失败时返回对应 I/O 错误。
fn read_skill_zip_paths(source_dir: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if !file_type.is_dir() && name.to_string_lossy().ends_with(".zip") {
            paths.push(entry.path());
        }
    }
    Ok(paths)
}

/// 创建保持 Go `slog` 调用参数顺序的技能解包日志属性。
fn log_attributes(
    entries: impl IntoIterator<Item = (impl Into<String>, serde_json::Value)>,
) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect()
}

/// 对应 Go `extractZip`。
///
/// 打开一个 ZIP 并按 archive 顺序解压。destination 在调用前不存在时，任一 entry
/// 失败都会递归删除本次创建的整个 destination；若目录原本存在，则保留已经写入的
/// 部分结果，精确复刻 Go defer 的条件清理语义。
///
/// # 参数
/// - `zip_path`：待读取的 ZIP 文件。
/// - `dest_dir`：词法清理后的解压根目录。
///
/// # 错误
/// ZIP 无法打开/解析、目录创建失败、entry 非法或写入失败时返回带 Go 前缀的错误。
pub(super) fn extract_zip(zip_path: impl AsRef<Path>, dest_dir: impl AsRef<Path>) -> Result<()> {
    let zip_path = zip_path.as_ref();
    let source = File::open(zip_path)
        .map_err(|error| anyhow!("opening zip {}: {error}", zip_path.display()))?;
    let mut archive = ZipArchive::new(source)
        .map_err(|error| anyhow!("opening zip {}: {error}", zip_path.display()))?;

    let clean_dest_dir = clean_path(dest_dir.as_ref());
    let destination_was_missing = match fs::metadata(&clean_dest_dir) {
        Ok(_) => false,
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    };
    create_dir_all_0755(&clean_dest_dir)
        .map_err(|error| anyhow!("creating destination {}: {error}", clean_dest_dir.display()))?;

    let mut cleanup = DestinationCleanup::new(clean_dest_dir.clone(), destination_was_missing);
    let mut total_extracted = 0_u64;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| anyhow!("opening zip {}: {error}", zip_path.display()))?;
        extract_zip_entry(&mut entry, &clean_dest_dir, &mut total_extracted)?;
    }
    cleanup.commit();
    Ok(())
}

/// 对应 Go `extractZipEntry`。
///
/// 拒绝 symlink 与越界路径，创建目录或父目录，并在写文件前按 ZIP header 的声明大小
/// 检查单文件和累计上限。累计值在随后写入失败时也不会回退，与 Go 指针更新顺序一致。
///
/// # 参数
/// - `file`：当前 archive entry 的可读句柄。
/// - `dest_dir`：已创建的词法根目录。
/// - `total_extracted`：本 ZIP 之前所有普通 entry 的声明解压字节数。
pub(super) fn extract_zip_entry<R: Read>(
    file: &mut ZipFile<'_, R>,
    dest_dir: &Path,
    total_extracted: &mut u64,
) -> Result<()> {
    if file.is_symlink() {
        return Err(anyhow!("zip contains symlink: {}", file.name()));
    }

    let target_path = sanitize_zip_entry_path(dest_dir, file.name())?;
    if file.is_dir() {
        create_dir_all_0755(&target_path)
            .map_err(|error| anyhow!("creating directory {}: {error}", file.name()))?;
        return Ok(());
    }

    if file.size() > MAX_ZIP_ENTRY_SIZE {
        return Err(anyhow!(
            "zip entry {} exceeds max file size ({} > {})",
            file.name(),
            file.size(),
            MAX_ZIP_ENTRY_SIZE
        ));
    }
    *total_extracted = total_extracted.saturating_add(file.size());
    if *total_extracted > MAX_ZIP_TOTAL_SIZE {
        return Err(anyhow!(
            "total extracted size exceeds limit ({} > {})",
            *total_extracted,
            MAX_ZIP_TOTAL_SIZE
        ));
    }

    if let Some(parent) = target_path.parent() {
        create_dir_all_0755(parent)
            .map_err(|error| anyhow!("creating parent dir for {}: {error}", file.name()))?;
    }
    write_zip_entry(file, &target_path)
}

/// 对应 Go `writeZipEntry`。
///
/// 以 ZIP entry 的 Unix permission 创建/截断目标文件，并通过 `take(max+1)` 对实际读取
/// 字节再次设限，防止伪造 header 绕过声明大小检查。现有文件的 mode 不会被重设，和
/// Go `os.OpenFile(..., O_TRUNC, mode)` 一致。
fn write_zip_entry<R: Read>(file: &mut ZipFile<'_, R>, target_path: &Path) -> Result<()> {
    let entry_name = file.name().to_owned();
    let mode = file.unix_mode().unwrap_or(0o666) & 0o777;
    let mut output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(target_path)
        .map_err(|error| anyhow!("creating file {}: {error}", target_path.display()))?;

    let mut limited = file.take(MAX_ZIP_ENTRY_SIZE + 1);
    let written = std::io::copy(&mut limited, &mut output)
        .map_err(|error| anyhow!("writing {}: {error}", target_path.display()))?;
    output
        .flush()
        .map_err(|error| anyhow!("writing {}: {error}", target_path.display()))?;
    if written > MAX_ZIP_ENTRY_SIZE {
        return Err(anyhow!(
            "zip entry {} actual size exceeds limit during extraction",
            entry_name
        ));
    }
    Ok(())
}

/// 使用 `0755` 创建完整目录树，复刻 Go `os.MkdirAll(path, 0755)`。
///
/// `std::fs::create_dir_all` 采用 Rust 标准库默认的 `0777`，在进程 umask 为 `000`
/// 时会产生比 Go 版本更宽的权限。显式设置 mode 后，新建的每一级目录仍会正常受
/// umask 约束；已经存在的目录不会被 chmod，这一点也与 Go 保持一致。
///
/// # 错误
/// 任一级目录无法创建，或路径已被不兼容的文件占用时返回底层 I/O 错误。
pub(super) fn create_dir_all_0755(path: impl AsRef<Path>) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o755).create(path)
}

/// 对应 Go `sanitizeZipEntryPath`。
///
/// 按 Linux `filepath.Clean`/`Join` 的词法规则生成 target，并要求它等于 destination
/// 或位于其组件边界之下。该检查不解析 symlink，避免无意改变当前 Go 行为。
pub(super) fn sanitize_zip_entry_path(dest_dir: &Path, entry_name: &str) -> Result<PathBuf> {
    let clean_dest_dir = clean_path(dest_dir);
    // Go `filepath.Join(dest, entry)` 会先以分隔符拼接再整体 Clean；entry 以 `/` 开头
    // 不会像 Rust `PathBuf::join` 那样丢弃 dest，因此这里显式构造同样的原始路径。
    let mut joined = clean_dest_dir.as_os_str().to_os_string();
    joined.push("/");
    joined.push(entry_name);
    let joined = PathBuf::from(joined);
    let target_path = clean_path(&joined);
    if target_path == clean_dest_dir || target_path.starts_with(&clean_dest_dir) {
        return Ok(target_path);
    }
    Err(anyhow!(
        "zip entry {:?} resolves outside destination: {}",
        entry_name,
        target_path.display()
    ))
}

/// 以 Linux `filepath.Clean` 语义消解 `.`、`..` 和重复分隔符，但不访问文件系统。
fn clean_path(path: &Path) -> PathBuf {
    let absolute = path.is_absolute();
    let mut clean = if absolute {
        PathBuf::from("/")
    } else {
        PathBuf::new()
    };
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                if !clean.pop() && !absolute {
                    clean.push("..");
                }
            }
            Component::Normal(value) => clean.push(value),
            Component::Prefix(_) => unreachable!("Linux target has no Windows path prefix"),
        }
    }
    if clean.as_os_str().is_empty() {
        clean.push(".");
    }
    clean
}

/// 仅在 destination 由本次解压创建且尚未成功时执行 Go defer 对应的递归清理。
struct DestinationCleanup {
    path: PathBuf,
    enabled: bool,
}

impl DestinationCleanup {
    /// 创建条件清理 guard；`enabled=false` 时绝不删除调用方已有目录。
    fn new(path: PathBuf, enabled: bool) -> Self {
        Self { path, enabled }
    }

    /// 标记整份 ZIP 成功，禁止 Drop 删除结果。
    fn commit(&mut self) {
        self.enabled = false;
    }
}

impl Drop for DestinationCleanup {
    /// 在错误展开时 best-effort 删除新建 destination，忽略清理错误以保留原始失败原因。
    fn drop(&mut self) {
        if self.enabled {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
