//! package manifest 解码、预检与固定安装计划的应用层。
//!
//! 本模块不记录 package spec、不拼接 shell，
//! 只生成固定 argv 并顺序调用 executor；首个失败立即终止。Tokio cancellation 与可选
//! deadline 表达取消边界，最终只返回不含 manifest 或子进程输出的结果。

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use nix::unistd::{AccessFlags, access, geteuid};
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio_util::sync::CancellationToken;

use super::{
    manifest::{MAX_MANIFEST_BYTES, Manifest, PackageManifest, validate_manifest},
    result::{Category, Manager, Result, Stage, Status},
};

/// package manager 固定执行目录。
const SANDBOX_WORK_DIR: &str = "/home/user";

/// 单次 package manager 调用的完整 argv 与失败归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Command {
    /// 结果中报告的 manager。
    pub manager: Manager,
    /// 结果中报告的 prepare/install 阶段。
    pub stage: Stage,
    /// 由 `PATH` 查找的固定可执行文件名。
    pub path: String,
    /// 不经 shell 展开的独立 argv 元素。
    pub args: Vec<String>,
}

/// executor 返回给应用层的最小、无敏感信息状态。
#[derive(Debug)]
pub(super) struct CommandResult {
    /// 子进程退出码；被信号终止时为 `-1`。
    pub exit_code: i32,
    /// 是否成功创建过子进程。
    pub started: bool,
    /// Linux wait status 是否表明子进程被信号终止。
    pub signaled: bool,
    /// 启动、等待或取消是否产生错误；正文永不进入外部结果。
    pub error: Option<io::Error>,
}

/// package manager executor 的可替换边界。
#[async_trait]
pub(super) trait CommandRunner: Send + Sync {
    /// 执行单个固定 argv，并返回不含 stdout/stderr 的进程状态。
    ///
    /// # 副作用
    /// 生产实现创建独立进程组；取消时向整个组发送 `SIGKILL` 并等待直接子进程。
    async fn run(&self, context: &ProvisionContext, command: &Command) -> CommandResult;
}

/// 取消上下文 在 package provisioning 内需要的取消与 deadline 子集。
#[derive(Debug, Clone)]
pub(crate) struct ProvisionContext {
    cancellation: CancellationToken,
    deadline: Option<tokio::time::Instant>,
}

impl ProvisionContext {
    /// 从外部取消 token 创建没有 deadline 的生产上下文。
    #[must_use]
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            deadline: None,
        }
    }

    /// 创建带 deadline 的上下文，供嵌入调用和 timeout 行为测试使用。
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_deadline(
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            cancellation,
            deadline: Some(deadline),
        }
    }

    /// 返回 token 已取消或 deadline 已到期。
    #[must_use]
    pub(super) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
            || self
                .deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
    }

    /// 异步等待 token 取消或 deadline 到期，以便 executor 中断在途子进程。
    pub(crate) async fn cancelled(&self) {
        match self.deadline {
            Some(deadline) => {
                tokio::select! {
                    () = self.cancellation.cancelled() => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => self.cancellation.cancelled().await,
        }
    }

    /// 返回当前取消原因的稳定协议分类；到期 deadline 优先于普通 token。
    ///
    #[must_use]
    pub(super) fn category(&self) -> Category {
        if self
            .deadline
            .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            Category::Timeout
        } else {
            Category::Cancelled
        }
    }
}

/// 校验 manifest 并执行固定 package-manager 计划。
pub struct Provisioner {
    runner: Arc<dyn CommandRunner>,
    get_uid: Arc<dyn Fn() -> u32 + Send + Sync>,
    work_dir: PathBuf,
}

impl Provisioner {
    /// 创建固定 `/home/user`、真实 UID 与 Tokio executor。
    #[must_use]
    pub fn new() -> Self {
        Self::with_dependencies(
            Arc::new(super::executor::ExecCommandRunner::new(SANDBOX_WORK_DIR)),
            Arc::new(|| geteuid().as_raw()),
            PathBuf::from(SANDBOX_WORK_DIR),
        )
    }

    /// 注入 executor、UID 和工作目录，供无 root/无真实安装副作用的单元测试使用。
    ///
    /// Rust 用关联构造器承载同一依赖注入边界。
    #[must_use]
    pub(super) fn with_dependencies(
        runner: Arc<dyn CommandRunner>,
        get_uid: Arc<dyn Fn() -> u32 + Send + Sync>,
        work_dir: PathBuf,
    ) -> Self {
        Self {
            runner,
            get_uid,
            work_dir,
        }
    }

    /// 读取 stdin、严格校验并顺序执行安装计划。
    /// manifest 解码直接处理取消，不为单个调用点保留独立 reader 类型或转发函数。
    ///
    /// # 参数
    /// - `context`：取消 token 与可选 deadline；decode 错误发生时若已取消，优先报告取消。
    /// - `input`：最大读取 1 MiB 的异步 stdin；内容不会进入日志或错误结果。
    ///
    /// # 返回
    /// 始终返回 v1 终态结果；调用方负责只序列化一次并按分类设置进程退出码。
    ///
    /// # 副作用
    /// schema 合法且预检通过后，按 apt、cargo、gem、go、npm、pip 顺序创建子进程。
    pub(crate) async fn run<R>(&self, context: &ProvisionContext, input: &mut R) -> Result
    where
        R: AsyncRead + Unpin + Send + ?Sized,
    {
        let started = Instant::now();
        let mut result = Result::failed();

        // 取消只与 manifest 解码竞争，不能包围整个 provisioning future：安装阶段由
        // executor 自己终止并回收进程组，直接丢弃该 future 会绕过这条清理路径。
        // `biased` 让已经就绪的解码优先：总是先调用 decodeManifest，合法的内存
        // reader 即使 context 已取消也会继续走到 preflight；只有仍为 Pending 的 stdin
        // 才由取消分支打断。若解码报错，下面仍再次检查取消状态，使同时发生的取消
        // 优先于解码错误。
        let manifest = tokio::select! {
            biased;
            manifest = decode_manifest(input) => manifest,
            () = context.cancelled() => {
                result.category = Some(context.category());
                result.stage = Some(Stage::Decode);
                return finish_result(result, started);
            }
        };
        let manifest = match manifest {
            Ok(manifest) => manifest,
            Err(ManifestSchemaError::Decode) => {
                result.category = Some(if context.is_cancelled() {
                    context.category()
                } else {
                    Category::InvalidManifest
                });
                result.stage = Some(Stage::Decode);
                return finish_result(result, started);
            }
            Err(ManifestSchemaError::Schema) => {
                result.category = Some(Category::InvalidManifest);
                result.stage = Some(Stage::Validate);
                return finish_result(result, started);
            }
        };
        if validate_manifest(&manifest).is_err() {
            result.category = Some(Category::InvalidManifest);
            result.stage = Some(Stage::Validate);
            return finish_result(result, started);
        }

        let packages = manifest
            .packages
            .as_ref()
            .expect("validated manifest must contain packages");
        result.package_count = Some(packages.count());
        if (self.get_uid)() != 0 || !is_executable_directory(&self.work_dir) {
            result.category = Some(Category::ExecutionEnvironment);
            result.stage = Some(Stage::Preflight);
            return finish_result(result, started);
        }

        let commands = packages.plan();
        if commands.is_empty() {
            result.status = Status::Succeeded;
            return finish_result(result, started);
        }
        if context.is_cancelled() {
            result.category = Some(context.category());
            result.stage = Some(Stage::Preflight);
            return finish_result(result, started);
        }

        for command in commands {
            let command_result = self.runner.run(context, &command).await;
            if command_result.error.is_none() && command_result.exit_code == 0 {
                continue;
            }
            result.manager = Some(command.manager);
            result.stage = Some(command.stage);
            if context.is_cancelled() {
                result.category = Some(context.category());
            } else if !command_result.started {
                result.category = Some(Category::ExecutionEnvironment);
                result.stage = Some(Stage::Start);
            } else if command_result.signaled {
                result.category = Some(Category::Internal);
            } else if command_result.exit_code != 0 {
                result.category = Some(Category::PackageManager);
                result.exit_code = Some(command_result.exit_code);
            } else {
                result.category = Some(Category::Internal);
            }
            return finish_result(result, started);
        }

        result.status = Status::Succeeded;
        finish_result(result, started)
    }
}

impl Default for Provisioner {
    /// 使用与 [`Provisioner::new`] 相同的固定 Linux sandbox 配置。
    fn default() -> Self {
        Self::new()
    }
}

/// manifest 解码错误发生在 JSON 完整性还是严格 schema 阶段。
#[derive(Debug)]
enum ManifestSchemaError {
    /// I/O、大小、语法或尾随值错误。
    Decode,
    /// JSON 合法，但字段或字段类型不符合严格 schema。
    Schema,
}

impl std::fmt::Display for ManifestSchemaError {
    ///
    /// CLI 不展示该正文，避免底层 decoder 的输入片段进入诊断；这里仅满足标准错误接口。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode => formatter.write_str("manifest decode failed"),
            Self::Schema => formatter.write_str("manifest schema validation failed"),
        }
    }
}

impl std::error::Error for ManifestSchemaError {
    ///
    /// Rust 在完成 decode/schema 分类后主动丢弃可能包含用户输入的底层错误，因此错误链终止。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// 读取有界 stdin 并分两遍区分 decode/schema 错误。
async fn decode_manifest<R>(input: &mut R) -> std::result::Result<Manifest, ManifestSchemaError>
where
    R: AsyncRead + Unpin + Send + ?Sized,
{
    let mut data = Vec::new();
    input
        .take((MAX_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut data)
        .await
        .map_err(|_| ManifestSchemaError::Decode)?;
    if data.len() > MAX_MANIFEST_BYTES {
        return Err(ManifestSchemaError::Decode);
    }

    // 第一遍只判断“是否恰好一个完整 JSON 值”；这样字段类型等 schema 错误不会被
    // 错误归到 decode。第二遍的手写 visitor 再拒绝未知、重复和大小写不匹配字段。
    // manifest 协议会把 JSON 字符串内的非法 UTF-8 字节替换为 U+FFFD，而不是把整份
    // 输入判成语法错误。先做同样的有损转换，避免输入错误地落入 decode 分支，或在
    // package manager 处理前被拒绝。
    let data = String::from_utf8_lossy(&data);
    serde_json::from_str::<serde_json::Value>(&data).map_err(|_| ManifestSchemaError::Decode)?;
    serde_json::from_str(&data).map_err(|_| ManifestSchemaError::Schema)
}

/// 将耗时写入结果并保证转换溢出时仍返回合法非负 `i64`。
fn finish_result(mut result: Result, started: Instant) -> Result {
    result.duration_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
    result
}

/// 检查固定工作目录存在、是目录且当前进程具有 search/execute 权限。
#[must_use]
fn is_executable_directory(path: &Path) -> bool {
    path.metadata().is_ok_and(|metadata| metadata.is_dir())
        && access(path, AccessFlags::X_OK).is_ok()
}

impl PackageManifest {
    /// 统计六个列表的 spec 总数。
    #[must_use]
    fn count(&self) -> usize {
        self.lists().iter().map(|specs| specs.values.len()).sum()
    }

    /// 生成严格固定顺序且完全不经过 shell 的 argv。
    #[must_use]
    fn plan(&self) -> Vec<Command> {
        let mut commands = Vec::new();
        if !self.apt.values.is_empty() {
            commands.push(Command {
                manager: Manager::Apt,
                stage: Stage::Prepare,
                path: "apt-get".to_owned(),
                args: vec!["update".to_owned()],
            });
            let mut args = vec!["install".to_owned(), "-y".to_owned(), "--".to_owned()];
            args.extend(self.apt.values.iter().cloned());
            commands.push(Command {
                manager: Manager::Apt,
                stage: Stage::Install,
                path: "apt-get".to_owned(),
                args,
            });
        }
        append_batch_command(
            &mut commands,
            Manager::Cargo,
            "cargo",
            &["install"],
            &self.cargo.values,
        );
        append_batch_command(
            &mut commands,
            Manager::Gem,
            "gem",
            &["install"],
            &self.gem.values,
        );
        for spec in &self.go.values {
            commands.push(Command {
                manager: Manager::Go,
                stage: Stage::Install,
                path: "go".to_owned(),
                args: vec!["install".to_owned(), spec.clone()],
            });
        }
        append_batch_command(
            &mut commands,
            Manager::Npm,
            "npm",
            &["install", "--global", "--"],
            &self.npm.values,
        );
        append_batch_command(
            &mut commands,
            Manager::Pip,
            "pip",
            &["install"],
            &self.pip.values,
        );
        commands
    }
}

/// 为非空列表追加一个批量安装命令。
fn append_batch_command(
    commands: &mut Vec<Command>,
    manager: Manager,
    path: &str,
    prefix: &[&str],
    specs: &[String],
) {
    if specs.is_empty() {
        return;
    }
    let mut args: Vec<String> = prefix
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect();
    args.extend(specs.iter().cloned());
    commands.push(Command {
        manager,
        stage: Stage::Install,
        path: path.to_owned(),
        args,
    });
}
