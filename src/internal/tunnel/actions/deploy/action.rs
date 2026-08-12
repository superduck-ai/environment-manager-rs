//! `deploy` tunnel action 的编排实现。
//!
//! Vercel 流程保持 Go 的构建、收集、首次创建、缺失文件上传、再次创建和轮询顺序；
//! Antspace 流程保持先打包完整项目再 multipart 上传的顺序。外部命令继承父进程环境，
//! stdout/stderr 共用同一 pipe 以复刻 `CombinedOutput`，取消时只终止直接子进程。

use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use nix::unistd::pipe;
use serde::Serialize;
use tokio::io::AsyncReadExt as _;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, Logger, default_logger};
use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter, ProgressUpdate,
};

use super::antspace::create_tarball;
use super::types::{DeployAction, DeployRequest, DeployResponse, VercelClient};
use super::vercel::{collect_files, project_name, validate_project_dir};

/// 空请求使用的固定 Baku 项目目录。
const DEFAULT_PROJECT_DIR: &str = "/home/claude/project";
/// 对应 Go：`(*DeployAction).Timeout` 的五分钟 action 上限。
pub const DEPLOY_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// 失败构建最多回传 combined output 的最后 10 KiB。
const BUILD_ERROR_OUTPUT_LIMIT: usize = 10 * 1024;
/// Vercel deployment 进入终态的最长轮询时间。
const VERCEL_READY_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// Antspace action 最终响应。
///
/// `error` 不使用 `skip_serializing_if`，因此成功时仍编码为空字符串，匹配 Go 匿名
/// struct 的字段行为。
#[derive(Serialize)]
struct AntspaceActionResponse {
    status: &'static str,
    url: String,
    app: String,
    files: usize,
    deployer: &'static str,
    error: &'static str,
}

/// 一次外部命令失败及失败前已经捕获的 combined output。
pub(super) struct CommandFailure {
    pub(super) error: String,
    pub(super) output: Vec<u8>,
}

/// 对应 Go：`NewDeployAction`。
///
/// 构造函数不验证凭据或 target；Go 在每次执行时才根据请求 target 做相应检查，允许
/// 同一个 action 同时持有 Vercel 与 Antspace 配置。
#[must_use]
pub fn new_deploy_action(
    logger: Option<Logger>,
    vercel_token: impl Into<String>,
    vercel_team_id: impl Into<String>,
    antspace_client: Option<super::AntspaceClient>,
    default_target: impl Into<String>,
) -> DeployAction {
    DeployAction {
        logger,
        vercel_token: vercel_token.into(),
        vercel_team_id: vercel_team_id.into(),
        antspace_client,
        default_target: default_target.into(),
    }
}

#[async_trait]
impl Action for DeployAction {
    /// 对应 Go：`(*DeployAction).Execute`。
    ///
    /// 请求体为空时使用零值参数；随后先检查 project_dir 边界，再选择 target。Vercel
    /// 路径的构建子进程与 HTTP 均遵守取消令牌，文件遍历通过 blocking pool 执行但不
    /// 人为加入 Go 不具备的中途取消点。
    async fn execute(
        &self,
        cancellation: CancellationToken,
        request: ActionRequest,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError> {
        let mut parameters = if request.body.is_empty() {
            DeployRequest::default()
        } else {
            serde_json::from_slice::<DeployRequest>(&request.body)
                .map_err(|error| format!("invalid request body: {error}"))?
        };
        if parameters.project_dir.is_empty() {
            parameters.project_dir = DEFAULT_PROJECT_DIR.to_owned();
        }
        validate_project_dir(&parameters.project_dir)?;
        if parameters.deploy_target.is_empty() {
            parameters.deploy_target.clone_from(&self.default_target);
        }

        match parameters.deploy_target.as_str() {
            "antspace" => {
                return self
                    .execute_antspace(&cancellation, &parameters, &request.session_id, progress)
                    .await;
            }
            "vercel" => {}
            target => {
                return Err(format!(
                    "unknown deploy target: {target} (expected \"vercel\" or \"antspace\")"
                )
                .into());
            }
        }

        if self.vercel_token.is_empty() {
            return Err("vercel deploy token not configured".into());
        }

        let started = Instant::now();
        let vercel_client = VercelClient::new(&self.vercel_token, &self.vercel_team_id);
        let project_name = project_name(&request.session_id);
        let logger = self.log();
        logger.info(
            "deploy_start",
            collect_log_attributes([
                (
                    "target",
                    serde_json::Value::String(parameters.deploy_target.clone()),
                ),
                (
                    "project_name",
                    serde_json::Value::String(project_name.clone()),
                ),
                (
                    "project_dir",
                    serde_json::Value::String(parameters.project_dir.clone()),
                ),
                (
                    "session_id",
                    serde_json::Value::String(request.session_id.clone()),
                ),
            ]),
        );

        // 六个阶段各自保留 warning 调用，使业务阶段与 Rust 调用位置
        // 均可直接辨识。
        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "building".to_owned(),
                message: "Running npm run build...".to_owned(),
                percent: 10.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }
        let build_started = Instant::now();
        if let Err(mut failure) =
            run_npm_build(&cancellation, Path::new(&parameters.project_dir)).await
        {
            if failure.output.len() > BUILD_ERROR_OUTPUT_LIMIT {
                failure.output = failure
                    .output
                    .split_off(failure.output.len() - BUILD_ERROR_OUTPUT_LIMIT);
            }
            let logger = self.log();
            logger.error(
                "deploy_build_failed",
                collect_log_attributes([
                    (
                        "duration_ms",
                        serde_json::Value::from(build_started.elapsed().as_millis() as u64),
                    ),
                    ("error", serde_json::Value::String(failure.error.clone())),
                ]),
            );
            return Err(format!(
                "build failed: {}\n{}",
                failure.error,
                String::from_utf8_lossy(&failure.output)
            )
            .into());
        }
        let logger = self.log();
        logger.info(
            "deploy_build_completed",
            collect_log_attributes([(
                "duration_ms",
                serde_json::Value::from(build_started.elapsed().as_millis() as u64),
            )]),
        );

        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "packaging".to_owned(),
                message: "Collecting build output...".to_owned(),
                percent: 30.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }
        let distribution_directory = Path::new(&parameters.project_dir).join("dist");
        let collect_path = distribution_directory.clone();
        let collected = tokio::task::spawn_blocking(move || collect_files(&collect_path))
            .await
            .map_err(|error| {
                format!(
                    "collect files from {}: {error}",
                    distribution_directory.display()
                )
            })?;
        // `collect_files` 已有一层同名包装；Go Execute 再包装一次，故这里刻意保留双层
        // `collect files from ...`，让错误链文本与原实现一致。
        let (files, file_data) = collected.map_err(|error| {
            format!(
                "collect files from {}: {error}",
                distribution_directory.display()
            )
        })?;
        if files.is_empty() {
            return Err(format!("no files found in {}", distribution_directory.display()).into());
        }

        let commit_sha = get_commit_sha(&cancellation, Path::new(&parameters.project_dir)).await;
        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "uploading".to_owned(),
                message: format!("Deploying {} files to Vercel...", files.len()),
                percent: 50.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }

        let (mut deployment, missing_files) = vercel_client
            .create_deployment(&cancellation, &project_name, &files)
            .await
            .map_err(|error| format!("create deployment: {error}"))?;
        if deployment.is_none() && missing_files.is_empty() {
            return Err("vercel returned no deployment and no missing files".into());
        }

        let mut files_uploaded = 0i64;
        if !missing_files.is_empty() {
            for sha in &missing_files {
                let data = file_data
                    .get(sha)
                    .ok_or_else(|| format!("missing file data for sha {sha}"))?;
                vercel_client
                    .upload_file(&cancellation, sha, data)
                    .await
                    .map_err(|error| format!("upload file {sha}: {error}"))?;
                files_uploaded += 1;
            }

            let (second_deployment, _second_missing) = vercel_client
                .create_deployment(&cancellation, &project_name, &files)
                .await
                .map_err(|error| format!("create deployment after upload: {error}"))?;
            deployment = second_deployment;
            if deployment.is_none() {
                return Err(format!(
                    "deployment still has {} missing files after upload",
                    missing_files.len()
                )
                .into());
            }
        }

        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "deploying".to_owned(),
                message: "Waiting for deployment...".to_owned(),
                percent: 60.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }
        let deployment = deployment.expect("deployment was checked before polling");
        let deployment = vercel_client
            .wait_for_ready(&cancellation, &deployment.id, VERCEL_READY_TIMEOUT)
            .await
            .map_err(|error| format!("wait for deployment: {error}"))?;

        let url = format!("https://{}", deployment.url);
        let response = DeployResponse {
            url: url.clone(),
            deploy_url: url,
            deploy_id: deployment.id,
            project_name: project_name.clone(),
            project_id: deployment.project_id,
            commit_sha,
            ready_state: deployment.ready_state,
            target: deployment.target,
            inspector_url: deployment.inspector_url,
            aliases: deployment.aliases,
            created_at: deployment.created_at,
            files_uploaded,
            files_cached: files.len() as i64 - files_uploaded,
        };
        let body = serde_json::to_vec(&response)
            .map_err(|error| format!("failed to marshal deploy response: {error}"))?;

        let logger = self.log();
        logger.info(
            "deploy_success",
            collect_log_attributes([
                ("url", serde_json::Value::String(response.url.clone())),
                (
                    "deploy_id",
                    serde_json::Value::String(response.deploy_id.clone()),
                ),
                ("project_name", serde_json::Value::String(project_name)),
                (
                    "duration_ms",
                    serde_json::Value::from(started.elapsed().as_millis() as u64),
                ),
                ("files_uploaded", serde_json::Value::from(files_uploaded)),
                (
                    "files_cached",
                    serde_json::Value::from(response.files_cached),
                ),
                ("session_id", serde_json::Value::String(request.session_id)),
            ]),
        );
        Ok(ActionResult {
            status_code: 200,
            body,
        })
    }

    /// 对应 Go：`(*DeployAction).Name`。
    fn name(&self) -> &str {
        "deploy"
    }

    /// 对应 Go：`(*DeployAction).Timeout`。
    fn timeout(&self) -> Duration {
        DEPLOY_TIMEOUT
    }
}

impl DeployAction {
    /// 对应 Go：`(*DeployAction).executeAntspace`。
    ///
    /// 整个项目在 blocking pool 中打包，随后上传；客户端理论上永远返回 `Some`，但仍
    /// 保留 Go 防御性 nil-result 检查对应的 `None` 错误。
    async fn execute_antspace(
        &self,
        cancellation: &CancellationToken,
        parameters: &DeployRequest,
        session_id: &str,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError> {
        let client = self
            .antspace_client
            .as_ref()
            .ok_or("antspace deploy not configured")?;
        let app_name = project_name(session_id);
        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "packaging".to_owned(),
                message: "Creating deployment tarball...".to_owned(),
                percent: 20.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }

        let project_directory = PathBuf::from(&parameters.project_dir);
        let (tarball, file_count) =
            tokio::task::spawn_blocking(move || create_tarball(project_directory))
                .await
                .map_err(|error| format!("create tarball task failed: {error}"))??;
        if let Err(error) = send_progress(
            Some(progress.as_ref()),
            ProgressUpdate {
                step: "uploading".to_owned(),
                message: format!("Deploying {file_count} files to Antspace..."),
                percent: 50.0,
            },
        )
        .await
        {
            let logger = self.log();
            logger.warn(
                "deploy_progress_failed",
                collect_log_attributes([("error", serde_json::Value::String(error.to_string()))]),
            );
        }

        let result = client
            .deploy(
                cancellation,
                &app_name,
                tarball,
                Some(Arc::clone(&progress)),
            )
            .await?
            .ok_or("antspace deploy returned nil result")?;
        let body = serde_json::to_vec(&AntspaceActionResponse {
            status: "deployed",
            url: result.url,
            app: result.service,
            files: file_count,
            deployer: "antspace",
            error: "",
        })
        .map_err(|error| format!("failed to marshal deploy response: {error}"))?;
        Ok(ActionResult {
            status_code: 200,
            body,
        })
    }

    /// 返回 action 配置的 logger；缺省时使用进程级 tracing logger。
    fn log(&self) -> &Logger {
        match self.logger.as_ref() {
            Some(logger) => logger,
            None => default_logger(),
        }
    }
}

/// 向可选 reporter 发送一次进度更新。
///
/// 对应 Go `sendProgress` 的 nil-safe 边界；Rust 的生产 action 总会提供 reporter，
/// `Option` 仍允许独立调用方明确关闭进度而无需伪造空实现。
async fn send_progress(
    progress: Option<&dyn ProgressReporter>,
    update: ProgressUpdate,
) -> Result<(), ActionError> {
    let Some(progress) = progress else {
        return Ok(());
    };
    progress.send_progress(update).await
}

/// 对应 Go：`getCommitSHA`。
///
/// 仅捕获 stdout，忽略 stderr；任何 spawn、取消或非零退出都返回空字符串。命令继承
/// 完整父进程环境并以项目目录为 cwd；取消只杀 direct child，随后像 Go `Output` 一样
/// 等待 child 和 stdout pipe 清理完成。
pub(super) async fn get_commit_sha(cancellation: &CancellationToken, directory: &Path) -> String {
    if cancellation.is_cancelled() {
        return String::new();
    }
    let mut command = Command::new("git");
    command
        .args(["rev-parse", "HEAD"])
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return String::new(),
    };
    let Some(mut stdout) = child.stdout.take() else {
        return String::new();
    };
    let reader = tokio::spawn(async move {
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).await.map(|_| output)
    });
    let status = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            // 与 npm 路径一致：不创建 process group，只终止 direct git child。
            let _direct_child_kill = child.start_kill();
            child.wait().await
        }
        result = child.wait() => result,
    };
    let output = match reader.await {
        Ok(Ok(output)) => output,
        Ok(Err(_)) | Err(_) => return String::new(),
    };
    if !status.is_ok_and(|status| status.success()) {
        return String::new();
    }
    String::from_utf8_lossy(&output).trim().to_owned()
}

/// 执行 `npm run build` 并复刻 Go `CombinedOutput`。
///
/// stdout 与 stderr 指向同一个匿名 pipe，因此内核写入顺序被保留；父进程继承环境，
/// stdin 指向 null。取消时明确调用 `start_kill` 终止**直接 child**，不创建 process
/// group，也不向孙进程发送信号。
async fn run_npm_build(
    cancellation: &CancellationToken,
    directory: &Path,
) -> Result<Vec<u8>, CommandFailure> {
    run_build_program(cancellation, directory, Path::new("npm")).await
}

/// 执行指定构建程序；生产入口传 `npm`，行为测试可传绝对脚本路径而不修改全局 PATH。
///
/// 参数仍固定为 `run build`，其余 pipe、环境继承和 direct-child 取消语义与
/// `run_npm_build` 完全相同。
pub(super) async fn run_build_program(
    cancellation: &CancellationToken,
    directory: &Path,
    program: &Path,
) -> Result<Vec<u8>, CommandFailure> {
    if cancellation.is_cancelled() {
        return Err(CommandFailure {
            error: "context canceled".to_owned(),
            output: Vec::new(),
        });
    }

    let (read_end, write_end) = pipe().map_err(|error| CommandFailure {
        error: error.to_string(),
        output: Vec::new(),
    })?;
    for descriptor in [&read_end, &write_end] {
        fcntl(descriptor, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).map_err(|error| {
            CommandFailure {
                error: error.to_string(),
                output: Vec::new(),
            }
        })?;
    }
    let stderr_end = write_end.try_clone().map_err(|error| CommandFailure {
        error: error.to_string(),
        output: Vec::new(),
    })?;
    let mut command = Command::new(program);
    command
        .args(["run", "build"])
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Err(CommandFailure {
                error: error.to_string(),
                output: Vec::new(),
            });
        }
    };
    // `Command` 不再需要；立即 drop 确保父进程没有保留 shared pipe 的写端。
    drop(command);

    let mut pipe_reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let reader = tokio::spawn(async move {
        let mut output = Vec::new();
        pipe_reader.read_to_end(&mut output).await.map(|_| output)
    });
    let status_result = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            // 只向 direct child 发信号；这是有意保持 Go CommandContext 的语义。
            let _direct_child_kill = child.start_kill();
            child.wait().await
        }
        result = child.wait() => result,
    };
    let output_result = reader.await;
    let output = match output_result {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return Err(CommandFailure {
                error: error.to_string(),
                output: Vec::new(),
            });
        }
        Err(error) => {
            return Err(CommandFailure {
                error: error.to_string(),
                output: Vec::new(),
            });
        }
    };
    let status = status_result.map_err(|error| CommandFailure {
        error: error.to_string(),
        output: output.clone(),
    })?;
    if status.success() {
        Ok(output)
    } else {
        Err(CommandFailure {
            error: format_exit_status(status),
            output,
        })
    }
}

/// 把 Unix `ExitStatus` 格式化为 Go `exec.ExitError` 的常见文本。
fn format_exit_status(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status: {code}");
    }
    match status.signal() {
        Some(libc::SIGKILL) => "signal: killed".to_owned(),
        Some(libc::SIGTERM) => "signal: terminated".to_owned(),
        Some(libc::SIGINT) => "signal: interrupt".to_owned(),
        Some(signal) => format!("signal: {signal}"),
        None => status.to_string(),
    }
}

/// 按调用顺序收集任意日志属性迭代器。
fn collect_log_attributes<I, K>(attributes: I) -> LogAttributes
where
    I: IntoIterator<Item = (K, serde_json::Value)>,
    K: Into<String>,
{
    attributes
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect()
}
