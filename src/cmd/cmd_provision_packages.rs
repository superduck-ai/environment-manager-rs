//! 一次性 package provisioning CLI、信号取消与最终 JSON 输出。
//!
//! 对应 Go `cmd/cmd_provision_packages.go`。Clap 只解析 `--protocol` 和 `--stdin`；
//! 命令本身严格要求 `v1` 且读取 stdin，随后调用 `internal/packages`。stdout 恰好写一个
//! 权威 JSON 对象，stderr 只返回有界诊断，绝不包含 manifest、spec 或 manager 输出。

use std::{
    fmt::Display,
    io::{self, Write},
    time::Instant,
};

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;

use crate::internal::packages::{
    Category, ProvisionContext, Provisioner, Result as ProvisionResult, Stage, Status,
};

use super::{
    error::{CliError, RuntimeError},
    utils::flag_arg,
};

/// CLI 校验完成后的 package provisioning 参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionPackagesArgs {
    /// stdin 协议版本；当前唯一合法运行值是 `v1`。
    pub protocol: String,
    /// 是否显式要求读取 stdin；Go 命令要求该 flag 为真。
    pub stdin: bool,
}

/// package provisioning 应用层的可替换边界。
#[async_trait]
trait PackageProvisioner: Send + Sync {
    /// 读取 manifest 并返回不含敏感输入的终态协议结果。
    async fn run(
        &self,
        context: &ProvisionContext,
        input: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ProvisionResult;
}

#[async_trait]
impl PackageProvisioner for Provisioner {
    /// 将 CLI 的动态异步 reader 转发给真实 package provisioning 应用层。
    async fn run(
        &self,
        context: &ProvisionContext,
        input: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ProvisionResult {
        Provisioner::run(self, context, input).await
    }
}

/// 构造 `provision-packages` 子命令及两个协议 flag。
pub(crate) fn add_provision_packages_command() -> Command {
    Command::new("provision-packages")
        .about("Installs Environment packages from a versioned stdin manifest")
        .arg(
            Arg::new("protocol")
                .long("protocol")
                .help("Package provisioning protocol version"),
        )
        .arg(flag_arg("stdin", "Read the package manifest from stdin"))
}

/// 把 Clap matches 转成 owned 参数。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<ProvisionPackagesArgs, CliError> {
    Ok(ProvisionPackagesArgs {
        protocol: matches
            .get_one::<String>("protocol")
            .cloned()
            .unwrap_or_default(),
        stdin: matches.get_flag("stdin"),
    })
}

/// 对应 Go `AddProvisionPackagesCommand` 的生产运行路径。
///
/// Go `interruptReaderOnCancellation` 被折叠到本函数创建并传递的取消 context，以及
/// `Provisioner::run` 的解码分支中，不再保留一次性 reader 适配器。
///
/// 函数为 SIGINT/SIGTERM 建立取消 token，并在完成后停止信号任务。manifest 解码在
/// 应用层直接等待同一 token，避免为一次性 stdin 读取维护额外 reader 适配器。
/// 所有输出都由 `run_with_provisioner` 集中生成，避免信号分支多写 JSON。
pub async fn run_provision_packages(arguments: &ProvisionPackagesArgs) -> Result<(), RuntimeError> {
    // Go 只在两个协议 flag 都合法后注册 SIGINT/SIGTERM。先处理错误调用，既保持该
    // 初始化顺序，也避免极窄信号窗口把本应稳定的 invalid_manifest 结果改成取消。
    if arguments.protocol != "v1" || !arguments.stdin {
        let started = Instant::now();
        let mut output = io::stdout().lock();
        return emit_provision_result(&mut output, invalid_invocation_result(started));
    }

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_interrupt_or_terminate().await;
        signal_cancellation.cancel();
    });
    let context = ProvisionContext::new(cancellation);
    let mut input = tokio::io::stdin();
    let mut output = io::stdout().lock();
    let result = run_with_provisioner(
        arguments,
        &context,
        &Provisioner::new(),
        &mut input,
        &mut output,
    )
    .await;
    signal_task.abort();
    result
}

/// 使用注入 provisioner、reader 和 writer 执行命令协议，供生产与测试共享。
///
/// 对应 Go 测试辅助函数 `executeProvisionPackagesCommand` 和
/// `executeProvisionPackagesCommandWithReader`；Rust 的异步 I/O 抽象无需分别维护
/// `io.Reader` 与可关闭 reader 两条入口，因此安全地合并为一个实现。
async fn run_with_provisioner(
    arguments: &ProvisionPackagesArgs,
    context: &ProvisionContext,
    provisioner: &dyn PackageProvisioner,
    input: &mut (dyn AsyncRead + Unpin + Send),
    output: &mut dyn Write,
) -> Result<(), RuntimeError> {
    let started = Instant::now();
    let result = if arguments.protocol != "v1" || !arguments.stdin {
        invalid_invocation_result(started)
    } else {
        provisioner.run(context, input).await
    };
    emit_provision_result(output, result)
}

/// 同时等待 SIGINT 和 SIGTERM；注册失败时退化为只等待仍可用的信号源。
async fn wait_for_interrupt_or_terminate() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt()).ok();
    let mut terminate = signal(SignalKind::terminate()).ok();
    match (&mut interrupt, &mut terminate) {
        (Some(interrupt), Some(terminate)) => {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        }
        (Some(interrupt), None) => {
            let _signal = interrupt.recv().await;
        }
        (None, Some(terminate)) => {
            let _signal = terminate.recv().await;
        }
        // Linux 正常运行时两个注册都成功；若运行时已经无法注册信号，pending 保持
        // 任务静默，主命令完成后会 abort 它，不制造虚假的取消结果。
        (None, None) => std::future::pending().await,
    }
}

/// 对应 Go `invalidInvocationResult`，为缺 flag 或未知协议产生 validate 失败。
fn invalid_invocation_result(started: Instant) -> ProvisionResult {
    let mut result = ProvisionResult::failed();
    result.category = Some(Category::InvalidManifest);
    result.stage = Some(Stage::Validate);
    result.duration_ms = elapsed_milliseconds(started);
    result
}

/// 对应 Go `emitProvisionResult`，写出唯一 JSON 行并映射稳定进程退出码。
fn emit_provision_result(
    output: &mut dyn Write,
    result: ProvisionResult,
) -> Result<(), RuntimeError> {
    if serde_json::to_writer(&mut *output, &result).is_err() || output.write_all(b"\n").is_err() {
        let finalize = ProvisionResult {
            version: 1,
            status: Status::Failed,
            category: Some(Category::Internal),
            manager: None,
            stage: Some(Stage::Finalize),
            package_count: result.package_count,
            duration_ms: result.duration_ms,
            exit_code: None,
        };
        return Err(exit_error(
            finalize.process_exit_code(),
            result_diagnostic(&finalize),
        ));
    }
    let exit_code = result.process_exit_code();
    if exit_code == 0 {
        Ok(())
    } else {
        Err(exit_error(exit_code, result_diagnostic(&result)))
    }
}

/// 创建携带稳定退出码和有界诊断的运行时错误。
fn exit_error(exit_code: i32, diagnostic: String) -> RuntimeError {
    RuntimeError::with_exit_code(diagnostic, exit_code)
}

/// 对应 Go `resultDiagnostic`，只展示结果元数据，不展示 category 或用户输入。
fn result_diagnostic(result: &ProvisionResult) -> String {
    format!(
        "package provisioning status={} manager={} stage={} package_count={} duration_ms={} exit_code={}",
        match result.status {
            Status::Succeeded => "succeeded",
            Status::Failed => "failed",
        },
        result.manager.map_or("", |manager| manager.as_str()),
        result.stage.map_or("", |stage| stage.as_str()),
        optional_integer(result.package_count),
        result.duration_ms,
        optional_integer(result.exit_code),
    )
}

/// 对应 Go `optionalInt`，把省略的整数渲染为固定 `unknown`。
fn optional_integer<T>(value: Option<T>) -> String
where
    T: Display,
{
    value.map_or_else(|| "unknown".to_owned(), |value| value.to_string())
}

/// 把单调时钟耗时转换为不溢出的非负毫秒数。
fn elapsed_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[path = "cmd_provision_packages_test.rs"]
mod cmd_provision_packages_test;
