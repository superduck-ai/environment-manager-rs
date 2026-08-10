//! `environment-manager` Linux GNU 二进制入口。
//!
//! 对应 Go `main.go`：这里只规范化 argv、解析兼容 CLI、选择 stdout/stderr 并把
//! 强类型调用交给运行时。业务逻辑位于镜像模块树，避免入口随着迁移变成第二套实现。

use std::io::Write as _;

use environment_manager::cmd::{CobraCommand, Invocation, OutputStream};

/// 启动 Tokio 多线程运行时，并以 Go 入口约定的退出码结束进程。
///
/// `std::process::exit` 刻意只位于最外层；内部执行函数返回整数，便于后续 CLI
/// golden 测试在不终止测试进程的情况下检查所有输出分支。
#[tokio::main]
async fn main() {
    std::process::exit(run().await);
}

/// 解析当前进程参数并分派到对应的异步业务运行时。
///
/// 帮助和版本文本按 [`OutputStream`] 写入指定标准流。completion 与默认 sandbox
/// 配置也在本函数直接执行；其余命令由镜像模块树负责完整的网络、进程和清理流程。
async fn run() -> i32 {
    let invocation = match environment_manager::cmd::parse_from(
        env!("CARGO_PKG_VERSION"),
        std::env::args_os(),
    ) {
        Ok(invocation) => invocation,
        Err(error) => {
            write_cli_text(error.stream(), error.rendered());
            return error.exit_code();
        }
    };

    match invocation {
        Invocation::Completion(arguments) => {
            let script = environment_manager::cmd::render_completion_script(
                arguments.shell,
                arguments.no_descriptions,
            );
            let _ = std::io::stdout().lock().write_all(script.as_bytes());
            0
        }
        Invocation::PrintSandboxSettings(_) => print_sandbox_settings(),
        Invocation::Poll(arguments) => {
            let cancellation = tokio_util::sync::CancellationToken::new();
            let result = environment_manager::cmd::run_poll(
                &arguments,
                &cancellation,
                &mut std::io::stdout().lock(),
            )
            .await;
            command_result(CobraCommand::Poll, result)
        }
        Invocation::Setup(arguments) => {
            let cancellation = tokio_util::sync::CancellationToken::new();
            command_result(
                CobraCommand::Setup,
                environment_manager::cmd::run_setup(&arguments, &cancellation).await,
            )
        }
        Invocation::CodeSign(arguments) => command_result(
            CobraCommand::CodeSign,
            environment_manager::cmd::run_code_sign(&arguments).await,
        ),
        Invocation::Orchestrator(arguments) => {
            let executable = std::env::args_os()
                .next()
                .unwrap_or_else(|| std::ffi::OsString::from("environment-runner"));
            command_result(
                CobraCommand::Orchestrator,
                environment_manager::cmd::run_orchestrator(&arguments, &executable).await,
            )
        }
        Invocation::TaskRun(arguments) => {
            task_run_command_result(environment_manager::cmd::run_task_run(&arguments).await)
        }
        Invocation::ProvisionPackages(arguments) => {
            match environment_manager::cmd::run_provision_packages(&arguments).await {
                Ok(()) => 0,
                Err(error) => {
                    eprintln!("Error: {error}");
                    environment_manager::cmd::provision_packages_exit_code(&error)
                }
            }
        }
    }
}

/// 把业务命令结果转换为 Cobra 根入口的统一错误输出和退出码。
///
/// 子命令只返回未带根前缀的错误；这里集中添加 `Error: ` 和末尾换行，避免网络或
/// 安装模块自行决定 stderr 格式。成功路径不额外输出任何内容。
fn command_result(command: CobraCommand, result: Result<(), String>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprint!(
                "{}",
                environment_manager::cmd::render_runtime_error(command, &error)
            );
            1
        }
    }
}

/// 输出可能包含任意 Claude/npm `CombinedOutput` 字节的 task-run 错误。
///
/// 普通错误由兼容 renderer 生成 Cobra 首个 `Error:`、usage 和 Go `main` 的第二个
/// `Error:`；V0 nil dereference 则直接使用 Go runtime transcript 和退出码 2。两类
/// 输出都必须 `write_all` 原始字节，任何 `Display`、`eprint!` 或 UTF-8 转换都会
/// 不可逆地改变子进程错误或崩溃资产。
fn task_run_command_result(result: Result<(), environment_manager::cmd::RuntimeError>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(error) => {
            let (rendered, exit_code) =
                environment_manager::cmd::render_task_run_runtime_error(&error);
            let _ = std::io::stderr().lock().write_all(&rendered);
            exit_code
        }
    }
}

/// 输出与 Go `runPrintSandboxSettings` 相同的两空格缩进 JSON 和末尾换行。
fn print_sandbox_settings() -> i32 {
    let mut stdout = std::io::stdout().lock();
    if let Err(error) = environment_manager::cmd::run_print_sandbox_settings(&mut stdout) {
        eprintln!("Error: {error}");
        return 1;
    }
    0
}

/// 把 CLI 提前退出文本写到其声明的标准流，并忽略关闭管道导致的二次错误。
fn write_cli_text(stream: OutputStream, text: &str) {
    match stream {
        OutputStream::Stdout => {
            let _ = std::io::stdout().lock().write_all(text.as_bytes());
        }
        OutputStream::Stderr => {
            let _ = std::io::stderr().lock().write_all(text.as_bytes());
        }
    }
}

#[cfg(test)]
#[path = "main_test.rs"]
mod main_test;
