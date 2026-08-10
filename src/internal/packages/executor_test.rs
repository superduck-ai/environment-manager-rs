//! package manager executor 的输出隔离测试。

use std::{os::unix::fs::PermissionsExt as _, sync::Arc};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::{
    application::{Command, CommandRunner, ProvisionContext},
    executor::ExecCommandRunner,
    result::{Manager, Stage},
};

/// 对应 Go `TestExecCommandRunnerDiscardsProcessOutput`。
#[tokio::test]
async fn test_exec_command_runner_discards_process_output() {
    run_executor_output_helper().await;
}

/// 对应 Go `runExecutorOutputHelper`，执行会同时写 stdout/stderr 的受控脚本。
async fn run_executor_output_helper() {
    let directory = TempDir::new().unwrap();
    let executable = directory.path().join("test-package-manager");
    std::fs::write(
        &executable,
        b"#!/bin/sh\nprintf 'manager-secret-stdout'\nprintf 'manager-secret-stderr' >&2\nexit 23\n",
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&executable, permissions).unwrap();

    let runner = Arc::new(ExecCommandRunner::new(directory.path()));
    let result = runner
        .run(
            &ProvisionContext::new(CancellationToken::new()),
            &Command {
                manager: Manager::Npm,
                stage: Stage::Install,
                path: executable.to_string_lossy().into_owned(),
                args: Vec::new(),
            },
        )
        .await;
    assert!(result.started);
    assert_eq!(result.exit_code, 23);
    assert!(!result.signaled);
}
