//! package provisioning 应用层的 manifest、计划、分类和取消测试。

use std::{
    collections::VecDeque,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::{
    application::{Command, CommandResult, CommandRunner, ProvisionContext, Provisioner},
    result::{Category, Manager, Stage, Status},
};

/// 可复制的 fake executor 终态，避免在测试队列中克隆 `io::Error`。
#[derive(Debug, Clone, Copy)]
struct RecordedResult {
    exit_code: i32,
    started: bool,
    signaled: bool,
    failed: bool,
}

impl RecordedResult {
    /// 创建成功启动且退出码为零的默认 fake 结果。
    const fn success() -> Self {
        Self {
            exit_code: 0,
            started: true,
            signaled: false,
            failed: false,
        }
    }

    /// 转换为生产 trait 使用的不含敏感信息结果。
    fn into_command_result(self) -> CommandResult {
        CommandResult {
            exit_code: self.exit_code,
            started: self.started,
            signaled: self.signaled,
            error: self
                .failed
                .then(|| io::Error::other("injected command failure")),
        }
    }
}

/// 记录固定计划并按队列返回结果的 fake executor。
#[derive(Default)]
struct RecordingRunner {
    commands: Mutex<Vec<Command>>,
    results: Mutex<VecDeque<RecordedResult>>,
    cancel_on_run: Option<CancellationToken>,
}

#[async_trait]
impl CommandRunner for RecordingRunner {
    /// 保存调用顺序，并可在返回前触发外部 token 以测试取消分类优先级。
    async fn run(&self, _context: &ProvisionContext, command: &Command) -> CommandResult {
        self.commands.lock().unwrap().push(command.clone());
        if let Some(cancellation) = &self.cancel_on_run {
            cancellation.cancel();
        }
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(RecordedResult::success)
            .into_command_result()
    }
}

/// 对应 Go `newTestProvisioner`，使用 root UID、临时可执行目录和 fake executor 构造实例。
fn new_test_provisioner(runner: Arc<dyn CommandRunner>) -> (Provisioner, TempDir) {
    let work_dir = TempDir::new().unwrap();
    let provisioner =
        Provisioner::with_dependencies(runner, Arc::new(|| 0), work_dir.path().to_path_buf());
    (provisioner, work_dir)
}

/// 为本文件保留简短的测试调用入口，实际构造语义集中在 [`new_test_provisioner`]。
fn test_provisioner(runner: Arc<dyn CommandRunner>) -> (Provisioner, TempDir) {
    new_test_provisioner(runner)
}

/// 使用内存 reader 运行 provisioner，简化不涉及阻塞 stdin 的测试。
async fn run_manifest(
    provisioner: &Provisioner,
    context: &ProvisionContext,
    input: &str,
) -> super::result::Result {
    let mut reader = std::io::Cursor::new(input.as_bytes().to_vec());
    provisioner.run(context, &mut reader).await
}

/// 对应 Go `TestProvisionerRejectsMalformedManifest`。
#[tokio::test]
async fn test_provisioner_rejects_malformed_manifest() {
    let (provisioner, _work_dir) = test_provisioner(Arc::new(RecordingRunner::default()));
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":"#,
    )
    .await;
    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.category, Some(Category::InvalidManifest));
    assert_eq!(result.stage, Some(Stage::Decode));
    assert_eq!(result.package_count, None);
    assert_eq!(result.process_exit_code(), 2);
}

/// 对应 Go `TestProvisionerStopsAtFirstPackageManagerFailure`。
#[tokio::test]
async fn test_provisioner_stops_at_first_package_manager_failure() {
    let runner = Arc::new(RecordingRunner {
        results: Mutex::new(VecDeque::from([
            RecordedResult::success(),
            RecordedResult {
                exit_code: 37,
                started: true,
                signaled: false,
                failed: true,
            },
        ])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","apt":["ffmpeg"],"npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::PackageManager));
    assert_eq!(result.manager, Some(Manager::Apt));
    assert_eq!(result.stage, Some(Stage::Install));
    assert_eq!(result.exit_code, Some(37));
    assert_eq!(result.process_exit_code(), 10);
    assert_eq!(runner.commands.lock().unwrap().len(), 2);
}

/// 对应 Go `TestProvisionerDoesNotReportSignalAsPackageManagerExitCode`。
#[tokio::test]
async fn test_provisioner_does_not_report_signal_as_package_manager_exit_code() {
    let runner = Arc::new(RecordingRunner {
        results: Mutex::new(VecDeque::from([RecordedResult {
            exit_code: -1,
            started: true,
            signaled: true,
            failed: true,
        }])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::Internal));
    assert_eq!(result.exit_code, None);
    assert_eq!(result.process_exit_code(), 12);
}

/// 对应 Go `TestProvisionerClassifiesRunnerInternalFailure`。
#[tokio::test]
async fn test_provisioner_classifies_runner_internal_failure() {
    let runner = Arc::new(RecordingRunner {
        results: Mutex::new(VecDeque::from([RecordedResult {
            exit_code: 0,
            started: true,
            signaled: false,
            failed: true,
        }])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::Internal));
    assert_eq!(result.stage, Some(Stage::Install));
    assert_eq!(result.exit_code, None);
}

/// 对应 Go `TestProvisionerHandlesEmptyPackagesWithoutStartingAProcess`。
#[tokio::test]
async fn test_provisioner_handles_empty_packages_without_starting_a_process() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages"}}"#,
    )
    .await;
    assert_eq!(result.status, Status::Succeeded);
    assert_eq!(result.package_count, Some(0));
    assert_eq!(result.process_exit_code(), 0);
    assert!(runner.commands.lock().unwrap().is_empty());
}

/// 对应 Go `TestProvisionerRequiresRootAndWorkingDirectory`。
#[tokio::test]
async fn test_provisioner_requires_root_and_working_directory() {
    let input = r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#;
    let existing = TempDir::new().unwrap();
    let cases = [
        (
            Arc::new(|| 501_u32) as Arc<dyn Fn() -> u32 + Send + Sync>,
            existing.path().to_path_buf(),
        ),
        (
            Arc::new(|| 0),
            PathBuf::from("/definitely/missing/environment-manager-work-dir"),
        ),
    ];
    for (get_uid, work_dir) in cases {
        let provisioner =
            Provisioner::with_dependencies(Arc::new(RecordingRunner::default()), get_uid, work_dir);
        let result = run_manifest(
            &provisioner,
            &ProvisionContext::new(CancellationToken::new()),
            input,
        )
        .await;
        assert_eq!(result.category, Some(Category::ExecutionEnvironment));
        assert_eq!(result.stage, Some(Stage::Preflight));
    }
}

/// 对应 Go `TestProvisionerDoesNotStartAProcessWhenAlreadyCancelled`。
#[tokio::test]
async fn test_provisioner_does_not_start_a_process_when_already_cancelled() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(cancellation),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::Cancelled));
    assert_eq!(result.stage, Some(Stage::Preflight));
    assert!(runner.commands.lock().unwrap().is_empty());
}

/// 对应 Go `TestProvisionerClassifiesExpiredDeadline`。
#[tokio::test]
async fn test_provisioner_classifies_expired_deadline() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let context = ProvisionContext::with_deadline(
        CancellationToken::new(),
        tokio::time::Instant::now() - Duration::from_secs(1),
    );
    let result = run_manifest(
        &provisioner,
        &context,
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::Timeout));
    assert_eq!(result.stage, Some(Stage::Preflight));
    assert!(runner.commands.lock().unwrap().is_empty());
}

/// 对应 Go `TestProvisionerClassifiesCancellationDuringInstall`。
#[tokio::test]
async fn test_provisioner_classifies_cancellation_during_install() {
    let cancellation = CancellationToken::new();
    let runner = Arc::new(RecordingRunner {
        cancel_on_run: Some(cancellation.clone()),
        results: Mutex::new(VecDeque::from([RecordedResult {
            exit_code: 0,
            started: true,
            signaled: false,
            failed: true,
        }])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(cancellation),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::Cancelled));
    assert_eq!(result.manager, Some(Manager::Npm));
    assert_eq!(result.stage, Some(Stage::Install));
}

/// 对应 Go `TestProvisionerDiscardsPackageManagerOutput`；executor 的真实流隔离另有测试。
#[tokio::test]
async fn test_provisioner_discards_package_manager_output() {
    let runner = Arc::new(RecordingRunner {
        results: Mutex::new(VecDeque::from([RecordedResult {
            exit_code: 23,
            started: true,
            signaled: false,
            failed: true,
        }])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    let encoded = serde_json::to_string(&result).unwrap();
    assert_eq!(result.exit_code, Some(23));
    assert!(!encoded.contains("typescript"));
}

/// 对应 Go `TestProvisionerRejectsManifestLargerThanOneMiB`。
#[tokio::test]
async fn test_provisioner_rejects_manifest_larger_than_one_mi_b() {
    let (provisioner, _work_dir) = test_provisioner(Arc::new(RecordingRunner::default()));
    let input = " ".repeat((1 << 20) + 1);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        &input,
    )
    .await;
    assert_eq!(result.category, Some(Category::InvalidManifest));
    assert_eq!(result.stage, Some(Stage::Decode));
}

/// 对应 Go `TestProvisionerClassifiesMissingPackageManagerBinary`。
#[tokio::test]
async fn test_provisioner_classifies_missing_package_manager_binary() {
    let runner = Arc::new(RecordingRunner {
        results: Mutex::new(VecDeque::from([RecordedResult {
            exit_code: 0,
            started: false,
            signaled: false,
            failed: true,
        }])),
        ..RecordingRunner::default()
    });
    let (provisioner, _work_dir) = test_provisioner(runner);
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#,
    )
    .await;
    assert_eq!(result.category, Some(Category::ExecutionEnvironment));
    assert_eq!(result.manager, Some(Manager::Npm));
    assert_eq!(result.stage, Some(Stage::Start));
}

/// 对应 Go `TestProvisionerCancelsRunningPackageManager`。
#[tokio::test]
async fn test_provisioner_cancels_running_package_manager() {
    let cancellation = CancellationToken::new();
    let runner = Arc::new(CommandRunnerFunc);
    let (provisioner, _work_dir) = test_provisioner(runner);
    let context = ProvisionContext::new(cancellation.clone());
    let mut reader = std::io::Cursor::new(
        br#"{"version":1,"packages":{"type":"packages","npm":["typescript"]}}"#.to_vec(),
    );
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancellation.cancel();
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        provisioner.run(&context, &mut reader),
    )
    .await
    .unwrap();
    cancel_task.await.unwrap();
    assert_eq!(result.category, Some(Category::Cancelled));
    assert_eq!(result.manager, Some(Manager::Npm));
}

/// 等待 context 取消后返回的 fake executor。
struct CommandRunnerFunc;

#[async_trait]
impl CommandRunner for CommandRunnerFunc {
    /// 对应 Go `commandRunnerFunc.Run`，模拟在途安装直到 context 完成。
    async fn run(&self, context: &ProvisionContext, _command: &Command) -> CommandResult {
        context.cancelled().await;
        CommandResult {
            exit_code: -1,
            started: true,
            signaled: true,
            error: Some(io::Error::new(io::ErrorKind::Interrupted, "cancelled")),
        }
    }
}

/// 对应 Go `TestProvisionerValidatesManifestContract`。
#[tokio::test]
async fn test_provisioner_validates_manifest_contract() {
    let cases = [
        (
            r#"{"version":1,"packages":{"type":"packages"},"extra":true}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","brew":[]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"Version":1,"packages":{"type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"Packages":{"type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"Type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","NPM":[]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":2,"version":1,"packages":{"type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages"},"packages":{"type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"tools","type":"packages"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","npm":[],"npm":[]}}"#,
            Stage::Validate,
        ),
        ("", Stage::Decode),
        (
            r#"{"version":1,"packages":{"type":"packages"}} {}"#,
            Stage::Decode,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages"}} trailing"#,
            Stage::Decode,
        ),
        ("[]", Stage::Validate),
        (
            r#"{"version":1,"packages":{"type":"packages","npm":[1]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":2,"packages":{"type":"packages"}}"#,
            Stage::Validate,
        ),
        (r#"{"version":1}"#, Stage::Validate),
        (r#"{"version":1,"packages":null}"#, Stage::Validate),
        (
            r#"{"version":1,"packages":{"type":"tools"}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","npm":null}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["  --dry-run"]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["demo @ https://user:secret@example.test/demo.whl"]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["demo @ https://user:%zz@example.test/demo.whl"]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["demo @ https://@example.test/demo.whl"]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["demo @ Kttps://user@example.test/demo.whl"]}}"#,
            Stage::Validate,
        ),
        (
            r#"{"version":1,"packages":{"type":"packages","pip":["demo @ ſsh://user@example.test/demo.whl"]}}"#,
            Stage::Validate,
        ),
    ];
    for (input, stage) in cases {
        let (provisioner, _work_dir) = test_provisioner(Arc::new(RecordingRunner::default()));
        let result = run_manifest(
            &provisioner,
            &ProvisionContext::new(CancellationToken::new()),
            input,
        )
        .await;
        assert_eq!(result.category, Some(Category::InvalidManifest), "{input}");
        assert_eq!(result.stage, Some(stage), "{input}");
        assert_eq!(result.process_exit_code(), 2);
    }
}

/// 验证 Go encoding/json 会替换字符串内非法 UTF-8，而不会把完整 JSON 归为 decode 错误。
#[tokio::test]
async fn test_provisioner_replaces_invalid_utf8_inside_json_string() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let mut input = br#"{"version":1,"packages":{"type":"packages","pip":["demo"]}}"#.to_vec();
    let marker = input
        .windows(4)
        .position(|window| window == b"demo")
        .expect("fixture 应包含 package spec");
    input.insert(marker + 4, 0xff);
    let result = provisioner
        .run(
            &ProvisionContext::new(CancellationToken::new()),
            &mut std::io::Cursor::new(input),
        )
        .await;
    assert_eq!(result.status, Status::Succeeded);
    assert_eq!(result.package_count, Some(1));
    assert_eq!(runner.commands.lock().unwrap().len(), 1);
}

/// 对应 Go `TestProvisionerLeavesMalformedNonCredentialURLToPackageManager`。
#[tokio::test]
async fn test_provisioner_leaves_malformed_non_credential_url_to_package_manager() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","pip":["demo @ https://example.test/%zz/demo.whl"]}}"#,
    )
    .await;
    assert_eq!(result.status, Status::Succeeded);
    assert_eq!(runner.commands.lock().unwrap().len(), 1);
}

/// 对应 Go `TestProvisionerAllowsAtSignOutsideURLAuthority`。
#[tokio::test]
async fn test_provisioner_allows_at_sign_outside_url_authority() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r##"{"version":1,"packages":{"type":"packages","pip":["demo @ https://example.test?email=user@example.org","other @ https://example.test#user@example.org"]}}"##,
    )
    .await;
    assert_eq!(result.status, Status::Succeeded);
    assert_eq!(runner.commands.lock().unwrap().len(), 1);
}

/// 对应 Go `TestProvisionerRunsPackageManagersInContractOrder`。
#[tokio::test]
async fn test_provisioner_runs_package_managers_in_contract_order() {
    let runner = Arc::new(RecordingRunner::default());
    let (provisioner, _work_dir) = test_provisioner(runner.clone());
    let result = run_manifest(
        &provisioner,
        &ProvisionContext::new(CancellationToken::new()),
        r#"{"version":1,"packages":{"type":"packages","apt":["ffmpeg","curl"],"cargo":["ripgrep@14.1.1"],"gem":["rake:13.2.1"],"go":["example.test/one@v1","example.test/two@v2"],"npm":["typescript@5.9.3"],"pip":["numpy==2.3.5"]}}"#,
    )
    .await;
    assert_eq!(result.status, Status::Succeeded);
    assert_eq!(result.package_count, Some(8));
    let commands = runner.commands.lock().unwrap();
    let observed: Vec<(Manager, Stage, &str)> = commands
        .iter()
        .map(|command| (command.manager, command.stage, command.path.as_str()))
        .collect();
    assert_eq!(
        observed,
        vec![
            (Manager::Apt, Stage::Prepare, "apt-get"),
            (Manager::Apt, Stage::Install, "apt-get"),
            (Manager::Cargo, Stage::Install, "cargo"),
            (Manager::Gem, Stage::Install, "gem"),
            (Manager::Go, Stage::Install, "go"),
            (Manager::Go, Stage::Install, "go"),
            (Manager::Npm, Stage::Install, "npm"),
            (Manager::Pip, Stage::Install, "pip"),
        ]
    );
    assert_eq!(commands[1].args, ["install", "-y", "--", "ffmpeg", "curl"]);
    assert_eq!(
        commands[6].args,
        ["install", "--global", "--", "typescript@5.9.3"]
    );
}
