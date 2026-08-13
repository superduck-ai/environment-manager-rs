//! `provision-packages` CLI 的单 JSON、退出码、flag 和取消测试。

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

use crate::internal::packages::{Category, Manager, Result as ProvisionResult, Stage, Status};

use super::{
    PackageProvisioner, ProvisionContext, ProvisionPackagesArgs, Provisioner, run_with_provisioner,
};

/// 返回固定结果的 fake provisioner。
struct StubPackageProvisioner {
    result: ProvisionResult,
}

#[async_trait]
impl PackageProvisioner for StubPackageProvisioner {
    /// 忽略输入并返回测试指定的无敏感结果。
    async fn run(
        &self,
        _context: &ProvisionContext,
        _input: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ProvisionResult {
        self.result.clone()
    }
}

/// 等待 context 取消后返回 cancelled 结果的 fake provisioner。
struct PackageProvisionerFunc;

#[async_trait]
impl PackageProvisioner for PackageProvisionerFunc {
    /// 模拟一直运行到 SIGINT/SIGTERM 的安装流程。
    async fn run(
        &self,
        context: &ProvisionContext,
        _input: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ProvisionResult {
        context.cancelled().await;
        let mut result = ProvisionResult::failed();
        result.category = Some(Category::Cancelled);
        result.stage = Some(Stage::Preflight);
        result.duration_ms = 1;
        result
    }
}

/// 在第一次读取前通知测试任务的 reader，用于构造取消时序 fixture。
struct NotifyingReadCloser<R> {
    inner: R,
    read_started: Option<oneshot::Sender<()>>,
}

impl<R> AsyncRead for NotifyingReadCloser<R>
where
    R: AsyncRead + Unpin,
{
    /// 保证测试只在 stdin 已进入阻塞读取后取消。
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(read_started) = this.read_started.take() {
            let _result = read_started.send(());
        }
        Pin::new(&mut this.inner).poll_read(context, buffer)
    }
}

/// 创建合法 v1/stdin 参数。
fn valid_arguments() -> ProvisionPackagesArgs {
    ProvisionPackagesArgs {
        protocol: "v1".to_owned(),
        stdin: true,
    }
}

#[tokio::test]
async fn test_provision_packages_command_writes_one_final_json() {
    let provisioner = StubPackageProvisioner {
        result: ProvisionResult {
            version: 1,
            status: Status::Succeeded,
            category: None,
            manager: None,
            stage: None,
            package_count: Some(2),
            duration_ms: 17,
            exit_code: None,
        },
    };
    let context = ProvisionContext::new(CancellationToken::new());
    let mut input = std::io::Cursor::new(b"{}".to_vec());
    let mut output = Vec::new();
    let arguments = valid_arguments();
    run_with_provisioner(&arguments, &context, &provisioner, &mut input, &mut output)
        .await
        .unwrap();
    assert_eq!(
        output,
        br#"{"version":1,"status":"succeeded","package_count":2,"duration_ms":17}
"#
    );
}

#[tokio::test]
async fn test_provision_packages_command_returns_stable_failure_exit() {
    let provisioner = StubPackageProvisioner {
        result: ProvisionResult {
            version: 1,
            status: Status::Failed,
            category: Some(Category::PackageManager),
            manager: Some(Manager::Npm),
            stage: Some(Stage::Install),
            package_count: None,
            duration_ms: 8,
            exit_code: Some(23),
        },
    };
    let context = ProvisionContext::new(CancellationToken::new());
    let mut input = std::io::Cursor::new(b"{}".to_vec());
    let mut output = Vec::new();
    let error = run_with_provisioner(
        &valid_arguments(),
        &context,
        &provisioner,
        &mut input,
        &mut output,
    )
    .await
    .unwrap_err();
    assert_eq!(error.exit_code(), 10);
    let (rendered, exit_code) = crate::cmd::render_runtime_error(&error);
    assert_eq!(exit_code, 10);
    assert!(rendered.starts_with(b"Error: package provisioning status=failed"));
    assert!(error.to_string().contains("status=failed"));
    assert!(!error.to_string().contains("category="));
    assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(!String::from_utf8_lossy(&output).contains("typescript"));
}

#[tokio::test]
async fn test_provision_packages_command_reports_missing_protocol_flags_as_json() {
    let cases = [
        ProvisionPackagesArgs {
            protocol: String::new(),
            stdin: false,
        },
        ProvisionPackagesArgs {
            protocol: "v1".to_owned(),
            stdin: false,
        },
        ProvisionPackagesArgs {
            protocol: "v2".to_owned(),
            stdin: true,
        },
    ];
    for arguments in cases {
        let context = ProvisionContext::new(CancellationToken::new());
        let mut input = std::io::Cursor::new(b"{}".to_vec());
        let mut output = Vec::new();
        let error = run_with_provisioner(
            &arguments,
            &context,
            &StubPackageProvisioner {
                result: ProvisionResult::failed(),
            },
            &mut input,
            &mut output,
        )
        .await
        .unwrap_err();
        assert_eq!(error.exit_code(), 2);
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(r#""category":"invalid_manifest""#));
        assert!(output.contains(r#""stage":"validate""#));
    }
}

#[tokio::test]
async fn test_provision_packages_command_emits_invalid_manifest_result() {
    let context = ProvisionContext::new(CancellationToken::new());
    let mut input = std::io::Cursor::new(br#"{"version":"#.to_vec());
    let mut output = Vec::new();
    let arguments = valid_arguments();
    let provisioner = Provisioner::new();
    let error = run_with_provisioner(&arguments, &context, &provisioner, &mut input, &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.exit_code(), 2);
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains(r#""category":"invalid_manifest""#));
    assert!(output.contains(r#""stage":"decode""#));
}

/// 未知 flag 由 Clap 原生处理。
#[test]
fn test_provision_packages_command_maps_unknown_flag_to_cli_exit() {
    let unknown = format!("--{}", "secret".repeat(1000));
    let error = crate::cmd::parse_from(
        "dev",
        [
            "environment-runner".to_owned(),
            "provision-packages".to_owned(),
            unknown,
        ],
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), 2);
    assert!(error.rendered().starts_with("error:"));
    assert!(error.rendered().contains("unexpected argument"));
}

#[tokio::test]
async fn test_provision_packages_command_converts_interrupt_to_cancelled_result() {
    let cancellation = CancellationToken::new();
    let context = ProvisionContext::new(cancellation.clone());
    let mut input = std::io::Cursor::new(b"{}".to_vec());
    let mut output = Vec::new();
    let arguments = valid_arguments();
    let cancel = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancellation.cancel();
    };
    let run = run_with_provisioner(
        &arguments,
        &context,
        &PackageProvisionerFunc,
        &mut input,
        &mut output,
    );
    let (error, ()) = tokio::join!(run, cancel);
    assert_eq!(error.unwrap_err().exit_code(), 11);
    assert!(
        String::from_utf8(output)
            .unwrap()
            .contains(r#""category":"cancelled""#)
    );
}

#[tokio::test]
async fn test_provision_packages_command_cancels_blocked_stdin_read() {
    let cancellation = CancellationToken::new();
    let context = ProvisionContext::new(cancellation.clone());
    let (reader, _writer) = tokio::io::duplex(64);
    let (read_started, wait_for_read) = oneshot::channel();
    let mut input = NotifyingReadCloser {
        inner: reader,
        read_started: Some(read_started),
    };
    let mut output = Vec::new();
    let arguments = valid_arguments();
    let provisioner = Provisioner::new();
    let cancel = async move {
        let _result = wait_for_read.await;
        cancellation.cancel();
    };
    let run = run_with_provisioner(&arguments, &context, &provisioner, &mut input, &mut output);
    let (error, ()) = tokio::join!(run, cancel);
    let error = error.unwrap_err();
    assert_eq!(error.exit_code(), 11);
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains(r#""category":"cancelled""#));
    assert!(output.contains(r#""stage":"decode""#));
}

/// 确认位置参数使用 Clap 原生 usage error 和退出码 2。
#[test]
fn test_provision_packages_rejects_positionals() {
    let error = crate::cmd::parse_from(
        "dev",
        ["environment-runner", "provision-packages", "unexpected"],
    )
    .unwrap_err();
    assert_eq!(error.exit_code(), 2);
    assert!(error.rendered().starts_with("error:"));
    assert!(error.rendered().contains("unexpected"));
    assert!(error.rendered().contains("Usage:"));
}
