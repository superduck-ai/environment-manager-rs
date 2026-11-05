//! Deploy 行为测试。
//!
//! 覆盖 中的命名、目录边界、文件索引、tar.gz、Antspace multipart、Vercel
//! 两类请求与轮询，并额外锁定取消只能终止直接构建子进程的约束。

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use flate2::read::GzDecoder;
use serde_json::json;
use sha1::{Digest as _, Sha1};
use tar::Archive;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_bytes, body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ProgressReporter, ProgressUpdate,
};

use super::action::{get_commit_sha, run_build_program};
use super::*;

/// 线程安全记录 reporter，供 HTTP client 和 action 测试检查发送顺序。
#[derive(Default)]
struct RecordingProgress {
    updates: Mutex<Vec<ProgressUpdate>>,
    fail: bool,
}

#[async_trait]
impl ProgressReporter for RecordingProgress {
    /// 保存更新；`fail` 打开时在保存后返回错误，验证进度失败不覆盖部署成功。
    async fn send_progress(&self, update: ProgressUpdate) -> Result<(), ActionError> {
        self.updates.lock().unwrap().push(update);
        if self.fail {
            Err("progress failed".into())
        } else {
            Ok(())
        }
    }
}

/// 创建使用 wiremock server 的 Vercel client。
fn new_test_vercel_client(server: &MockServer) -> VercelClient {
    VercelClient {
        token: "vercel-token".to_owned(),
        team_id: "team".to_owned(),
        http_client: reqwest::Client::new(),
        base_url: server.uri(),
    }
}

/// 验证凭据、client、target 和 action 元数据。
#[test]
fn test_new_deploy_action() {
    let antspace = new_antspace_client("https://control.example", "token");
    let action = new_deploy_action("vercel-token", "team", Some(antspace), "antspace");
    assert_eq!(action.vercel_token, "vercel-token");
    assert_eq!(action.vercel_team_id, "team");
    assert!(action.antspace_client.is_some());
    assert_eq!(action.default_target, "antspace");
    assert_eq!(action.name(), "deploy");
    assert_eq!(action.timeout(), DEPLOY_TIMEOUT);
}

/// 验证控制面地址、凭据和 HTTP client 初始化。
#[test]
fn test_new_antspace_client() {
    let antspace = new_antspace_client("https://control.example", "token");
    assert_eq!(antspace.control_plane_url, "https://control.example");
    assert_eq!(antspace.auth_token, "token");
}

/// 验证 session 前缀、20 字节截断、Unicode 非法尾部和字符 allow-list。
#[test]
fn test_project_name() {
    assert_eq!(project_name("session_Foo.Bar_123"), "baku-foo-bar-123");
    assert_eq!(project_name("session_staging_ABC.def_X"), "baku-abc-def-x");
    assert_eq!(
        project_name("session_ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        "baku-abcdefghijklmnopqrst"
    );
    assert_eq!(project_name("Already-OK"), "baku-already-ok");
    assert_eq!(
        project_name("123456789012345678€"),
        "baku-123456789012345678-"
    );
}

/// 验证目录校验只做词法 clean，且必须位于 `/home/claude/` 下。
#[test]
fn test_validate_project_dir() {
    assert!(validate_project_dir("/home/claude/project").is_ok());
    assert!(validate_project_dir("/home/claude/project/../project").is_ok());
    for invalid in ["/home/claude", "/home/claude/..", "/tmp/project"] {
        let error = validate_project_dir(invalid).unwrap_err().to_string();
        assert!(error.contains("project_dir must be under /home/claude/"));
    }
}

/// 验证收集顺序、SHA-1、原始数据映射及 symlink 排除。
#[test]
fn test_collect_files() {
    let directory = tempdir().unwrap();
    must_write_file(directory.path().join("index.html"), b"<h1>Hello</h1>");
    must_write_file(directory.path().join("assets/app.js"), b"console.log('hi')");
    std::os::unix::fs::symlink(
        directory.path().join("index.html"),
        directory.path().join("linked.html"),
    )
    .unwrap();

    let (files, data) = collect_files(directory.path()).unwrap();
    assert_eq!(
        files
            .iter()
            .map(|entry| entry.file.as_str())
            .collect::<Vec<_>>(),
        ["assets/app.js", "index.html"]
    );
    for (name, contents) in [
        ("assets/app.js", b"console.log('hi')".as_slice()),
        ("index.html", b"<h1>Hello</h1>".as_slice()),
    ] {
        assert_collected_file(&files, &data, name, contents);
    }
}

/// 解包 tar.gz，验证根/子目录 header、文件内容与 symlink 排除。
#[test]
fn test_create_tarball() {
    let directory = tempdir().unwrap();
    must_write_file(directory.path().join("index.html"), b"home");
    must_write_file(directory.path().join("src/app.js"), b"app");
    std::os::unix::fs::symlink(
        directory.path().join("index.html"),
        directory.path().join("linked.html"),
    )
    .unwrap();

    let (tarball, file_count) = create_tarball(directory.path()).unwrap();
    assert_eq!(file_count, 2);
    let contents = read_tarball(&tarball);
    assert_eq!(contents.get("index.html").unwrap(), b"home");
    assert_eq!(contents.get("src/app.js").unwrap(), b"app");
    assert!(!contents.contains_key("linked.html"));
}

/// 验证 Antspace multipart 的 header、字段、文件和最终 100% 进度。
#[tokio::test]
async fn test_antspace_deploy_sends_multipart_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/deploy"))
        .and(header("authorization", "Bearer token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "deployed",
            "url": "https://app.example",
            "service": "svc"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let client = AntspaceClient {
        control_plane_url: server.uri(),
        auth_token: "token".to_owned(),
        http_client: reqwest::Client::new(),
    };
    let progress = Arc::new(RecordingProgress::default());
    let result = client
        .deploy(
            &CancellationToken::new(),
            "app-name",
            b"tarball bytes".to_vec(),
            Some(progress.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.url, "https://app.example");
    assert_eq!(result.service, "svc");

    let requests = server.received_requests().await.unwrap();
    let request = &requests[0];
    let content_type = request
        .headers
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.starts_with("multipart/form-data; boundary="));
    let body = &request.body;
    assert!(contains_bytes(body, b"name=\"app\""));
    assert!(contains_bytes(body, b"app-name"));
    assert!(contains_bytes(
        body,
        b"name=\"dist\"; filename=\"dist.tar.gz\""
    ));
    assert!(contains_bytes(body, b"tarball bytes"));

    assert_eq!(
        *progress.updates.lock().unwrap(),
        [ProgressUpdate {
            step: "deployed".to_owned(),
            message: "Deployed to https://app.example".to_owned(),
            percent: 100.0,
        }]
    );
}

/// 验证 Antspace 非 200 原 body 与无效 JSON 的诊断上下文。
#[tokio::test]
async fn test_antspace_read_deploy_response_errors() {
    let unavailable = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("control plane unavailable"))
        .mount(&unavailable)
        .await;
    let client = AntspaceClient {
        control_plane_url: unavailable.uri(),
        auth_token: String::new(),
        http_client: reqwest::Client::new(),
    };
    let error = client
        .deploy(&CancellationToken::new(), "app", Vec::new(), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("deploy request failed (status 503): control plane unavailable"));

    let invalid = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&invalid)
        .await;
    let client = AntspaceClient {
        control_plane_url: invalid.uri(),
        auth_token: String::new(),
        http_client: reqwest::Client::new(),
    };
    let error = client
        .deploy(&CancellationToken::new(), "app", Vec::new(), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("failed to parse deploy response"));
    assert!(error.contains("body: not json"));

    let response_error = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "error": "quota exceeded"
        })))
        .mount(&response_error)
        .await;
    let client = AntspaceClient {
        control_plane_url: response_error.uri(),
        auth_token: String::new(),
        http_client: reqwest::Client::new(),
    };
    let error = client
        .deploy(&CancellationToken::new(), "app", Vec::new(), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("antspace deploy error: quota exceeded"));
}

/// 验证 Vercel 首次 missing-files 分支与成功 deployment 解码。
#[tokio::test]
async fn test_vercel_create_deployment() {
    let files = vec![FileEntry {
        file: "index.html".to_owned(),
        sha: "abc123".to_owned(),
        size: 5,
    }];
    let missing_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v13/deployments"))
        .and(query_param("teamId", "team"))
        .and(header("authorization", "Bearer vercel-token"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({
            "files": files,
            "name": "baku-session",
            "project": "baku-session",
            "projectSettings": {
                "buildCommand": "",
                "framework": null,
                "installCommand": "",
                "outputDirectory": "."
            },
            "target": "production"
        })))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "missing_files",
                "message": "missing",
                "missing": ["abc123"]
            }
        })))
        .mount(&missing_server)
        .await;
    let (deployment, missing) = new_test_vercel_client(&missing_server)
        .create_deployment(&CancellationToken::new(), "baku-session", &files)
        .await
        .unwrap();
    assert!(deployment.is_none());
    assert_eq!(missing, ["abc123"]);
    let requests = missing_server.received_requests().await.unwrap();
    assert_vercel_request(&requests[0], "POST", "/v13/deployments");
    assert_deployment_payload(&requests[0], "baku-session", &files);

    let success_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "dep_1",
            "url": "app.vercel.app",
            "name": "baku-session",
            "readyState": "BUILDING",
            "target": "production",
            "projectId": "proj_1",
            "aliases": ["alias.vercel.app"],
            "createdAt": 123
        })))
        .mount(&success_server)
        .await;
    let (deployment, missing) = new_test_vercel_client(&success_server)
        .create_deployment(&CancellationToken::new(), "baku-session", &files)
        .await
        .unwrap();
    let deployment = deployment.unwrap();
    assert!(missing.is_empty());
    assert_eq!(deployment.id, "dep_1");
    assert_eq!(deployment.project_id, "proj_1");
    assert_eq!(deployment.aliases.unwrap(), ["alias.vercel.app"]);
    let requests = success_server.received_requests().await.unwrap();
    assert_vercel_request(&requests[0], "POST", "/v13/deployments");
    assert_deployment_payload(&requests[0], "baku-session", &files);

    let forbidden_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&forbidden_server)
        .await;
    let error = new_test_vercel_client(&forbidden_server)
        .create_deployment(&CancellationToken::new(), "baku-session", &files)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("deployment failed (status 403): forbidden"));
}

/// 验证上传 header/body 与 API 错误正文。
#[tokio::test]
async fn test_vercel_upload_file() {
    let upload_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/files"))
        .and(query_param("teamId", "team"))
        .and(header("authorization", "Bearer vercel-token"))
        .and(header("content-type", "application/octet-stream"))
        .and(header("x-vercel-digest", "abc123"))
        .and(body_bytes(b"file data".as_slice()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&upload_server)
        .await;
    new_test_vercel_client(&upload_server)
        .upload_file(&CancellationToken::new(), "abc123", b"file data")
        .await
        .unwrap();
    let requests = upload_server.received_requests().await.unwrap();
    assert_vercel_request(&requests[0], "POST", "/v2/files");

    let error_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upload failed"))
        .mount(&error_server)
        .await;
    let error = new_test_vercel_client(&error_server)
        .upload_file(&CancellationToken::new(), "abc123", b"file data")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("file upload failed (status 500): upload failed"));
}

/// 覆盖 READY、失败、HTTP 错误和预取消。
#[tokio::test]
async fn test_vercel_wait_for_ready() {
    let ready_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v13/deployments/dep_1"))
        .and(query_param("teamId", "team"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "dep_1",
            "url": "app.vercel.app",
            "readyState": "READY"
        })))
        .mount(&ready_server)
        .await;
    let deployment = new_test_vercel_client(&ready_server)
        .wait_for_ready(&CancellationToken::new(), "dep_1", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(deployment.ready_state, "READY");
    let requests = ready_server.received_requests().await.unwrap();
    assert_vercel_request(&requests[0], "GET", "/v13/deployments/dep_1");

    let failed_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "dep_1",
            "readyState": "ERROR"
        })))
        .mount(&failed_server)
        .await;
    let error = new_test_vercel_client(&failed_server)
        .wait_for_ready(&CancellationToken::new(), "dep_1", Duration::from_secs(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("deployment failed with state: ERROR"));

    let status_error_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&status_error_server)
        .await;
    let error = new_test_vercel_client(&status_error_server)
        .wait_for_ready(&CancellationToken::new(), "dep_1", Duration::from_secs(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("deployment status check failed (status 404): not found"));

    let cancelled_server = MockServer::start().await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = new_test_vercel_client(&cancelled_server)
        .wait_for_ready(&cancellation, "dep_1", Duration::from_secs(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("deployment wait cancelled"));
    assert!(
        cancelled_server
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
}

/// 验证仓库 HEAD 和非仓库降级为空字符串。
#[tokio::test]
async fn test_get_commit_sha() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let status = Command::new("git").arg("--version").status().unwrap();
    assert!(status.success(), "git executable is required");

    let directory = tempdir().unwrap();
    run_git(directory.path(), &["init"]);
    run_git(
        directory.path(),
        &["config", "user.email", "test@example.com"],
    );
    run_git(directory.path(), &["config", "user.name", "Test User"]);
    must_write_file(directory.path().join("README.md"), b"hello");
    run_git(directory.path(), &["add", "README.md"]);
    run_git(directory.path(), &["commit", "-m", "initial"]);

    let expected = run_git(directory.path(), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    let actual = get_commit_sha(&CancellationToken::new(), directory.path()).await;
    assert_eq!(actual, expected);

    let non_repository = tempdir().unwrap();
    assert_eq!(
        get_commit_sha(&CancellationToken::new(), non_repository.path()).await,
        ""
    );
}

/// 验证 JSON、目录和 target 配置错误均在构建/网络副作用前返回。
#[tokio::test]
async fn test_execute_early_errors() {
    let action = new_deploy_action("", "", None, "antspace");
    let progress: Arc<dyn ProgressReporter> = Arc::new(RecordingProgress::default());

    let error = action
        .execute(
            CancellationToken::new(),
            ActionRequest {
                body: b"{".to_vec(),
                ..ActionRequest::default()
            },
            Arc::clone(&progress),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("invalid request body"));

    for (body, expected) in [
        (
            r#"{"project_dir":"/tmp/project","target":"antspace"}"#,
            "project_dir must be under",
        ),
        (
            r#"{"project_dir":"/home/claude/project","target":"bad"}"#,
            "unknown deploy target",
        ),
        (
            r#"{"project_dir":"/home/claude/project","target":"antspace"}"#,
            "antspace deploy not configured",
        ),
    ] {
        let error = action
            .execute(
                CancellationToken::new(),
                ActionRequest {
                    body: body.as_bytes().to_vec(),
                    ..ActionRequest::default()
                },
                Arc::clone(&progress),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

/// 验证共享 pipe 保留 stdout/stderr 顺序并格式化非零退出码。
#[tokio::test]
async fn build_combines_output_and_reports_exit_status() {
    let directory = tempdir().unwrap();
    let script = directory.path().join("builder.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf stdout\nprintf stderr >&2\nexit 7\n",
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    let output = run_build_program(&CancellationToken::new(), directory.path(), &script)
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(7));
    assert_eq!(output.output.expect_combined(), b"stdoutstderr");
}

/// 验证取消只 kill direct child，并等待继承 pipe 的孙进程自行关闭输出端。
#[tokio::test]
async fn cancelling_build_kills_only_direct_child() {
    // builder 的孙进程通过 PATH 查找 sleep。若并行测试临时清空 PATH，孙进程会
    // 提前退出并关闭 pipe，进而把 direct-child 语义误报为失败。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let directory = tempdir().unwrap();
    let script = directory.path().join("builder.sh");
    fs::write(
        &script,
        "#!/bin/sh\n(sleep 1; : > \"$PWD/grandchild-alive\") &\n: > \"$PWD/grandchild-started\"\nwait\n",
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_directory = directory.path().to_owned();
    let task_script = script.clone();
    let mut task = tokio::spawn(async move {
        run_build_program(&task_cancellation, &task_directory, &task_script).await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while !directory.path().join("grandchild-started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();

    // 捕获命令会等待共享 pipe EOF。direct child 已被 kill 后，继承 pipe 的
    // grandchild 尚存，因此任务此刻应继续阻塞；这同时证明实现没有偷偷杀整个组。
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut task)
            .await
            .is_err()
    );
    let failure = tokio::time::timeout(Duration::from_secs(2), &mut task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.to_string(), "command was cancelled");
    assert!(
        directory.path().join("grandchild-alive").exists(),
        "grandchild must survive direct-child cancellation and close its pipe naturally"
    );
}

/// 创建父目录并写入测试文件。
fn must_write_file(path: impl AsRef<Path>, data: &[u8]) {
    let path = path.as_ref();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, data).unwrap();
}

/// 核对名称、SHA-1、长度和内容映射。
fn assert_collected_file(
    files: &[FileEntry],
    data: &HashMap<String, Vec<u8>>,
    name: &str,
    content: &[u8],
) {
    let entry = files
        .iter()
        .find(|entry| entry.file == name)
        .unwrap_or_else(|| panic!("{name} was not collected"));
    let expected_sha = Sha1::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(entry.sha, expected_sha);
    assert_eq!(entry.size, content.len());
    assert_eq!(data.get(&entry.sha).unwrap(), content);
}

/// 核对方法、路径、teamId 和 bearer header。
fn assert_vercel_request(request: &wiremock::Request, expected_method: &str, expected_path: &str) {
    assert_eq!(request.method.as_str(), expected_method);
    assert_eq!(request.url.path(), expected_path);
    assert_eq!(
        request
            .url
            .query_pairs()
            .find(|(key, _)| key == "teamId")
            .map(|(_, value)| value.into_owned()),
        Some("team".to_owned())
    );
    assert_eq!(
        request
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer vercel-token"
    );
}

/// 核对 deployment JSON 的 wire 字段和值。
fn assert_deployment_payload(
    request: &wiremock::Request,
    expected_project: &str,
    expected_files: &[FileEntry],
) {
    assert_eq!(
        request
            .headers
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );
    let payload: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(payload["name"], expected_project);
    assert_eq!(payload["project"], expected_project);
    assert_eq!(payload["target"], "production");
    assert_eq!(
        payload["files"],
        serde_json::to_value(expected_files).unwrap()
    );
    assert!(payload["projectSettings"]["framework"].is_null());
    assert_eq!(payload["projectSettings"]["buildCommand"], "");
    assert_eq!(payload["projectSettings"]["installCommand"], "");
    assert_eq!(payload["projectSettings"]["outputDirectory"], ".");
}

/// 解压 tar.gz 并只返回普通文件内容。
fn read_tarball(tarball: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let decoder = GzDecoder::new(tarball);
    let mut archive = Archive::new(decoder);
    let mut contents = BTreeMap::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let name = entry.path().unwrap().to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut bytes).unwrap();
        contents.insert(name, bytes);
    }
    contents
}

/// 在 fixture 仓库执行 Git 并返回 combined output。
fn run_git(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// 判断 multipart 原始 body 是否包含指定连续字节。
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
