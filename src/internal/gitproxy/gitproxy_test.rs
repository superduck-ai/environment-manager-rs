//! Git proxy 的 Linux ARM64 行为兼容测试。
//!
//! 用例逐项对应 Go `gitproxy_test.go`，并补充恢复出的禁重定向回调与取消后再次
//! 清理语义。测试服务器同样使用 Axum，避免引入 OpenSSL 或平台专属网络依赖。

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Router, body::Body, response::Response};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::{
    HeaderValue, Request, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HOST, LOCATION, USER_AGENT, WWW_AUTHENTICATE},
};
use http_body_util::BodyExt as _;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger,
};

use super::handler::{
    Handler, is_valid_git_path, new_http_client, sanitize_error, sanitize_string,
};
use super::{Config, RepoAuth, new_manager, new_server};

#[derive(Debug)]
struct TestHttpServer {
    base_url: String,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl TestHttpServer {
    async fn start(app: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(serve_shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            base_url: format!("http://{address}"),
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.task.await.unwrap();
    }
}

#[derive(Debug)]
struct UpstreamGitRequest {
    method: String,
    path: String,
    query: String,
    authorization: String,
    token_backend: String,
    content_type: String,
    user_agent: String,
    host: String,
    body: String,
}

fn repo_auth(repo: &str, host: &str, auth_token: &str) -> RepoAuth {
    RepoAuth {
        repo: repo.to_owned(),
        host: host.to_owned(),
        auth_token: auth_token.to_owned(),
    }
}

fn handler_for(mut config: Config, repo_tokens: HashMap<String, String>) -> Handler {
    if config.logger.is_none() {
        config.logger = Some(gitproxy_test_logger());
    }
    Handler::new(Arc::new(config), new_http_client().unwrap(), repo_tokens)
}

async fn response_body(response: Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_new_server_validates_config() {
    let cases = [
        (None, "config is required"),
        (
            Some(Config {
                session_ingress_url: "http://upstream".to_owned(),
                repo_auths: vec![repo_auth("a/b", "", "token")],
                ..Config::default()
            }),
            "session ID is required",
        ),
        (
            Some(Config {
                session_id: "session".to_owned(),
                repo_auths: vec![repo_auth("a/b", "", "token")],
                ..Config::default()
            }),
            "session ingress URL is required",
        ),
        (
            Some(Config {
                session_id: "session".to_owned(),
                session_ingress_url: "http://upstream".to_owned(),
                ..Config::default()
            }),
            "at least one repository auth is required",
        ),
        (
            Some(Config {
                session_id: "session".to_owned(),
                session_ingress_url: "http://upstream".to_owned(),
                repo_auths: vec![repo_auth("", "", "token")],
                ..Config::default()
            }),
            "repo is required for RepoAuth at index 0",
        ),
        (
            Some(Config {
                session_id: "session".to_owned(),
                session_ingress_url: "http://upstream".to_owned(),
                repo_auths: vec![repo_auth("a/b", "", "")],
                ..Config::default()
            }),
            "auth token is required for repo a/b",
        ),
        (
            Some(Config {
                session_id: "session".to_owned(),
                session_ingress_url: "http://upstream".to_owned(),
                repo_auths: vec![repo_auth("a/b", "", "token")],
                token_backend: "bad".to_owned(),
                ..Config::default()
            }),
            "unsupported token backend: bad",
        ),
    ];

    for (config, expected) in cases {
        let error = new_server(config).err().expect("config should be rejected");
        assert_eq!(error.to_string(), expected);
    }
}

#[tokio::test]
async fn test_server_start_stop_and_manager_proxy_ur_ls() {
    let manager = new_manager(Some(Config {
        session_ingress_url: "http://session-ingress.test".to_owned(),
        session_id: "session-123".to_owned(),
        repo_auths: vec![
            repo_auth("anthropic/demo", "github", "github-token"),
            repo_auth("org/repo", "ghe.example.com", "ghe-token"),
        ],
        logger: Some(gitproxy_test_logger()),
        ..Config::default()
    }))
    .unwrap();
    let cancellation = CancellationToken::new();

    assert!(!manager.is_running());
    assert_eq!(manager.get_proxy_url("anthropic/demo"), "");

    let base_url = manager.start(&cancellation).await.unwrap();
    assert!(manager.is_running());
    assert!(base_url.starts_with("http://127.0.0.1:"));
    assert_eq!(manager.start(&cancellation).await.unwrap(), base_url);

    let github_url = manager.get_proxy_url("anthropic/demo");
    assert!(github_url.starts_with("http://local_proxy@127.0.0.1:"));
    assert!(github_url.ends_with("/git/anthropic/demo"));
    let ghe_url = manager.get_proxy_url("org/repo");
    assert!(ghe_url.starts_with("http://local_proxy@127.0.0.1:"));
    assert!(ghe_url.ends_with("/git/ghe.example.com/org/repo"));

    manager.stop(&cancellation).await.unwrap();
    assert!(!manager.is_running());
    manager.stop(&cancellation).await.unwrap();
}

#[tokio::test]
async fn test_handler_serve_http_challenges_and_rejects_invalid_requests() {
    let handler = handler_for(
        Config {
            session_ingress_url: "http://session-ingress.test".to_owned(),
            session_id: "session-123".to_owned(),
            ..Config::default()
        },
        HashMap::from([("anthropic/demo".to_owned(), "repo-token".to_owned())]),
    );

    let response = handler
        .serve_http(
            Request::builder()
                .uri("/bad/path")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(response_body(response).await, "Invalid path format\n");

    let response = handler
        .serve_http(
            Request::builder()
                .uri("/git/anthropic/demo.git/info/refs")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        "Basic realm=\"Git Proxy\""
    );
    assert_eq!(response_body(response).await, "");

    let response = handler
        .serve_http(
            Request::builder()
                .method("POST")
                .uri("/git/anthropic/demo.git/%2e%2e/config")
                .header(AUTHORIZATION, "Basic client-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response_body(response).await;
    assert!(!body.contains("client-token"));
    assert_eq!(body, "Proxy error: invalid git path\n");
}

#[tokio::test]
async fn test_handler_forwards_git_hub_request_to_session_ingress() {
    let (logger, records) = gitproxy_recording_logger();
    let (observed_tx, mut observed_rx) = mpsc::channel(1);
    let app = Router::new().fallback(move |request: Request<Body>| {
        let observed_tx = observed_tx.clone();
        async move {
            let (parts, body) = request.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            observed_tx
                .send(UpstreamGitRequest {
                    method: parts.method.to_string(),
                    path: parts.uri.path().to_owned(),
                    query: parts.uri.query().unwrap_or_default().to_owned(),
                    authorization: parts
                        .headers
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                    token_backend: parts
                        .headers
                        .get("x-token-backend")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                    content_type: parts
                        .headers
                        .get(CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                    user_agent: parts
                        .headers
                        .get(USER_AGENT)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                    host: parts
                        .headers
                        .get(HOST)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                    body: String::from_utf8(body.to_vec()).unwrap(),
                })
                .await
                .unwrap();

            let mut response = Response::new(Body::from("git response"));
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-git-upload-pack-result"),
            );
            response
        }
    });
    let upstream = TestHttpServer::start(app).await;

    let handler = handler_for(
        Config {
            session_ingress_url: format!("{}/", upstream.base_url),
            session_id: "session/%2e;,:@&=+$中文".to_owned(),
            token_backend: "tvs".to_owned(),
            logger: Some(logger),
            ..Config::default()
        },
        HashMap::from([(
            "org;,:@&=+$中文/repo;,:@&=+$中文".to_owned(),
            "repo-token".to_owned(),
        )]),
    );
    let response = handler
        .serve_http(
            Request::builder()
                .method("POST")
                .uri("/git/org%3B%2C%3A%40%26%3D%2B%24%E4%B8%AD%E6%96%87/repo%3B%2C%3A%40%26%3D%2B%24%E4%B8%AD%E6%96%87.git/objects/a%3Bb%2Cc%3A%40%26%3D%2B%24%E4%B8%AD%E6%96%87?service=git-upload-pack")
                .header(AUTHORIZATION, "Bearer client-auth")
                .header(HOST, "spoofed.invalid")
                .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                .header(USER_AGENT, "git/2.39")
                .header("x-token-backend", "client-spoof")
                .body(Body::from("client body"))
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-git-upload-pack-result"
    );
    assert_eq!(response_body(response).await, "git response");

    let observed = observed_rx.recv().await.unwrap();
    assert_eq!(observed.method, "POST");
    assert_eq!(
        observed.path,
        "/v1/session_ingress/session/session%2F%252e%3B%2C:@&=+$%E4%B8%AD%E6%96%87/git_proxy/org%3B%2C:@&=+$%E4%B8%AD%E6%96%87/repo%3B%2C:@&=+$%E4%B8%AD%E6%96%87.git/objects%2Fa%3Bb%2Cc:@&=+$%E4%B8%AD%E6%96%87"
    );
    assert_eq!(observed.query, "service=git-upload-pack");
    assert_eq!(
        observed.authorization,
        format!("Basic {}", STANDARD.encode("git:repo-token"))
    );
    assert_eq!(observed.token_backend, "tvs");
    assert_eq!(
        observed.content_type,
        "application/x-git-upload-pack-request"
    );
    assert_eq!(observed.user_agent, "git/2.39");
    assert_eq!(observed.body, "client body");
    assert_ne!(observed.host, "spoofed.invalid");

    let logs = recorded_logs(&records);
    assert_eq!(
        logs.iter()
            .map(|record| record.message.as_str())
            .collect::<Vec<_>>(),
        [
            "Received git request",
            "Forwarding request to session ingress proxy",
            "Sending authenticated request to upstream",
            "Received response from upstream",
            "Successfully proxied request",
        ]
    );
    assert_eq!(
        logs[1]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["upstream_url", "repo", "git_path"]
    );
    assert_eq!(
        logs[2]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repo", "method"]
    );
    assert_eq!(
        logs[3]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repo", "status"]
    );
    assert_eq!(logs[3].attributes["status"], serde_json::json!(200));
    assert_eq!(
        logs[4]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repo", "status", "bytes_written"]
    );
    assert_eq!(logs[4].attributes["bytes_written"], serde_json::json!(12));

    upstream.stop().await;
}

/// 丢弃测试日志的最小 handler，避免默认 logger 写入测试进程 stderr。
struct GitProxyDiscardHandler;

impl LogHandler for GitProxyDiscardHandler {
    fn enabled(&self, _level: LogLevel) -> bool {
        false
    }

    fn handle(&self, _record: &LogRecord) -> Result<(), LogHandlerError> {
        Ok(())
    }

    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(Self)
    }

    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(Self)
    }
}

/// 对应 Go：`gitproxyTestLogger`，返回丢弃所有记录的显式 logger。
fn gitproxy_test_logger() -> Logger {
    Logger::new(Arc::new(GitProxyDiscardHandler))
}

/// 保存 gitproxy 测试产生的完整结构化记录，以便校验 Go 字段顺序和标量类型。
#[derive(Clone, Default)]
struct GitProxyRecordingHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl LogHandler for GitProxyRecordingHandler {
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(record.clone());
        Ok(())
    }

    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 创建记录 logger，并返回共享记录槽用于操作完成后的断言。
fn gitproxy_recording_logger() -> (Logger, Arc<Mutex<Vec<LogRecord>>>) {
    let handler = GitProxyRecordingHandler::default();
    let records = Arc::clone(&handler.records);
    (Logger::new(Arc::new(handler)), records)
}

/// 返回记录槽的拥有型快照，避免断言期间持有 mutex guard。
fn recorded_logs(records: &Arc<Mutex<Vec<LogRecord>>>) -> Vec<LogRecord> {
    records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

#[tokio::test]
async fn test_manager_and_server_emit_go_lifecycle_logs() {
    let (logger, records) = gitproxy_recording_logger();
    let manager = new_manager(Some(Config {
        session_ingress_url: "http://session-ingress.test".to_owned(),
        session_id: "session-logs".to_owned(),
        repo_auths: vec![repo_auth("anthropic/demo", "github", "repo-token")],
        logger: Some(logger),
        ..Config::default()
    }))
    .unwrap();
    let cancellation = CancellationToken::new();

    manager.start(&cancellation).await.unwrap();
    manager.stop(&cancellation).await.unwrap();

    let logs = recorded_logs(&records);
    assert_eq!(
        logs.iter()
            .map(|record| record.message.as_str())
            .collect::<Vec<_>>(),
        [
            "Starting git proxy manager",
            "Git proxy server started",
            "Git proxy manager started",
            "Stopping git proxy manager",
            "Git proxy server stopped",
            "Git proxy manager stopped",
        ]
    );
    assert!(logs.iter().all(|record| record.level == LogLevel::Info));
    for index in [1, 2] {
        assert_eq!(
            logs[index]
                .attributes
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["base_url", "port"]
        );
        assert!(logs[index].attributes["base_url"].is_string());
        assert!(logs[index].attributes["port"].is_number());
    }
}

#[tokio::test]
async fn test_handler_emits_go_challenge_and_rejection_logs() {
    let (logger, records) = gitproxy_recording_logger();
    let handler = handler_for(
        Config {
            session_ingress_url: "http://session-ingress.test".to_owned(),
            session_id: "session-logs".to_owned(),
            logger: Some(logger),
            ..Config::default()
        },
        HashMap::from([("anthropic/demo".to_owned(), "repo-token".to_owned())]),
    );

    let malformed = handler
        .serve_http(
            Request::builder()
                .uri("/bad/path")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    let challenge = handler
        .serve_http(
            Request::builder()
                .uri("/git/anthropic/demo.git/info/refs")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(challenge.status(), StatusCode::UNAUTHORIZED);
    drop(challenge);

    let invalid = handler
        .serve_http(
            Request::builder()
                .method("POST")
                .uri("/git/anthropic/demo.git/%2e%2e/config")
                .header(AUTHORIZATION, "Basic client-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(invalid.status(), StatusCode::BAD_GATEWAY);

    let unauthorized = handler
        .serve_http(
            Request::builder()
                .uri("/git/other/repository.git/HEAD")
                .header(AUTHORIZATION, "Basic client-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(unauthorized.status(), StatusCode::BAD_GATEWAY);

    let logs = recorded_logs(&records);
    assert_eq!(
        logs.iter()
            .map(|record| record.message.as_str())
            .collect::<Vec<_>>(),
        [
            "Invalid path format - missing components",
            "Received git request",
            "Challenging unauthenticated info/refs request",
            "Received git request",
            "Invalid git path detected",
            "Failed to proxy git request",
            "Received git request",
            "No auth token configured for repository",
            "Failed to proxy git request",
        ]
    );
    assert_eq!(
        logs[0]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["path"]
    );
    assert_eq!(
        logs[1]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["method", "path", "org", "repo", "git_path", "has_auth"]
    );
    assert_eq!(logs[1].attributes["has_auth"], serde_json::json!(false));
    assert_eq!(logs[3].attributes["has_auth"], serde_json::json!(true));
    assert_eq!(
        logs[4]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["git_path", "org", "repo"]
    );
    assert_eq!(
        logs[5]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["error", "org", "repo", "git_path"]
    );
    assert_eq!(
        logs[7]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repo"]
    );
}

#[tokio::test]
async fn test_handler_forwards_ghe_path_and_sanitizes_error_bodies() {
    let (observed_tx, mut observed_rx) = mpsc::channel(1);
    let app = Router::new().fallback(move |request: Request<Body>| {
        let observed_tx = observed_tx.clone();
        async move {
            observed_tx
                .send((
                    request.uri().path().to_owned(),
                    request
                        .headers()
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned(),
                ))
                .await
                .unwrap();
            let mut response = Response::new(Body::from(
                "Authorization: Bearer sk-ant-secret token=abc Basic Z2l0OnNlY3JldA==",
            ));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    });
    let upstream = TestHttpServer::start(app).await;

    let handler = handler_for(
        Config {
            session_ingress_url: upstream.base_url.clone(),
            session_id: "session-123".to_owned(),
            ..Config::default()
        },
        HashMap::from([("org/repo".to_owned(), "ghe-token".to_owned())]),
    );
    let response = handler
        .serve_http(
            Request::builder()
                .uri("/git/ghe.example.com/org/repo.git/HEAD")
                .header(AUTHORIZATION, "Basic client-auth")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response_body(response).await;
    for secret in ["sk-ant-secret", "token=abc", "Z2l0OnNlY3JldA=="] {
        assert!(!body.contains(secret), "response leaked {secret:?}: {body}");
    }
    assert!(body.contains("[REDACTED]"));

    let (path, authorization) = observed_rx.recv().await.unwrap();
    assert_eq!(
        path,
        "/v1/session_ingress/session/session-123/git_proxy/org/repo.git/HEAD"
    );
    assert_eq!(
        authorization,
        format!("Basic {}", STANDARD.encode("git:ghe-token"))
    );

    upstream.stop().await;
}

#[tokio::test]
async fn test_handler_does_not_follow_upstream_redirects() {
    let (attacker_tx, mut attacker_rx) = mpsc::channel::<()>(1);
    let attacker_app = Router::new().fallback(move || {
        let attacker_tx = attacker_tx.clone();
        async move {
            attacker_tx.send(()).await.unwrap();
            StatusCode::NO_CONTENT
        }
    });
    let attacker = TestHttpServer::start(attacker_app).await;

    let location = format!("{}/stolen", attacker.base_url);
    let redirect_app = Router::new().fallback(move || {
        let location = location.clone();
        async move {
            let mut response = Response::new(Body::from("redirect body"));
            *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
            response
                .headers_mut()
                .insert(LOCATION, HeaderValue::from_str(&location).unwrap());
            response
        }
    });
    let redirect = TestHttpServer::start(redirect_app).await;

    let handler = handler_for(
        Config {
            session_ingress_url: redirect.base_url.clone(),
            session_id: "session-123".to_owned(),
            ..Config::default()
        },
        HashMap::from([("anthropic/demo".to_owned(), "repo-token".to_owned())]),
    );
    let response = handler
        .serve_http(
            Request::builder()
                .uri("/git/anthropic/demo.git/HEAD")
                .header(AUTHORIZATION, "Basic client-auth")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(response_body(response).await, "redirect body");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), attacker_rx.recv())
            .await
            .is_err(),
        "redirect target unexpectedly received repository credentials"
    );

    redirect.stop().await;
    attacker.stop().await;
}

#[tokio::test]
async fn test_server_stop_cancellation_preserves_state_for_later_cleanup() {
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let release = CancellationToken::new();
    let upstream_release = release.clone();
    let upstream_app = Router::new().fallback(move || {
        let started_tx = started_tx.clone();
        let upstream_release = upstream_release.clone();
        async move {
            started_tx.send(()).await.unwrap();
            upstream_release.cancelled().await;
            "done"
        }
    });
    let upstream = TestHttpServer::start(upstream_app).await;

    let manager = new_manager(Some(Config {
        session_ingress_url: upstream.base_url.clone(),
        session_id: "session-123".to_owned(),
        repo_auths: vec![repo_auth("anthropic/demo", "github", "repo-token")],
        logger: Some(gitproxy_test_logger()),
        ..Config::default()
    }))
    .unwrap();
    let base_url = manager.start(&CancellationToken::new()).await.unwrap();
    let request_task = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!("{base_url}/git/anthropic/demo.git/HEAD"))
            .header(AUTHORIZATION, "Basic client-auth")
            .send()
            .await
            .unwrap()
    });
    started_rx.recv().await.unwrap();

    let canceled = CancellationToken::new();
    canceled.cancel();
    let error = manager.stop(&canceled).await.unwrap_err();
    assert_eq!(error.to_string(), "failed to stop server: context canceled");
    assert!(manager.is_running());

    release.cancel();
    assert_eq!(request_task.await.unwrap().status(), StatusCode::OK);
    manager.stop(&CancellationToken::new()).await.unwrap();
    assert!(!manager.is_running());
    upstream.stop().await;
}

#[test]
fn test_sanitize_and_git_path_helpers() {
    let input =
        "Authorization: Bearer sk-ant-abc Basic Z2l0OnNlY3JldA== token=secret sk-ant-ccsr-jwt";
    let sanitized = sanitize_string(input);
    for secret in [
        "sk-ant-abc",
        "Z2l0OnNlY3JldA==",
        "token=secret",
        "sk-ant-ccsr-jwt",
    ] {
        assert!(
            !sanitized.contains(secret),
            "sanitize_string leaked {secret}"
        );
    }

    let error = std::io::Error::other(input);
    assert!(!sanitize_error(Some(&error)).contains("sk-ant-abc"));
    assert_eq!(sanitize_error(None), "");

    for path in [
        "info/refs",
        "git-upload-pack",
        "git-receive-pack",
        "HEAD",
        "objects/ab/cd",
        "refs/heads/main",
    ] {
        assert!(is_valid_git_path(path), "{path:?} should be valid");
    }
    for path in ["", "/HEAD", "../HEAD", "objects/../config", "config"] {
        assert!(!is_valid_git_path(path), "{path:?} should be invalid");
    }
}
