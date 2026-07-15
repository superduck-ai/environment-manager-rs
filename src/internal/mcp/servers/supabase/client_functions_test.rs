//! 对应 Go `internal/mcp/servers/supabase/client_functions_test.go` 的 API 与部署测试。

use std::{collections::BTreeMap, os::unix::fs::symlink};

use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::{Client, FunctionDeployMetadata, collect_function_files, require_dir_under};

/// 对应 Go `supabaseAPIRequest`，便于逐请求断言 method/path/header/body。
struct SupabaseApiRequest {
    method: String,
    path: String,
    authorization: String,
    content_type: String,
    body: String,
}

/// 对应 Go `deployRequest`，保存 multipart 部署请求的完整可观察面。
struct DeployRequest {
    path: String,
    query: String,
    authorization: String,
    content_type: String,
    metadata: FunctionDeployMetadata,
    files: BTreeMap<String, String>,
}

/// 从捕获请求中读取一个 header；缺失值按 Go Header.Get 返回空字符串。
fn request_header(request: &Request, name: &str) -> String {
    request
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// 将 wiremock 请求转换为 Management API 观测结构。
fn supabase_api_request(request: &Request) -> SupabaseApiRequest {
    SupabaseApiRequest {
        method: request.method.as_str().to_owned(),
        path: request.url.path().to_owned(),
        authorization: request_header(request, "authorization"),
        content_type: request_header(request, "content-type"),
        body: String::from_utf8_lossy(&request.body).into_owned(),
    }
}

/// 从 Content-Disposition 中取一个双引号参数。
fn quoted_parameter(headers: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=\"");
    let start = headers.find(&prefix)? + prefix.len();
    let end = headers[start..].find('"')? + start;
    Some(headers[start..end].to_owned())
}

/// 对应 Go `readMultipartDeployRequest`。
///
/// Reqwest 在测试中产生的 multipart body 使用 CRLF 和随机 boundary；解析器只依赖
/// MIME 的 boundary/header/body 分隔规则，不固定随机值。这样既能核对 metadata 的
/// Go `Encoder.Encode` 换行，也能逐一验证 function/shared 文件名和正文。
fn read_multipart_deploy_request(
    request: &Request,
) -> (FunctionDeployMetadata, BTreeMap<String, String>) {
    let content_type = request_header(request, "content-type");
    let boundary = content_type
        .split(';')
        .find_map(|part| part.trim().strip_prefix("boundary="))
        .map(|value| value.trim_matches('"'))
        .expect("multipart boundary");
    let marker = format!("--{boundary}");
    let body = String::from_utf8_lossy(&request.body);
    let mut metadata = None;
    let mut files = BTreeMap::new();

    for raw_part in body.split(&marker) {
        let part = raw_part.trim_start_matches("\r\n");
        if part.is_empty() || part.starts_with("--") {
            continue;
        }
        let (headers, payload) = part
            .split_once("\r\n\r\n")
            .expect("multipart part header/body");
        let payload = payload.strip_suffix("\r\n").unwrap_or(payload);
        match quoted_parameter(headers, "name").as_deref() {
            Some("metadata") => {
                metadata = Some(
                    serde_json::from_str(payload.trim_end()).expect("解析 deploy metadata JSON"),
                );
            }
            Some("file") => {
                let filename = quoted_parameter(headers, "filename").expect("file filename");
                files.insert(filename, payload.to_owned());
            }
            other => panic!("未知 multipart 字段: {other:?}"),
        }
    }
    (metadata.expect("metadata part"), files)
}

#[tokio::test]
async fn test_client_methods_use_recovered_management_api_paths() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/projects/project-ref/database/migrations"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/projects/project-ref/database/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"[{"id":1}]"#))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/projects/project-ref/database/migrations"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"[{"version":"20240531010101","name":"create_todos"}]"#),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/projects/project-ref/types/typescript"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"types":"export type Database = {}"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let http = reqwest::Client::new();
    let client = Client::new("pat-token", "project-ref", server.uri())
        .expect("client")
        .with_http_clients(http.clone(), http);
    let cancellation = CancellationToken::new();
    client
        .apply_migration(&cancellation, "create_todos", "create table todos(id int);")
        .await
        .expect("apply migration");
    assert_eq!(
        client.run_query(&cancellation, "select 1").await.unwrap(),
        r#"[{"id":1}]"#
    );
    let migrations = client.list_migrations(&cancellation).await.unwrap();
    assert_eq!(migrations.len(), 1);
    assert_eq!(migrations[0].name, "create_todos");
    assert_eq!(
        client.generate_types(&cancellation).await.unwrap(),
        "export type Database = {}"
    );

    let requests = server.received_requests().await.expect("捕获 API 请求");
    let seen = requests
        .iter()
        .map(supabase_api_request)
        .collect::<Vec<_>>();
    assert_eq!(seen.len(), 4);
    assert!(
        seen.iter()
            .all(|request| request.authorization == "Bearer pat-token")
    );
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/projects/project-ref/database/migrations");
    assert!(seen[0].content_type.starts_with("application/json"));
    assert!(seen[0].body.contains(r#""name":"create_todos""#));
    assert!(
        seen[0]
            .body
            .contains(r#""query":"create table todos(id int);""#)
    );
    assert_eq!(seen[1].method, "POST");
    assert_eq!(seen[1].path, "/projects/project-ref/database/query");
    assert!(seen[1].body.contains(r#""query":"select 1""#));
    assert_eq!(seen[2].method, "GET");
    assert_eq!(seen[2].path, "/projects/project-ref/database/migrations");
    assert!(seen[2].body.is_empty());
    assert_eq!(seen[3].method, "GET");
    assert_eq!(seen[3].path, "/projects/project-ref/types/typescript");
    assert!(seen[3].body.is_empty());
}

#[tokio::test]
async fn test_client_do_request_surfaces_api_error_messages() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string(r#"{"message":"bad sql"}"#))
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::new("pat-token", "project-ref", server.uri()).expect("client");
    let error = client
        .run_query(&CancellationToken::new(), "bad")
        .await
        .expect_err("API error");
    assert!(error.to_string().contains("API error 400: bad sql"));
}

#[tokio::test]
async fn test_deploy_function_builds_recovered_multipart_request() {
    let project = tempdir().expect("project");
    let function = project.path().join("supabase/functions/hello_fn");
    let shared = project.path().join("supabase/functions/_shared");
    std::fs::create_dir_all(&function).expect("function dir");
    std::fs::create_dir_all(&shared).expect("shared dir");
    std::fs::write(
        function.join("index.ts"),
        b"Deno.serve(() => new Response('ok'))",
    )
    .expect("index.ts");
    std::fs::write(shared.join("util.ts"), b"export const value = 1;").expect("util.ts");

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/projects/project-ref/functions/deploy"))
        .and(query_param("slug", "hello_fn"))
        .respond_with(ResponseTemplate::new(201).set_body_string(
            r#"{"id":"fn-id","slug":"hello_fn","name":"hello_fn","status":"ACTIVE","version":7}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::new("pat-token", "project-ref", server.uri())
        .expect("client")
        .with_http_clients(reqwest::Client::new(), reqwest::Client::new());
    let result = client
        .deploy_function(
            &CancellationToken::new(),
            "hello_fn",
            &function,
            Some(&shared),
            project.path(),
        )
        .await
        .expect("deploy function");
    assert_eq!(result.version, 7);
    assert_eq!(result.status, "ACTIVE");
    assert_eq!(client.project_ref(), "project-ref");

    let requests = server.received_requests().await.expect("捕获 deploy 请求");
    let request = &requests[0];
    let (metadata, files) = read_multipart_deploy_request(request);
    let observed = DeployRequest {
        path: request.url.path().to_owned(),
        query: request.url.query().unwrap_or_default().to_owned(),
        authorization: request_header(request, "authorization"),
        content_type: request_header(request, "content-type"),
        metadata,
        files,
    };
    assert_eq!(observed.path, "/projects/project-ref/functions/deploy");
    assert_eq!(observed.query, "slug=hello_fn");
    assert_eq!(observed.authorization, "Bearer pat-token");
    assert!(observed.content_type.starts_with("multipart/form-data"));
    assert_eq!(observed.metadata.name, "hello_fn");
    assert_eq!(observed.metadata.entrypoint_path, "index.ts");
    assert!(observed.metadata.verify_jwt);
    assert_eq!(
        observed.files.get("index.ts").map(String::as_str),
        Some("Deno.serve(() => new Response('ok'))")
    );
    assert_eq!(
        observed.files.get("_shared/util.ts").map(String::as_str),
        Some("export const value = 1;")
    );
}

#[tokio::test]
async fn test_deploy_function_validation_and_api_statuses() {
    let project = tempdir().expect("project");
    let function = project.path().join("supabase/functions/hello");
    std::fs::create_dir_all(&function).expect("function dir");
    let mut client = Client::new("pat-token", "project-ref", "http://127.0.0.1").expect("client");
    let error = client
        .deploy_function(
            &CancellationToken::new(),
            "1bad",
            &function,
            None,
            project.path(),
        )
        .await
        .expect_err("invalid slug");
    assert!(error.to_string().contains("invalid slug"));

    let error = client
        .deploy_function(
            &CancellationToken::new(),
            "hello",
            &function,
            None,
            project.path(),
        )
        .await
        .expect_err("missing index");
    assert!(error.to_string().contains("entrypoint index.ts not found"));
    std::fs::write(function.join("index.ts"), b"ok").expect("index.ts");

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(402).set_body_string("plan limit"))
        .expect(1)
        .mount(&server)
        .await;
    client = Client::new("pat-token", "project-ref", server.uri())
        .expect("client")
        .with_http_clients(reqwest::Client::new(), reqwest::Client::new());
    let error = client
        .deploy_function(
            &CancellationToken::new(),
            "hello",
            &function,
            None,
            project.path(),
        )
        .await
        .expect_err("plan limit");
    assert!(error.to_string().contains("max functions reached"));
}

#[test]
fn test_collect_function_files_and_trust_root_validation() {
    let project = tempdir().expect("project");
    let function = project.path().join("supabase/functions/hello");
    let shared = project.path().join("supabase/functions/_shared");
    std::fs::create_dir_all(&function).expect("function dir");
    std::fs::create_dir_all(&shared).expect("shared dir");
    std::fs::write(function.join("index.ts"), b"index").expect("index.ts");
    std::fs::write(shared.join("util.ts"), b"util").expect("util.ts");
    symlink(function.join("index.ts"), function.join("link.ts")).expect("symlink");

    let (files, total_size) =
        collect_function_files(&function, Some(&shared), project.path()).expect("collect files");
    assert_eq!(total_size, 9);
    let paths = files
        .iter()
        .map(|file| file.rel_path.as_str())
        .collect::<Vec<_>>();
    assert!(paths.contains(&"index.ts"));
    assert!(paths.contains(&"_shared/util.ts"));
    assert!(!paths.contains(&"link.ts"));

    let outside = tempdir().expect("outside");
    let root = std::fs::canonicalize(project.path()).expect("canonical trust root");
    let error = require_dir_under(outside.path(), &root).expect_err("拒绝 trust root 外目录");
    assert!(error.to_string().contains("outside trust root"));
}
