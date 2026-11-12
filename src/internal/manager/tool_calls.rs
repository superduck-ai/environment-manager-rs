use crate::internal::mcp::servers::runtime_tools::{Registry, ToolResult};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Deserialize)]
struct Call {
    call_uuid: String,
    attempt: u32,
    tool: String,
    #[serde(default)]
    input: Value,
    state: String,
    deadline_at: DateTime<Utc>,
}
#[derive(Deserialize)]
struct Claims {
    calls: Vec<Call>,
}
#[derive(Serialize, Deserialize)]
struct Completion {
    status: String,
    body: Vec<u8>,
}
struct Runner {
    client: Client,
    endpoint: String,
    token: String,
    registry: Arc<Registry>,
    directory: PathBuf,
}

pub(super) async fn run(
    base: String,
    token: String,
    registry: Arc<Registry>,
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(35))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(std::io::Error::other)?;
    let directory = std::env::var_os("OMA_TOOL_CALL_LEDGER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/lib/oma/tool-calls"));
    tokio::fs::create_dir_all(&directory).await?;
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).await?;
    let runner = Arc::new(Runner {
        client,
        endpoint: format!("{}/v1/code/sandbox/tool-calls", base.trim_end_matches('/')),
        token,
        registry,
        directory,
    });
    runner.claim_loop(cancellation).await
}

impl Runner {
    async fn claim_loop(self: Arc<Self>, cancellation: CancellationToken) -> std::io::Result<()> {
        let mut executions = tokio::task::JoinSet::new();
        for call in self.saved_results().await? {
            let runner = self.clone();
            let parent = cancellation.clone();
            executions.spawn(async move { runner.replay(call, &parent).await });
        }
        loop {
            while let Some(result) = executions.try_join_next() {
                Self::execution_finished(result);
            }
            if executions.len() == 4 {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    Some(result) = executions.join_next() => Self::execution_finished(result),
                }
                continue;
            }
            let slots = 4 - executions.len();
            let block_ms = if executions.is_empty() { 25000 } else { 1000 };
            let request = self
                .client
                .post(format!(
                    "{}/claims?block_ms={block_ms}&max={slots}",
                    self.endpoint
                ))
                .bearer_auth(&self.token)
                .send();
            let response = tokio::select! {
                () = cancellation.cancelled() => break,
                response = request => response,
            };
            if let Ok(response) = response {
                if response.status().is_success() {
                    if let Ok(claims) = response.json::<Claims>().await {
                        if claims.calls.is_empty() {
                            tokio::select! {
                                () = cancellation.cancelled() => break,
                                () = tokio::time::sleep(Duration::from_millis(100)) => {},
                            }
                        }
                        for call in claims.calls.into_iter().take(slots) {
                            let runner = self.clone();
                            let parent = cancellation.clone();
                            executions.spawn(async move { runner.execute(call, &parent).await });
                        }
                        continue;
                    }
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => break,
                () = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }
        while let Some(result) = executions.join_next().await {
            Self::execution_finished(result);
        }
        Ok(())
    }

    fn execution_finished(result: Result<std::io::Result<()>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "tool call execution interrupted"),
            Err(error) => tracing::warn!(error = %error, "tool call execution task failed"),
        }
    }

    async fn saved_results(&self) -> std::io::Result<Vec<Call>> {
        let mut calls = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.ends_with(".tmp") {
                tokio::fs::remove_file(entry.path()).await?;
                continue;
            }
            let Some((call_uuid, attempt)) = name
                .strip_suffix(".result")
                .and_then(|stem| stem.rsplit_once('-'))
            else {
                continue;
            };
            let (Ok(_), Ok(attempt)) = (uuid::Uuid::parse_str(call_uuid), attempt.parse()) else {
                continue;
            };
            calls.push(Call {
                call_uuid: call_uuid.to_owned(),
                attempt,
                tool: String::new(),
                input: Value::Null,
                state: String::new(),
                deadline_at: Utc::now(),
            });
        }
        Ok(calls)
    }

    async fn replay(&self, call: Call, parent: &CancellationToken) -> std::io::Result<()> {
        let bytes = tokio::fs::read(self.ledger(&call, "result")).await?;
        let completion: Completion = serde_json::from_slice(&bytes)?;
        if self.report(&call, completion, parent).await? {
            self.forget(&call).await?;
        }
        Ok(())
    }

    async fn forget(&self, call: &Call) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.ledger(call, "result")).await {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    fn ledger(&self, call: &Call, suffix: &str) -> PathBuf {
        self.directory
            .join(format!("{}-{}.{}", call.call_uuid, call.attempt, suffix))
    }
    async fn event(&self, call: &Call, event: &str) -> Result<Call, reqwest::Error> {
        self.client
            .post(format!("{}/{}/events", self.endpoint, call.call_uuid))
            .bearer_auth(&self.token)
            .json(&json!({"attempt":call.attempt,"event":event}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }
    async fn execute(&self, call: Call, parent: &CancellationToken) -> std::io::Result<()> {
        if uuid::Uuid::parse_str(&call.call_uuid).is_err() {
            return Err(std::io::Error::other("invalid call UUID"));
        }
        let result_path = self.ledger(&call, "result");
        if tokio::fs::try_exists(&result_path).await? {
            return self.replay(call, parent).await;
        }
        let marker = self.directory.join(format!("{}.started", call.call_uuid));
        if tokio::fs::try_exists(&marker).await? {
            return Ok(());
        }
        let Some(tool) = self.registry.get(&call.tool) else {
            return Err(std::io::Error::other("unknown claimed tool"));
        };
        if let Err(error) = tool.validate(&call.input) {
            return self
                .complete(&call, "failed", tool.error_result(error), parent)
                .await;
        }
        let accepted = self
            .event(&call, "started")
            .await
            .map_err(std::io::Error::other)?;
        if accepted.state != "running" {
            return Ok(());
        }
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(marker)
            .await?;
        file.write_all(format!("{}", call.attempt).as_bytes())
            .await?;
        file.sync_all().await?;
        tokio::fs::File::open(&self.directory)
            .await?
            .sync_all()
            .await?;
        let cancellation = parent.child_token();
        let execution = tool.handle(cancellation.clone(), call.input.clone());
        tokio::pin!(execution);
        let delay = (call.deadline_at - Utc::now()).to_std().unwrap_or_default();
        let deadline = tokio::time::sleep(delay);
        tokio::pin!(deadline);
        let heartbeat = self.heartbeat(&call, &cancellation);
        tokio::pin!(heartbeat);
        let mut heartbeat_done = false;
        let mut stopped = false;
        let result = loop {
            tokio::select! {
                biased;
                result = &mut execution => break result.unwrap_or_else(|error|tool.error_result(error)),
                () = parent.cancelled(), if !stopped => { cancellation.cancel(); stopped=true; }
                () = &mut deadline, if !stopped => { cancellation.cancel(); stopped=true; }
                () = &mut heartbeat, if !heartbeat_done => {heartbeat_done=true;stopped=cancellation.is_cancelled();}
            }
        };
        let status = if stopped {
            "stopped"
        } else if result.is_error {
            "failed"
        } else {
            "succeeded"
        };
        self.complete(&call, status, result, parent).await
    }
    async fn heartbeat(&self, call: &Call, cancellation: &CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {()=cancellation.cancelled()=>return,_=interval.tick()=>{}}
            let state = tokio::select! {()=cancellation.cancelled()=>return,state=self.event(call,"heartbeat")=>state};
            if let Ok(state) = state {
                if ["cancel_requested", "lost", "cancelled", "expired"]
                    .contains(&state.state.as_str())
                {
                    cancellation.cancel();
                    return;
                }
            }
        }
    }
    async fn complete(
        &self,
        call: &Call,
        status: &str,
        result: ToolResult,
        parent: &CancellationToken,
    ) -> std::io::Result<()> {
        let mut body = encode_result(&result)?;
        let mut status = status.to_owned();
        if body.len() > 8 * 1024 * 1024 {
            status = "failed".into();
            body = serde_json::to_vec(&ToolResult::error(
                "Tool output exceeds 8 MiB. Use narrower line ranges or search limits.",
            ))?;
        }
        let completion = Completion { status, body };
        let bytes = serde_json::to_vec(&completion)?;
        let path = self.ledger(call, "result");
        let temporary = self.ledger(call, "tmp");
        tokio::fs::write(&temporary, bytes).await?;
        tokio::fs::File::open(&temporary).await?.sync_all().await?;
        tokio::fs::rename(temporary, path).await?;
        tokio::fs::File::open(&self.directory)
            .await?
            .sync_all()
            .await?;
        if self.report(call, completion, parent).await? {
            self.forget(call).await?;
        }
        Ok(())
    }
    async fn report(
        &self,
        call: &Call,
        completion: Completion,
        parent: &CancellationToken,
    ) -> std::io::Result<bool> {
        let hash = format!("{:x}", Sha256::digest(&completion.body));
        let report_cancellation = parent.child_token();
        let heartbeat = self.heartbeat(call, &report_cancellation);
        tokio::pin!(heartbeat);
        let mut heartbeat_done = false;
        let mut delay = Duration::from_millis(500);
        loop {
            let request = self
                .client
                .put(format!("{}/{}/result", self.endpoint, call.call_uuid))
                .bearer_auth(&self.token)
                .header("Oma-Tool-Call-Attempt", call.attempt)
                .header("Oma-Result-Status", &completion.status)
                .header("Oma-Result-Sha256", &hash)
                .header("Content-Type", "application/json")
                .body(completion.body.clone())
                .send();
            tokio::pin!(request);
            let response = loop {
                tokio::select! {
                    response=&mut request=>break response,
                    ()=&mut heartbeat,if !heartbeat_done=>{heartbeat_done=true;}
                }
            };
            if let Ok(response) = response {
                if response.status().is_success()
                    || [
                        StatusCode::BAD_REQUEST,
                        StatusCode::PAYLOAD_TOO_LARGE,
                        StatusCode::UNAUTHORIZED,
                        StatusCode::FORBIDDEN,
                        StatusCode::NOT_FOUND,
                        StatusCode::CONFLICT,
                    ]
                    .contains(&response.status())
                {
                    return Ok(true);
                }
            }
            tokio::select! {
                ()=parent.cancelled()=>return Ok(false),
                ()=tokio::time::sleep(delay)=>{},
                ()=&mut heartbeat,if !heartbeat_done=>{heartbeat_done=true;}
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

fn encode_result(result: &ToolResult) -> Result<Vec<u8>, serde_json::Error> {
    let mut value = serde_json::to_value(result)?;
    if let Some(content) = value.get_mut("content").and_then(Value::as_array_mut) {
        for block in content {
            if block.get("type").and_then(Value::as_str) == Some("text")
                && block.get("text").is_none()
            {
                block["text"] = json!("");
            }
        }
    }
    serde_json::to_vec(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completion_bytes_keep_empty_text_blocks() {
        let value: Value =
            serde_json::from_slice(&encode_result(&ToolResult::text("")).unwrap()).unwrap();
        assert_eq!(value["content"][0]["text"], json!(""));
    }

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[tokio::test]
    async fn terminal_start_response_prevents_execution() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let call = Call {
            call_uuid: uuid::Uuid::new_v4().to_string(),
            attempt: 1,
            tool: "bash".into(),
            input: json!({"command":"touch forbidden"}),
            state: "dispatched".into(),
            deadline_at: Utc::now() + chrono::Duration::seconds(30),
        };
        Mock::given(method("POST")).and(path(format!("/{}/events",call.call_uuid))).respond_with(ResponseTemplate::new(200).set_body_json(json!({"call_uuid":call.call_uuid,"attempt":1,"tool":"bash","state":"lost","deadline_at":call.deadline_at}))).mount(&server).await;
        let runner = Runner {
            client: Client::new(),
            endpoint: server.uri(),
            token: "test".into(),
            registry: Arc::new(Registry::builtin(directory.path().to_path_buf(), None)),
            directory: directory.path().join("ledger"),
        };
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        runner
            .execute(call, &CancellationToken::new())
            .await
            .unwrap();
        assert!(!directory.path().join("forbidden").exists());
    }

    #[tokio::test]
    async fn claims_execute_independently_with_available_slots() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let calls = vec![
            json!({"call_uuid": uuid::Uuid::new_v4().to_string(), "attempt": 1,
                "tool": "bash", "state": "dispatched",
                "deadline_at": Utc::now() + chrono::Duration::seconds(30),
                "input": {"command": "touch slow-started; while [ ! -e release ]; do sleep 0.05; done"}}),
            json!({"call_uuid": uuid::Uuid::new_v4().to_string(), "attempt": 1,
                "tool": "bash", "state": "dispatched",
                "deadline_at": Utc::now() + chrono::Duration::seconds(30),
                "input": {"command": "touch quick-finished"}}),
        ];
        for call in &calls {
            let mut running = call.clone();
            running["state"] = json!("running");
            Mock::given(method("POST"))
                .and(path(format!(
                    "/{}/events",
                    call["call_uuid"].as_str().unwrap()
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(running))
                .mount(&server)
                .await;
        }
        let delivered = AtomicBool::new(false);
        Mock::given(method("POST"))
            .and(path("/claims"))
            .respond_with(move |_: &wiremock::Request| {
                let response = if delivered.swap(true, Ordering::SeqCst) {
                    vec![]
                } else {
                    calls.clone()
                };
                ResponseTemplate::new(200).set_body_json(json!({"calls": response}))
            })
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let runner = Arc::new(Runner {
            client: Client::new(),
            endpoint: server.uri(),
            token: "test".into(),
            registry: Arc::new(Registry::builtin(directory.path().to_path_buf(), None)),
            directory: directory.path().join("ledger"),
        });
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(runner.claim_loop(cancellation.clone()));
        let parallel = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if directory.path().join("slow-started").exists()
                    && directory.path().join("quick-finished").exists()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        tokio::fs::write(directory.path().join("release"), b"")
            .await
            .unwrap();
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            parallel.is_ok(),
            "a blocked call prevented another call from executing"
        );
    }

    #[tokio::test]
    async fn repeated_delivery_after_settled_report_does_not_execute_again() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let call = Call {
            call_uuid: uuid::Uuid::new_v4().to_string(),
            attempt: 1,
            tool: "bash".into(),
            input: json!({"command":"printf x >> counter"}),
            state: "dispatched".into(),
            deadline_at: Utc::now() + chrono::Duration::seconds(30),
        };
        Mock::given(method("POST")).and(path(format!("/{}/events",call.call_uuid))).respond_with(ResponseTemplate::new(200).set_body_json(json!({"call_uuid":call.call_uuid,"attempt":1,"tool":"bash","state":"running","deadline_at":call.deadline_at}))).mount(&server).await;
        Mock::given(method("PUT"))
            .and(path(format!("/{}/result", call.call_uuid)))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let runner = Runner {
            client: Client::new(),
            endpoint: server.uri(),
            token: "test".into(),
            registry: Arc::new(Registry::builtin(directory.path().to_path_buf(), None)),
            directory: directory.path().join("ledger"),
        };
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        runner
            .execute(call.clone(), &CancellationToken::new())
            .await
            .unwrap();
        let settled = call.clone();
        runner
            .execute(call, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(directory.path().join("counter"))
                .await
                .unwrap(),
            b"x"
        );
        assert!(
            !tokio::fs::try_exists(runner.ledger(&settled, "result"))
                .await
                .unwrap()
        );
    }

    fn saved_call() -> Call {
        Call {
            call_uuid: uuid::Uuid::new_v4().to_string(),
            attempt: 2,
            tool: "bash".into(),
            input: json!({"command":"printf x >> counter"}),
            state: "dispatched".into(),
            deadline_at: Utc::now() + chrono::Duration::seconds(30),
        }
    }

    async fn runner_with_saved_result(
        server: &MockServer,
        directory: &std::path::Path,
        call: &Call,
    ) -> Arc<Runner> {
        let runner = Arc::new(Runner {
            client: Client::new(),
            endpoint: server.uri(),
            token: "test".into(),
            registry: Arc::new(Registry::builtin(directory.to_path_buf(), None)),
            directory: directory.join("ledger"),
        });
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        let completion = Completion {
            status: "succeeded".into(),
            body: br#"{"content":[{"type":"text","text":"saved"}],"isError":false}"#.to_vec(),
        };
        tokio::fs::write(
            runner.ledger(call, "result"),
            serde_json::to_vec(&completion).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(runner.ledger(call, "tmp"), b"partial")
            .await
            .unwrap();
        Mock::given(method("POST"))
            .and(path("/claims"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"calls": []})))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/{}/events", call.call_uuid)))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"call_uuid":call.call_uuid,"attempt":call.attempt,"tool":"bash","state":"running","deadline_at":call.deadline_at})))
            .mount(server)
            .await;
        runner
    }

    #[tokio::test]
    async fn transient_report_failure_keeps_saved_result_for_next_start() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let call = saved_call();
        let runner = runner_with_saved_result(&server, directory.path(), &call).await;
        Mock::given(method("PUT"))
            .and(path(format!("/{}/result", call.call_uuid)))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(runner.clone().claim_loop(cancellation.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method.as_str() != "PUT")
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            tokio::fs::try_exists(runner.ledger(&call, "result"))
                .await
                .unwrap()
        );
        assert!(!directory.path().join("counter").exists());
    }

    #[tokio::test]
    async fn saved_result_is_reported_on_start_without_executing_again() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let call = saved_call();
        let runner = runner_with_saved_result(&server, directory.path(), &call).await;
        Mock::given(method("PUT"))
            .and(path(format!("/{}/result", call.call_uuid)))
            .and(wiremock::matchers::header("Oma-Tool-Call-Attempt", "2"))
            .and(wiremock::matchers::header("Oma-Result-Status", "succeeded"))
            .and(wiremock::matchers::body_string(
                r#"{"content":[{"type":"text","text":"saved"}],"isError":false}"#,
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(runner.clone().claim_loop(cancellation.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::fs::try_exists(runner.ledger(&call, "result"))
                .await
                .unwrap()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !tokio::fs::try_exists(runner.ledger(&call, "tmp"))
                .await
                .unwrap()
        );
        assert!(!directory.path().join("counter").exists());
    }
}
