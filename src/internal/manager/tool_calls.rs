//! Brain 模式沙箱工具调用的领取、执行、本地 ledger 与上报。
use crate::internal::mcp::servers::runtime_tools::{Registry, ToolResult};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(super) const DRAIN_GRACE: Duration = Duration::from_secs(10);
pub(super) const ABORT_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_NOTE: &str =
    "Sandbox manager shut down before the command finished; the output above may be partial.";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    body: Vec<u8>,
}
impl Completion {
    /// 把 ledger 中的完成记录映射为上报使用的结果状态与原因，兼容旧版本的 stopped。
    fn wire(&self) -> (&str, Option<&str>) {
        match (self.status.as_str(), self.reason.as_deref()) {
            ("stopped", _) => ("interrupted", Some("stop_requested")),
            (status, reason) => (status, reason),
        }
    }
}
#[derive(Clone, Default)]
struct Signals {
    draining: CancellationToken,
    abort: CancellationToken,
    halt: CancellationToken,
}
struct Runner {
    client: Client,
    endpoint: String,
    token: String,
    registry: Arc<Registry>,
    directory: PathBuf,
    drain_grace: Duration,
    abort_budget: Duration,
}

/// 创建领取客户端与本地 ledger 目录，并运行领取循环直到排空信号结束。
pub(super) async fn run(
    base: String,
    token: String,
    registry: Arc<Registry>,
    draining: CancellationToken,
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
        drain_grace: DRAIN_GRACE,
        abort_budget: ABORT_BUDGET,
    });
    runner.claim_loop(draining).await
}

impl Runner {
    /// 补报 ledger 中未确认的结果后按空闲槽位领取调用，收到排空信号后进入排空流程。
    async fn claim_loop(self: Arc<Self>, draining: CancellationToken) -> std::io::Result<()> {
        let signals = Signals {
            draining: draining.clone(),
            ..Signals::default()
        };
        let mut executions = tokio::task::JoinSet::new();
        for call in self.saved_results().await? {
            let runner = self.clone();
            let signals = signals.clone();
            executions.spawn(async move { runner.replay(call, &signals.halt).await });
        }
        loop {
            while let Some(result) = executions.try_join_next() {
                Self::execution_finished(result);
            }
            if executions.len() == 4 {
                tokio::select! {
                    () = draining.cancelled() => break,
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
                () = draining.cancelled() => break,
                response = request => response,
            };
            if let Ok(response) = response {
                if response.status().is_success() {
                    if let Ok(claims) = response.json::<Claims>().await {
                        if claims.calls.is_empty() {
                            tokio::select! {
                                () = draining.cancelled() => break,
                                () = tokio::time::sleep(Duration::from_millis(100)) => {},
                            }
                        }
                        for call in claims.calls.into_iter().take(slots) {
                            let runner = self.clone();
                            let signals = signals.clone();
                            executions.spawn(async move { runner.execute(call, &signals).await });
                        }
                        continue;
                    }
                }
            }
            tokio::select! {
                () = draining.cancelled() => break,
                () = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }
        self.drain(executions, &signals).await;
        Ok(())
    }

    /// 按排空宽限期等待在途调用自然结束，超时后中止执行，再在上报预算内等待结果上报。
    async fn drain(
        &self,
        mut executions: tokio::task::JoinSet<std::io::Result<()>>,
        signals: &Signals,
    ) {
        for (budget, signal) in [
            (self.drain_grace, &signals.abort),
            (self.abort_budget, &signals.halt),
        ] {
            let joined = tokio::time::timeout(budget, async {
                while let Some(result) = executions.join_next().await {
                    Self::execution_finished(result);
                }
            })
            .await;
            if joined.is_ok() {
                return;
            }
            signal.cancel();
        }
        while let Some(result) = executions.join_next().await {
            Self::execution_finished(result);
        }
    }

    /// 记录单个调用任务的结束错误，不影响其他调用。
    fn execution_finished(result: Result<std::io::Result<()>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "tool call execution interrupted"),
            Err(error) => tracing::warn!(error = %error, "tool call execution task failed"),
        }
    }

    /// 扫描 ledger 中已保存但未确认的结果，并删除未完成 rename 的临时文件。
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

    /// 读取 ledger 中保存的结果并补报，服务端确认后删除结果文件。
    async fn replay(&self, call: Call, halt: &CancellationToken) -> std::io::Result<()> {
        let bytes = tokio::fs::read(self.ledger(&call, "result")).await?;
        let completion: Completion = serde_json::from_slice(&bytes)?;
        if self.report(&call, completion, halt).await? {
            self.forget(&call).await?;
        }
        Ok(())
    }

    /// 删除已被服务端确认或永久拒绝的结果文件，保留 started 标记以拒绝重复执行。
    async fn forget(&self, call: &Call) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.ledger(call, "result")).await {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    /// 返回调用在 ledger 中按调用 UUID、attempt 与后缀命名的文件路径。
    fn ledger(&self, call: &Call, suffix: &str) -> PathBuf {
        self.directory
            .join(format!("{}-{}.{}", call.call_uuid, call.attempt, suffix))
    }
    /// 上报 started、heartbeat 或 reject 事件，并返回服务端当前的调用状态。
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
    /// 在提交点 started 被接受后执行工具，按停止请求、deadline 或退出中止并记录中断原因。
    async fn execute(&self, call: Call, signals: &Signals) -> std::io::Result<()> {
        if uuid::Uuid::parse_str(&call.call_uuid).is_err() {
            return Err(std::io::Error::other("invalid call UUID"));
        }
        let result_path = self.ledger(&call, "result");
        if tokio::fs::try_exists(&result_path).await? {
            return self.replay(call, &signals.halt).await;
        }
        let marker = self.directory.join(format!("{}.started", call.call_uuid));
        if tokio::fs::try_exists(&marker).await? {
            return Ok(());
        }
        let Some(tool) = self.registry.get(&call.tool) else {
            let failure = ToolResult::error(format!("Unknown sandbox tool: {}", call.tool));
            return self
                .complete(&call, "failed", None, failure, &signals.halt)
                .await;
        };
        if let Err(error) = tool.validate(&call.input) {
            return self
                .complete(
                    &call,
                    "failed",
                    None,
                    tool.error_result(error),
                    &signals.halt,
                )
                .await;
        }
        if signals.draining.is_cancelled() {
            self.event(&call, "reject")
                .await
                .map_err(std::io::Error::other)?;
            return Ok(());
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
        let cancellation = signals.abort.child_token();
        let execution = tool.handle(cancellation.clone(), call.input.clone());
        tokio::pin!(execution);
        let delay = (call.deadline_at - Utc::now()).to_std().unwrap_or_default();
        let deadline = tokio::time::sleep(delay);
        tokio::pin!(deadline);
        let heartbeat = self.heartbeat(&call, &cancellation);
        tokio::pin!(heartbeat);
        let mut heartbeat_done = false;
        let mut interruption = None;
        let result = loop {
            tokio::select! {
                biased;
                result = &mut execution => break result.unwrap_or_else(|error|tool.error_result(error)),
                () = signals.abort.cancelled(), if interruption.is_none() => { interruption = Some("executor_shutdown"); cancellation.cancel(); }
                () = &mut deadline, if interruption.is_none() => { interruption = Some("deadline"); cancellation.cancel(); }
                () = &mut heartbeat, if !heartbeat_done => {
                    heartbeat_done = true;
                    if cancellation.is_cancelled() && interruption.is_none() {
                        interruption = Some("stop_requested");
                    }
                }
            }
        };
        let status = if interruption.is_some() {
            "interrupted"
        } else if result.is_error {
            "failed"
        } else {
            "succeeded"
        };
        self.complete(&call, status, interruption, result, &signals.halt)
            .await
    }
    /// 周期性续约执行租约，服务端要求停止或调用已结束时取消本次执行。
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
    /// 把结果原子写入 ledger 后上报，退出中止时在结果末尾追加说明。
    async fn complete(
        &self,
        call: &Call,
        status: &str,
        reason: Option<&str>,
        mut result: ToolResult,
        halt: &CancellationToken,
    ) -> std::io::Result<()> {
        if reason == Some("executor_shutdown") {
            result
                .content
                .extend(ToolResult::text(SHUTDOWN_NOTE).content);
        }
        let mut body = encode_result(&result)?;
        let mut status = status.to_owned();
        let mut reason = reason.map(str::to_owned);
        if body.len() > 8 * 1024 * 1024 {
            status = "failed".into();
            reason = None;
            body = serde_json::to_vec(&ToolResult::error(
                "Tool output exceeds 8 MiB. Use narrower line ranges or search limits.",
            ))?;
        }
        let completion = Completion {
            status,
            reason,
            body,
        };
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
        if self.report(call, completion, halt).await? {
            self.forget(call).await?;
        }
        Ok(())
    }
    /// 按 SHA-256 幂等上报结果并持续心跳；收到确认或永久拒绝返回 true，停止信号到达时返回 false。
    async fn report(
        &self,
        call: &Call,
        completion: Completion,
        halt: &CancellationToken,
    ) -> std::io::Result<bool> {
        let hash = format!("{:x}", Sha256::digest(&completion.body));
        let (status, reason) = completion.wire();
        let report_cancellation = halt.child_token();
        let heartbeat = self.heartbeat(call, &report_cancellation);
        tokio::pin!(heartbeat);
        let mut heartbeat_done = false;
        let mut delay = Duration::from_millis(500);
        loop {
            let mut request = self
                .client
                .put(format!("{}/{}/result", self.endpoint, call.call_uuid))
                .bearer_auth(&self.token)
                .header("Oma-Tool-Call-Attempt", call.attempt)
                .header("Oma-Result-Status", status)
                .header("Oma-Result-Sha256", &hash)
                .header("Content-Type", "application/json");
            if let Some(reason) = reason {
                request = request.header("Oma-Result-Reason", reason);
            }
            let request = request.body(completion.body.clone()).send();
            tokio::pin!(request);
            let response = loop {
                tokio::select! {
                    ()=halt.cancelled()=>return Ok(false),
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
                ()=halt.cancelled()=>return Ok(false),
                ()=tokio::time::sleep(delay)=>{},
                ()=&mut heartbeat,if !heartbeat_done=>{heartbeat_done=true;}
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

/// 把工具结果序列化为上报字节，补齐空文本块的 text 字段。
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
            drain_grace: DRAIN_GRACE,
            abort_budget: ABORT_BUDGET,
        };
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        runner.execute(call, &Signals::default()).await.unwrap();
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
            drain_grace: DRAIN_GRACE,
            abort_budget: ABORT_BUDGET,
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
            drain_grace: DRAIN_GRACE,
            abort_budget: ABORT_BUDGET,
        };
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        runner
            .execute(call.clone(), &Signals::default())
            .await
            .unwrap();
        let settled = call.clone();
        runner.execute(call, &Signals::default()).await.unwrap();
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
            drain_grace: Duration::from_millis(100),
            abort_budget: Duration::from_millis(100),
        });
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        let completion = Completion {
            status: "succeeded".into(),
            reason: None,
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

    fn bash_call(command: &str, deadline: chrono::Duration) -> Call {
        Call {
            call_uuid: uuid::Uuid::new_v4().to_string(),
            attempt: 1,
            tool: "bash".into(),
            input: json!({ "command": command }),
            state: "dispatched".into(),
            deadline_at: Utc::now() + deadline,
        }
    }

    async fn short_runner(server: &MockServer, directory: &std::path::Path) -> Arc<Runner> {
        let runner = Arc::new(Runner {
            client: Client::new(),
            endpoint: server.uri(),
            token: "test".into(),
            registry: Arc::new(Registry::builtin(directory.to_path_buf(), None)),
            directory: directory.join("ledger"),
            drain_grace: Duration::from_millis(300),
            abort_budget: Duration::from_secs(2),
        });
        tokio::fs::create_dir_all(&runner.directory).await.unwrap();
        runner
    }

    async fn mount_events(server: &MockServer, call: &Call, state: &str) {
        Mock::given(method("POST"))
            .and(path(format!("/{}/events", call.call_uuid)))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"call_uuid":call.call_uuid,"attempt":call.attempt,"tool":"bash","state":state,"deadline_at":call.deadline_at})))
            .mount(server)
            .await;
    }

    async fn reported_results(server: &MockServer) -> Vec<wiremock::Request> {
        server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.method.as_str() == "PUT")
            .collect()
    }

    fn header(request: &wiremock::Request, name: &str) -> String {
        request
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }

    #[tokio::test]
    async fn draining_manager_returns_unstarted_call_without_executing() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let runner = short_runner(&server, directory.path()).await;
        let call = bash_call("touch forbidden", chrono::Duration::seconds(30));
        mount_events(&server, &call, "pending").await;
        let signals = Signals::default();
        signals.draining.cancel();
        runner.execute(call.clone(), &signals).await.unwrap();
        let events: Vec<Value> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("/events"))
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect();
        assert_eq!(events, vec![json!({"attempt": 1, "event": "reject"})]);
        assert!(!directory.path().join("forbidden").exists());
        assert!(reported_results(&server).await.is_empty());
    }

    #[tokio::test]
    async fn unknown_claimed_tool_reports_failure() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let runner = short_runner(&server, directory.path()).await;
        let mut call = bash_call("true", chrono::Duration::seconds(30));
        call.tool = "missing".into();
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        runner.execute(call, &Signals::default()).await.unwrap();
        let results = reported_results(&server).await;
        assert_eq!(results.len(), 1);
        assert_eq!(header(&results[0], "Oma-Result-Status"), "failed");
        assert!(
            String::from_utf8_lossy(&results[0].body).contains("Unknown sandbox tool: missing")
        );
    }

    #[tokio::test]
    async fn local_deadline_reports_interruption_with_reason() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let runner = short_runner(&server, directory.path()).await;
        let call = bash_call(
            "echo before-deadline; sleep 30",
            chrono::Duration::milliseconds(800),
        );
        mount_events(&server, &call, "running").await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        runner.execute(call, &Signals::default()).await.unwrap();
        let results = reported_results(&server).await;
        assert_eq!(results.len(), 1);
        assert_eq!(header(&results[0], "Oma-Result-Status"), "interrupted");
        assert_eq!(header(&results[0], "Oma-Result-Reason"), "deadline");
        assert!(String::from_utf8_lossy(&results[0].body).contains("before-deadline"));
    }

    #[tokio::test]
    async fn shutdown_lets_running_call_finish_within_grace() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let runner = short_runner(&server, directory.path()).await;
        let call = bash_call("sleep 0.1; echo finished", chrono::Duration::seconds(30));
        mount_events(&server, &call, "running").await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let signals = Signals::default();
        let mut executions = tokio::task::JoinSet::new();
        let worker = runner.clone();
        let execution_signals = signals.clone();
        executions.spawn(async move { worker.execute(call, &execution_signals).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path().ends_with("/events"))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        signals.draining.cancel();
        runner.drain(executions, &signals).await;
        let results = reported_results(&server).await;
        assert_eq!(results.len(), 1);
        assert_eq!(header(&results[0], "Oma-Result-Status"), "succeeded");
        assert!(!signals.abort.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_interrupts_after_grace_and_reports_partial_output() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let runner = short_runner(&server, directory.path()).await;
        let call = bash_call(
            "echo partial-output; sleep 30",
            chrono::Duration::seconds(60),
        );
        mount_events(&server, &call, "running").await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let signals = Signals::default();
        let mut executions = tokio::task::JoinSet::new();
        let worker = runner.clone();
        let execution_signals = signals.clone();
        let saved = call.clone();
        executions.spawn(async move { worker.execute(call, &execution_signals).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        signals.draining.cancel();
        tokio::time::timeout(Duration::from_secs(5), runner.drain(executions, &signals))
            .await
            .unwrap();
        let results = reported_results(&server).await;
        assert_eq!(results.len(), 1);
        assert_eq!(header(&results[0], "Oma-Result-Status"), "interrupted");
        assert_eq!(
            header(&results[0], "Oma-Result-Reason"),
            "executor_shutdown"
        );
        let body = String::from_utf8_lossy(&results[0].body);
        assert!(body.contains("partial-output"));
        assert!(body.contains(SHUTDOWN_NOTE));
        assert!(
            !tokio::fs::try_exists(runner.ledger(&saved, "result"))
                .await
                .unwrap()
        );
    }

    #[test]
    fn legacy_stopped_ledger_entries_report_as_stop_requested_interruptions() {
        let completion: Completion =
            serde_json::from_value(json!({"status": "stopped", "body": []})).unwrap();
        assert_eq!(completion.wire(), ("interrupted", Some("stop_requested")));
    }
}
