//! Bash execution result shared by structured content and its JSON text fallback.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    #[default]
    NotStarted,
    Running,
    Completed,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub status: ExecutionStatus,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signal: String,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

pub fn output_schema() -> Value {
    serde_json::from_str(include_str!("execution_schema.json")).expect("execution schema")
}

#[derive(Clone, Copy, Default)]
struct ClockState {
    waiters: usize,
    started: Option<std::time::Instant>,
    elapsed: std::time::Duration,
}

pub struct ToolClock {
    state: tokio::sync::watch::Sender<ClockState>,
    call_id: String,
    call_uuid: String,
}

type RunningClocks =
    std::sync::Mutex<std::collections::BTreeMap<String, std::sync::Weak<ToolClock>>>;
static RUNNING_CLOCKS: std::sync::OnceLock<RunningClocks> = std::sync::OnceLock::new();

fn running_clocks() -> &'static RunningClocks {
    RUNNING_CLOCKS.get_or_init(Default::default)
}

pub fn pause_running_tool(call_uuid: &str) -> Option<OAuthWaitGuard> {
    running_clocks()
        .lock()
        .ok()?
        .get(call_uuid)?
        .upgrade()
        .map(|clock| clock.pause())
}

pub fn current_tool_identity() -> Option<(String, String)> {
    TOOL_CLOCK
        .try_with(|clock| (clock.call_id.clone(), clock.call_uuid.clone()))
        .ok()
}

pub struct OAuthWaitGuard(std::sync::Arc<ToolClock>);

tokio::task_local! {
    static TOOL_CLOCK: std::sync::Arc<ToolClock>;
}

impl Default for ToolClock {
    fn default() -> Self {
        Self {
            state: tokio::sync::watch::channel(ClockState::default()).0,
            call_id: String::new(),
            call_uuid: String::new(),
        }
    }
}

impl ToolClock {
    pub fn for_call(call_id: String, call_uuid: String) -> std::sync::Arc<Self> {
        let clock = std::sync::Arc::new(Self {
            call_id,
            call_uuid: call_uuid.clone(),
            ..Self::default()
        });
        let mut clocks = running_clocks().lock().unwrap();
        clocks.retain(|_, value| value.strong_count() != 0);
        clocks.insert(call_uuid, std::sync::Arc::downgrade(&clock));
        clock
    }
    pub fn pause(self: &std::sync::Arc<Self>) -> OAuthWaitGuard {
        self.state.send_modify(|state| {
            if state.waiters == 0 {
                state.started = Some(std::time::Instant::now());
            }
            state.waiters += 1;
        });
        OAuthWaitGuard(self.clone())
    }

    pub async fn scope<F: std::future::Future>(
        self: &std::sync::Arc<Self>,
        future: F,
    ) -> F::Output {
        TOOL_CLOCK.scope(self.clone(), future).await
    }

    pub async fn sleep(&self, duration: std::time::Duration) {
        let started = std::time::Instant::now();
        let mut changes = self.state.subscribe();
        let initial = *changes.borrow();
        let baseline = initial.elapsed
            + initial
                .started
                .map_or(std::time::Duration::ZERO, |at| at.elapsed());
        loop {
            let state = *changes.borrow_and_update();
            if state.waiters > 0 {
                if changes.changed().await.is_err() {
                    return;
                }
                continue;
            }
            let active = started
                .elapsed()
                .saturating_sub(state.elapsed.saturating_sub(baseline));
            tokio::select! {
                biased;
                result = changes.changed() => { if result.is_err() { return; } },
                () = tokio::time::sleep(duration.saturating_sub(active)) => return,
            }
        }
    }
}

impl Drop for OAuthWaitGuard {
    fn drop(&mut self) {
        self.0.state.send_modify(|state| {
            state.waiters -= 1;
            if state.waiters == 0
                && let Some(started) = state.started.take()
            {
                state.elapsed += started.elapsed();
            }
        });
    }
}

pub async fn execution_timeout(duration: std::time::Duration) {
    match TOOL_CLOCK.try_with(std::sync::Arc::clone) {
        Ok(clock) => clock.sleep(duration).await,
        Err(_) => tokio::time::sleep(duration).await,
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn authorization_wait_does_not_consume_tool_timeout() {
        let clock = Arc::new(ToolClock::default());
        let waiting = clock.pause();
        let task_clock = clock.clone();
        let task = tokio::spawn(async move {
            task_clock
                .scope(execution_timeout(Duration::from_millis(80)))
                .await;
        });
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!task.is_finished());
        let second = clock.pause();
        drop(waiting);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!task.is_finished());
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
}
