//! session 活动记录器的行为回归测试。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{
    ActivityRecorderError, EventPoster, NoopActivityRecorder, RecorderOptions,
    new_activity_recorder,
};
use crate::internal::api::{ApiError, LogCategory, LogLevel, SessionError, SessionEvent};

const NEVER_FAIL: usize = usize::MAX;

/// 记录发送顺序、调用时取消状态并可在指定调用注入错误的测试 poster。
#[derive(Debug)]
struct RecordingEventPoster {
    events: Mutex<Vec<SessionEvent>>,
    cancellation_states: Mutex<Vec<bool>>,
    fail_on_call: AtomicUsize,
    calls: Notify,
}

impl RecordingEventPoster {
    /// 创建默认始终发送成功的测试 poster。
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            cancellation_states: Mutex::new(Vec::new()),
            fail_on_call: AtomicUsize::new(NEVER_FAIL),
            calls: Notify::new(),
        }
    }

    /// 配置从一开始计数的失败调用序号。
    fn fail_on_call(&self, call: usize) {
        self.fail_on_call.store(call, Ordering::SeqCst);
    }

    /// 等待至少指定数量的事件并返回一致快照。
    async fn wait_for_events(&self, count: usize) -> Vec<SessionEvent> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.calls.notified();
                let events = self
                    .events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if events.len() >= count {
                    return events;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("等待 {count} 个 session event 超时"))
    }

    /// 返回所有调用进入 poster 时观察到的取消状态。
    fn cancellation_states(&self) -> Vec<bool> {
        self.cancellation_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// 创建没有失败注入且缓冲区为空的 poster。
fn new_recording_event_poster() -> Arc<RecordingEventPoster> {
    Arc::new(RecordingEventPoster::new())
}

#[async_trait]
impl EventPoster for RecordingEventPoster {
    /// 保存事件，并在配置的调用上返回确定性错误。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        self.cancellation_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(cancellation.is_cancelled());
        let call = {
            let mut events = self
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            events.push(event.clone());
            events.len()
        };
        self.calls.notify_waiters();

        if self.fail_on_call.load(Ordering::SeqCst) == call {
            Err(ApiError::message("post failed"))
        } else {
            Ok(())
        }
    }
}

/// 用于验证用户文案和 fatal 属性边界的 session 错误。
#[derive(Debug, Error)]
#[error("{message}")]
struct FakeSessionError {
    message: String,
    fatal: bool,
}

impl SessionError for FakeSessionError {
    /// 返回测试指定的稳定用户文案。
    fn get_user_message(&self) -> String {
        self.message.clone()
    }

    /// 返回测试指定的 fatal 属性。
    fn is_fatal(&self) -> bool {
        self.fatal
    }
}

/// 验证构造器默认值、自定义值及逐字段零值回退。
#[tokio::test]
async fn test_new_activity_recorder_defaults_and_options() {
    let default_recorder =
        new_activity_recorder(CancellationToken::new(), new_recording_event_poster(), None);
    assert_eq!(default_recorder.interval(), Duration::from_secs(30));
    assert_eq!(
        default_recorder.max_duration(),
        Duration::from_secs(15 * 60)
    );

    let custom_recorder = new_activity_recorder(
        CancellationToken::new(),
        new_recording_event_poster(),
        Some(RecorderOptions {
            interval: Duration::from_secs(1),
            max_duration: Duration::from_secs(2 * 60),
        }),
    );
    assert_eq!(custom_recorder.interval(), Duration::from_secs(1));
    assert_eq!(custom_recorder.max_duration(), Duration::from_secs(2 * 60));

    let partial_recorder = new_activity_recorder(
        CancellationToken::new(),
        new_recording_event_poster(),
        Some(RecorderOptions {
            interval: Duration::ZERO,
            max_duration: Duration::from_secs(7),
        }),
    );
    assert_eq!(partial_recorder.interval(), Duration::from_secs(30));
    assert_eq!(partial_recorder.max_duration(), Duration::from_secs(7));
}

/// 验证普通活动被包装为带 extra 的 env_manager_log 事件。
#[tokio::test]
async fn test_record_activity_posts_env_manager_log_event() {
    let poster = new_recording_event_poster();
    let recorder = new_activity_recorder(CancellationToken::new(), poster.clone(), None);

    recorder.record_activity(
        LogLevel::Info,
        LogCategory::Init,
        "hello".to_owned(),
        BTreeMap::from([("phase".to_owned(), json!("setup"))]),
    );

    let events = poster.wait_for_events(1).await;
    assert_eq!(events[0].event_type, "env_manager_log");
    let data = events[0].data.as_ref().expect("日志事件应包含 data");
    assert_eq!(data["level"], "info");
    assert_eq!(data["category"], "init");
    assert_eq!(data["content"], "hello");
    assert_eq!(data["extra"]["phase"], "setup");
}

/// 验证失败结果忽略父取消信号，并严格按 assistant、result 顺序发送。
#[tokio::test]
async fn test_record_failure_result_posts_assistant_and_result_events() {
    let parent_cancellation = CancellationToken::new();
    parent_cancellation.cancel();
    let poster = new_recording_event_poster();
    let recorder = new_activity_recorder(parent_cancellation, poster.clone(), None);
    let session_error = FakeSessionError {
        message: "setup failed".to_owned(),
        fatal: true,
    };

    recorder
        .record_failure_result(&session_error)
        .await
        .expect("失败结果事件应发送成功");

    let events = poster.wait_for_events(2).await;
    assert_eq!(events[0].event_type, "assistant");
    let assistant = events[0]
        .message
        .as_ref()
        .expect("assistant 事件应包含 message");
    assert_eq!(assistant.content[0].text, "setup failed");
    assert_eq!(events[1].event_type, "result");
    assert_eq!(events[1].subtype.as_deref(), Some("error_during_execution"));
    assert_eq!(events[1].errors, ["setup failed"]);
    assert_eq!(poster.cancellation_states(), [false, false]);
}

/// 验证首个 poster 错误被标记为 synthetic assistant 阶段错误。
#[tokio::test]
async fn test_record_failure_result_wraps_poster_errors() {
    let poster = new_recording_event_poster();
    poster.fail_on_call(1);
    let recorder = new_activity_recorder(CancellationToken::new(), poster.clone(), None);
    let session_error = FakeSessionError {
        message: "failed".to_owned(),
        fatal: true,
    };

    let error = recorder
        .record_failure_result(&session_error)
        .await
        .expect_err("poster 错误应返回调用方");
    assert!(matches!(
        error,
        ActivityRecorderError::SyntheticAssistant(_)
    ));
    assert_eq!(poster.wait_for_events(1).await.len(), 1);

    // 用同一个 table-driven 场景验证第二次发送失败；这里重新创建 recorder，
    // 避免首个失败注入留下的事件计数影响 result 阶段断言。
    let poster = new_recording_event_poster();
    poster.fail_on_call(2);
    let recorder = new_activity_recorder(CancellationToken::new(), poster.clone(), None);
    let error = recorder
        .record_failure_result(&session_error)
        .await
        .expect_err("result poster 错误应返回调用方");
    assert!(matches!(error, ActivityRecorderError::ResultEvent(_)));
    assert_eq!(poster.wait_for_events(2).await.len(), 2);
}

/// 验证长任务立即发送首条日志，并在最大时长后终报及自动停止。
#[tokio::test]
async fn test_record_long_running_activity_posts_progress_and_auto_stops() {
    let poster = new_recording_event_poster();
    let recorder = new_activity_recorder(
        CancellationToken::new(),
        poster.clone(),
        Some(RecorderOptions {
            interval: Duration::from_secs(60 * 60),
            max_duration: Duration::from_millis(30),
        }),
    );

    let stopper =
        recorder.record_long_running_activity(LogCategory::Init, "Cloning repository".to_owned());
    let events = poster.wait_for_events(2).await;
    assert_eq!(
        events[0].data.as_ref().expect("首条日志应包含 data")["content"],
        "Cloning repository"
    );
    assert_eq!(
        events[1].data.as_ref().expect("终报日志应包含 data")["content"],
        "Cloning repository (still running after 0 minutes.  This is the last message about this...)"
    );
    stopper.stop().await;
}

/// 验证 noop recorder 的三个入口都不会失败且 stopper 始终有效。
#[tokio::test]
async fn test_noop_activity_recorder() {
    let recorder = NoopActivityRecorder;
    let session_error = FakeSessionError {
        message: "ignored".to_owned(),
        fatal: false,
    };

    recorder.record_activity(
        LogLevel::Info,
        LogCategory::Init,
        "ignored".to_owned(),
        BTreeMap::new(),
    );
    recorder
        .record_failure_result(&session_error)
        .await
        .expect("noop recorder 不应返回错误");
    let stopper = recorder.record_long_running_activity(LogCategory::Init, "ignored".to_owned());
    stopper.stop().await;
}
