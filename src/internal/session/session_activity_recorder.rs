//! session 活动事件的真实记录器。
//!
//! 本文件机械对应 Go 的 `session_activity_recorder.go`。即时日志使用 Tokio
//! 后台任务，长任务复用 `PeriodicInvoker`，失败结果则在脱离父取消信号的
//! 五秒窗口内按顺序发送。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::api::{
    LogCategory, LogLevel, SessionError, new_env_manager_log_event, new_env_manager_log_event_data,
    new_result_event, new_synthetic_assistant_message,
};
use crate::internal::logger::{LogAttributes, Logger};
use crate::internal::util::{PeriodicInvoker, Stopper};

use super::{
    ActivityRecorder, ActivityRecorderError, EventPoster, LongRunningActivityState,
    RecorderOptions, SessionActivityRecorder,
};

const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_DURATION: Duration = Duration::from_secs(15 * 60);
const FAILURE_RESULT_TIMEOUT: Duration = Duration::from_secs(5);

/// 创建 session 活动记录器，并对每个零值时序选项分别应用 Go 默认值。
#[must_use]
pub fn new_activity_recorder(
    cancellation: CancellationToken,
    poster: Arc<dyn EventPoster>,
    logger: Logger,
    options: Option<RecorderOptions>,
) -> SessionActivityRecorder {
    let options = options.unwrap_or_default();
    let interval = if options.interval.is_zero() {
        DEFAULT_INTERVAL
    } else {
        options.interval
    };
    let max_duration = if options.max_duration.is_zero() {
        DEFAULT_MAX_DURATION
    } else {
        options.max_duration
    };

    SessionActivityRecorder {
        cancellation,
        poster,
        logger,
        interval,
        max_duration,
    }
}

impl SessionActivityRecorder {
    /// 返回普通长任务日志的发送间隔。
    #[must_use]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// 返回长任务自动停止日志的最大持续时间。
    #[must_use]
    pub fn max_duration(&self) -> Duration {
        self.max_duration
    }

    /// 构造环境管理器日志事件并在 Tokio 后台任务中发送。
    pub fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        let event = new_env_manager_log_event(new_env_manager_log_event_data(
            level,
            category,
            message,
            Some(extra),
        ));
        let poster = Arc::clone(&self.poster);
        let cancellation = self.cancellation.clone();
        let logger = self.logger.clone();

        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // 正常入口始终位于 Tokio runtime。这里没有对应的 Go 日志调用点，
            // 因此不能为 Rust 结构性误用额外制造一条外部可见日志。
            return;
        };
        runtime.spawn(async move {
            if let Err(error) = poster.post_event(&cancellation, &event).await {
                logger.error("Failed to post log event", error_attributes(&error));
            }
        });
    }

    /// 在独立五秒取消窗口内依次发送 assistant 错误和最终 result 事件。
    pub async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        let user_message = session_error.get_user_message();
        let poster = Arc::clone(&self.poster);
        let logger = self.logger.clone();

        // 新 token 不继承父 token 的已取消状态，对应 Go 的 context.WithoutCancel。
        let detached_cancellation = CancellationToken::new();
        let operation = async {
            let assistant_event = new_synthetic_assistant_message(user_message.clone());
            poster
                .post_event(&detached_cancellation, &assistant_event)
                .await
                .map_err(|error| {
                    logger.error(
                        "Failed to post synthetic assistant event",
                        error_attributes(&error),
                    );
                    ActivityRecorderError::SyntheticAssistant(error)
                })?;

            // 必须等 assistant 成功后才构造并发送 result，保持后端可观察顺序。
            let result_event =
                new_result_event("error_during_execution", vec![user_message.clone()]);
            poster
                .post_event(&detached_cancellation, &result_event)
                .await
                .map_err(|error| {
                    logger.error("Failed to post result event", error_attributes(&error));
                    ActivityRecorderError::ResultEvent(error)
                })
        };

        // 两次网络请求共享一个超时预算，而不是各自获得完整五秒。
        let result = tokio::time::timeout(FAILURE_RESULT_TIMEOUT, operation).await;
        detached_cancellation.cancel();
        result.unwrap_or(Err(ActivityRecorderError::FailureResultTimeout))
    }

    /// 立即发送首条进度日志，周期续报，并在最大时长后发出最后一条日志。
    #[must_use]
    pub fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        let state = Arc::new(LongRunningActivityState::new());
        let message = Arc::new(message);
        let callback_poster = Arc::clone(&self.poster);
        let callback_cancellation = self.cancellation.clone();
        let callback_category = category.clone();
        let callback_message = Arc::clone(&message);
        let callback_state = Arc::clone(&state);
        let callback_logger = self.logger.clone();

        let invoker = Arc::new(
            PeriodicInvoker::new(self.logger.clone(), self.interval, move || {
                let poster = Arc::clone(&callback_poster);
                let cancellation = callback_cancellation.clone();
                let category = callback_category.clone();
                let message = Arc::clone(&callback_message);
                let state = Arc::clone(&callback_state);
                let logger = callback_logger.clone();
                async move {
                    // 在任何 await 之前推进状态，防止慢请求使后续调用重复首条文案。
                    let content = state.next_content(message.as_str());
                    let event = new_env_manager_log_event(new_env_manager_log_event_data(
                        LogLevel::Info,
                        category,
                        content,
                        None,
                    ));
                    if let Err(error) = poster.post_event(&cancellation, &event).await {
                        logger.error("Failed to post event", error_attributes(&error));
                        return Err(std::io::Error::other(format!(
                            "failed to post event: {error}"
                        )));
                    }
                    Ok(())
                }
            })
            .unwrap_or_else(|error| panic!("{error}")),
        );

        if let Err(error) = invoker.start() {
            // Go 的 Start 在重复启动时 panic；新建实例正常不会失败。Rust 的
            // NoRuntime 同样属于编程错误，不能降级成 Go 中不存在的日志和 noop。
            panic!("{error}");
        }

        let timer_invoker = Arc::clone(&invoker);
        let timer_poster = Arc::clone(&self.poster);
        let timer_cancellation = self.cancellation.clone();
        let max_duration = self.max_duration;
        let timer_logger = self.logger.clone();
        tokio::spawn(async move {
            tokio::time::sleep(max_duration).await;

            // Stop 与定时器可能同时发生；此检查复刻 Go 对已停止任务不再终报的语义。
            if !timer_invoker.is_running() {
                return;
            }

            let elapsed_minutes = max_duration.as_secs() / 60;
            let content = format!(
                "{} (still running after {} minutes.  This is the last message about this...)",
                message.as_str(),
                elapsed_minutes
            );
            let event = new_env_manager_log_event(new_env_manager_log_event_data(
                LogLevel::Info,
                category.clone(),
                content.clone(),
                None,
            ));
            // Go 明确忽略最后一次发送错误；无论后端结果如何都必须停止周期任务。
            let _ = timer_poster.post_event(&timer_cancellation, &event).await;
            timer_logger.warn(
                format!(
                    "Automatically stopping logging long-running activity after {elapsed_minutes} minutes"
                ),
                [
                    (
                        "category".to_owned(),
                        serde_json::to_value(&category)
                            .expect("LogCategory always serializes as a string"),
                    ),
                    ("content".to_owned(), serde_json::json!(content)),
                ]
                .into_iter()
                .collect(),
            );
            timer_invoker.stop();
        });

        invoker
    }
}

/// 构造 Go session logger 使用的单一 `error` 属性。
fn error_attributes(error: &impl std::fmt::Display) -> LogAttributes {
    [("error".to_owned(), serde_json::json!(error.to_string()))]
        .into_iter()
        .collect()
}

#[async_trait]
impl ActivityRecorder for SessionActivityRecorder {
    /// 通过统一接口异步后台发送活动日志。
    fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        SessionActivityRecorder::record_activity(self, level, category, message, extra);
    }

    /// 通过统一接口启动长任务进度日志。
    fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        SessionActivityRecorder::record_long_running_activity(self, category, message)
    }

    /// 通过统一接口发送 session 失败结果。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        SessionActivityRecorder::record_failure_result(self, session_error).await
    }
}
