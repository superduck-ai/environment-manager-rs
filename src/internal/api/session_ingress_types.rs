//! 错误文案和事件构造器。
//!
//! 构造器显式填充协议要求的默认字段；UUID 和时间戳是唯一允许在 wire 测试中归一化
//! 的动态值。

use std::collections::BTreeMap;

use chrono::Utc;
use serde_json::to_value;
use uuid::Uuid;

use super::{
    AssistantMessage, ClaudeCodeExecutionError, ContentBlock, EnvManagerLogEventData,
    InitScriptError, LogCategory, LogLevel, SessionError, SessionEvent, SourceProcessingError,
    UsageMetrics,
};

impl SessionError for ClaudeCodeExecutionError {
    /// 返回稳定英文用户文案。
    fn get_user_message(&self) -> String {
        "An error occurred while executing Claude Code. You can try again by sending a new message or starting a new session.".to_owned()
    }

    /// 返回事件携带的 fatal 标志。
    fn is_fatal(&self) -> bool {
        self.fatal
    }
}

impl SessionError for SourceProcessingError {
    /// 有类型时插入 source 名称。
    fn get_user_message(&self) -> String {
        if self.source_type.is_empty() {
            "An error occurred while processing source. You can try again by sending a new message or starting a new session.".to_owned()
        } else {
            format!(
                "An error occurred while processing {} source. You can try again by sending a new message or starting a new session.",
                self.source_type
            )
        }
    }

    /// 返回事件携带的 fatal 标志。
    fn is_fatal(&self) -> bool {
        self.fatal
    }
}

impl SessionError for InitScriptError {
    /// 保留换行和标点。
    fn get_user_message(&self) -> String {
        let mut message = format!("Setup script failed with exit code {}.", self.exit_code);
        if !self.output.is_empty() {
            message.push_str(&format!("\n\nScript output:\n{}", self.output));
        }
        message.push_str("\n\nEdit your environment's setup script and start a new session.");
        message
    }

    /// init script 失败在 中始终为 fatal。
    fn is_fatal(&self) -> bool {
        true
    }
}

/// 包装日志数据并生成 UUID。
pub fn new_env_manager_log_event(data: EnvManagerLogEventData) -> SessionEvent {
    SessionEvent {
        event_type: "env_manager_log".to_owned(),
        uuid: Uuid::new_v4().to_string(),
        data: Some(to_value(data).expect("EnvManagerLogEventData serialization is infallible")),
        ..SessionEvent::default()
    }
}

/// 使用 UTC 当前时间并把缺省 extra 变为空 map。
pub fn new_env_manager_log_event_data(
    level: LogLevel,
    category: LogCategory,
    content: impl Into<String>,
    extra: Option<BTreeMap<String, serde_json::Value>>,
) -> EnvManagerLogEventData {
    EnvManagerLogEventData {
        level,
        category,
        content: content.into(),
        timestamp: Utc::now(),
        extra: extra.unwrap_or_default(),
    }
}

/// 创建前端可显示的合成 assistant 错误消息。
pub fn new_synthetic_assistant_message(error_text: impl Into<String>) -> SessionEvent {
    SessionEvent {
        event_type: "assistant".to_owned(),
        uuid: Uuid::new_v4().to_string(),
        message: Some(AssistantMessage {
            role: "assistant".to_owned(),
            model: "<synthetic>".to_owned(),
            content: vec![ContentBlock {
                block_type: "text".to_owned(),
                text: error_text.into(),
            }],
            stop_reason: "stop_sequence".to_owned(),
            usage: UsageMetrics::default(),
        }),
        is_api_error_message: Some(true),
        ..SessionEvent::default()
    }
}

/// 显式填充 result 事件的数值零值和空集合。
pub fn new_result_event(subtype: impl Into<String>, errors: Vec<String>) -> SessionEvent {
    SessionEvent {
        event_type: "result".to_owned(),
        uuid: Uuid::new_v4().to_string(),
        subtype: Some(subtype.into()),
        is_error: Some(!errors.is_empty()),
        duration_ms: Some(0),
        duration_api_ms: Some(0),
        num_turns: Some(0),
        total_cost_usd: Some(0.0),
        errors,
        model_usage: BTreeMap::new(),
        permission_denials: Vec::new(),
        usage: Some(UsageMetrics::default()),
        ..SessionEvent::default()
    }
}
