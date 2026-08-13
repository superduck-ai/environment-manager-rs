//! 环境类型注册表的行为回归测试。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::auth::AuthContext;
use crate::internal::config::{SessionMode, StartupContext};
use crate::internal::session::ActivityRecorder;
use crate::internal::util::BoxError;

use super::{EnvironmentType, Registration, new_env_types};

/// 注册表测试使用的最小环境实现。
#[derive(Debug)]
struct FakeEnvironmentType {
    cwd: String,
}

#[async_trait]
impl EnvironmentType for FakeEnvironmentType {
    /// 测试环境无需保存 session 模式。
    fn set_session_mode(&mut self, _mode: SessionMode) {}

    /// 测试环境无需保存启动上下文。
    fn set_startup_context(&mut self, _context: Option<StartupContext>) {}

    /// 测试环境无需保存认证上下文。
    fn set_auth_context(&mut self, _context: Option<AuthContext>) {}

    /// 测试环境初始化始终成功。
    async fn initialize(
        &mut self,
        _cancellation: &CancellationToken,
        _activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), BoxError> {
        Ok(())
    }

    /// 返回构造测试环境时指定的工作目录。
    fn get_cwd(&self) -> &str {
        &self.cwd
    }

    /// 测试环境不注入额外变量。
    fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }
}

/// 验证注册、工厂原始 JSON 透传以及未知类型错误。
#[test]
fn test_env_types_register_and_create() {
    let mut registry = new_env_types();
    assert!(registry.is_empty());

    let captured_raw = Arc::new(Mutex::new(Vec::new()));
    let factory_capture = Arc::clone(&captured_raw);
    registry.register(Registration {
        type_name: "fake".to_owned(),
        factory: Some(Arc::new(move |raw| {
            *factory_capture
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = raw.to_vec();
            Ok(Box::new(FakeEnvironmentType {
                cwd: "/workspace".to_owned(),
            }))
        })),
        description: String::new(),
    });

    let environment = registry
        .create("fake", br#"{"hello":"world"}"#)
        .expect("已注册环境应创建成功");
    assert_eq!(environment.get_cwd(), "/workspace");
    assert_eq!(
        *captured_raw
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        br#"{"hello":"world"}"#
    );
    let error = registry
        .create("missing", &[])
        .err()
        .expect("未知环境类型应返回错误");
    assert_eq!(error.to_string(), "environment type \"missing\" not found");

    registry.register(Registration {
        type_name: "broken".to_owned(),
        factory: Some(Arc::new(|_| {
            Err(Box::new(std::io::Error::other("factory failed")) as BoxError)
        })),
        description: String::new(),
    });
    let Err(error) = registry.create("broken", &[]) else {
        panic!("工厂错误应直接透传");
    };
    assert_eq!(error.to_string(), "factory failed");
}

/// 验证缺少名称或 factory 的注册项沿用 panic 行为。
#[test]
fn test_env_types_register_panics_for_invalid_registration() {
    let missing_type = std::panic::catch_unwind(|| {
        let mut registry = new_env_types();
        registry.register(Registration {
            type_name: String::new(),
            factory: Some(Arc::new(|_| {
                Ok(Box::new(FakeEnvironmentType { cwd: String::new() }))
            })),
            description: String::new(),
        });
    });
    assert!(panic_text(missing_type).contains("TypeName"));

    let missing_factory = std::panic::catch_unwind(|| {
        let mut registry = new_env_types();
        registry.register(Registration {
            type_name: "fake".to_owned(),
            factory: None,
            description: String::new(),
        });
    });
    assert!(panic_text(missing_factory).contains("Factory"));
}

/// 把 catch_unwind 的动态 payload 转成稳定文本。
fn panic_text(result: Result<(), Box<dyn std::any::Any + Send>>) -> String {
    let payload = result.expect_err("测试操作应 panic");
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "unknown panic payload".to_owned(),
        },
    }
}
