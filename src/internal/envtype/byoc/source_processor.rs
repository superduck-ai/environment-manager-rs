//! BYOC 到生产 `sources` 子系统的默认适配器。
//!
//! BYOC 保留 [`SourceProcessor`] trait 以便单元测试替换外部 Git 副作用；生产构造
//! 则通过本适配器直接创建 `SourceHandlerManager`，不要求上层 manager 再手工注入。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::api::{LogCategory, LogLevel, SessionError};
use crate::internal::session::{ActivityRecorder, ActivityRecorderError};
use crate::internal::sources::{ProcessMode, new_source_handler_manager};
use crate::internal::util::{BoxError, Stopper};

use super::{SourceProcessMode, SourceProcessRequest, SourceProcessor, SourceProcessorError};

/// 调用真实 `internal::sources` 实现的生产 source processor。
#[derive(Debug, Default)]
pub(super) struct ProductionSourceProcessor;

#[async_trait]
impl SourceProcessor for ProductionSourceProcessor {
    /// 创建与 branch checkout 路径参数相同的 manager，然后串行处理 sources。
    async fn process_sources(
        &self,
        cancellation: &CancellationToken,
        request: &SourceProcessRequest,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError> {
        let recorder = scoped_activity_recorder(activity_recorder);
        let manager = build_manager(request, recorder)
            .map_err(|error| SourceProcessorError::CreateManager(Box::new(error) as BoxError))?;
        let result = manager
            .process_sources(cancellation, &request.sources)
            .await
            .map_err(|error| SourceProcessorError::ProcessSources(Box::new(error) as BoxError));
        // manager 的类型携带 recorder 借用；显式 drop 记录 局部 manager 的结束点，
        // 编译器也会禁止 manager 被返回、spawn 或保存到更长生命周期中。
        drop(manager);
        result
    }

    /// 创建 fresh/no-validation manager，并执行 中独立的 Git proxy 后处理阶段。
    async fn setup_git_proxy_after_sources_processed(
        &self,
        cancellation: &CancellationToken,
        request: &SourceProcessRequest,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError> {
        let recorder = scoped_activity_recorder(activity_recorder);
        let manager = build_manager(request, recorder)
            .map_err(|error| SourceProcessorError::CreateManager(Box::new(error) as BoxError))?;
        let result = manager
            .setup_git_proxy_after_sources_processed(cancellation, &request.sources)
            .await
            .map_err(|error| SourceProcessorError::SetupGitProxy(Box::new(error) as BoxError));
        // proxy manager 只属于本次局部 manager 构造；manager 的生命周期
        // 参数保证其中的 handler 不得带走 recorder 借用。
        drop(manager);
        result
    }
}

/// 把 BYOC 请求无损映射为 sources 的生产 manager 参数。
///
/// `'recorder` 由当前 `SourceProcessor` 方法收到的 recorder 引用推导。返回 manager
/// 显式携带该生命周期，因此只能在同一个 processor future 内使用。
fn build_manager<'recorder>(
    request: &SourceProcessRequest,
    activity_recorder: Option<Arc<dyn ActivityRecorder + 'recorder>>,
) -> crate::internal::sources::SourcesResult<
    crate::internal::sources::SourceHandlerManager<'recorder>,
> {
    let repo_branches = (!request.repo_branches.is_empty()).then(|| {
        request
            .repo_branches
            .iter()
            .map(|(repo, branch)| (repo.clone(), branch.clone()))
            .collect::<HashMap<_, _>>()
    });
    let process_mode = match request.mode {
        SourceProcessMode::Fresh => ProcessMode::from("fresh"),
        SourceProcessMode::AllowPrefetched => ProcessMode::from("allow-prefetched"),
    };

    new_source_handler_manager(
        request.cwd.clone(),
        request.auth_context.clone().map(Arc::new),
        request.api_base_url.clone(),
        repo_branches,
        activity_recorder,
        process_mode,
        request.should_validate,
    )
}

/// 让 sources manager 在当前 processor future 内借用 initialize 的 recorder。
struct ScopedActivityRecorder<'a> {
    inner: &'a (dyn ActivityRecorder + Send + Sync),
}

#[async_trait]
impl ActivityRecorder for ScopedActivityRecorder<'_> {
    /// 同步转发普通 activity。
    fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        self.inner.record_activity(level, category, message, extra);
    }

    /// 同步转发长任务 activity，并保留原 stopper 所有权。
    fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        self.inner.record_long_running_activity(category, message)
    }

    /// 在同一 processor future 的借用范围内转发 failure result。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        self.inner.record_failure_result(session_error).await
    }
}

/// 把 recorder 借用限制在局部 sources manager 的存活期。
///
/// 返回的 trait object 保留输入引用的 `'recorder`，没有 `'static` 默认值，也不需要
/// 任何生命周期擦除。Arc 仅共享 wrapper 所有权，并不会延长其内部引用的有效期。
fn scoped_activity_recorder<'recorder>(
    recorder: Option<&'recorder (dyn ActivityRecorder + Send + Sync)>,
) -> Option<Arc<dyn ActivityRecorder + 'recorder>> {
    recorder.map(|inner| {
        let scoped: Arc<dyn ActivityRecorder + 'recorder> =
            Arc::new(ScopedActivityRecorder { inner });
        scoped
    })
}
