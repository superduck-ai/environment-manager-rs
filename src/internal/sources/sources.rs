//! source handler 的注册、分派与 Git 后处理编排。
//!
//! 本文件机械对应 Go `sources.go`。handler 顺序、缺失 handler 的非 fatal 记录、
//! 单个处理失败后的立即返回，以及 remote URL 更新的 best-effort 行为均保持不变。

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use tokio_util::sync::CancellationToken;

use super::{
    ProcessMode, SourceHandler, SourceHandlerManager, SourceSlot, SourcesError, SourcesResult,
    new_git_handler,
};
use crate::internal::{api::SourceProcessingError, auth::AuthContext, session::ActivityRecorder};

/// 对应 Go `NewSourceHandlerManager`，校验 base directory 并注册默认 Git handler。
///
/// 空路径严格返回 `base directory cannot be empty`；其余参数原样传给 Git handler，
/// manager 自身不复制认证或 branch 业务。`'recorder` 贯穿 manager、默认 Git handler
/// 与动态 handler 容器，保证任何一个对象都不能逃出 activity recorder 的有效期。
#[allow(clippy::too_many_arguments)]
pub fn new_source_handler_manager<'recorder>(
    base_dir: impl Into<PathBuf>,
    auth_context: Option<Arc<AuthContext>>,
    sdk_base_url: impl Into<String>,
    repo_branches: Option<HashMap<String, String>>,
    activity_recorder: Option<Arc<dyn ActivityRecorder + 'recorder>>,
    process_mode: ProcessMode,
    should_validate: bool,
) -> SourcesResult<SourceHandlerManager<'recorder>> {
    let base_dir = base_dir.into();
    if base_dir.as_os_str().is_empty() {
        return Err(SourcesError::message("base directory cannot be empty"));
    }

    let git_handler = new_git_handler(
        base_dir.clone(),
        auth_context,
        sdk_base_url,
        repo_branches,
        activity_recorder.clone(),
        process_mode,
        should_validate,
    );
    // trait object 显式继承 recorder 生命周期；默认的 `dyn Trait` 会隐含 `'static`，
    // 那会再次迫使局部 recorder 做不安全生命周期擦除。
    let git_handler: Arc<dyn SourceHandler + 'recorder> = Arc::new(git_handler);
    Ok(SourceHandlerManager {
        handlers: vec![git_handler],
        base_dir,
        activity_recorder,
        should_validate,
    })
}

impl<'recorder> SourceHandlerManager<'recorder> {
    /// 对应 Go `RegisterHandler`；`None` 与 nil interface 一样被静默忽略。
    ///
    /// 新 handler 最长只能活到 `'recorder`；因此测试或初始化代码可以注册借用局部
    /// 状态的实现，但 manager 不能把它保存到该状态销毁之后。
    pub fn register_handler(&mut self, handler: Option<Arc<dyn SourceHandler + 'recorder>>) {
        if let Some(handler) = handler {
            self.handlers.push(handler);
        }
    }

    /// 对应 Go `ProcessSources`，按输入顺序串行处理 source。
    ///
    /// 串行是外部语义的一部分：后一个 source 能观察到前一个 source 建立的目录。
    /// 缺失 handler 只记录非 fatal 结果并继续；handler 失败记录 fatal 结果并立即返回。
    pub async fn process_sources<S>(
        &self,
        cancellation: &CancellationToken,
        sources: &[S],
    ) -> SourcesResult<()>
    where
        S: SourceSlot,
    {
        if sources.is_empty() {
            tracing::debug!("{}", "No sources to process");
            return Ok(());
        }

        tracing::debug!("num_sources" = ?(sources.len()), "{}", "Starting to process sources");
        let started = Instant::now();
        let mut processed_count = 0_usize;

        for (index, slot) in sources.iter().enumerate() {
            let Some(source) = slot.source_value() else {
                return Err(SourcesError::message(format!("source[{index}] is nil")));
            };
            let source_type = source.get_type();
            tracing::debug!("source_index" = ?(index + 1), "total_count" = ?(sources.len()), "source_type" = ?(source_type), "{}", "Processing source");

            let Some(handler) = self.find_handler(source_type) else {
                if let Some(recorder) = &self.activity_recorder {
                    let error = SourceProcessingError {
                        fatal: false,
                        source_type: source_type.to_owned(),
                    };
                    let _ = recorder.record_failure_result(&error).await;
                }
                tracing::warn!("source_type" = ?(source_type), "{}", "No handler found for source type, skipping");
                continue;
            };

            if let Err(error) = handler.process(cancellation, source).await {
                if let Some(recorder) = &self.activity_recorder {
                    let source_error = SourceProcessingError {
                        fatal: true,
                        source_type: source_type.to_owned(),
                    };
                    let _ = recorder.record_failure_result(&source_error).await;
                }
                tracing::error!("source_type" = ?(source_type), "error" = ?(error.to_string()), "{}", "Failed to process source");
                return Err(SourcesError::message(format!(
                    "failed to process source type {source_type}: {error}"
                )));
            }

            processed_count += 1;
            tracing::debug!("source_type" = ?(source_type), "source_index" = ?(index + 1), "total_count" = ?(sources.len()), "{}", "Source processed successfully");
        }

        tracing::debug!("duration_ms" = ?(started.elapsed().as_millis()), "total_count" = ?(sources.len()), "processed_count" = ?(processed_count), "{}", "Completed processing all sources");
        Ok(())
    }

    /// 对应 Go `UpdateRemoteURLs`，对所有 Git source best-effort 更新 origin。
    ///
    /// 找不到具备更新能力的 handler 时返回成功。单仓库更新失败只写 warning，后续
    /// source 仍继续处理，方法最终也返回成功。
    pub async fn update_remote_urls<S>(
        &self,
        cancellation: &CancellationToken,
        sources: &[S],
    ) -> SourcesResult<()>
    where
        S: SourceSlot,
    {
        if sources.is_empty() {
            tracing::debug!("{}", "No sources to update");
            return Ok(());
        }

        tracing::debug!("num_sources" = ?(sources.len()), "{}", "Updating git remote URLs for existing repositories");

        let Some(updater) = self
            .handlers
            .iter()
            .find_map(|handler| handler.as_git_remote_updater())
        else {
            tracing::warn!("{}", "No git handler found for updating remote URL");
            return Ok(());
        };

        for slot in sources {
            let Some(source) = slot.source_value() else {
                continue;
            };
            if source.get_type() != "git_repository" {
                continue;
            }
            if let Err(error) = updater.update_remote_url(cancellation, source).await {
                tracing::warn!("source_type" = ?(source.get_type()), "error" = ?(error.to_string()), "{}", "Failed to update remote URL, continuing");
            }
        }
        Ok(())
    }

    /// 对应 Go `SetupGitProxyAfterSourcesProcessed`，把全部 source 交给首个 Git handler。
    ///
    /// 空输入和缺少 Git handler 都是成功 no-op；具体 proxy 的缺失或启动失败由 Git
    /// handler 返回，避免在 manager 层吞掉本应终止初始化的错误。
    pub async fn setup_git_proxy_after_sources_processed<S>(
        &self,
        cancellation: &CancellationToken,
        sources: &[S],
    ) -> SourcesResult<()>
    where
        S: SourceSlot,
    {
        if sources.is_empty() {
            return Ok(());
        }

        let Some(git_handler) = self
            .handlers
            .iter()
            .find_map(|handler| handler.as_git_handler())
        else {
            tracing::debug!("{}", "No git handler found for proxy setup");
            return Ok(());
        };

        let source_values = sources
            .iter()
            .map(SourceSlot::source_value)
            .collect::<Vec<_>>();
        git_handler
            .setup_git_proxy_after_sources_processed(cancellation, &source_values)
            .await
    }

    /// 按注册顺序查找第一个可处理给定标签的 handler。
    fn find_handler(&self, source_type: &str) -> Option<&Arc<dyn SourceHandler + 'recorder>> {
        self.handlers
            .iter()
            .find(|handler| handler.can_handle(source_type))
    }

    /// 返回构造时保存的 base directory，供上层 adapter 无损复用 manager 配置。
    #[must_use]
    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// 返回是否启用 repository access validation。
    #[must_use]
    pub fn should_validate(&self) -> bool {
        self.should_validate
    }
}
