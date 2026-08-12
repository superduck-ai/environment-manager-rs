//! 对应 Go `internal/api/retry.go` 的 HTTP 重试策略。
//!
//! 仅对 Go 明确列出的状态码和传输错误重试；请求体必须可克隆，避免重试 POST 时发送
//! 空 body。等待和请求阶段都监听统一取消信号。

use std::time::Duration;

use rand::Rng;
use reqwest::{Request, Response, StatusCode};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::client::GoResponseCloseError;
use super::client::{
    GoRequestErrorContext, mark_go_transport_response, prepare_go_transport_request,
};
use super::{ApiError, HttpClient};

/// HTTP 重试参数，对应 Go `RetryConfig`。
#[derive(Clone, Debug, PartialEq)]
pub struct RetryConfig {
    /// 首次请求失败后的最大重试次数，不含最初请求。
    pub max_retries: usize,
    /// 第一次重试前的基础等待时间。
    pub initial_backoff: Duration,
    /// 指数退避的最大基础值，jitter 在封顶后增加。
    pub max_backoff: Duration,
    /// 每次重试的指数倍数。
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    /// 对应 Go `defaultRetryConfig`，返回 3 次、1 秒起步、10 秒封顶的策略。
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            backoff_factor: 2.0,
        }
    }
}

/// 判断响应码是否属于 Go 实现允许自动重试的集合。
fn is_retryable_status_code(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

/// 对应 Go `calculateBackoff`，计算指数退避并增加 `[0, 30%)` 正 jitter。
fn calculate_backoff(attempt: usize, config: &RetryConfig) -> Duration {
    if attempt == 0 {
        return config.initial_backoff;
    }

    let factor = config.backoff_factor.powi(attempt.saturating_sub(1) as i32);
    let base = config
        .initial_backoff
        .mul_f64(factor)
        .min(config.max_backoff);
    let jitter = rand::rng().random_range(0.0..0.3);
    base + base.mul_f64(jitter)
}

impl HttpClient {
    /// 对应 Go `RetryableHTTPDo`，执行可重放请求并在明确的临时错误上退避重试。
    ///
    /// # 参数
    /// - `cancellation`：manager 统一取消信号，请求和 backoff 均会响应。
    /// - `request`：已完整构造的 Reqwest 请求，body 必须支持 `try_clone`。
    /// - `retry_config`：调用点覆盖策略；为空时使用 client 默认值。
    ///
    /// # 返回
    /// 非重试状态的原始响应；传输失败、重试耗尽或取消时返回兼容错误文本。
    pub async fn retryable_http_do(
        &self,
        cancellation: &CancellationToken,
        request: &Request,
        retry_config: Option<&RetryConfig>,
    ) -> Result<Response, ApiError> {
        let config = retry_config.unwrap_or(&self.retry_config);
        if request.try_clone().is_none() {
            return Err(ApiError::message(
                "failed to read request body: request body is not replayable",
            ));
        }

        let mut last_error = String::new();
        for attempt in 0..=config.max_retries {
            let mut attempt_request = request.try_clone().ok_or_else(|| {
                ApiError::message("failed to read request body: request body is not replayable")
            })?;
            let error_context = GoRequestErrorContext::from_request(&attempt_request);
            let auto_gzip = prepare_go_transport_request(&mut attempt_request);
            tracing::debug!("attempt" = ?(attempt), "max_retries" = ?(config.max_retries), "method" = ?(attempt_request.method().as_str()), "url" = ?(attempt_request.url().as_str()), "{}", "Executing HTTP request");

            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(None),
                response = self.http_client.execute(attempt_request) => response.map_err(Some),
            };

            match result {
                Ok(mut response) if !is_retryable_status_code(response.status()) => {
                    mark_go_transport_response(&mut response, auto_gzip);
                    #[cfg(test)]
                    if let Some(error) = &self.response_close_error_for_test {
                        response
                            .extensions_mut()
                            .insert(GoResponseCloseError::new(error.clone()));
                    }
                    return Ok(response);
                }
                Ok(response) => {
                    last_error =
                        format!("request failed with status {}", response.status().as_u16());
                    // 丢弃响应会关闭连接 body；与 Go 显式 Close 的生命周期等价。
                    drop(response);
                }
                Err(Some(error)) => {
                    last_error = format!("request failed: {}", error_context.request_error(&error));
                }
                Err(None) => {
                    last_error = format!("request failed: {}", error_context.canceled());
                }
            }

            if attempt >= config.max_retries {
                continue;
            }

            let backoff = calculate_backoff(attempt + 1, config);
            tracing::warn!("attempt" = ?(attempt + 1), "backoff_ms" = ?(backoff.as_millis()), "error" = ?(&last_error), "{}", "Request failed, retrying");
            // Go 先让当前 `http.Client.Do` 返回 context error，再在 retry backoff 的
            // select 中观察取消。因此只有“仍有下一次重试”时错误才是 retry
            // cancelled；最后一次 attempt 必须落入统一的 exhausted 包装。
            if cancellation.is_cancelled() {
                return Err(ApiError::message(
                    "retry cancelled by context: context canceled",
                ));
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(ApiError::message("retry cancelled by context: context canceled"));
                }
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        Err(ApiError::message(format!(
            "request failed after {} retries: {last_error}",
            config.max_retries
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_backoff_matches_go_range() {
        let config = RetryConfig {
            max_retries: 1,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            backoff_factor: 2.0,
        };
        let backoff = calculate_backoff(1, &config);
        assert!(backoff >= Duration::from_millis(100));
        assert!(backoff < Duration::from_millis(130));
    }
}
