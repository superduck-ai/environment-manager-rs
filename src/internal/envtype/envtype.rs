//! 环境类型注册与创建逻辑。
//!
//! 本文件实现环境类型注册表的创建、覆盖注册以及按类型名称调用 Factory。
//!
//! 注册表保留 Go 的覆盖语义：相同名称后注册者覆盖前者。无效注册仍 panic，
//! 因为它属于启动期程序配置错误；运行时未知类型和工厂错误则使用类型化错误。

use thiserror::Error;

use crate::internal::util::BoxError;

use super::{EnvTypes, EnvironmentType, Registration};

/// 环境注册表创建实例时可能返回的错误。
#[derive(Debug, Error)]
pub enum EnvTypeError {
    /// 请求的环境类型没有注册。
    #[error("environment type {type_name:?} not found")]
    NotFound {
        /// 调用方请求的原始类型名称。
        type_name: String,
    },
    /// 注册工厂拒绝配置或无法构造环境。
    ///
    /// 使用 transparent 保留 Go `Create` 直接返回 factory 错误的文本。
    #[error(transparent)]
    Factory(BoxError),
}

/// 创建空但已初始化的环境类型注册表。
#[must_use]
pub fn new_env_types() -> EnvTypes {
    EnvTypes::default()
}

impl EnvTypes {
    /// 注册或覆盖一个环境类型。
    ///
    /// 空 `type_name` 或缺少 factory 是静态组装错误，与 Go 一样立即 panic。
    pub fn register(&mut self, registration: Registration) {
        assert!(
            !registration.type_name.is_empty(),
            "environment type must have a TypeName"
        );
        assert!(
            registration.factory.is_some(),
            "environment type must have a Factory"
        );
        self.registry
            .insert(registration.type_name.clone(), registration);
    }

    /// 对应 Go：`internal/envtype/envtype.go` 中的 `EnvTypes.Create`。
    ///
    /// 使用命中的工厂原样传递 JSON 字节，并创建环境实例。
    ///
    /// # 参数
    /// - `type_name`：配置声明的稳定环境类型名称。
    /// - `raw_config`：后端返回的原始 JSON 配置。
    ///
    /// # 返回
    /// 未注册类型返回 `NotFound`；工厂解析或校验失败时返回透明的 `Factory` 错误。
    pub fn create(
        &self,
        type_name: &str,
        raw_config: &[u8],
    ) -> Result<Box<dyn EnvironmentType>, EnvTypeError> {
        let registration = self
            .registry
            .get(type_name)
            .ok_or_else(|| EnvTypeError::NotFound {
                type_name: type_name.to_owned(),
            })?;
        let factory = registration
            .factory
            .as_ref()
            .expect("register rejects registrations without a factory");
        factory(raw_config).map_err(EnvTypeError::Factory)
    }

    /// 返回当前注册项数量，主要供启动诊断和测试使用。
    #[must_use]
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    /// 判断注册表是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }
}
