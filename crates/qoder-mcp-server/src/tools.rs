//! 工具注册表、别名参数映射和内容块。
use crate::{environment::EnvironmentProvider, util};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use tokio_util::sync::CancellationToken;
#[path = "bash.rs"]
mod bash;
#[path = "diff.rs"]
mod diff;
#[path = "files.rs"]
mod files;
#[path = "gif_decode.rs"]
mod gif_decode;
#[path = "glob.rs"]
mod glob;
#[path = "image_resize.rs"]
mod image_resize;
#[path = "images.rs"]
mod images;
#[path = "jpeg_decode.rs"]
mod jpeg_decode;
#[path = "jpeg_encode.rs"]
mod jpeg_encode;
#[path = "png_encode.rs"]
mod png_encode;
#[path = "search.rs"]
mod search;
#[path = "webp_decode.rs"]
mod webp_decode;

/// 文本和图片共用的 MCP 内容块，空字段按兼容协议省略。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContentBlock {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data: String,
    #[serde(rename = "mimeType", default, skip_serializing_if = "String::is_empty")]
    pub mime_type: String,
}
/// 可交给模型修正的工具执行结果。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolResult {
    pub content: Vec<ContentBlock>,
    #[serde(
        rename = "isError",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub is_error: bool,
}
impl ToolResult {
    /// 创建一个文本内容块，空文本省略 text 字段。
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![ContentBlock {
                kind: "text".into(),
                text: text.into(),
                data: String::new(),
                mime_type: String::new(),
            }],
            is_error: false,
        }
    }
    /// 创建工具级错误结果。
    pub fn error(text: impl Into<String>) -> Self {
        Self {
            is_error: true,
            ..Self::text(text)
        }
    }
}
/// 可扩展的异步工具接口。
#[async_trait]
pub trait Tool: Send + Sync {
    /// 返回完整 MCP 工具声明。
    fn definition(&self) -> Value;
    /// 验证必需字段，不额外收紧兼容接口的参数类型。
    fn validate(&self, arguments: &Value) -> Result<(), String> {
        for key in self.definition()["inputSchema"]["required"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let key = key.as_str().unwrap_or_default();
            if arguments.get(key).is_none_or(Value::is_null) {
                return Err(format!("missing required parameter: {key}"));
            }
        }
        Ok(())
    }
    /// 执行工具，并接受调用方取消信号。
    async fn handle(
        &self,
        cancellation: CancellationToken,
        arguments: Value,
    ) -> Result<ToolResult, String>;
}
/// 并发安全的工具注册表。
#[derive(Default)]
pub struct Registry {
    tools: RwLock<BTreeMap<String, Arc<dyn Tool>>>,
}
impl Registry {
    /// 注册工具；重复名称返回兼容错误。
    pub fn register(&self, tool: Arc<dyn Tool>) -> Result<(), String> {
        let name = util::string(&tool.definition(), "name").to_owned();
        let mut tools = self.tools.write().unwrap_or_else(|e| e.into_inner());
        if tools.contains_key(&name) {
            return Err(format!("tool {name} already registered"));
        }
        tools.insert(name, tool);
        Ok(())
    }
    /// 获取工具的共享句柄。
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }
    /// 返回工具快照；排序消除哈希遍历噪声。
    pub fn list(&self) -> Vec<Arc<dyn Tool>> {
        self.tools
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    /// 构建六个内置工具及六个兼容别名。
    pub fn builtin(workdir: PathBuf, environment: Option<Arc<dyn EnvironmentProvider>>) -> Self {
        let registry = Self::default();
        let defs: Vec<Value> = serde_json::from_str(include_str!("definitions.json"))
            .expect("checked-in tool definitions");
        for definition in defs {
            let name = util::string(&definition, "name").to_owned();
            let tool: Arc<dyn Tool> = Arc::new(Builtin {
                definition,
                workdir: workdir.clone(),
                environment: environment.clone(),
            });
            registry.register(tool.clone()).expect("unique builtin");
            let alias = match name.as_str() {
                "bash" => "run_in_terminal",
                "read" => "file_read",
                "write" => "file_write",
                "edit" => "file_edit",
                "grep" => "grep_run",
                _ => "glob_run",
            };
            registry
                .register(Arc::new(Alias {
                    name: alias.into(),
                    target: tool,
                }))
                .expect("unique alias");
        }
        registry
    }
}
struct Builtin {
    definition: Value,
    workdir: PathBuf,
    environment: Option<Arc<dyn EnvironmentProvider>>,
}
#[async_trait]
impl Tool for Builtin {
    /// 内置工具使用从参考服务提取的精确声明。
    fn definition(&self) -> Value {
        self.definition.clone()
    }
    /// 分派异步命令和搜索；文件操作放到阻塞任务中。
    async fn handle(
        &self,
        cancellation: CancellationToken,
        arguments: Value,
    ) -> Result<ToolResult, String> {
        let name = util::string(&self.definition, "name");
        match name {
            "bash" => Ok(bash::run(
                &self.workdir,
                self.environment.as_deref(),
                cancellation,
                &arguments,
            )
            .await),
            "grep" => Ok(search::grep(&self.workdir, cancellation, &arguments).await),
            "glob" => Ok(glob::run(&self.workdir, cancellation, &arguments).await),
            _ => {
                let name = name.to_owned();
                let workdir = self.workdir.clone();
                tokio::task::spawn_blocking(move || match name.as_str() {
                    "read" => files::read(&arguments),
                    "write" => files::write(&workdir, &arguments),
                    _ => files::edit(&workdir, &arguments),
                })
                .await
                .map_err(|e| e.to_string())
            }
        }
    }
}
struct Alias {
    name: String,
    target: Arc<dyn Tool>,
}
impl Alias {
    /// 别名映射保留旧接口中对超时、行号和过滤字段的处理。
    fn translate(&self, arguments: &Value) -> Value {
        let Some(a) = arguments.as_object() else {
            return arguments.clone();
        };
        let mut out = serde_json::Map::new();
        for (key, value) in a {
            if self.name == "run_in_terminal"
                && (matches!(key.as_str(), "wait_time" | "session_id")
                    || key == "timeout" && value.as_f64().is_some_and(|n| n <= 0.0))
            {
                continue;
            }
            let key = match (self.name.as_str(), key.as_str()) {
                ("file_read", "startLine") => "start_line",
                ("file_read", "endLine") => "end_line",
                ("grep_run", "glob") => "include",
                _ => key,
            };
            out.insert(key.to_owned(), value.clone());
        }
        Value::Object(out)
    }
}
#[async_trait]
impl Tool for Alias {
    /// 别名的 schema 和说明保持与基础工具一致。
    fn definition(&self) -> Value {
        let mut d = self.target.definition();
        d["name"] = json!(self.name);
        d
    }
    /// 映射后再执行必需字段验证。
    fn validate(&self, arguments: &Value) -> Result<(), String> {
        self.target.validate(&self.translate(arguments))
    }
    /// 映射后调用基础实现。
    async fn handle(
        &self,
        cancellation: CancellationToken,
        arguments: Value,
    ) -> Result<ToolResult, String> {
        self.target
            .handle(cancellation, self.translate(&arguments))
            .await
    }
}
