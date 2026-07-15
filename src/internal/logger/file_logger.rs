//! stderr JSON 与附加 JSONL 文件的组合日志 handler。
//!
//! 每条记录先交给 inner handler，再以独立 JSON 对象追加到文件。所有派生
//! handler 共享同一文件 mutex，因此同一组 handler 的数据和换行不会彼此交错；
//! 字段顺序、错误包装和关闭后的行为保持 Go 恢复实现的可观察语义。

use std::{
    collections::BTreeMap,
    fmt,
    fs::{DirBuilder, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

use chrono::{DateTime, SecondsFormat, Utc};
use thiserror::Error;

use super::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger};

const DEFAULT_LOG_PATH: &str = "/tmp/env-manager.log";

/// 为 inner handler 错误增加稳定上下文，同时保留原错误的 source chain。
#[derive(Debug, Error)]
#[error("inner handler error: {source}")]
struct InnerHandlerError {
    #[source]
    source: LogHandlerError,
}

/// 同时委托 inner handler 并追加 JSONL 文件的处理器。
pub struct FileLoggingHandler {
    inner: Arc<dyn LogHandler>,
    file: Arc<Mutex<Option<File>>>,
    path: Arc<PathBuf>,
    min_level: LogLevel,
}

impl FileLoggingHandler {
    /// 显式关闭所有派生 handler 共享的日志文件。
    ///
    /// 第一次调用消费底层文件描述符并返回系统 close 错误；再次调用会返回带
    /// `failed to close log file` 上下文的已关闭错误。inner handler 不受影响。
    pub fn close(&self) -> io::Result<()> {
        // 与 Go 的共享 `*sync.Mutex` 一致，关闭和写入必须串行；恢复 poison 可避免
        // 某个调用方 panic 后让整个日志设施永久不可用。
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let file = file.take().ok_or_else(|| {
            contextual_io_error(
                io::ErrorKind::Other,
                format!(
                    "failed to close log file: close {}: file already closed",
                    self.path.display()
                ),
            )
        })?;

        nix::unistd::close(file).map_err(|error| {
            let error = io::Error::from(error);
            contextual_io_error(
                error.kind(),
                format!(
                    "failed to close log file: close {}: {error}",
                    self.path.display()
                ),
            )
        })
    }

    /// 把记录序列化为单行 JSON，并按 Go 的两次 write 语义追加到共享文件。
    fn write_to_file(&self, record: &LogRecord) {
        let mut entry = BTreeMap::new();
        entry.insert(
            "timestamp".to_owned(),
            serde_json::Value::String(format_timestamp(SystemTime::now())),
        );
        entry.insert(
            "level".to_owned(),
            serde_json::Value::String(record.level.as_str().to_owned()),
        );
        entry.insert(
            "message".to_owned(),
            serde_json::Value::String(record.message.clone()),
        );
        entry.insert(
            "time".to_owned(),
            serde_json::Value::String(format_timestamp(record.time)),
        );
        if !record.attributes.is_empty() {
            entry.insert(
                "attributes".to_owned(),
                serde_json::Value::Object(sorted_json_object(&record.attributes)),
            );
        }

        // `serde_json::Value` 当前总能编码；仍保留静默失败分支，以复刻 Go
        // `json.Marshal` 失败时既不落盘也不向 stderr 输出的行为。
        let Ok(encoded) = go_encoding_json_to_vec(&entry) else {
            return;
        };

        // 一把共享锁覆盖数据与换行两次系统调用，既保持派生 handler 间的行边界，
        // 又保留 Go 会分别报告正文写入和换行写入错误的细节。
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let data_result = write_once(file.as_mut(), &encoded, self.path.as_ref());
        if let Err(error) = data_result {
            write_stderr_line(format_args!("Failed to write log data: {error}"));
        }
        let newline_result = write_once(file.as_mut(), b"\n", self.path.as_ref());
        if let Err(error) = newline_result {
            write_stderr_line(format_args!("Failed to write newline: {error}"));
        }
    }
}

impl LogHandler for FileLoggingHandler {
    /// 完全采用 inner handler 的级别判断，与 Go wrapper 的透明过滤行为一致。
    fn enabled(&self, level: LogLevel) -> bool {
        self.inner.enabled(level)
    }

    /// 先委托 inner；inner 失败时保留 source chain 并跳过文件写入。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.inner
            .handle(record)
            .map_err(|source| Box::new(InnerHandlerError { source }) as LogHandlerError)?;
        self.write_to_file(record);
        Ok(())
    }

    /// 仅在 inner 上绑定属性，并让派生 handler 共享文件、路径和 mutex。
    ///
    /// 这是恢复 Go 实现的一个重要细节：绑定属性由 inner 输出，但文件 JSON 只
    /// 收集传入 `LogRecord` 自身的属性。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(Self {
            inner: self.inner.with_attrs(attributes),
            file: Arc::clone(&self.file),
            path: Arc::clone(&self.path),
            min_level: self.min_level,
        })
    }

    /// 仅在 inner 上进入命名 group，并共享同一文件生命周期状态。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        Arc::new(Self {
            inner: self.inner.with_group(name),
            file: Arc::clone(&self.file),
            path: Arc::clone(&self.path),
            min_level: self.min_level,
        })
    }
}

/// 创建日志目录、以 append 模式打开文件并返回组合 handler。
///
/// 新目录使用 `0755`，新文件使用 `0644`；两者仍会受到进程 umask 约束。错误
/// 文本增加与 Go 实现相同的创建目录或打开文件上下文。
pub fn new_file_logging_handler(
    inner: Arc<dyn LogHandler>,
    log_file: impl AsRef<Path>,
    min_level: LogLevel,
) -> io::Result<Arc<FileLoggingHandler>> {
    let log_file = log_file.as_ref();
    if let Some(parent) = log_file.parent()
        && parent != Path::new("")
        && parent != Path::new("/")
    {
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        builder.mode(0o755);
        builder.create(parent).map_err(|error| {
            contextual_io_error(
                error.kind(),
                format!("failed to create log directory: {error}"),
            )
        })?;
    }

    let mut options = OpenOptions::new();
    options.create(true).write(true).append(true);
    #[cfg(unix)]
    options.mode(0o644);
    let file = options.open(log_file).map_err(|error| {
        contextual_io_error(error.kind(), format!("failed to open log file: {error}"))
    })?;
    Ok(Arc::new(FileLoggingHandler {
        inner,
        file: Arc::new(Mutex::new(Some(file))),
        path: Arc::new(log_file.to_path_buf()),
        min_level,
    }))
}

/// 创建同时输出 stderr JSON 与 `/tmp/env-manager.log` JSONL 的 logger。
///
/// 文件创建失败时会打印一次 warning 并退化为仅 stderr，确保日志设施故障不会
/// 阻止主服务启动；最低级别由 stderr inner handler 统一判断。
pub fn create_logger_with_file_output(level: LogLevel) -> Logger {
    create_logger_with_path(level, PathBuf::from(DEFAULT_LOG_PATH))
}

/// 创建可指定文件路径的 logger，供模块测试复用并保持生产默认路径单一来源。
fn create_logger_with_path(level: LogLevel, path: PathBuf) -> Logger {
    let stderr: Arc<dyn LogHandler> = Arc::new(JsonStreamHandler::stderr(level));
    match new_file_logging_handler(Arc::clone(&stderr), path, level) {
        Ok(handler) => Logger::new(handler),
        Err(error) => {
            write_stderr_line(format_args!(
                "Warning: Failed to create file logger: {error}"
            ));
            Logger::new(stderr)
        }
    }
}

/// 把平坦属性按照非空 group 路径包装为嵌套 JSON 对象。
fn merge_grouped_attributes(
    target: &mut LogAttributes,
    groups: &[String],
    attributes: LogAttributes,
) {
    if attributes.is_empty() {
        return;
    }
    let Some((group, remaining_groups)) = groups.split_first() else {
        target.extend(attributes);
        return;
    };

    // `WithGroup("")` 在 Go slog 中不创建空 key；保留当前层继续处理后续 group。
    if group.is_empty() {
        merge_grouped_attributes(target, remaining_groups, attributes);
        return;
    }

    let value = target
        .entry(group.clone())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    if !value.is_object() {
        // slog 能在极端同名冲突中输出重复 JSON key，而 `serde_json::Map` 不能表达
        // 重复 key。生产调用没有该冲突；这里选择让后创建的 group 与通常的 JSON
        // consumer 观察结果一致，避免把 record 属性静默丢失。
        *value = serde_json::Value::Object(serde_json::Map::new());
    }
    let object = value
        .as_object_mut()
        .expect("刚刚确保 group value 是 JSON object");
    merge_grouped_attributes(object, remaining_groups, attributes);
}

/// 递归按 key 排序 JSON object，复刻 `encoding/json` 对 map key 的排序规则。
fn sorted_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => serde_json::Value::Object(sorted_json_object(object)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(sorted_json_value).collect())
        }
        scalar => scalar.clone(),
    }
}

/// 把任意 JSON object 递归转换为按 UTF-8 key 升序插入的 object。
fn sorted_json_object(object: &LogAttributes) -> serde_json::Map<String, serde_json::Value> {
    object
        .iter()
        .map(|(key, value)| (key.clone(), sorted_json_value(value)))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect()
}

/// 使用 Go `encoding/json.Marshal` 的额外字符转义规则生成紧凑 JSON。
fn go_encoding_json_to_vec<T: serde::Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(value).map(|encoded| escape_go_json_bytes(&encoded, true))
}

/// 使用 Go slog JSON handler 的转义规则生成紧凑 JSON。
fn go_slog_json_to_vec<T: serde::Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(value).map(|encoded| escape_go_json_bytes(&encoded, false))
}

/// 补齐 Serde 与 Go JSON encoder 在 HTML 字符和行分隔符上的 wire 差异。
fn escape_go_json_bytes(encoded: &[u8], escape_html: bool) -> Vec<u8> {
    let mut output = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index..].starts_with(&[0xe2, 0x80, 0xa8]) {
            output.extend_from_slice(br"\u2028");
            index += 3;
            continue;
        }
        if encoded[index..].starts_with(&[0xe2, 0x80, 0xa9]) {
            output.extend_from_slice(br"\u2029");
            index += 3;
            continue;
        }
        if escape_html {
            let replacement = match encoded[index] {
                b'<' => Some(br"\u003c".as_slice()),
                b'>' => Some(br"\u003e".as_slice()),
                b'&' => Some(br"\u0026".as_slice()),
                _ => None,
            };
            if let Some(replacement) = replacement {
                output.extend_from_slice(replacement);
                index += 1;
                continue;
            }
        }
        output.push(encoded[index]);
        index += 1;
    }
    output
}

/// 将 `SystemTime` 编码为 Go RFC3339Nano 风格，并裁掉小数秒末尾的零。
fn format_timestamp(time: SystemTime) -> String {
    let timestamp: DateTime<Utc> = time.into();
    let formatted = timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let Some((prefix, fractional_with_zone)) = formatted.split_once('.') else {
        return formatted;
    };
    let Some(fractional) = fractional_with_zone.strip_suffix('Z') else {
        return formatted;
    };
    let fractional = fractional.trim_end_matches('0');
    if fractional.is_empty() {
        format!("{prefix}Z")
    } else {
        format!("{prefix}.{fractional}Z")
    }
}

/// 执行一次非重试写入；关闭后构造与 `os.File.Write` 相近的路径错误文本。
fn write_once(file: Option<&mut File>, bytes: &[u8], path: &Path) -> io::Result<()> {
    let Some(file) = file else {
        return Err(contextual_io_error(
            io::ErrorKind::Other,
            format!("write {}: file already closed", path.display()),
        ));
    };
    // Go 原实现忽略短写返回的字节数且不调用 WriteAll；这里刻意只调用一次
    // `write`，避免悄然改变故障场景下的可观察行为。
    file.write(bytes).map(drop)
}

/// 创建保留 `io::ErrorKind` 且展示给调用方稳定上下文的 I/O 错误。
fn contextual_io_error(kind: io::ErrorKind, message: String) -> io::Error {
    io::Error::new(kind, message)
}

/// 尽力向 stderr 写一行诊断；stderr 本身失败时不让日志路径 panic。
fn write_stderr_line(message: fmt::Arguments<'_>) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

/// 将 JSON 行写入某个共享 stream 的最小 handler。
struct JsonStreamHandler {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    min_level: LogLevel,
    bound_attributes: LogAttributes,
    groups: Vec<String>,
}

impl JsonStreamHandler {
    /// 创建写向进程 stderr 的 JSON handler，并配置最低输出级别。
    fn stderr(min_level: LogLevel) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(io::stderr()))),
            min_level,
            bound_attributes: LogAttributes::new(),
            groups: Vec::new(),
        }
    }

    /// 使用测试提供的共享 writer 构造 handler，避免通过进程级 stderr 捕获判断 wire。
    #[cfg(test)]
    fn with_writer(writer: Box<dyn Write + Send>, min_level: LogLevel) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            min_level,
            bound_attributes: LogAttributes::new(),
            groups: Vec::new(),
        }
    }
}

/// 为相邻回归测试暴露 JSON stream handler 的 trait 视图。
#[cfg(test)]
pub(super) fn json_stream_handler_for_test(
    writer: Box<dyn Write + Send>,
    min_level: LogLevel,
) -> Arc<dyn LogHandler> {
    Arc::new(JsonStreamHandler::with_writer(writer, min_level))
}

impl LogHandler for JsonStreamHandler {
    /// 按构造时最低级别过滤记录，供外层 logger 在构造记录前快速判断。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }

    /// 把记录编码为一条 slog 风格 JSON，并在 mutex 内一次性写出。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        if !self.enabled(record.level) {
            return Ok(());
        }
        let mut attributes = self.bound_attributes.clone();
        merge_grouped_attributes(&mut attributes, &self.groups, record.attributes.clone());
        let mut value = serde_json::Map::new();
        value.insert(
            "time".to_owned(),
            serde_json::Value::String(format_timestamp(record.time)),
        );
        value.insert(
            "level".to_owned(),
            serde_json::Value::String(record.level.as_str().to_owned()),
        );
        if let Some(source) = &record.source {
            // Go `slog.JSONHandler` 的内建字段顺序固定为 time、level、source、msg；
            // source 内部也必须保持 function、file、line，不能交给按 key 排序的
            // 文件 JSON helper。文件 handler 独立读取 record attrs，因此不会落盘。
            let mut source_value = serde_json::Map::new();
            source_value.insert(
                "function".to_owned(),
                serde_json::Value::String(source.function.clone()),
            );
            source_value.insert(
                "file".to_owned(),
                serde_json::Value::String(source.file.clone()),
            );
            source_value.insert(
                "line".to_owned(),
                serde_json::Value::Number(source.line.into()),
            );
            value.insert("source".to_owned(), serde_json::Value::Object(source_value));
        }
        value.insert(
            "msg".to_owned(),
            serde_json::Value::String(record.message.clone()),
        );
        for (key, value_part) in attributes {
            value.insert(key, value_part);
        }
        let mut encoded =
            go_slog_json_to_vec(&value).map_err(|error| Box::new(error) as LogHandlerError)?;
        encoded.push(b'\n');
        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write_all(&encoded)
            .map_err(|error| Box::new(error) as LogHandlerError)
    }

    /// 派生绑定更多顶层属性且共享相同 writer 的 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut bound_attributes = self.bound_attributes.clone();
        merge_grouped_attributes(&mut bound_attributes, &self.groups, attributes);
        Arc::new(Self {
            writer: Arc::clone(&self.writer),
            min_level: self.min_level,
            bound_attributes,
            groups: self.groups.clone(),
        })
    }

    /// 派生进入非空 group 且共享相同 writer 的 handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        let mut groups = self.groups.clone();
        if !name.is_empty() {
            groups.push(name.to_owned());
        }
        Arc::new(Self {
            writer: Arc::clone(&self.writer),
            min_level: self.min_level,
            bound_attributes: self.bound_attributes.clone(),
            groups,
        })
    }
}
