//! Status action 的数据结构。
//!
//! 类型字段顺序与 JSON rename 保持 Go 行为；执行逻辑仍在 `action.rs`，从而让类型
//! 对照文件只描述状态，不复制网络探测流程。

use serde::{Deserialize, Serialize};

use crate::internal::logger::Logger;

/// 对应 Go：`zz_recovered_types.go` 中的 `StatusAction`。
///
/// logger 只记录探测属性，不参与返回值或错误判断。
pub struct StatusAction {
    /// 可选结构化 logger；缺省时 status action 保持静默。
    pub(super) logger: Option<Logger>,
}

/// 对应 Go：`zz_recovered_types.go` 中的 `statusRequest`。
///
/// `port == 0` 的默认值在执行阶段应用，避免反序列化时改变显式零值语义。
#[derive(Debug, Default, Deserialize)]
pub(super) struct StatusRequest {
    /// 要探测的 loopback TCP 端口。
    pub(super) port: i32,
}

/// 对应 Go：`zz_recovered_types.go` 中的 `statusResult`。
///
/// 字段声明顺序固定为 `listening,port`，以保持 serde_json 的输出字节顺序。
#[derive(Debug, Serialize)]
pub(super) struct StatusResult {
    /// 连接是否在一秒内建立成功。
    pub(super) listening: bool,
    /// 实际探测端口，包括零值映射后的默认 3000。
    pub(super) port: i32,
}
