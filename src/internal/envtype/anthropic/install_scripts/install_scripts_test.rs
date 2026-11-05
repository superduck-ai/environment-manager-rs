//! 语言安装脚本文本测试。

use super::{go, node, python};

///
/// 验证三个脚本替换全部 `$1`，并保留 bash 前导字节。
#[test]
fn test_scripts_replace_requested_version() {
    for script in [python("3.12"), node("22.1.0"), go("1.24.0")] {
        assert!(!script.contains("$1"));
        assert!(script.starts_with("#!/bin/bash\nset -e\n"));
        assert!(script.ends_with('\n'));
    }
}
