# 现成 Go 二进制与 Rust 的 MCP 功能 E2E

参考输入是用户提供的 Linux x86-64 可执行文件 `/Users/arthur/GolandProjects/qoder-mcp-server/mcp-server`。测试不访问 Go 源码、不调用 Go 工具链。参考文件只读挂载，执行权限只添加到容器内的临时副本。

`run.py` 构建当前工作区的 Rust `mcp-server`，默认使用 release profile，然后分别在同配置的 Ubuntu 24.04 容器运行 Go 和 Rust。Go 使用 `linux/amd64`；Rust 使用 Docker 的原生 ARM64 或 AMD64。两端检查 bash/ripgrep 版本一致，不比较跨架构运行时间的精确毫秒值。

## 执行

从项目根目录运行：

```sh
python3 tests/qoder_compatibility/binary_e2e/run.py \
  --go-binary /Users/arthur/GolandProjects/qoder-mcp-server/mcp-server

# 当前机器已有 Rust 构建环境时可以复用，避免重新下载构建镜像。
python3 tests/qoder_compatibility/binary_e2e/run.py \
  --rust-build-image environment-manager-qoder-oracle:latest \
  --output target/qoder-binary-e2e-fixed-release

# debug 诊断，使用独立报告目录。
python3 tests/qoder_compatibility/binary_e2e/run.py \
  --profile debug --output target/qoder-binary-e2e-fixed-debug

# 对比较器注入错误，验证不会把共同失败、缺失步骤或类型变化判为通过。
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s tests/qoder_compatibility/binary_e2e -p 'test_*.py'
```

需要 Python 3 和可运行 AMD64 容器的 Docker。Python 测试本身只用标准库。默认 Rust 构建镜像为 `rust:1.97.0-bookworm`，可用 `--rust-build-image` 或 `QODER_RUST_BUILD_IMAGE` 指定已有镜像。构建产物放在 `target/binary-e2e-linux`，Cargo 下载使用命名缓存卷。

任何响应差异、文件副作用差异、独立断言失败或用例缺失，都会使命令以非零状态退出。不会把已知差异加入豁免列表。Docker/编译/测试执行失败也不能算作验收成功。

## 功能设计

| 功能流程 | 核对内容 |
| --- | --- |
| 初始化 → 通知 → ping → tools/list | 协议版本、capabilities、12 个工具的完整 schema/说明；通知无响应且不执行 write |
| JSON-RPC 错误和参数 | ID 的类型和值、未知方法/工具、错误码/文本、重复 arguments 合并/覆盖/null 清空、全部工具的必填字段和参数类型 |
| 写入 → 读取 → 编辑 → 覆盖 → 删除 → 再读取 | 每一步完整响应、真实文件 SHA256/大小/权限、目录与符号链接；别名与 camelCase 参数映射 |
| 文本读取 | 分页、边界、空文件、Unicode、无效 UTF-8、二进制、BOM/CRLF、危险设备、缺失文件/目录、长输出与分页补读 |
| 图片读取 | PNG/JPEG/GIF/WebP 的内容块、MIME、完整 base64 字节；大尺寸和大字节数引起的缩放/重新编码；损坏及不支持的格式 |
| glob → grep → edit → 再搜索 | 真实 ripgrep、glob/brace/extglob/字符类、过滤/类型/多行、内容/计数/文件模式、上下文/分页、找不到和非法模式 |
| bash / run_in_terminal | stdout/stderr、退出码、空输出、Unicode、继承和覆盖环境、子 shell、工作目录、无 stdin/TTY、交互命令限制、别名超时参数 |
| 超时和后台任务 | 超时后子进程不能写入标记；后台任务必须完成；通过返回的 output_file 再走 MCP read，核对完整日志及退出状态 |
| Unix 环境 socket | 大快照尾部空白、分块与半个 UTF-8 字符、map/null、多个 JSON、非法/缺失/超时、512 KiB/1 MiB/超限、调用环境覆盖 |
| HTTP 并发与断开 | 长命令期间 ping 能完成；客户端断开后已启动的子进程不能写入标记，服务随后仍响应 ping |
| 传输边界 | HTTP 方法/路径/重定向/通知状态和功能响应头；stdio 空行、多行及 EOF 前无换行的消息 |

轨迹包括工作目录快照和生成输出文件的 SHA256。`timings_ms` 保留诊断时间，但不直接比较跨架构调度时间。后台完成以日志中已出现退出状态为条件，避免在子进程刚结束、服务尚未追加日志时提前读取。

只归一化 `tools/list` 的枚举顺序、受控大输出/后台结果中的系统生成文件名和 PID。图片 base64、普通用户文本、错误消息、JSON 字段缺失和类型均完整比较；特别验证 `false` 不会因 Python 的 `False == 0` 而与数字 ID 混淆。另有看似临时路径/PID 的用户文本用例，确保它们不被全局替换。HTTP 的 Date、传输分块和 Content-Length 属于传输包装，不参与本功能比较。

这套用例通过可执行文件公开的 MCP 接口验证行为。动态插件注册及 ServiceClient 的程序内 API 没有可从该二进制调用的 MCP 入口，仍由 Rust 集成测试验证；文件系统满盘、特殊 UID 等底层故障也保留在原有专项语料中。

## 2026-10-10 修复后验证

Rust release 和 debug 均完成每端 **502 个步骤**（stdio 245、HTTP 257），各自与原 Go 二进制对照 **零差异**。主流程计数器记录 497 个 MCP 输入，另有 10 个直接 HTTP 路由测试。两端的独立功能断言均通过，两个 profile 的验收命令均以零状态退出。

已修复环境 socket 的两个问题：标量未收到结束字节或 EOF 时，不提前接受；大快照采用 serde 流式扫描及有界通道，每个输入只扫描一次，再执行兼容解码，避免 debug 下反复解析导致超时。保留原来的 500 ms 超时、取消行为和 1 MiB 限制。debug 本次恰好 1 MiB 的快照约 64 ms 完成。

补充布尔值、数字、字符串的裸值/带分隔符对照、无效 UTF-8、孤立代理项和分块 null；此前裸 null 的两处差异以及 debug 容量边界差异均已消除。Rust 独立测试同时覆盖这些标量边界、容量边界和读取取消，18 项测试通过；Clippy 和格式检查通过。

参考文件 SHA256：`322e851af2d82be0d525b9898cd8f565815b53cfc237de4dbdd5b81e4e593a35`。
Rust release SHA256：`fc37af37e808d670f5a5979a6c77ccfafbeebae3b97511f700f66a027bfe90dc`。
Rust debug SHA256：`105cab8d441f2661e0e589b27b99fb1729cebf37418e25f17d09fb8217973497`。

完整记录分别位于 `target/qoder-binary-e2e-fixed-release/` 和 `target/qoder-binary-e2e-fixed-debug/` 的 `go.json`、`rust.json`、`comparison.json`。修复前报告仍保留在原有目录，可核对实际消除的差异。这些结果是当前有限功能语料的实测，不等同于对所有输入的证明。
