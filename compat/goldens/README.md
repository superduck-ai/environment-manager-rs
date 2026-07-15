# CLI 差分 golden

`cli.json` 由 Linux ARM64 上的固定 Go 1.25.7 oracle 生成，记录每个用例的退出码、
stdout 和 stderr。普通门禁会先确认当前 Go oracle 仍与该文件逐字节一致，再比较 Rust；
因此 Go 基线漂移和 Rust 行为差异不会混在一起。

只允许在审核 Go HEAD、工作树 diff 和镜像 digest 后执行：

```bash
scripts/compat/run_differential.sh --update-goldens
```

golden 不应手工“修到能过”。新增随机 UUID、端口或时间戳的用例时，原始 transcript
仍写入 golden；仅在 `compat/differential/cli_cases.json` 为该用例显式声明白名单
normalizer，比较器才会归一化该字段。
