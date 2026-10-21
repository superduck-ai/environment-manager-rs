# WebP 输入

`blue-purple-pink.lossy.webp` 和 `blue-purple-pink.lossless.webp` 来自 `golang.org/x/image v0.16.0/testdata`，采用同目录 `LICENSE` 中的 Go Authors BSD-3-Clause 许可。

从已有专项测试的输入副本按 RIFF 长度提取原始 WebP（分别 2450 和 19574 字节），去除了专项测试附加的填充。E2E 同时测试原始小文件，以及运行时追加填充以触发解码和重新编码的副本；大副本不进入版本库。无需 Go 工具链生成测试输入。
