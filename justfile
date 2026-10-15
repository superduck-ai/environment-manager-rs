set positional-arguments

# 默认编译 Linux ARM64 release 可执行文件。
default: build

# 通过 Docker 编译，可追加 Cargo build 参数。
build *args:
    scripts/linux-arm64/docker-build.sh "$@"

