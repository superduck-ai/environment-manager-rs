set positional-arguments

# 默认编译 Linux ARM64 release 可执行文件。
default: build

# 通过 Docker 编译，可追加 Cargo build 参数。
build *args:
    scripts/linux-arm64/docker-build.sh "$@"

alias build-arm64 := build

build-amd64 *args:
    scripts/linux-amd64/docker-build.sh "$@"
