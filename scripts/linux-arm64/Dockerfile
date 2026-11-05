FROM docker.io/library/almalinux@sha256:2c999b3bd705fad8b115741d9036ae2499148ba162752f09f2f4ab62b0c07320

ARG RUST_VERSION=1.97.0
ARG ALMALINUX_MIRROR=https://mirrors.aliyun.com/almalinux

ENV RUSTUP_DIST_SERVER=https://rsproxy.cn \
    RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
    CARGO_HOME=/root/.cargo \
    RUSTUP_HOME=/root/.rustup \
    PATH=/root/.cargo/bin:$PATH

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN test "$(uname -m)" = aarch64 \
    && sed -ri 's|^mirrorlist=|# mirrorlist=|' /etc/yum.repos.d/almalinux-*.repo \
    && sed -ri "s|^# baseurl=https://repo.almalinux.org/almalinux|baseurl=${ALMALINUX_MIRROR}|" /etc/yum.repos.d/almalinux-*.repo \
    && dnf -y --setopt=install_weak_deps=False install \
        binutils ca-certificates file gcc gcc-c++ git gzip make \
        pkgconf-pkg-config tar xz \
    && dnf clean all \
    && mkdir -p "${CARGO_HOME}" \
    && curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
        https://rsproxy.cn/rustup-init.sh -o /tmp/rustup-init.sh \
    && sh /tmp/rustup-init.sh -y --profile minimal --no-modify-path \
        --default-toolchain "${RUST_VERSION}" --component clippy,rustfmt \
        --target aarch64-unknown-linux-gnu \
    && rm /tmp/rustup-init.sh \
    && test "$(rustc -vV | sed -n 's/^host: //p')" = aarch64-unknown-linux-gnu

COPY config.toml /root/.cargo/config.toml

WORKDIR /workspace
CMD ["bash"]
