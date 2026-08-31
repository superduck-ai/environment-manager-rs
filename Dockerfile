# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

# The build stage runs on BUILDPLATFORM. When the target differs from the host,
# cargo-zigbuild performs a real GNU cross-build instead of running rustc under
# QEMU (which is unreliable for the x86_64 Rust compiler on ARM Macs).
ARG ALMALINUX_IMAGE=docker.io/library/almalinux@sha256:d2515c769e7b73f95c4fde38c0a505336ff38f14990c0b7253b77060a049a743

FROM --platform=$BUILDPLATFORM ${ALMALINUX_IMAGE} AS builder

ARG RUST_VERSION=1.97.0
ARG CARGO_ZIGBUILD_VERSION=0.23.3
ARG ZIG_VERSION=0.14.1
ARG ENVIRONMENT_MANAGER_VERSION=dev
ARG BUILD_GIT_HASH=unknown
ARG BUILD_TIME=unknown
ARG ALMALINUX_MIRROR=https://mirrors.aliyun.com/almalinux

WORKDIR /workspace

RUN test -n "$RUST_VERSION" \
    && case "$ALMALINUX_MIRROR" in \
      https://*) ;; \
      *) echo "ALMALINUX_MIRROR must be an HTTPS URL" >&2; exit 1 ;; \
    esac \
    && sed -ri 's|^mirrorlist=|# mirrorlist=|' /etc/yum.repos.d/almalinux-*.repo \
    && sed -ri \
      "s|^# baseurl=https://repo.almalinux.org/almalinux|baseurl=${ALMALINUX_MIRROR}|" \
      /etc/yum.repos.d/almalinux-*.repo \
    && dnf -y --setopt=install_weak_deps=False install \
      binutils ca-certificates file gcc gcc-c++ git gzip make \
      perl-interpreter pkgconf-pkg-config python3-pip tar xz \
    && dnf clean all \
    && rm -rf /var/cache/dnf \
    && curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
      https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path

ENV PATH=/root/.cargo/bin:$PATH \
    CARGO_TARGET_DIR=/tmp/environment-manager-target

# Copy dependency metadata before source. The build cache mounts below preserve
# downloaded crates and compiled dependencies across source-only changes.
# build.rs also needs the vendored proto tree when Cargo compiles it.
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY gen/ ./gen/

RUN python3 -m pip install --no-cache-dir "ziglang==$ZIG_VERSION" \
    && rustup toolchain install "$RUST_VERSION" --profile minimal \
      --component clippy --component rustfmt \
    && cargo "+$RUST_VERSION" install --locked \
      --version "$CARGO_ZIGBUILD_VERSION" cargo-zigbuild

ARG BUILDARCH
ARG TARGETARCH

RUN case "$TARGETARCH" in \
      amd64) rust_target=x86_64-unknown-linux-gnu ;; \
      arm64) rust_target=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && rustup target add --toolchain "$RUST_VERSION" "$rust_target"

COPY src/ ./src/
COPY tests/ ./tests/

RUN --mount=type=cache,id=environment-manager-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=environment-manager-cargo-git,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,id=environment-manager-cargo-target-$TARGETARCH,target=/tmp/environment-manager-target,sharing=locked \
    case "$TARGETARCH" in \
      amd64) rust_target=x86_64-unknown-linux-gnu ;; \
      arm64) rust_target=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && case "$BUILDARCH" in \
      amd64) build_rust_target=x86_64-unknown-linux-gnu ;; \
      arm64) build_rust_target=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported BUILDARCH: $BUILDARCH" >&2; exit 1 ;; \
    esac \
    && if [ "$rust_target" = "$build_rust_target" ]; then \
      ENVIRONMENT_MANAGER_VERSION="$ENVIRONMENT_MANAGER_VERSION" \
        BUILD_GIT_HASH="$BUILD_GIT_HASH" BUILD_TIME="$BUILD_TIME" \
        cargo "+$RUST_VERSION" build --release --locked --target "$rust_target"; \
    else \
      ENVIRONMENT_MANAGER_VERSION="$ENVIRONMENT_MANAGER_VERSION" \
        BUILD_GIT_HASH="$BUILD_GIT_HASH" BUILD_TIME="$BUILD_TIME" \
        cargo "+$RUST_VERSION" zigbuild --release --locked \
          --target "${rust_target}.2.17"; \
    fi \
    && install -Dm0755 \
      "$CARGO_TARGET_DIR/$rust_target/release/environment-manager" \
      /out/environment-manager \
    && file /out/environment-manager \
    && readelf -hW /out/environment-manager \
    && readelf -dW /out/environment-manager \
    && case "$TARGETARCH" in \
      amd64) readelf -lW /out/environment-manager \
        | grep -Fq '[Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]' ;; \
      arm64) readelf -lW /out/environment-manager \
        | grep -Fq '[Requesting program interpreter: /lib/ld-linux-aarch64.so.1]' ;; \
    esac \
    && highest_glibc="$(readelf --version-info -W /out/environment-manager \
      | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' | sort -Vu | tail -n 1)" \
    && test -n "$highest_glibc" \
    && test "$(printf '%s\n%s\n' GLIBC_2.34 "$highest_glibc" | sort -Vu | tail -n 1)" = GLIBC_2.34

# Run source gates natively on BUILDPLATFORM. Target execution is checked later
# in verification-runtime, after the artifact has been extracted.
FROM builder AS verify

RUN --mount=type=cache,id=environment-manager-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=environment-manager-cargo-git,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,id=environment-manager-cargo-target-$TARGETARCH,target=/tmp/environment-manager-target,sharing=locked \
    cargo "+$RUST_VERSION" fmt --all --check \
    && cargo "+$RUST_VERSION" clippy --all-targets --locked -- -D warnings \
    && cargo "+$RUST_VERSION" build --locked --bin environment-manager \
    && env CARGO_BIN_EXE_environment-manager="$CARGO_TARGET_DIR/debug/environment-manager" \
      cargo "+$RUST_VERSION" test --all-targets --locked

# Minimal OCI artifact consumed by managed-agent-sandbox through a named
# BuildKit image context. It intentionally contains only the payload.
FROM scratch AS artifact

ARG BUILD_GIT_HASH=unknown
ARG BUILD_TIME=unknown
ARG ENVIRONMENT_MANAGER_VERSION=dev

COPY --from=builder /out/environment-manager /usr/local/bin/environment-manager

LABEL org.opencontainers.image.title="environment-manager" \
      org.opencontainers.image.description="Linux GNU environment-manager binary artifact" \
      org.opencontainers.image.version="$ENVIRONMENT_MANAGER_VERSION" \
      org.opencontainers.image.revision="$BUILD_GIT_HASH" \
      org.opencontainers.image.created="$BUILD_TIME"

# A payload-free GNU userspace used only to execute an extracted artifact
# during local verification. Keeping it separate from artifact ensures the
# published image remains binary-only.
FROM --platform=$TARGETPLATFORM ${ALMALINUX_IMAGE} AS verification-runtime

# Optional directly runnable image for smoke tests and manual inspection. The
# complete agent environment remains the responsibility of managed-agent-sandbox.
FROM --platform=$TARGETPLATFORM ${ALMALINUX_IMAGE} AS runtime

ARG BUILD_GIT_HASH=unknown
ARG BUILD_TIME=unknown
ARG ENVIRONMENT_MANAGER_VERSION=dev

RUN dnf -y --setopt=install_weak_deps=False install ca-certificates \
    && dnf clean all \
    && rm -rf /var/cache/dnf

COPY --from=builder /out/environment-manager /usr/local/bin/environment-manager

LABEL org.opencontainers.image.title="environment-manager" \
      org.opencontainers.image.description="Runnable Linux GNU environment-manager image" \
      org.opencontainers.image.version="$ENVIRONMENT_MANAGER_VERSION" \
      org.opencontainers.image.revision="$BUILD_GIT_HASH" \
      org.opencontainers.image.created="$BUILD_TIME"

ENTRYPOINT ["/usr/local/bin/environment-manager"]
