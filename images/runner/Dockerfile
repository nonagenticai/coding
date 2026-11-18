# Runner image: the toolchains plus `coding-runner`, which is the ENTRYPOINT.
#
# One Pod per job, under the `coding-kata` RuntimeClass. The Pod's root filesystem is read-only,
# so every writable path below is under /tmp (the 8 GiB scratch emptyDir the sandbox mounts):
#   /tmp/src     the job's checked-out source          (written by coding-runner)
#   /tmp/src.target  cargo's target directory          (set by coding-runner)
#   /tmp/cargo   CARGO_HOME                            (must be writable for the registry cache)
#   /tmp/uv-cache  uv's cache                          (set by coding-runner for Python steps)
#
# Two profiles share this image, chosen per job by `coding-runner`:
#   rust    cargo fmt / clippy / build / test (the Rust toolchain below)
#   python  uv lock / sync, ruff, ty, pytest  (uv + one CPython, installed read-only below)
#
# No docker, no container runtime, no cluster client: this image only runs cargo, rustfmt,
# clippy, uv, ruff, ty, pytest and git on behalf of one job. Its own dependencies are installed
# from Debian, rustup and python-build-standalone; the job's crates and wheels come from
# crates.io and pypi.org over the sandbox's HTTPS egress.
#
# RUST_TOOLCHAIN must equal rust-toolchain.toml and the control-plane Dockerfile.
ARG RUST_TOOLCHAIN=1.100.0

FROM debian:12-slim AS build
ARG RUST_TOOLCHAIN
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# build-essential/ca-certificates/cmake/perl/pkg-config: linking the control plane's deps
# (aws-lc-sys). git: `[patch.crates-io]` in Cargo.toml pins sqlx-sqlite to a git rev, so the
# build fetches one repository.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake curl git perl pkg-config \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal --default-toolchain "${RUST_TOOLCHAIN}" \
    && rustc --version

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY migrations ./migrations
# Only the runner binary is built: the image carries no control-plane code paths.
RUN --mount=type=cache,target=/opt/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin coding-runner \
    && install -D -m 0755 target/release/coding-runner /out/coding-runner

# sccache: the client side of the compilation cache. A static musl binary, fetched and
# sha256-verified from the release's own asset exactly as `.github/workflows/rust-library.yml`
# does, so the job cache and the CI cache speak one dialect (same bucket, different key prefix).
FROM debian:12-slim AS sccache
ARG SCCACHE_VERSION=v0.19.0
# sha256 of sccache-v0.19.0-x86_64-unknown-linux-musl.tar.gz
ARG SCCACHE_SHA256=aa73c7270c997894038c0d8f0d297865f57569e1687883322bc431713d4bb9dd
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL --retry 5 --retry-delay 10 --retry-all-errors \
        -o /tmp/sccache.tar.gz \
        "https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    && echo "${SCCACHE_SHA256}  /tmp/sccache.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/sccache.tar.gz -C /tmp \
    && install -D -m 0755 "/tmp/sccache-${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" /out/sccache

# The runtime stage is `postgres:18` because the runner starts a throwaway Postgres 18 from it
# (src/runner/postgres.rs). It has to be THIS base: those binaries are built for the image's
# own Debian (trixie, ICU 76), and copying them onto a bookworm base fails on missing shared
# libraries (`libpq.so.5`, then `libicuuc.so.76`). The Rust binary from the build stage is
# built against an older glibc, which runs forward without complaint.
FROM postgres:18
ARG RUST_TOOLCHAIN
# build-essential/libssl: linking a target repo's native dependencies (aws-lc-sys, ring, …).
# git: cloning the job's source. ca-certificates: crates.io + git over TLS.
# redis-server: the throwaway Redis the runner starts for a test step (src/runner/redis.rs);
# the estate's harnesses fail closed without TEST_REDIS_URL/REDIS_URL, and a kata Pod has no docker.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake git perl pkg-config redis-server \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 app \
    && useradd --system --uid 65532 --gid 65532 --home-dir /tmp --create-home app


# RUSTUP_TOOLCHAIN is pinned so a target repo's rust-toolchain.toml cannot make rustup try to
# download a toolchain into the read-only RUSTUP_HOME mid-job.
ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/tmp/cargo \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    RUSTUP_TOOLCHAIN=${RUST_TOOLCHAIN} \
    PATH=/opt/cargo/bin:/usr/local/bin:/usr/lib/postgresql/18/bin:/usr/bin:/bin

COPY --from=build /opt/rustup /opt/rustup
# Runtime CARGO_HOME is /tmp/cargo, so the build stage's registry cache is dead weight here; the
# toolchain lives in /opt/rustup and only the shims in /opt/cargo/bin are needed beside it.
COPY --from=build /opt/cargo/bin /opt/cargo/bin
COPY --from=build /out/coding-runner /usr/local/bin/coding-runner
# The compilation cache's client. Its endpoint and credentials arrive per job, never from here.
COPY --from=sccache /out/sccache /usr/local/bin/sccache

# The Python profile's toolchain: uv (the public archetype's version) and one CPython it manages.
# The interpreter is read-only here and `coding-runner` points uv at it
# (UV_PYTHON_INSTALL_DIR) with downloads off, so a job cannot fetch another interpreter into its
# 8 GiB scratch; a repository that pins a different Python version fails with uv's own message.
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
RUN uv python install 3.14

# The toolchain is pre-installed; rustup and uv must never try to download at run time.
# The Postgres check proves the throwaway cluster can actually start in this image: a runner that
# discovers the missing binary mid-job reports a setup error for a plan that never ran.
RUN /opt/cargo/bin/rustc --version \
    && /opt/cargo/bin/cargo --version \
    && /opt/cargo/bin/cargo fmt --version \
    && /opt/cargo/bin/cargo clippy --version \
    && uv --version \
    && uv python find 3.14 \
    && uv run --no-project python -V \
    && initdb --version \
    && pg_ctl --version \
    && redis-server --version \
    && sccache --version \
    && test -f /usr/share/postgresql/18/postgres.bki

# No WORKDIR under a read-only root; the runner creates /tmp/src itself.
WORKDIR /tmp
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/coding-runner"]
