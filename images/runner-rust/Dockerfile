# Runner image: the Rust toolchain plus `coding-runner`, which is the ENTRYPOINT.
#
# One Pod per job, under the `coding-kata` RuntimeClass. The Pod's root filesystem is read-only,
# so every writable path below is under /tmp (the 8 GiB scratch emptyDir the sandbox mounts):
#   /tmp/src     the job's checked-out source          (written by coding-runner)
#   /tmp/src.target  cargo's target directory          (set by coding-runner)
#   /tmp/cargo   CARGO_HOME                            (must be writable for the registry cache)
#
# No docker, no container runtime, no cluster client: this image only runs cargo, rustfmt,
# clippy and git on behalf of one job. Its own dependencies are installed from Debian and
# rustup; the job's crates come from crates.io over the sandbox's HTTPS egress.
#
# RUST_TOOLCHAIN must equal rust-toolchain.toml and rust_toolchain in .github/workflows/ci.yml.
ARG RUST_TOOLCHAIN=1.99.0

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
COPY --from=build /opt/cargo /opt/cargo
COPY --from=build /out/coding-runner /usr/local/bin/coding-runner
# The toolchain is pre-installed; rustup must never try to download at run time.
# The Postgres check proves the throwaway cluster can actually start in this image: a runner that
# discovers the missing binary mid-job reports a setup error for a plan that never ran.
RUN /opt/cargo/bin/rustc --version \
    && /opt/cargo/bin/cargo --version \
    && /opt/cargo/bin/cargo fmt --version \
    && /opt/cargo/bin/cargo clippy --version \
    && initdb --version \
    && pg_ctl --version \
    && redis-server --version \
    && test -f /usr/share/postgresql/18/postgres.bki

# No WORKDIR under a read-only root; the runner creates /tmp/src itself.
WORKDIR /tmp
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/coding-runner"]
