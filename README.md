# coding

A single Rust service that queues code checks over HTTP, runs each one in an isolated Kubernetes
Pod (one Pod per job), and returns structured results. It carries no Docker and no container
runtime of its own.

Two consumers:

- **Automation** — a code toolset over HTTP (`POST /api/jobs`, `POST /api/jobs/status`).
- **Developers** — `coding check`, a pre-push/pre-commit gate that runs the repository's own
  checks in the same job image CI uses, from a machine with no toolchain and no Docker: cargo
  fmt/clippy/test for Rust, uv/ruff/ty/pytest for Python.

## Quality gate: `coding check`

```bash
# CODING_URL points at the service (or a tunnel to it)
CODING_URL=http://127.0.0.1:8687 coding check          # every check, one Pod
CODING_URL=http://127.0.0.1:8687 coding check --kind lint   # static checks only
CODING_URL=http://127.0.0.1:8687 coding check --profile python  # force a toolchain
CODING_URL=http://127.0.0.1:8687 coding check --dry-run     # show what would be sent
coding hook install                                     # pre-push gate
coding hook install --hook pre-commit --kind lint       # committed fast gate, if you want one
git config coding.url http://127.0.0.1:8687             # what the hook reads
```

What it sends: a gzip tar of `git ls-files` plus untracked-not-ignored files, read from the
working tree — every tracked file, `target/` and `.env` never, and a fixed set of secret-shaped
names (`*.pem`, `*.key`, `id_rsa*`, …) skipped with a warning. Caps: 32 MiB compressed, 128 MiB
uncompressed, 100 000 files, 32 MiB per file. The archive is stored once (tenant-scoped) and the
Pod fetches it back with a capability token minted per attempt, verifying its sha256.

Exit codes are the contract a hook branches on:

| code | meaning |
|---|---|
| 0 | every check passed |
| 1 | a check failed — the code is the problem |
| 2 | usage, configuration, or a kind this runner cannot execute |
| 3 | infrastructure: unreachable app, 5xx, timeout, cancelled, a dead runner |
| 130 | interrupted |

A **hook fails open**: only exit 1 blocks the push; an unreachable app lets it through with a
warning, because a gate that blocks work when the service is down gets deleted. The hook reads
`coding.url` / `coding.tenant` from git config and refuses to overwrite or remove a hook it did
not write (use `--force`).

Measured latency (one cold Pod): a tiny crate is 12–43 s end to end; a medium Rust workspace is
a few minutes (fmt + clippy + test). That is why pre-push is the default and `--kind lint` exists
for anyone who wants a commit-time gate.

### Profiles

The tree decides the toolchain; there is no language-specific job kind.

| profile | detected by | the `check` plan |
|---|---|---|
| `rust` | `Cargo.toml` + `Cargo.lock` | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --locked` |
| `python` | `pyproject.toml` + `uv.lock` | `uv lock --check`, `uv sync --all-extras --locked`, `ruff check`, `ruff format --check`, `ty check src`, `pytest -v --tb=short` |

`--profile auto` (the default) detects the profile from the packed tree; a repository carrying
both toolchains must choose with `--profile rust|python`, and a tree without either pair is a
usage error before anything is uploaded. `rust` is the server-side default and the field is
omitted from the request for it, so a Rust job sends exactly the body it sent before profiles
existed. The runner checks the same marker again against the materialised tree and answers
`unsupported` (exit 2), never a failing check.

The Python plan mirrors the estate's Python CI command for command — including `ty check src`, the
archetype's scope: checking the whole project reports diagnostics in `tests/` that CI never sees,
and a gate stricter than CI blocks a push CI would accept. Two limits today: the image carries
CPython 3.14 only (a repository pinning another version fails with uv's own message), and
`--kind build` is `unsupported` for Python because the Python archetype has no build gate to
mirror.

### Multi-source jobs (the unified run)

A job may carry several **named** sources instead of one:

```json
{"kind": "check", "input": {"sources": [
  {"name": "target",   "profile": "rust",   "source": {"kind": "git", "url": "https://github.com/o/target-rs", "ref": "<sha>"}},
  {"name": "source-1", "profile": "python", "source": {"kind": "git", "url": "https://github.com/o/source-py", "ref": "<sha>"}}
]}}
```

Each source is materialised at `/tmp/sources/<name>` (a single-source job keeps `/tmp/src`) and
its profile's plan runs against it, in the declared order, in the **same Pod and against the same
throwaway PostgreSQL/Redis**. That is how the rewrite gate's "pytest against the sources and the
target's tests on the same diff" is one run instead of two jobs that can disagree.

- 1–8 sources; names match `^[a-z][a-z0-9-]{0,31}$` and are unique.
- `input.source` and `input.sources` are mutually exclusive, and with `sources` the job-level
  `input.profile` must be omitted: each named source carries its own profile, and the runner
  re-checks each tree's marker before any step runs (`unsupported`, exit 2).
- Every check names its source (`checks[].source`), and the result lists `sources[]`
  (`name`, `kind`, `files`, `commit`); a single-source job keeps exactly its old result shape.
- The inline-text cap (48 KiB) is summed across sources, and the serialised input is bounded so
  the Pod's 64 KiB spec env can never overflow. A multi-source job can take longer than one plan:
  raise `timeout_seconds` when it does.

The worker refuses a `sources` job **before a Pod exists** when the pinned runner image predates
support (`CODING_RUNNER_MULTI_SOURCE` unset or not affirmative) and refuses any source whose
profile is not listed in `CODING_RUNNER_PROFILES` — so a mixed rollout fails with a legible
message rather than an `invalid job spec` from the Pod.

The worker runs up to `CODING_WORKER_CONCURRENCY` jobs at once (default 4). Every claim carries
its own fencing token, so a slot that lost its lease can never heartbeat, complete, fail or
cancel the job a newer claim owns, and a slot that panics cannot leave its lease renewing;
shutdown drains every in-flight job. A job's Pod lives for that job's own deadline (the request
timeout plus five minutes of pull/exit slack), not a fixed 15 minutes, so a long check is not
killed mid-run. `CODING_RUNNER_PROFILES` (default `rust`) says which profiles the configured
runner image carries; a job naming any other profile is refused before a Pod is created.

A `test` or `check` job starts a **throwaway PostgreSQL 18 inside its own Pod** and exports
`TEST_DATABASE_URL`, the same DSN CI hands to the same harnesses. It starts immediately before
the test step (never during `fmt`/`clippy`); a `TEST_DATABASE_URL` already in the environment
wins. Beside it the job starts a **throwaway Redis** and exports `TEST_REDIS_URL` and `REDIS_URL`
(an exported one wins), plus a second private server exported as `TEST_REDIS_PRIVATE_URL` for a
test whose `FLUSHALL` must not touch its siblings. A server that is still not listening is exit
3, never a failed check: a dead database is not a verdict about your code. For the `python`
profile the test step also exports `DATABASE_URL` in the SQLAlchemy spelling
(`postgresql+psycopg://…`, the same server) and `REDIS_HOST`/`REDIS_PORT`, because the estate's
Python services read those names.

## Stack

Rust 1.100.0 (pinned in `rust-toolchain.toml` and both Dockerfiles' `ARG RUST_TOOLCHAIN`), edition
2024, axum 0.8, tokio, sqlx 0.9 (runtime queries only, PostgreSQL, embedded migrations), utoipa
+ utoipa-axum (OpenAPI), tracing (JSON logs), thiserror, serde.

## Layout

| path | what |
|---|---|
| `src/lib.rs` | router + OpenAPI; register every handler with `routes!` in `api_router()` |
| `src/routes/jobs.rs` | the job contract: create, status, read, cancel, events |
| `src/routes/sources.rs` | upload a working tree (`POST /api/sources`) and serve it to a Pod by token |
| `src/domain.rs` | the closed contract: `JobKind`, `JobInput`, `Source` (inline, git, upload) |
| `src/store.rs` | PostgreSQL access, tenant-scoped; the source reaper lives here |
| `src/worker.rs` | claim → execute → report, lease heartbeat, the reapers |
| `src/sandbox.rs` | one job Pod per job: manifest, create/reattach, state, logs, delete, reap |
| `src/runner/` | the in-Pod executor (`coding-runner`): source materialisation and closed plans |
| `src/source_archive.rs` | the gzip-tar packer and the hostile-input validator |
| `src/client/` | `coding check` (pack → upload → poll → verdict) and `coding hook install` |
| `src/tenant.rs` | `TenantId` extractor for the trusted `X-Tenant-Id` header |
| `src/config.rs` | env config: `DATABASE_URL`, `BIND_ADDR` (required, no default port), optional `DATABASE_MIGRATE_URL` |
| `migrations/` | `0001_init`, `0002_core` (jobs, events, workspaces), `0003_sources` |
| `src/db.rs` | pool + `MIGRATOR` (`migrations/*.sql`, run at startup) |
| `src/error.rs` | `AppError` renders `{"detail": ...}` like FastAPI |
| `parity.toml`, `parity/` | parity contract with the Python source(s) |
| `images/runner/` | the job image: the Rust and Python toolchains + `coding-runner` |
| `Dockerfile` | the control-plane image |

## Run and test

```bash
<coding> serve                 # the HTTP API (also the default with no subcommand)
<coding> worker                # the job worker loop
<coding> check --url …         # the developer gate; needs no env, no DB
<coding> --print-openapi       # OpenAPI JSON on stdout; needs no env, no DB
DATABASE_URL=postgres://... BIND_ADDR=0.0.0.0:<port> <coding>
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo test --locked             # DB test: TEST_DATABASE_URL / DATABASE_URL, else an
                                # ephemeral Docker postgres:18, else it fails (never skips)
```

Migrations run at startup. Through a transaction-pooling PgBouncer, set `DATABASE_MIGRATE_URL`
to a direct PostgreSQL URL: sqlx's migrator holds a session-level advisory lock.

## CI

`.github/workflows/ci-public.yml` runs on GitHub-hosted runners: betterleaks secret scan, the
private-reference guard, `cargo fmt`, `cargo clippy -D warnings`, the full test suite against
PostgreSQL 18 and Redis service containers, doctests and `cargo audit`. The image job builds the
control-plane image and smokes `--version` / `--print-openapi`; it pushes to ghcr.io only when
the repository variable `PUBLISH_IMAGE=true`.

The job image (`images/runner/Dockerfile`) is built separately, with the same public bases.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Licensing

There is no licence gate in this service: it needs no licence key, token, or network call to
build, test or run. Every feature is available by default.
