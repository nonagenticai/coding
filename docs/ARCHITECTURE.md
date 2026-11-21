# coding — architecture

One Rust crate, three faces:

| binary | run as | what it is |
|---|---|---|
| `coding` | `coding serve` | the HTTP API (2 replicas) |
| `coding` | `coding worker` | the queue consumer that runs one kata Pod per job (1 replica) |
| `coding` | `coding check` / `coding hook install` | the developer client and its git hooks |
| `coding-runner` | Pod ENTRYPOINT | the in-Pod executor; never talks to the cluster |

## Job lifecycle

1. `POST /api/jobs` with `X-Tenant-Id` stores a row (tenant-scoped) and returns **202** with the
   job. Input is a closed contract (`deny_unknown_fields`): `kind`, `input`, optional
   `timeout_seconds` (1–86400), optional `idempotency_key`.
2. The worker claims the row, prepares every source (an uploaded archive gets a capability URL,
   token and sha256; a github.com git source gets one minted App installation token, reused from a
   per-process cache), and creates **one Pod per job** in `coding-runs`.
3. `coding-runner` materialises the source(s), starts a throwaway PostgreSQL 18 and Redis lazily
   (only immediately before a step that needs one), runs the compiled-in plan, and writes one
   NDJSON line: `{"coding_result": …}`. It carries no cluster credential and no docker.
4. The worker reads the result line (bounded at 192 KiB), records the verdict, deletes the Pod.
   A Pod that succeeded without a result line is `runner_failed` — never an invented pass.

State is always readable at `GET /api/jobs/{id}`, `GET /api/jobs/status`, and the event log at
`GET /api/jobs/{id}/events`; `POST /api/jobs/{id}/cancel` deletes the Pod and marks the job.

## Contracts

- `JobKind`: `test`, `lint`, `build`, `check`, `github_op` (worker-side delivery, no Pod), and
  `parity_openapi` (in-Pod document comparison); the remaining kinds are declared placeholders.
- `JobInput`: exactly one of `source` (single) or `sources` (1–8 named sources, each with its own
  `profile`); both together or neither is a 422 `invalid_job_input`.
- `Source`: `inline` (paths + content, 48 KiB summed), `git` (url, ref, depth), `upload`
  (`POST /api/sources` first — server fills url/token/sha256; a client-supplied one is refused).
- `Profile`: `rust` (default, omitted on the wire) | `python`.

## Runner plans

Plans are compiled-in tables (`src/runner/mod.rs`), never a shell string:

- rust: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build`,
  `cargo test`.
- python: `uv lock --check`/`uv sync`, `ruff check`, `ruff format --check`, `ty` (scoped to
  `src`, like the estate's CI), `pytest`.

Single-source jobs materialise at `/tmp/src`; multi-source jobs at `/tmp/sources/<name>` with a
per-source `.target`. One Pod serves every source, so a Rust target and its pinned Python sources
share one throwaway PostgreSQL/Redis and one result.

A source that does not carry the marker its profile needs (`Cargo.toml` / `pyproject.toml`) is
`unsupported` (exit 2) for the whole job before any step runs — never a misleading failing check.
A service that will not start or dies mid-step is exit 3, not a verdict about the code.

`parity_openapi` has no command plan: after the trees materialise, `src/parity.rs` reads each
source's root `openapi.json` and compares them in-process, bounded by document size, `$ref` depth
and a capped report. It runs inside the Pod on purpose — the documents are untrusted input, and
the Pod is where untrusted input is parsed. Its marker is the document itself, not a build file.

## Sandbox

`src/sandbox.rs` builds the Pod: read-only root, one 8 GiB `emptyDir` at `/tmp`, the pinned
`CODING_RUNNER_IMAGE`, and only `CODING_JOB_SPEC` in the environment. Resources follow the kind:
`build`/`test`/`check` request 1 CPU / 2 GiB and limit 4 CPU / 10 GiB on the large kata profile
(its VM must *boot* with the memory — above ~6 GiB it is never hot-plugged); `lint` shares that
profile and limits 4 CPU / 7 GiB; everything else 500m / 1 CPU / 2 GiB. Profile and multi-source
capability are refused
**before a Pod exists** when the pinned runner image predates them (`CODING_RUNNER_PROFILES`,
`CODING_RUNNER_MULTI_SOURCE`).

A Pod waiting on an image or config reason, or one the scheduler calls `Unschedulable`, is
`Stuck`: retryable infrastructure reported before the job timeout, never a verdict about the
code. The worker's clock starts at Pod creation and adds the `Pending` interval back (capped at
120 s), because the runner's own clock starts when its container does.

## Store and tenancy

PostgreSQL (`sqlx`, runtime queries only, migrations embedded from `migrations/`). The tenant is
the trusted `X-Tenant-Id` header (`src/tenant.rs`), with `CODING_DEFAULT_TENANT` as the fallback
used by the client when none is configured. Every store query is tenant-scoped.

## HTTP surface

`/health`, `/api/health`, `/ready`, `/api/jobs` (POST), `/api/jobs/status` (POST), `/api/jobs/{id}`
(GET), `/api/jobs/{id}/cancel` (POST), `/api/jobs/{id}/events` (GET), `/api/sources` (POST),
`/api/sources/{id}/blob` (GET), `/api/openapi.json`. Every route is registered once, through
`routes!` in `src/lib.rs`, so the served router and the OpenAPI document cannot drift.

## Module map

| module | role |
|---|---|
| `domain.rs` | the closed contract and its validation |
| `routes/` | the handlers above |
| `store.rs` | tenant-scoped PostgreSQL access; the source reaper |
| `worker.rs` | claim → prepare → create → reap → record; concurrency, lease, reapers |
| `sandbox.rs` | the job Pod: manifest, create/reattach, state, logs, delete |
| `runner/` | the in-Pod executor: materialise, plans, throwaway PG/Redis, result |
| `parity.rs` | the in-Pod `OpenAPI` diff for `parity_openapi` (slice and union forms) |
| `deliver.rs` | the worker-side `github_op` delivery (branch + PR, no Pod) |
| `source_archive.rs` | the gzip-tar packer and the hostile-input validator |
| `client/` | `coding check` and the generated git hooks |
| `github_app.rs` | App JWT → installation token for github.com clones |
| `tenant.rs` | the trusted-header extractor |
| `config.rs` | environment configuration |
