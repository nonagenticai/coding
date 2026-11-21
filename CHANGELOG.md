# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The check path: snapshot collection moves each git blob out of the collector's map instead of
  cloning it (peak RSS on `index`/`HEAD` snapshots 137 → 79 MiB on the 942-file corpus); the client
  polls at 250 ms for the first 10 s and the worker polls a running Pod at 500 ms, so a finished
  check is noticed in ~0.4 s rather than ~2 s.
- The pre-push hook packs and uploads the snapshot once and runs `build` then `test` against that
  one source; it used to pack and upload the identical archive twice.
- Rust toolchain 1.99.0 → 1.100.0 in `rust-toolchain.toml` and both Dockerfiles' `ARG
  RUST_TOOLCHAIN`, matching the estate's other Rust services. No behaviour change.

### Added

- `coding check --gates <kinds>`: run several job kinds against one uploaded snapshot, in order.
  A failed check stops the later gates and prints them as not run; the pre-push hook uses
  `--gates build,test`.

- `docs/ARCHITECTURE.md` and `docs/RUNBOOK.md`: the job lifecycle, the closed contract, the
  sandbox, failure classes, deployment and the local gate in one place.

- Multi-source jobs: `input.sources` carries 1–8 named sources, each with its own toolchain
  profile. Each is materialised under `/tmp/sources/<name>` and its plan runs against it in the
  same Pod, against the same throwaway PostgreSQL/Redis — one run that checks a Rust target and
  its pinned Python sources together. Checks name their source and the result lists `sources[]`;
  a single-source job keeps its exact old wire shape. The worker refuses a `sources` job before a
  Pod unless `CODING_RUNNER_MULTI_SOURCE` enables it, and checks every named source's profile.

### Fixed

- The GitHub App's installation token could be attached to a URL whose real host was not
  github.com (`https://github.com:443@evil.example/…` passed the guard); it now parses the URL and
  requires https, the exact host, no userinfo and the default port.
- `github_op` delivery: request timeouts (a stalled connection could hold a worker slot for the
  whole job timeout), blob uploads in parallel bounded chunks instead of one serial round trip per
  file, bounded retries honouring `Retry-After`, and a lease retry now reuses the job's branch and
  open PR instead of creating a `-2` branch and a duplicate PR.
- Runner startup is bounded: a Pod stuck in image pull or scheduling fails fast as retryable
  infrastructure, a slow pull no longer consumes the job's own budget, and Postgres/Redis startup
  and sccache stats are bounded by the job deadline and SIGTERM.
- The worker abandons a job when its lease is lost instead of continuing to duplicate work; a
  cancel landing during the lease sweep is cancelled instead of stranded `queued`; a failed log
  read after a successful Pod is retryable infrastructure, not `runner_failed`.
- `parity_openapi` bounds `$ref` expansion (a depth-31 diamond could exhaust a 2 GiB Pod), merges
  3.1 `$ref` siblings instead of dropping them, and caps differences and extras while building the
  report.
- The client retries a write only when the connection was never established and sends an
  idempotency key with a job create, so a lost response cannot duplicate an upload or a Pod.
- Archive validation and hashing run off the async reactor, and an upload's body read is bounded at
  180 s so a client cannot hold both upload slots forever.
- The Python profile's `ty` step checks `src` (the archetype's scope), not the whole project. A
  whole-project check reports diagnostics in `tests/` that CI never sees and blocked a push the
  estate's CI would accept.
- A runner Pod that exited 0 without writing its result line used to be reported as a successful
  job (`{"exit":"succeeded"}`) with no verdict at all — a gate that passes when its verdict is
  missing. It is now a non-retryable `runner_failed`. The runner also bounds its own result line
  (192 KiB, output tails dropped with a marker) so a job with several dozen checks cannot be cut
  by the worker's 256 KiB log read.
- The source reaper now also sees upload ids referenced from a job's `sources` list, so a queued
  multi-source job cannot have its archive reaped under it.
- A multi-source job's step failures (timeout, cancel, a service that will not start or dies, a
  step that cannot start) name the source they happened in, and every upload id in `sources[]` is
  ownership-checked at create time (404), not left for the worker to discover after a 202.

### Added (earlier unreleased work)

- Language profiles: a job names the toolchain (`rust` or `python`) in its input, and the runner
  executes that profile's compiled-in plan in the same Pod. `rust` is the default and is omitted
  from the wire, so existing callers are unchanged.
- The Python profile mirrors the estate's Python CI: `uv lock --check`, `uv sync --all-extras
  --locked`, `ruff check`, `ruff format --check`, `ty check`, `pytest`. The test step also
  exports `DATABASE_URL` (psycopg spelling) and `REDIS_HOST`/`REDIS_PORT` beside the existing
  `TEST_DATABASE_URL`/`TEST_REDIS_URL`.
- `coding check --profile auto|rust|python`: `auto` detects the toolchain from the packed tree and
  refuses an ambiguous or unsupported layout with exit 2 before uploading anything.
- The runner image carries uv 0.13.7 and one read-only CPython 3.14; `CODING_RUNNER_PROFILES`
  (default `rust`) lists what the worker's image can execute, and a job naming anything else is
  refused before a Pod is created.

