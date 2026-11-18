# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Rust toolchain 1.99.0 → 1.100.0 in `rust-toolchain.toml` and both Dockerfiles' `ARG
  RUST_TOOLCHAIN`, matching the estate's other Rust services. No behaviour change.

### Added

- `docs/ARCHITECTURE.md` and `docs/RUNBOOK.md`: the job lifecycle, the closed contract, the
  sandbox, failure classes, deployment and the local gate in one place.

- Multi-source jobs: `input.sources` carries 1–8 named sources, each with its own toolchain
  profile. Each is materialised under `/tmp/sources/<name>` and its plan runs against it in the
  same Pod, against the same throwaway PostgreSQL/Redis — one run that checks a Rust target and
  its pinned Python sources together. Checks name their source and the result lists `sources[]`;
  a single-source job keeps its exact old wire shape. The worker refuses a `sources` job before a
  Pod unless `CODING_RUNNER_MULTI_SOURCE` enables it, and checks every named source's profile.

### Fixed

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

