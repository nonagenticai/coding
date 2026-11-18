# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

