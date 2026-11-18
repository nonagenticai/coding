# coding — runbook

## Deploy

Flux owns `gitops-production/clusters/strato/services/coding/`.

- **Control plane**: `ghcr.io/nonagenticai/coding`, pinned in the Deployment and moved by the
  `flux-system:coding` ImagePolicy. Built and pushed by `ci-public.yml` on `main`.
- **Runner**: `ghcr.io/nonagenticai/coding-runner`, built by `runner-image.yml` when `src/**` or
  `rust-toolchain.toml` changes. It is **not** image-automated: the digest is pinned in the
  worker's `CODING_RUNNER_IMAGE` and must be bumped in the same gitops commit as any new
  `CODING_RUNNER_*` capability flag.

A runner capability is rolled out in one commit: new digest + the flag that enables it. Setting
the flag against an old digest is refused at job create (`sandbox_not_configured`), before a Pod.

## Environment

| variable | process | meaning |
|---|---|---|
| `DATABASE_URL`, `BIND_ADDR` | api, worker | required; no default port |
| `DATABASE_MIGRATE_URL` | api | direct connection for migrations (session advisory lock) |
| `CODING_DEFAULT_TENANT` | api | tenant for clients that send no header |
| `CODING_API_URL` | worker | where the worker reports |
| `CODING_WORKER_CONCURRENCY` | worker | default 4, max 8 (bounded by the kata quota) |
| `CODING_RUNS_NAMESPACE` | worker | where job Pods are created (`coding-runs`) |
| `CODING_RUNNER_IMAGE` | worker | the pinned runner digest |
| `CODING_RUNNER_PROFILES` | worker | e.g. `rust,python` |
| `CODING_RUNNER_MULTI_SOURCE` | worker | `1` enables `input.sources` |
| `CODING_KATA_RUNTIME_CLASS` | worker | `coding-kata` |
| `CODING_RUNS_PULL_SECRET` | worker | image pull secret name, when the registry needs one |
| `CODING_GITHUB_APP_ID`, `CODING_GITHUB_APP_INSTALLATION_ID`, `CODING_GITHUB_APP_PRIVATE_KEY_PATH` | worker | App identity for private clones |
| `CODING_GITHUB_API_BASE` | worker | optional; GitHub Enterprise API base |
| `CODING_JOB_SPEC` | runner | the one-job spec; set by the worker, never by a caller |

## Verify a deployment

```bash
kubectl -n coding get deploy,pods
kubectl -n coding get deploy coding-worker -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}'
kubectl -n coding port-forward deploy/coding 8687:8687   # then: curl -s localhost:8687/health
```

Read the values back; a flag in git is not a flag in the Pod. After a Flux merge, confirm the
Kustomization's `lastAppliedRevision` names a commit your merge is an ancestor of.

## Debug a job

1. `GET /api/jobs/{id}` — state, summary, checks; `GET /api/jobs/{id}/events` — the timeline.
2. `kubectl -n coding-runs get pods` — one Pod per running job; `kubectl -n coding-runs logs -f <pod>`.
3. The Pod's last line is the result. Missing result + exit 0 = `runner_failed` (a bug, not a pass).

### Failure classes

| signal | meaning |
|---|---|
| 400 / 422 `invalid_job_input` | the closed contract refused the body; read the message |
| `sandbox_not_configured` | the job needs a runner capability whose flag is off |
| `unsupported` (exit 2) | the tree lacks the marker its profile needs (`Cargo.toml` / `pyproject.toml`) |
| exit 3 | a throwaway PostgreSQL/Redis died or never started — infrastructure, not the code |
| `runner_failed` | the Pod ended without a result line |
| `timeout` | the job's `timeout_seconds` (default 900) expired; the Pod is reaped |

## Local gate (before a push)

This repository's gate needs PostgreSQL 18 + Redis for the DB-backed tests, so `rbuild --gate`
(archetype defaults, no services) is **not** the gate here. On m1:

```bash
ALLOW_LOCAL_RUST=1 cargo fmt --all -- --check      # format locally; rbuild -- fmt would format m1's copy
rbuild -- clippy --all-targets -- -D warnings
rbuild -- test --locked                             # PG18 + Redis started by the harness
rbuild -- audit
```

Preconditions: >10 GB free on m1 and a reachable Redis. Long runs: `--detach`, `--status`, `--logs`.

## Images and secrets

- Control-plane image: `ci-public.yml`'s image job (build, `--version`, OpenAPI smoke, push).
- Runner image: `runner-image.yml`; it also smoke-tests the toolchains and the throwaway
  PostgreSQL/Redis inside the image.
- GitHub App credentials: SOPS secret `coding-github-app` in ns `coding`
  (`github_app_id`, `github_app_installation_id`, `github_app_private_key`).
