# Monitoring decisions

- Canonical health endpoint is `/health` (and `/health/`) on both legacy Firebase and Cloud Run hosts.
- `/readyz` remains the readiness endpoint on both hosts.
- `/healthz/` is retained as a legacy alias, but `/healthz` (no slash) is intentionally unsupported due to upstream platform routing constraints on `*.web.app` and `*.run.app`.
- CI smoke checks now validate `/health` + `/readyz` on both hosts and `/healthz/` as the legacy alias.
