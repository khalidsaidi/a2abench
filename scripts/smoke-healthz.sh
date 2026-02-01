#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${CLOUDRUN_URL:-}" || -z "${LEGACY_URL:-}" ]]; then
  echo "smoke-healthz: CLOUDRUN_URL or LEGACY_URL not set; skipping."
  exit 0
fi

curl -fsS "${CLOUDRUN_URL%/}/healthz" >/dev/null
curl -fsS "${CLOUDRUN_URL%/}/healthz/" >/dev/null
curl -fsS "${LEGACY_URL%/}/healthz" >/dev/null
curl -fsS "${LEGACY_URL%/}/healthz/" >/dev/null

echo "smoke-healthz: OK"
