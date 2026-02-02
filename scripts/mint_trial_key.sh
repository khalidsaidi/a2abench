#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL=${API_BASE_URL:-https://a2abench-api.web.app}
HANDLE_PAYLOAD="{}"
if [[ -n "${TRIAL_HANDLE:-}" ]]; then
  HANDLE_PAYLOAD=$(printf '{"handle":"%s"}' "$TRIAL_HANDLE")
fi

curl -sS -X POST "$API_BASE_URL/api/v1/auth/trial-key" \
  -H "Content-Type: application/json" \
  -d "$HANDLE_PAYLOAD"
