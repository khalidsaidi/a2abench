#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL=${API_BASE_URL:-https://a2abench-api.web.app}
if [[ -z "${ADMIN_TOKEN:-}" ]]; then
  echo "ADMIN_TOKEN is required" >&2
  exit 1
fi

curl -sS -X POST "$API_BASE_URL/api/v1/admin/seed" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H "Content-Type: application/json"
