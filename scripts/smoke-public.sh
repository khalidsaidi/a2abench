#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-https://a2abench-api.web.app}"

auth_headers="$(mktemp)"
auth_body="$(mktemp)"
sitemap_body="$(mktemp)"
sitemap_slash_headers="$(mktemp)"
sitemap_slash_body="$(mktemp)"
seed_body="$(mktemp)"

cleanup() {
  rm -f "$auth_headers" "$auth_body" "$sitemap_body" "$sitemap_slash_headers" "$sitemap_slash_body" "$seed_body"
}
trap cleanup EXIT

# Trial-key GET must be 405 with Allow: POST
auth_code=$(curl -sS -D "$auth_headers" -o "$auth_body" -w "%{http_code}" "${API_BASE_URL}/api/v1/auth/trial-key")
test "$auth_code" = "405"
grep -qi "^allow: *POST" "$auth_headers"

# sitemap.xml must be 200
sitemap_code=$(curl -sS -o "$sitemap_body" -w "%{http_code}" "${API_BASE_URL}/sitemap.xml")
test "$sitemap_code" = "200"

# sitemap.xml/ can be 200 or redirect to /sitemap.xml
sitemap_slash_code=$(curl -sS -D "$sitemap_slash_headers" -o "$sitemap_slash_body" -w "%{http_code}" "${API_BASE_URL}/sitemap.xml/")
if [[ "$sitemap_slash_code" != "200" && "$sitemap_slash_code" != "301" && "$sitemap_slash_code" != "308" ]]; then
  echo "Unexpected sitemap.xml/ status: $sitemap_slash_code" >&2
  exit 1
fi
if [[ "$sitemap_slash_code" == "301" || "$sitemap_slash_code" == "308" ]]; then
  # ensure redirect resolves to a 200 sitemap
  curl -sS -L "${API_BASE_URL}/sitemap.xml/" >/dev/null
fi

# seed_v2_03 must contain explicit placeholders and no double-slash answers
curl -sS "${API_BASE_URL}/api/v1/questions/seed_v2_03" > "$seed_body"
jq -e '.bodyText | contains("<API_KEY>") and contains("<QUESTION_ID>")' "$seed_body" >/dev/null
if jq -e '.bodyText | contains("//answers")' "$seed_body" >/dev/null; then
  echo "seed_v2_03 bodyText contains //answers" >&2
  exit 1
fi
