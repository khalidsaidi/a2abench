#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL=${API_BASE_URL:-https://a2abench-api.web.app}
MCP_URL=${MCP_URL:-https://a2abench-mcp.web.app/mcp}

resp=$(curl -sS -X POST "$API_BASE_URL/api/v1/auth/trial-key" -H "Content-Type: application/json" -d '{}')
API_KEY=$(node -e "const fs=require('fs'); const data=JSON.parse(fs.readFileSync(0,'utf8')); console.log(data.apiKey || '');" <<< "$resp")

if [[ -z "$API_KEY" ]]; then
  echo "Failed to mint trial key" >&2
  echo "$resp" >&2
  exit 1
fi

title="Agent trial smoke $(date -u +%Y%m%d%H%M%S)Z"
question_payload=$(printf '{"title":"%s","bodyMd":"%s","tags":["trial","smoke"]}' "$title" "Smoke test question created by scripts/smoke-trial-write.sh")
question=$(curl -sS -X POST "$API_BASE_URL/api/v1/questions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d "$question_payload")
question_id=$(node -e "const fs=require('fs'); const data=JSON.parse(fs.readFileSync(0,'utf8')); console.log(data.id || '');" <<< "$question")

if [[ -z "$question_id" ]]; then
  echo "Failed to create question" >&2
  echo "$question" >&2
  exit 1
fi

answer_payload='{"bodyMd":"Smoke test answer created by scripts/smoke-trial-write.sh"}'
answer=$(curl -sS -X POST "$API_BASE_URL/api/v1/questions/$question_id/answers" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d "$answer_payload")
answer_id=$(node -e "const fs=require('fs'); const data=JSON.parse(fs.readFileSync(0,'utf8')); console.log(data.id || '');" <<< "$answer")

if [[ -z "$answer_id" ]]; then
  echo "Failed to create answer" >&2
  echo "$answer" >&2
  exit 1
fi

encoded_title=$(node -e "console.log(encodeURIComponent(process.argv[1] || ''))" "$title")
search=$(curl -sS "$API_BASE_URL/api/v1/search?q=$encoded_title")
node -e "const fs=require('fs'); const data=JSON.parse(fs.readFileSync(0,'utf8')); if(!(data.results||[]).some(r=>r.id==='$question_id')){console.error('Search did not include new question'); process.exit(1);}" <<< "$search"

fetch=$(curl -sS "$API_BASE_URL/api/v1/questions/$question_id")
node -e "const fs=require('fs'); const data=JSON.parse(fs.readFileSync(0,'utf8')); if(data.id!=='$question_id'){console.error('Fetch mismatch'); process.exit(1);}" <<< "$fetch"

# MCP search
mcp_search='{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{"query":"'"$title"'"}}}'
code=$(curl -sS -o /tmp/mcp_search.json -w "%{http_code}" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -X POST "$MCP_URL" -d "$mcp_search")
if [[ "$code" != "200" ]]; then
  echo "MCP search failed: $code" >&2
  cat /tmp/mcp_search.json >&2
  exit 1
fi

mcp_fetch='{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fetch","arguments":{"id":"'"$question_id"'"}}}'
code=$(curl -sS -o /tmp/mcp_fetch.json -w "%{http_code}" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -X POST "$MCP_URL" -d "$mcp_fetch")
if [[ "$code" != "200" ]]; then
  echo "MCP fetch failed: $code" >&2
  cat /tmp/mcp_fetch.json >&2
  exit 1
fi

echo "smoke-trial-write: OK"
