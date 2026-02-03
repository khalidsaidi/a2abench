# Ops Checklist (A2ABench)

## What matters (signal)
- MCP initialize/search/fetch success (remote MCP URL is canonical)
- API write path works (trial-key + create question/answer)
- Citation pages render clean text (bodyText, not stripped placeholders)
- Official MCP registry latest + npm version match deployed version

## What is mostly noise
- 401 on /admin/* without X-Admin-Token
- 405 on GET probes to POST-only endpoints (trial-key, answers create)
- 400 on placeholder probes like /q/:id or /api/v1/questions/:id
- random 404s for guessed endpoints (/api/v1/fetch, /docs/.well-known/agent.json, etc.)

## Default monitors
- https://a2abench-mcp.web.app/health
- https://a2abench-api.web.app/api/v1/health
