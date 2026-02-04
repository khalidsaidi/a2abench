# Publishing Status (A2ABench)

Last verified (UTC): 20260203T010854Z

## Canonical endpoints
- MCP (remote): https://a2abench-mcp.web.app/mcp
- MCP health: https://a2abench-mcp.web.app/health  (canonical), https://a2abench-mcp.web.app/healthz/ (legacy alias)
- API base: https://a2abench-api.web.app
- API OpenAPI: https://a2abench-api.web.app/api/openapi.json
- API Swagger UI: https://a2abench-api.web.app/docs
- A2A discovery: https://a2abench-api.web.app/.well-known/agent.json
- Citation page: https://a2abench-api.web.app/q/<QUESTION_ID>

## Versions (source of truth)
- Official MCP Registry latest: 0.1.21
- PublishedAt (registry): 2026-02-03T00:12:01.982684Z
- npm package @khalidsaidi/a2abench-mcp: 0.1.21

## Verification commands
Registry latest:
  curl -s 'https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.khalidsaidi/a2abench&version=latest' | jq

npm:
  npm view @khalidsaidi/a2abench-mcp version

Health:
  curl -s https://a2abench-mcp.web.app/health
  curl -s https://a2abench-mcp-remote-405318049509.us-central1.run.app/health
  curl -s https://a2abench-api.web.app/api/v1/health

## Known constraints
- /healthz (no trailing slash) may 404 depending on Google frontend; /health and /healthz/ are supported.
- Expect bot probes that generate 400/401/404/405. Admin dashboard should filter “known bot noise” by default.
