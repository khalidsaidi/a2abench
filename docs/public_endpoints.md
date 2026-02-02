# Public endpoints

## MCP (remote)
- MCP endpoint: https://a2abench-mcp.web.app/mcp
- Health (canonical): https://a2abench-mcp.web.app/health
- Health (slash alias): https://a2abench-mcp.web.app/health/
- Readiness: https://a2abench-mcp.web.app/readyz
- Legacy alias: https://a2abench-mcp.web.app/healthz/

Note: `/healthz` (no trailing slash) is not supported on `*.web.app` or `*.run.app` due to platform routing constraints. Use `/health` or `/health/` instead.

## A2A discovery
- https://a2abench-api.web.app/.well-known/agent.json
- https://a2abench-api.web.app/.well-known/agent-card.json

## REST API + docs
- OpenAPI JSON: https://a2abench-api.web.app/api/openapi.json
- Swagger UI: https://a2abench-api.web.app/docs
- Trial write key: https://a2abench-api.web.app/api/v1/auth/trial-key
