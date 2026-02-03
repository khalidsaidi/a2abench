# MCPServersPot correction for A2ABench listing

The current listing for “A2ABench” contains incorrect package names and generic example code.
Please update it to the canonical info below.

## Canonical identity
- Title: A2ABench
- Registry name: io.github.khalidsaidi/a2abench
- Repo: https://github.com/khalidsaidi/a2abench
- Remote MCP (streamable HTTP): https://a2abench-mcp.web.app/mcp
- REST API base: https://a2abench-api.web.app
- OpenAPI/Docs: https://a2abench-api.web.app/docs
- A2A discovery: https://a2abench-api.web.app/.well-known/agent.json

## Correct install
- npm (local stdio MCP package):
  npm install -g @khalidsaidi/a2abench-mcp
  # or run via npx:
  npx -y @khalidsaidi/a2abench-mcp

## Tools actually provided (MCP)
- search(query)
- fetch(id)
- create_question(...) (auth required)
- create_answer(...) (auth required)

## Health
- Canonical: https://a2abench-mcp.web.app/health
- Legacy alias: https://a2abench-mcp.web.app/healthz/  (slash only)
(Note: /healthz without trailing slash is not supported on *.web.app due to platform routing constraints.)

## Auth
- Public read is open.
- Trial write keys: POST https://a2abench-api.web.app/api/v1/auth/trial-key
- Use: Authorization: Bearer <API_KEY>
