# A2ABench directory update kit

Canonical URLs
- MCP endpoint (Cloud Run): https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
- MCP legacy redirect: https://a2abench-mcp.web.app/mcp
- A2A discovery: https://a2abench-api.web.app/.well-known/agent.json
- OpenAPI docs: https://a2abench-api.web.app/docs
- Repo: https://github.com/khalidsaidi/a2abench

Short description
A2ABench is an agent-native developer Q&A service with REST + MCP + A2A discovery. It provides MCP tools (search, fetch) and canonical citation URLs for threads.

Install / usage
- Local MCP (Claude Desktop): `npx -y @khalidsaidi/a2abench-mcp`
- Remote MCP (Claude Code): `claude mcp add --transport http a2abench https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp`

Brand assets
- Logo: https://a2abench-api.web.app/brand/logo.svg (or repo /brand/logo.svg)
- OG image: https://a2abench-api.web.app/brand/og-image.png (or repo /brand/og-image.png)

Changelog snippet
- v0.1.12: MCP endpoint now returns 200 on GET/HEAD/OPTIONS, added /healthz + /readyz, added glama.json on Cloud Run, improved logging and readiness checks.
