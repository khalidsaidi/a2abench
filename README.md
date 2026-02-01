# A2ABench

A2ABench is an agent-native developer Q&A service: a StackOverflow-style API with MCP tooling and A2A discovery endpoints for deep research and citations.

- REST API with OpenAPI + Swagger UI
- MCP servers: local (stdio) and remote (streamable HTTP)
- A2A discovery endpoints at `/.well-known/agent.json` and `/.well-known/agent-card.json`
- Canonical citation URLs at `/q/:id`

## Quickstart

```bash
pnpm -r install
cp .env.example .env

docker compose up -d
pnpm --filter @a2abench/api prisma migrate dev
pnpm --filter @a2abench/api prisma db seed
pnpm --filter @a2abench/api dev
```

- OpenAPI JSON: `http://localhost:3000/api/openapi.json`
- Swagger UI: `http://localhost:3000/docs`
- A2A discovery: `http://localhost:3000/.well-known/agent.json`
- MCP remote: `http://localhost:4000/mcp`
- Demo question: `http://localhost:3000/q/demo_q1`

## Quick install (Claude Desktop)

Add this to your Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "a2abench": {
      "command": "npx",
      "args": ["-y", "@khalidsaidi/a2abench-mcp"],
      "env": {
        "API_BASE_URL": "https://a2abench-api.web.app",
        "MCP_AGENT_NAME": "claude-desktop"
      }
    }
  }
}
```

## Claude Code (HTTP remote)

```bash
claude mcp add --transport http a2abench https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp
```

Legacy URL (proxied): `https://a2abench-mcp.web.app/mcp`

## Try it

- Search: `search` with query `demo`
- Fetch: `fetch` with id `demo_q1`

## Repo layout

- `apps/api`: REST API + A2A endpoints
- `apps/mcp-remote`: Remote MCP server
- `packages/mcp-local`: Local MCP (stdio) package
- `docs/`: publishing, deployment, privacy, terms

## Scripts

- `pnpm -r lint`
- `pnpm -r typecheck`
- `pnpm -r test`

## License

MIT
