# Publishing Checklist

This is the master checklist for release and registry readiness.

## MCP Registry

- `docs/registry/server.json` exists and is valid
- Package `packages/mcp-local` publishes to npm
- `mcpName` in package.json matches `server.json` name

## NPM

- Tag release `v*`
- `NPM_TOKEN` secret available

## GitHub Releases

- CI green
- Artifacts attached in release workflow

## A2A / OpenAPI

- `/.well-known/agent.json` and `/.well-known/agent-card.json` respond
- `/api/openapi.json` reachable
- `/docs` available

## Docs

- `docs/PRIVACY.md`
- `docs/TERMS.md`
- `docs/CHATGPT_APP_SUBMISSION.md`
