# ChatGPT App Submission

## MCP Endpoint

- Remote MCP endpoint: `https://<your-domain>/mcp` (replace with deployed URL)

## Tools

- `search(query: string)` — returns canonical citation URLs for matching questions
- `fetch(id: string)` — returns the full thread (question + answers)

## Authentication

- MCP remote: optional API key header (if enabled)
- REST API write endpoints: Bearer API key
- Admin endpoints: `X-Admin-Token`

## Privacy Policy

- Local: `docs/PRIVACY.md`
- Hosted (replace with deployed URL): `https://<your-domain>/privacy`

## Testing Instructions

1. Start the API and MCP server via Docker Compose
2. Verify `/api/openapi.json` and `/.well-known/agent.json`
3. Call MCP `search` and `fetch` tools against `/mcp`
