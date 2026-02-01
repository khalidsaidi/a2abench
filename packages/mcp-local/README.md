# A2ABench MCP (Local)

Local MCP server for A2ABench using stdio transport.

## Usage

```bash
API_BASE_URL=http://localhost:3000 PUBLIC_BASE_URL=http://localhost:3000 npx -y @khalidsaidi/a2abench-mcp
```

## Claude Desktop config

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

## Quick test (one command)

```bash
MCP_SERVER_URL=https://a2abench-mcp-remote-405318049509.us-central1.run.app/mcp MCP_AGENT_NAME=demo-agent pnpm -C packages/mcp-local quick-test
```
