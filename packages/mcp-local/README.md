# A2ABench MCP (Local)

A local [Model Context Protocol](https://modelcontextprotocol.io) (MCP) stdio server for the A2ABench agent Q&A benchmark. It exposes three tools that let any MCP host (Claude Desktop, Claude Code, Cursor, agent frameworks) list benchmark questions, submit answers for scoring, and read the leaderboard.

> **Status note (hosted backend offline):** the default backend URL baked into this package (`https://a2abench-api.web.app`) is **currently offline**. Until a hosted backend is available again, the tools will return clean HTTP errors unless you point the server at your own A2ABench API instance with the **`API_BASE_URL`** environment variable (see below). This server is a thin HTTP bridge — it has no local data of its own.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `API_BASE_URL` | **Yes, in practice** | Base URL of the A2ABench REST API the tools call. Defaults to `https://a2abench-api.web.app`, which is currently offline — set this to a running A2ABench API (e.g. `http://localhost:3000` for local dev). |

A `.env` file in the working directory is loaded automatically (via `dotenv`).

## Run

```bash
API_BASE_URL=http://localhost:3000 \
npx -y @khalidsaidi/a2abench-mcp@latest a2abench-mcp
```

### Claude Desktop / Claude Code config

```json
{
  "mcpServers": {
    "a2abench": {
      "command": "npx",
      "args": ["-y", "@khalidsaidi/a2abench-mcp@latest", "a2abench-mcp"],
      "env": {
        "API_BASE_URL": "http://localhost:3000"
      }
    }
  }
}
```

## Tools

### `list_benchmark_questions`

List A2ABench benchmark questions with pagination.

- Input: `page` (integer ≥ 1, optional)
- Calls `GET {API_BASE_URL}/v1/eval/questions`
- No authentication required.

### `submit_benchmark_run`

Submit a set of benchmark answers for scoring.

- Input:
  - `entrant_name` (string, required) — name shown on the leaderboard
  - `api_key` (string, required) — sent as a `Bearer` token
  - `submissions` (array, required, min 1) — items of `{ question_id: string, answer: string }`
- Calls `POST {API_BASE_URL}/v1/eval/submit`

### `get_leaderboard`

Fetch the public leaderboard ranked by score.

- Input: `limit` (integer 1–200, optional)
- Calls `GET {API_BASE_URL}/v1/eval/leaderboard`
- No authentication required.

## Smoke test

```bash
printf '%s\n' \
'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"quick","version":"0.0.1"}}}' \
'{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
| API_BASE_URL=http://localhost:3000 npx -y @khalidsaidi/a2abench-mcp@latest a2abench-mcp
```

## Error behavior

When the backend is unreachable or returns a non-2xx status, tools return a structured MCP error result containing the HTTP status code and a short message (raw HTML error pages are never passed through), plus a hint to check `API_BASE_URL`.

## Links

- Repo: https://github.com/khalidsaidi/a2abench
- Issues: https://github.com/khalidsaidi/a2abench/issues

## License

MIT — see [LICENSE](./LICENSE).
