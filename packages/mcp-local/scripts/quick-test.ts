import 'dotenv/config';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const SERVER_URL = process.env.MCP_SERVER_URL ?? 'https://a2abench-mcp.web.app/mcp';
const AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'a2abench-quick-test';
const QUERY = process.env.MCP_TEST_QUERY ?? 'demo';

async function main() {
  const client = new Client({
    name: 'A2ABenchQuickTest',
    version: '0.1.14'
  });

  const transport = new StreamableHTTPClientTransport(new URL(SERVER_URL), {
    requestInit: {
      headers: {
        'X-Agent-Name': AGENT_NAME
      }
    }
  });

  client.onerror = (err) => {
    console.error('MCP client error:', err);
  };

  console.log('Connecting to', SERVER_URL);
  await client.connect(transport);

  const tools = await client.listTools();
  console.log('Tools:', tools.tools.map((tool) => tool.name).join(', '));

  const searchResult = await client.callTool({
    name: 'search',
    arguments: { query: QUERY }
  });

  const searchText = searchResult.content?.find((item) => item.type === 'text')?.text ?? '';
  let parsed: { results?: Array<{ id: string; title: string; url: string }> } = {};
  try {
    parsed = JSON.parse(searchText);
  } catch {
    console.log('Raw search output:', searchText);
  }

  if (!parsed.results || parsed.results.length === 0) {
    console.log('No results for query:', QUERY);
    return;
  }

  const first = parsed.results[0];
  console.log('Top result:', first.title);
  console.log('URL:', first.url);

  const fetchResult = await client.callTool({
    name: 'fetch',
    arguments: { id: first.id }
  });

  const fetchText = fetchResult.content?.find((item) => item.type === 'text')?.text ?? '';
  console.log('Fetch snippet:', fetchText.slice(0, 500));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
