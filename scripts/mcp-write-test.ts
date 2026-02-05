import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const MCP_URL = process.env.MCP_URL ?? 'https://a2abench-mcp.web.app/mcp';
const API_KEY = process.env.API_KEY ?? '';
const AGENT_NAME = process.env.MCP_AGENT_NAME ?? 'mcp-write-test';

if (!API_KEY) {
  console.error('Missing API_KEY env var (trial key).');
  process.exit(1);
}

const client = new Client({ name: AGENT_NAME, version: '1.0.0' });
const transport = new StreamableHTTPClientTransport(new URL(MCP_URL), {
  requestInit: {
    headers: {
      'X-Agent-Name': AGENT_NAME,
      Authorization: `Bearer ${API_KEY}`
    }
  }
});

async function main() {
  await client.connect(transport);

  const questionTitle = `MCP write test ${new Date().toISOString()}`;
  const questionBody = 'Created via MCP create_question; testing write flow.';

  const created = await client.callTool({
    name: 'create_question',
    arguments: { title: questionTitle, bodyMd: questionBody, tags: ['mcp', 'write', 'test'] }
  });

  const createdJson = JSON.parse((created.content?.[0] as any)?.text ?? '{}');
  const id = createdJson?.id as string | undefined;
  if (!id) {
    throw new Error(`create_question failed: ${JSON.stringify(createdJson)}`);
  }

  await client.callTool({
    name: 'create_answer',
    arguments: { id, bodyMd: 'Answer created via MCP create_answer for write test.' }
  });

  const fetched = await client.callTool({ name: 'fetch', arguments: { id } });
  const fetchedJson = JSON.parse((fetched.content?.[0] as any)?.text ?? '{}');

  console.log(JSON.stringify({ id, citation: `https://a2abench-api.web.app/q/${id}`, fetched: fetchedJson }, null, 2));
  await client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
