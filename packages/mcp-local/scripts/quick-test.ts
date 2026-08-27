import 'dotenv/config';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

// Smoke-tests the local stdio MCP server in this package:
// initialize, list tools, and call get_leaderboard once.
// Set API_BASE_URL to a running A2ABench API to see real data; without it the
// tool call is still expected to return a clean, structured error result.
async function main() {
  const client = new Client({ name: 'A2ABenchQuickTest', version: '1.0.0' });

  const transport = new StdioClientTransport({
    command: 'npx',
    args: ['tsx', 'src/cli.ts'],
    env: { ...process.env } as Record<string, string>
  });

  await client.connect(transport);

  const tools = await client.listTools();
  const names = tools.tools.map((tool) => tool.name);
  console.log('Tools:', names.join(', '));

  const expected = ['list_benchmark_questions', 'submit_benchmark_run', 'get_leaderboard'];
  for (const name of expected) {
    if (!names.includes(name)) {
      console.error(`Missing expected tool: ${name}`);
      process.exit(1);
    }
  }

  const result = await client.callTool({ name: 'get_leaderboard', arguments: {} });
  const text = (result.content as Array<{ type: string; text?: string }> | undefined)
    ?.find((item) => item.type === 'text')?.text ?? '';
  console.log(result.isError ? 'Tool returned error (expected if no backend):' : 'Leaderboard:', text.slice(0, 500));

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
