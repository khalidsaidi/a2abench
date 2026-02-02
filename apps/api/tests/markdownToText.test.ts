import test from 'node:test';
import assert from 'node:assert/strict';
import { markdownToText } from '../src/markdown.js';

test('markdownToText preserves inline code URLs', () => {
  const input = 'Visit `https://a2abench-api.web.app/docs` for docs.';
  const output = markdownToText(input);
  assert.ok(output.includes('https://a2abench-api.web.app/docs'));
});

test('markdownToText preserves inline commands', () => {
  const input = 'Run: `curl https://example.com --flag` now.';
  const output = markdownToText(input);
  assert.ok(output.includes('curl https://example.com --flag'));
});

test('markdownToText preserves fenced code blocks', () => {
  const input = 'Example:\n```bash\ncurl -sS https://example.com\n```\nDone.';
  const output = markdownToText(input);
  assert.ok(output.includes('curl -sS https://example.com'));
});

test('markdownToText preserves link URLs', () => {
  const input = '[OpenAPI](/api/openapi.json)';
  const output = markdownToText(input);
  assert.ok(output.includes('/api/openapi.json'));
});
