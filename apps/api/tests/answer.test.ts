import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildEvidenceSnippet,
  buildCitations,
  parseAnswerJson,
  runAnswer,
  type Thread
} from '../src/answer.js';

test('buildEvidenceSnippet preserves code and answers', () => {
  const thread: Thread = {
    id: 't1',
    title: 'Sample title',
    bodyMd: 'Use `curl https://example.com` to test.',
    answers: [
      { id: 'a1', bodyMd: '```bash\necho hello\n```' }
    ]
  };
  const snippet = buildEvidenceSnippet(thread, 2000);
  assert.ok(snippet.includes('Sample title'));
  assert.ok(snippet.includes('curl https://example.com'));
  assert.ok(snippet.includes('echo hello'));
});

test('buildCitations maps indices and warns on invalid', () => {
  const retrieved = [
    { id: '1', title: 'One', url: 'https://example.com/q/1', snippet: 'First answer.' }
  ];
  const { citations, warnings } = buildCitations([1, 2], [], retrieved);
  assert.equal(citations.length, 1);
  assert.ok(warnings.some((msg) => msg.includes('out of range')));
});

test('parseAnswerJson handles fenced JSON', () => {
  const parsed = parseAnswerJson('```json {"answer_markdown":"ok","used_indices":[1],"quotes":[{"index":1,"quote":"hi"}],"warnings":[]} ```');
  assert.equal(parsed?.answer_markdown, 'ok');
  assert.deepEqual(parsed?.used_indices, [1]);
});

test('runAnswer returns evidence-only when llm missing', async () => {
  const response = await runAnswer(
    { query: 'test' },
    {
      baseUrl: 'https://a2a.example',
      search: async () => [{ id: 't1', title: 'T1' }],
      fetch: async () => ({ id: 't1', title: 'T1', bodyMd: 'Hello' }),
      llm: null
    }
  );
  assert.ok(response.answer_markdown.includes('LLM not configured'));
  assert.equal(response.citations.length, 0);
  assert.equal(response.retrieved.length, 1);
});

test('runAnswer returns citations when llm succeeds', async () => {
  const thread: Thread = {
    id: 't1',
    title: 'T1',
    bodyMd: 'Question body',
    answers: [{ id: 'a1', bodyMd: 'Answer body' }]
  };
  const response = await runAnswer(
    { query: 'fastify', top_k: 1 },
    {
      baseUrl: 'https://a2a.example',
      search: async () => [{ id: 't1', title: 'T1' }],
      fetch: async () => thread,
      llm: async () => JSON.stringify({
        answer_markdown: 'Here is the answer.',
        used_indices: [1],
        quotes: [{ index: 1, quote: 'Answer body' }],
        warnings: []
      })
    }
  );
  assert.equal(response.citations.length, 1);
  assert.equal(response.citations[0].id, 't1');
  assert.ok(response.answer_markdown.includes('Here is the answer.'));
});

test('runAnswer falls back if llm returns invalid json', async () => {
  const response = await runAnswer(
    { query: 'test' },
    {
      baseUrl: 'https://a2a.example',
      search: async () => [{ id: 't1', title: 'T1' }],
      fetch: async () => ({ id: 't1', title: 'T1', bodyMd: 'Hello' }),
      llm: async () => 'not json'
    }
  );
  assert.ok(response.warnings.some((msg) => msg.includes('LLM failed')));
  assert.ok(response.answer_markdown.includes('LLM unavailable'));
});
