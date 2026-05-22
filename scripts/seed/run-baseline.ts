#!/usr/bin/env tsx
import 'dotenv/config';

const API_BASE = (process.env.API_BASE_URL ?? 'https://a2abench-api.web.app').replace(/\/$/, '');
const ENTRANT_NAME = process.env.BASELINE_ENTRANT_NAME;
const API_KEY = process.env.BASELINE_API_KEY;
const MODEL_NAME = process.env.BASELINE_MODEL;

if (!ENTRANT_NAME || !API_KEY || !MODEL_NAME) {
  console.error('Missing BASELINE_ENTRANT_NAME, BASELINE_API_KEY, or BASELINE_MODEL');
  process.exit(1);
}

function summarizePrompt(prompt: string): string {
  const trimmed = prompt.replace(/\s+/g, ' ').trim();
  return trimmed.length > 900 ? `${trimmed.slice(0, 900)}...` : trimmed;
}

async function callModel(prompt: string): Promise<string> {
  if (MODEL_NAME === 'gemini-2.5-flash' || MODEL_NAME === 'gemini-2.0-flash') {
    const { GoogleGenAI } = await import('@google/genai');
    const client = new GoogleGenAI({ vertexai: true, project: process.env.GOOGLE_CLOUD_PROJECT, location: process.env.GOOGLE_CLOUD_LOCATION ?? 'us-central1' });
    const response = await client.models.generateContent({
      model: MODEL_NAME,
      contents: [{ role: 'user', parts: [{ text: `Answer this developer question concisely and accurately:\n\n${prompt}` }] }]
    });
    return (response.text ?? '').trim();
  }

  if (MODEL_NAME.includes('claude')) {
    const key = process.env.ANTHROPIC_API_KEY;
    if (!key) throw new Error('ANTHROPIC_API_KEY is required for Claude baselines');
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': key,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: MODEL_NAME,
        max_tokens: 500,
        messages: [{ role: 'user', content: `Answer this developer question concisely and accurately:\n\n${prompt}` }]
      })
    });
    const json = await response.json() as { content?: Array<{ type: string; text?: string }>; error?: { message?: string } };
    if (!response.ok) throw new Error(json.error?.message ?? `Claude request failed: ${response.status}`);
    return (json.content?.find((c) => c.type === 'text')?.text ?? '').trim();
  }

  if (MODEL_NAME.startsWith('gpt-')) {
    const key = process.env.OPENAI_API_KEY;
    if (!key) throw new Error('OPENAI_API_KEY is required for GPT baselines');
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${key}`
      },
      body: JSON.stringify({
        model: MODEL_NAME,
        messages: [{ role: 'user', content: `Answer this developer question concisely and accurately:\n\n${prompt}` }],
        temperature: 0.2
      })
    });
    const json = await response.json() as { choices?: Array<{ message?: { content?: string } }>; error?: { message?: string } };
    if (!response.ok) throw new Error(json.error?.message ?? `OpenAI request failed: ${response.status}`);
    return (json.choices?.[0]?.message?.content ?? '').trim();
  }

  throw new Error(`Unsupported BASELINE_MODEL: ${MODEL_NAME}`);
}

async function main() {
  const questionsResponse = await fetch(`${API_BASE}/v1/eval/questions?page=1`);
  if (!questionsResponse.ok) throw new Error(`Failed to fetch questions: ${questionsResponse.status}`);
  const questionsJson = await questionsResponse.json() as { results: Array<{ id: string; prompt: string }> };

  const submissions = [] as Array<{ question_id: string; answer: string }>;
  for (const question of questionsJson.results.slice(0, 10)) {
    const answer = await callModel(summarizePrompt(question.prompt));
    submissions.push({ question_id: question.id, answer: answer || 'No answer generated.' });
  }

  const submitResponse = await fetch(`${API_BASE}/v1/eval/submit`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${API_KEY}`
    },
    body: JSON.stringify({
      entrant_name: ENTRANT_NAME,
      submissions
    })
  });

  const submitJson = await submitResponse.json();
  if (!submitResponse.ok) throw new Error(JSON.stringify(submitJson));
  console.log(JSON.stringify(submitJson, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
