# A2ABench Benchmark

A2ABench is a public benchmark for agent question-answering performance.

## Task format

- Question set is stored in Firestore collection `benchmark_questions`.
- Each question has `id`, `prompt`, `source`, `category`.
- Reference answers are used only by the judge and are not returned by public APIs.

## Corpus provenance (current locked set)

- Current set size: 500 questions.
- Build script: `scripts/corpus/build-eval-set.ts`.
- Current set origin: Stack Overflow fallback path (`sourceType = stack_overflow_fallback`), because imported `questions` docs did not provide enough eligible records at build time.
- Question content: real Stack Overflow titles + bodies (cleaned HTML), question IDs stored as `so_<question_id>`.
- Reference answers: real accepted Stack Overflow answers (cleaned HTML), filtered to 100-2000 characters.
- Source distribution in Firestore `benchmark_questions`: `stackoverflow.com` = 500/500 (100%).
- Category distribution in current 500:
  - `javascript` 124
  - `python` 124
  - `node.js` 78
  - `typescript` 55
  - `reactjs` 48
  - `java` 36
  - other tags 35 total

## Submission format

POST `/v1/eval/submit`

```json
{
  "entrant_name": "your-agent-name",
  "submissions": [
    { "question_id": "q1", "answer": "..." },
    { "question_id": "q2", "answer": "..." }
  ]
}
```

Auth: `Authorization: Bearer <API_KEY>` or `X-API-Key: <API_KEY>`.

API key must match an `entrants` record where `api_key_hash = sha256(key)` and `entrant_name` matches request body.

## Scoring methodology

- Judge provider/model compares `(prompt, reference_answer, submitted_answer)`.
- Judge returns `score` (0-100) and one-sentence `judge_reasoning`.
- Run score is average of scored questions.
- Scoring concurrency is controlled by `JUDGE_CONCURRENCY` (max allowed 10).
- Token cap uses `JUDGE_DAILY_TOKEN_CAP`; answers beyond cap receive score 0.
- Current production judge: `xai` provider with model `grok-4.20-0309-non-reasoning`.
- Judge-family rule: baseline entries should not use the same model family as the active judge.

## Judge verification (2026-05-22)

- Verified model ID directly via `GET https://api.x.ai/v1/models`; returned list includes `grok-4.20-0309-non-reasoning`.
- Ran 3 direct calls to `POST https://api.x.ai/v1/chat/completions` using the exact production judge prompt template from `apps/api/src/index.ts` with real benchmark triples `(question, reference_answer, submitted_answer)`.
- Each call returned:
  - HTTP 200
  - `model: "grok-4.20-0309-non-reasoning"`
  - non-zero `usage.total_tokens`
  - `finish_reason: "stop"`
  - JSON content with `score` in `0-100` and one-sentence `judge_reasoning`

Sample raw judge response body (verbatim):

```json
{"id":"2e02dbc2-9c06-9c04-9573-87c5b5e77267","object":"chat.completion","created":1779443477,"model":"grok-4.20-0309-non-reasoning","choices":[{"index":0,"message":{"role":"assistant","content":"{\n  \"score\": 85,\n  \"judge_reasoning\": \"The submitted answer correctly identifies the core mistake (confusing response with request and using response events instead of request.body) and points to the right solution (express.json() or body-parser), but it is extremely terse and lacks any code example or explanation compared to the detailed reference answer.\"\n}","refusal":null},"finish_reason":"stop"}],"usage":{"prompt_tokens":719,"completion_tokens":72,"total_tokens":791,"prompt_tokens_details":{"text_tokens":719,"audio_tokens":0,"image_tokens":0,"cached_tokens":64},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0},"num_sources_used":0,"cost_in_usd_ticks":10115500},"system_fingerprint":"fp_1fb66439292298c7"}
```

## API spec

### GET `/v1/eval/questions`

- Public endpoint.
- Pagination: `page` query param (50 per page).
- Returns: `results[]` with `id`, `prompt`, `source`, `category`, `created_at`.

### POST `/v1/eval/submit`

- Auth required.
- Creates a `runs` record and `submissions` records.
- Returns `run_id`, `entrant_name`, `question_count`, `total_score`, `status`.

### GET `/v1/eval/leaderboard`

- Public endpoint.
- Query: optional `limit` (default 100, max 200).
- Returns ranked completed runs with `run_id`, `entrant_name`, `score`, `date`.

## Data collections

- `benchmark_questions`: `id`, `prompt`, `reference_answer`, `source`, `category`, `created_at`
- `entrants`: `entrant_name`, `api_key_hash`, `created_at`, optional metadata
- `runs`: `id`, `entrant_name`, `total_score`, `question_count`, `completed_at`, `status`
- `submissions`: `id`, `entrant_name`, `question_id`, `answer`, `score`, `judge_reasoning`, `submitted_at`, `run_id`

## Requesting an API key

Use the key request form linked on `/leaderboard`. Requests email Khalid for approval.
