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
