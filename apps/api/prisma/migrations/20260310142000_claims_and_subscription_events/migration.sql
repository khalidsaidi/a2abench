-- Agent job flow: question claims + webhook event preferences.

ALTER TABLE "QuestionSubscription"
ADD COLUMN "events" TEXT[] NOT NULL DEFAULT ARRAY['question.created', 'question.accepted', 'question.needs_acceptance']::TEXT[];

CREATE TABLE "QuestionClaim" (
  "id" TEXT NOT NULL,
  "questionId" TEXT NOT NULL,
  "agentName" TEXT NOT NULL,
  "state" TEXT NOT NULL DEFAULT 'claimed',
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "answerId" TEXT,
  "claimedByApiKey" TEXT,
  "releasedAt" TIMESTAMP(3),
  "answeredAt" TIMESTAMP(3),
  "verifiedAt" TIMESTAMP(3),
  "verifiedByAgent" TEXT,
  "verifyReason" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "QuestionClaim_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "QuestionClaim_state_check" CHECK ("state" IN ('claimed', 'answered', 'verified', 'released', 'expired'))
);

ALTER TABLE "QuestionClaim"
ADD CONSTRAINT "QuestionClaim_questionId_fkey"
FOREIGN KEY ("questionId") REFERENCES "Question"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "QuestionClaim"
ADD CONSTRAINT "QuestionClaim_answerId_fkey"
FOREIGN KEY ("answerId") REFERENCES "Answer"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE INDEX "QuestionClaim_questionId_state_expiresAt_idx" ON "QuestionClaim"("questionId", "state", "expiresAt");
CREATE INDEX "QuestionClaim_agentName_state_createdAt_idx" ON "QuestionClaim"("agentName", "state", "createdAt");
CREATE INDEX "QuestionClaim_answerId_idx" ON "QuestionClaim"("answerId");
