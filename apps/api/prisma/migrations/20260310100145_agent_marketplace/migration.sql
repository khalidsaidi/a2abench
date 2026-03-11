-- Agent-driven marketplace primitives: reputation, votes, accepts, bounties, subscriptions.

ALTER TABLE "Answer"
ADD COLUMN "agentName" TEXT;

CREATE INDEX "Answer_agentName_idx" ON "Answer"("agentName");

CREATE TABLE "QuestionResolution" (
  "questionId" TEXT NOT NULL,
  "answerId" TEXT NOT NULL,
  "acceptedByAgentName" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "QuestionResolution_pkey" PRIMARY KEY ("questionId")
);

CREATE UNIQUE INDEX "QuestionResolution_answerId_key" ON "QuestionResolution"("answerId");
CREATE INDEX "QuestionResolution_acceptedByAgentName_idx" ON "QuestionResolution"("acceptedByAgentName");

ALTER TABLE "QuestionResolution"
ADD CONSTRAINT "QuestionResolution_questionId_fkey"
FOREIGN KEY ("questionId") REFERENCES "Question"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "QuestionResolution"
ADD CONSTRAINT "QuestionResolution_answerId_fkey"
FOREIGN KEY ("answerId") REFERENCES "Answer"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "QuestionBounty" (
  "id" TEXT NOT NULL,
  "questionId" TEXT NOT NULL,
  "amount" INTEGER NOT NULL,
  "currency" TEXT NOT NULL DEFAULT 'credits',
  "active" BOOLEAN NOT NULL DEFAULT true,
  "createdByAgentName" TEXT,
  "expiresAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "QuestionBounty_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "QuestionBounty_questionId_key" ON "QuestionBounty"("questionId");
CREATE INDEX "QuestionBounty_active_expiresAt_idx" ON "QuestionBounty"("active", "expiresAt");
CREATE INDEX "QuestionBounty_createdByAgentName_idx" ON "QuestionBounty"("createdByAgentName");

ALTER TABLE "QuestionBounty"
ADD CONSTRAINT "QuestionBounty_questionId_fkey"
FOREIGN KEY ("questionId") REFERENCES "Question"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "AnswerVote" (
  "id" TEXT NOT NULL,
  "answerId" TEXT NOT NULL,
  "voterAgentName" TEXT NOT NULL,
  "value" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "AnswerVote_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "AnswerVote_answerId_voterAgentName_key" ON "AnswerVote"("answerId", "voterAgentName");
CREATE INDEX "AnswerVote_voterAgentName_idx" ON "AnswerVote"("voterAgentName");

ALTER TABLE "AnswerVote"
ADD CONSTRAINT "AnswerVote_answerId_fkey"
FOREIGN KEY ("answerId") REFERENCES "Answer"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "AgentProfile" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "reputation" INTEGER NOT NULL DEFAULT 0,
  "answersCount" INTEGER NOT NULL DEFAULT 0,
  "acceptedCount" INTEGER NOT NULL DEFAULT 0,
  "voteScore" INTEGER NOT NULL DEFAULT 0,
  "credits" INTEGER NOT NULL DEFAULT 0,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "AgentProfile_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "AgentProfile_name_key" ON "AgentProfile"("name");

CREATE TABLE "AgentCreditLedger" (
  "id" TEXT NOT NULL,
  "agentName" TEXT NOT NULL,
  "delta" INTEGER NOT NULL,
  "reason" TEXT NOT NULL,
  "questionId" TEXT,
  "answerId" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "AgentCreditLedger_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "AgentCreditLedger_agentName_createdAt_idx" ON "AgentCreditLedger"("agentName", "createdAt");
CREATE INDEX "AgentCreditLedger_questionId_idx" ON "AgentCreditLedger"("questionId");
CREATE INDEX "AgentCreditLedger_answerId_idx" ON "AgentCreditLedger"("answerId");

CREATE TABLE "QuestionSubscription" (
  "id" TEXT NOT NULL,
  "agentName" TEXT NOT NULL,
  "tags" TEXT[] DEFAULT ARRAY[]::TEXT[],
  "webhookUrl" TEXT,
  "webhookSecret" TEXT,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "QuestionSubscription_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "QuestionSubscription_agentName_active_idx" ON "QuestionSubscription"("agentName", "active");
CREATE INDEX "QuestionSubscription_active_createdAt_idx" ON "QuestionSubscription"("active", "createdAt");
