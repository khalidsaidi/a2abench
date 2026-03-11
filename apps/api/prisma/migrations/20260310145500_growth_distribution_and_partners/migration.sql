-- Growth primitives: durable delivery queue, acceptance reminders,
-- source attribution, and partner team tracking.

ALTER TABLE "Question"
ADD COLUMN "sourceType" TEXT,
ADD COLUMN "sourceUrl" TEXT,
ADD COLUMN "sourceExternalId" TEXT,
ADD COLUMN "sourceTitle" TEXT,
ADD COLUMN "sourceImportedAt" TIMESTAMP(3),
ADD COLUMN "sourceImportedBy" TEXT;

CREATE INDEX "Question_sourceType_sourceExternalId_idx" ON "Question"("sourceType", "sourceExternalId");
CREATE INDEX "Question_sourceType_sourceImportedAt_idx" ON "Question"("sourceType", "sourceImportedAt");
CREATE INDEX "Question_sourceUrl_idx" ON "Question"("sourceUrl");

ALTER TABLE "QuestionSubscription"
ALTER COLUMN "events" SET DEFAULT ARRAY['question.created', 'question.accepted', 'question.needs_acceptance', 'question.acceptance_reminder']::TEXT[];

CREATE TABLE "DeliveryQueue" (
  "id" TEXT NOT NULL,
  "subscriptionId" TEXT NOT NULL,
  "agentName" TEXT NOT NULL,
  "event" TEXT NOT NULL,
  "payload" JSONB NOT NULL,
  "questionId" TEXT,
  "answerId" TEXT,
  "webhookUrl" TEXT,
  "webhookSecret" TEXT,
  "attemptCount" INTEGER NOT NULL DEFAULT 0,
  "maxAttempts" INTEGER NOT NULL DEFAULT 5,
  "nextAttemptAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lastAttemptAt" TIMESTAMP(3),
  "deliveredAt" TIMESTAMP(3),
  "lastStatus" INTEGER,
  "lastError" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "DeliveryQueue_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "DeliveryQueue"
ADD CONSTRAINT "DeliveryQueue_subscriptionId_fkey"
FOREIGN KEY ("subscriptionId") REFERENCES "QuestionSubscription"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE INDEX "DeliveryQueue_agentName_deliveredAt_nextAttemptAt_idx" ON "DeliveryQueue"("agentName", "deliveredAt", "nextAttemptAt");
CREATE INDEX "DeliveryQueue_subscriptionId_deliveredAt_nextAttemptAt_idx" ON "DeliveryQueue"("subscriptionId", "deliveredAt", "nextAttemptAt");
CREATE INDEX "DeliveryQueue_event_createdAt_idx" ON "DeliveryQueue"("event", "createdAt");
CREATE INDEX "DeliveryQueue_questionId_idx" ON "DeliveryQueue"("questionId");
CREATE INDEX "DeliveryQueue_answerId_idx" ON "DeliveryQueue"("answerId");

CREATE TABLE "AcceptanceReminder" (
  "id" TEXT NOT NULL,
  "questionId" TEXT NOT NULL,
  "answerId" TEXT,
  "stageHours" INTEGER NOT NULL,
  "dueAt" TIMESTAMP(3) NOT NULL,
  "sentAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "AcceptanceReminder_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "AcceptanceReminder"
ADD CONSTRAINT "AcceptanceReminder_questionId_fkey"
FOREIGN KEY ("questionId") REFERENCES "Question"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "AcceptanceReminder"
ADD CONSTRAINT "AcceptanceReminder_answerId_fkey"
FOREIGN KEY ("answerId") REFERENCES "Answer"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE UNIQUE INDEX "AcceptanceReminder_questionId_stageHours_key" ON "AcceptanceReminder"("questionId", "stageHours");
CREATE INDEX "AcceptanceReminder_sentAt_dueAt_idx" ON "AcceptanceReminder"("sentAt", "dueAt");
CREATE INDEX "AcceptanceReminder_questionId_sentAt_idx" ON "AcceptanceReminder"("questionId", "sentAt");
CREATE INDEX "AcceptanceReminder_answerId_idx" ON "AcceptanceReminder"("answerId");

CREATE TABLE "PartnerTeam" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "displayName" TEXT,
  "description" TEXT,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "targetWeeklyActiveAnswerers" INTEGER,
  "targetWeeklyAcceptanceRate" DOUBLE PRECISION,
  "targetWeeklyRetainedAnswerers" INTEGER,
  "targetPayoutPerAccepted" DOUBLE PRECISION,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PartnerTeam_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "PartnerTeam_name_key" ON "PartnerTeam"("name");
CREATE INDEX "PartnerTeam_active_createdAt_idx" ON "PartnerTeam"("active", "createdAt");

CREATE TABLE "PartnerTeamMember" (
  "id" TEXT NOT NULL,
  "teamId" TEXT NOT NULL,
  "agentName" TEXT NOT NULL,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PartnerTeamMember_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "PartnerTeamMember"
ADD CONSTRAINT "PartnerTeamMember_teamId_fkey"
FOREIGN KEY ("teamId") REFERENCES "PartnerTeam"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE UNIQUE INDEX "PartnerTeamMember_teamId_agentName_key" ON "PartnerTeamMember"("teamId", "agentName");
CREATE INDEX "PartnerTeamMember_agentName_active_idx" ON "PartnerTeamMember"("agentName", "active");
CREATE INDEX "PartnerTeamMember_teamId_active_idx" ON "PartnerTeamMember"("teamId", "active");
