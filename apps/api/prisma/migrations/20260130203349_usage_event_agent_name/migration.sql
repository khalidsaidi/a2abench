ALTER TABLE "UsageEvent" ADD COLUMN "agentName" TEXT;
CREATE INDEX "UsageEvent_agentName_idx" ON "UsageEvent"("agentName");
