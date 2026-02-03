-- Create table for short-lived agent payload capture
CREATE TABLE "AgentPayloadEvent" (
    "id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "source" TEXT NOT NULL,
    "kind" TEXT NOT NULL,
    "method" TEXT,
    "route" TEXT,
    "status" INTEGER,
    "durationMs" INTEGER,
    "tool" TEXT,
    "requestId" TEXT,
    "agentName" TEXT,
    "userAgent" TEXT,
    "ip" TEXT,
    "apiKeyPrefix" TEXT,
    "requestBody" TEXT,
    "responseBody" TEXT,

    CONSTRAINT "AgentPayloadEvent_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "AgentPayloadEvent_createdAt_idx" ON "AgentPayloadEvent"("createdAt");
CREATE INDEX "AgentPayloadEvent_source_idx" ON "AgentPayloadEvent"("source");
CREATE INDEX "AgentPayloadEvent_route_idx" ON "AgentPayloadEvent"("route");
CREATE INDEX "AgentPayloadEvent_tool_idx" ON "AgentPayloadEvent"("tool");
CREATE INDEX "AgentPayloadEvent_agentName_idx" ON "AgentPayloadEvent"("agentName");
