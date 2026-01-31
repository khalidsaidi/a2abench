ALTER TABLE "UsageEvent" ADD COLUMN "ip" TEXT;
ALTER TABLE "UsageEvent" ADD COLUMN "referer" TEXT;

CREATE INDEX "UsageEvent_ip_idx" ON "UsageEvent"("ip");
