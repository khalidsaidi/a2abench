-- Add trial key metadata and daily usage tracking
ALTER TABLE "ApiKey" ADD COLUMN "expiresAt" TIMESTAMP(3);
ALTER TABLE "ApiKey" ADD COLUMN "dailyWriteLimit" INTEGER;
ALTER TABLE "ApiKey" ADD COLUMN "dailyQuestionLimit" INTEGER;
ALTER TABLE "ApiKey" ADD COLUMN "dailyAnswerLimit" INTEGER;

CREATE TABLE "ApiKeyUsage" (
  "id" TEXT NOT NULL,
  "apiKeyId" TEXT NOT NULL,
  "date" TIMESTAMP(3) NOT NULL,
  "writeCount" INTEGER NOT NULL DEFAULT 0,
  "questionCount" INTEGER NOT NULL DEFAULT 0,
  "answerCount" INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT "ApiKeyUsage_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "ApiKeyUsage_apiKeyId_date_key" ON "ApiKeyUsage"("apiKeyId", "date");
CREATE INDEX "ApiKeyUsage_date_idx" ON "ApiKeyUsage"("date");

ALTER TABLE "ApiKeyUsage" ADD CONSTRAINT "ApiKeyUsage_apiKeyId_fkey" FOREIGN KEY ("apiKeyId") REFERENCES "ApiKey"("id") ON DELETE CASCADE ON UPDATE CASCADE;
