import crypto from 'node:crypto';

export type FeedbackSubmissionInput = {
  title: string;
  email: string;
  message: string;
  ip: string;
  userAgent: string;
};

function hashFeedbackEmail(email: string): string | null {
  const normalized = email.trim().toLowerCase();
  if (!normalized) return null;
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

function emailDomain(email: string): string | null {
  if (!email.includes('@')) return null;
  return email.split('@').pop()?.toLowerCase() ?? null;
}

export function toPrivateFeedbackRecord(input: FeedbackSubmissionInput) {
  return {
    title: input.title,
    message: input.message,
    email_hash: hashFeedbackEmail(input.email),
    email_domain: emailDomain(input.email),
    source: 'feedback_form',
    classification: 'pending_review',
    ip: input.ip,
    user_agent: input.userAgent
  };
}
