import test from 'node:test';
import assert from 'node:assert/strict';
import { toPrivateFeedbackRecord } from '../src/feedback.js';

test('toPrivateFeedbackRecord strips raw email and keeps hash/domain only', () => {
  const rawEmail = 'khalidsaidi66+fb-test@gmail.com';
  const record = toPrivateFeedbackRecord({
    title: 'Sample feedback',
    email: rawEmail,
    message: 'Please add an endpoint.',
    ip: '127.0.0.1',
    userAgent: 'node-test'
  });

  assert.equal(typeof record.email_hash, 'string');
  assert.equal(record.email_domain, 'gmail.com');
  assert.equal((record as Record<string, unknown>).email, undefined);

  const serialized = JSON.stringify(record);
  assert.equal(serialized.includes(rawEmail), false);
});
