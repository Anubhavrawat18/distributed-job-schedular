import { CronExpressionParser } from "cron-parser";

/**
 * Computes the next occurrence strictly after `after`.
 *
 * Lives in its own module rather than in scheduler.ts because the API needs it
 * too (to validate an expression and store the first occurrence), and importing
 * from scheduler.ts would execute that file's polling loop inside the API
 * process.
 */
export function nextOccurrence(
  cronExpression: string,
  timezone: string,
  after: Date,
): Date {
  return CronExpressionParser.parse(cronExpression, {
    currentDate: after,
    tz: timezone,
  })
    .next()
    .toDate();
}
