import { setTimeout as delay } from "node:timers/promises";

/** Await each observation before testing it; an unresolved Promise is not proof.
 * Return the successful observation and propagate failures without retrying mutations.
 * Callers must bound any I/O inside their read-only observation separately.
 */
export async function pollUntil(observe, { timeoutMs, intervalMs = 100, code = "poll_deadline" }) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const value = await observe();
    if (value) return value;
    const remaining = deadline - performance.now();
    if (remaining > 0) await delay(Math.min(intervalMs, remaining));
  }
  throw Object.assign(new Error(code), { code });
}
