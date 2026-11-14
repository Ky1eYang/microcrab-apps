import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { pollUntil } from "./poll.mjs";

test("awaits asynchronous false observations before accepting a result", async () => {
  let attempts = 0;
  const observed = { status: "idle" };
  const result = await pollUntil(async () => {
    await delay(1);
    return ++attempts === 3 ? observed : false;
  }, { timeoutMs: 1000, intervalMs: 1 });
  assert.equal(attempts, 3);
  assert.equal(result, observed);
});

test("propagates observation failures without retry", async () => {
  const failure = new Error("read failed");
  let attempts = 0;
  await assert.rejects(pollUntil(async () => {
    attempts++;
    throw failure;
  }, { timeoutMs: 1000 }), error => error === failure);
  assert.equal(attempts, 1);
});

test("rejects when asynchronous observations stay false", async () => {
  let attempts = 0;
  await assert.rejects(pollUntil(async () => {
    attempts++;
    await delay(1);
    return false;
  }, { timeoutMs: 20, intervalMs: 1, code: "canonical_idle_deadline" }),
  error => error.code === "canonical_idle_deadline");
  assert.ok(attempts > 0);
});
