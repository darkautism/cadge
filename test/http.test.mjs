import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Miniflare } from "miniflare";

const CLIENT_TOKEN = "client-test";
const AGENT_TOKEN = "agent-test";
const REQUEST_TIMEOUT_MS = 600;
const UNKNOWN_CLAIM_ID = "a984b3c9-c14a-48eb-9533-0c258edfe028";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function post(runtime, path, token, body) {
  const headers = new Headers();
  if (token !== undefined) headers.set("Authorization", `Bearer ${token}`);
  if (body !== undefined) headers.set("Content-Type", "application/json");
  return runtime.dispatchFetch(`http://cadge.test${path}`, {
    method: "POST",
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function createRuntime(t) {
  const runtime = new Miniflare({
    modules: true,
    scriptPath: fileURLToPath(new URL("../dist/index.js", import.meta.url)),
    compatibilityDate: "2026-07-30",
    durableObjects: { BROKER: { className: "Broker", useSQLite: true } },
    bindings: {
      CLIENT_TOKEN,
      AGENT_TOKEN,
      MODEL_ID: "cadge",
      REQUEST_TIMEOUT_MS: String(REQUEST_TIMEOUT_MS),
    },
  });
  t.after(() => runtime.dispose());
  await runtime.ready;

  // Initialize the actual Durable Object before starting any 600 ms deadline.
  const empty = await post(runtime, "/beg", AGENT_TOKEN);
  assert.equal(empty.status, 204);
  assert.match(empty.headers.get("Retry-After") ?? "", /^[1-9]\d*$/);
  return runtime;
}

function chatRequest(label, extra = {}) {
  return {
    model: "cadge",
    messages: [
      { role: "system", content: "Answer the user's request." },
      { role: "user", content: label },
    ],
    temperature: 0.25,
    max_tokens: 32,
    metadata: { trace: label, nested: { retain: [true, null, "unchanged"] } },
    ...extra,
  };
}

function completion(label, extra = {}) {
  return {
    id: `chatcmpl-${label}`,
    object: "chat.completion",
    created: 1_700_000_000,
    model: "cadge",
    choices: [
      {
        index: 0,
        message: { role: "assistant", content: `Answer for ${label}.` },
        finish_reason: "stop",
        logprobs: null,
      },
    ],
    usage: { prompt_tokens: 3, completion_tokens: 4, total_tokens: 7 },
    system_fingerprint: "fp_test",
    custom_result: { retain: label },
    ...extra,
  };
}

function startChat(runtime, body) {
  const pending = post(runtime, "/v1/chat/completions", CLIENT_TOKEN, body);
  // A failed assertion can dispose the runtime while this fetch is still pending.
  // Keep disposal rejections handled without changing what the caller awaits.
  void pending.catch(() => {});
  return pending;
}

async function claimRequests(runtime, count, concurrency = 1) {
  const claims = [];
  const deadline = performance.now() + 2_000;
  while (claims.length < count) {
    // Retry only empty polling responses: never redeliver or requeue a claim.
    // Concurrent client POSTs need not reach the broker before the first poll.
    const responses = await Promise.all(
      Array.from({ length: concurrency }, () => post(runtime, "/beg", AGENT_TOKEN)),
    );
    for (const response of responses) {
      if (response.status === 204) {
        assert.match(response.headers.get("Retry-After") ?? "", /^[1-9]\d*$/);
      } else {
        assert.equal(response.status, 200);
        claims.push(await response.json());
      }
    }
    assert.ok(claims.length <= count, "a request was claimed more than once");
    if (claims.length < count) {
      assert.ok(performance.now() < deadline, `expected ${count} pending requests, got ${claims.length}`);
      await delay(5);
    }
  }
  return claims;
}

async function assertAccepted(response) {
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { accepted: true });
}

async function assertApiError(response, status, code) {
  assert.equal(response.status, status);
  const body = await response.json();
  assert.equal(typeof body.error?.message, "string");
  if (code !== undefined) assert.equal(body.error.code, code);
  return body;
}

async function readStream(response) {
  assert.equal(response.status, 200);
  assert.match(response.headers.get("Content-Type") ?? "", /^text\/event-stream\b/i);
  const text = await response.text();
  const events = text
    .split(/\r?\n\r?\n/)
    .map((event) => event.split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n"))
    .filter(Boolean);
  assert.ok(events.length > 1, "the stream must contain chunks and a terminator");
  assert.equal(events.at(-1), "[DONE]");
  assert.equal(events.filter((event) => event === "[DONE]").length, 1);
  return events.slice(0, -1).map((event) => {
    const chunk = JSON.parse(event);
    assert.equal(chunk.object, "chat.completion.chunk");
    assert.ok(Array.isArray(chunk.choices));
    return chunk;
  });
}

test("client and agent credentials cannot cross-access the HTTP endpoints", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const endpoints = [
    ["/v1/chat/completions", AGENT_TOKEN, chatRequest("unauthorized")],
    ["/beg", CLIENT_TOKEN, undefined],
    ["/deliver", CLIENT_TOKEN, { id: UNKNOWN_CLAIM_ID, response: completion("unauthorized") }],
  ];
  for (const [path, wrongRoleToken, body] of endpoints) {
    for (const token of [wrongRoleToken, undefined, "not-a-valid-token"]) {
      const response = await post(runtime, path, token, body);
      assert.ok([401, 403].includes(response.status), `${path} accepted an unauthorized credential`);
      const error = await response.json();
      assert.equal(typeof error.error?.message, "string");
    }
  }
  const empty = await post(runtime, "/beg", AGENT_TOKEN);
  assert.equal(empty.status, 204, "unauthorized requests must not enter the queue");
});

test("twelve concurrent agents claim three jobs once and deliveries reach the original callers", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const submittedAt = Date.now();
  const jobs = ["first", "second", "third"].map((label) => {
    const request = chatRequest(label);
    return { label, request, response: startChat(runtime, request), completion: completion(label) };
  });
  const jobsByLabel = new Map(jobs.map((job) => [job.label, job]));
  const claims = await claimRequests(runtime, jobs.length, 12);

  assert.equal(claims.length, 3);
  assert.equal(new Set(claims.map((claim) => claim.id)).size, 3);
  for (const claim of claims) {
    assert.match(claim.id, UUID);
    assert.ok(Number.isInteger(claim.expires_at));
    assert.ok(claim.expires_at > submittedAt, "expires_at must be a Unix-millisecond deadline");
    const job = jobsByLabel.get(claim.request.metadata.trace);
    assert.ok(job, "the claim must belong to one of the waiting callers");
    assert.deepEqual(claim.request, job.request, "the entire original request must survive the broker");
  }

  const emptyClaims = await Promise.all(
    Array.from({ length: 12 }, () => post(runtime, "/beg", AGENT_TOKEN)),
  );
  for (const response of emptyClaims) assert.equal(response.status, 204);

  for (const claim of [claims[2], claims[0], claims[1]]) {
    const job = jobsByLabel.get(claim.request.metadata.trace);
    await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, {
      id: claim.id,
      response: job.completion,
    }));
  }
  for (const job of jobs) {
    const response = await job.response;
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), job.completion, "completion responses must pass through unchanged");
  }

  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claims[0].id,
    response: completion("duplicate"),
  }), 404);
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: UNKNOWN_CLAIM_ID,
    response: completion("unknown"),
  }), 404);
});

test("a claimed request times out and its stale capability cannot deliver", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("expires"));
  const [claim] = await claimRequests(runtime, 1);

  await assertApiError(await pending, 504, "request_timeout");
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id,
    response: completion("too-late"),
  }), 410);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("an unclaimed request times out instead of remaining available to agents", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("never-claimed"));
  await assertApiError(await pending, 504, "request_timeout");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("agent error delivery settles the waiting caller and consumes the capability", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("agent-error"));
  const [claim] = await claimRequests(runtime, 1);
  const delivery = { id: claim.id, error: { message: "The answering agent could not complete this request." } };
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, delivery));

  const response = await pending;
  assert.ok(response.status >= 400 && response.status <= 599);
  assert.equal((await response.json()).error.message, delivery.error.message);
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, delivery), 404);
});

test("simultaneous deliveries consume a claim once and return the winning answer", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("delivery-race"));
  const [claim] = await claimRequests(runtime, 1);
  const answers = [completion("winner-a"), completion("winner-b")];
  const deliveries = await Promise.all(answers.map(response =>
    post(runtime, "/deliver", AGENT_TOKEN, { id: claim.id, response })));
  assert.deepEqual(deliveries.map(response => response.status).sort(), [200, 404]);
  const winner = deliveries.findIndex(response => response.status === 200);
  const returned = await pending;
  assert.equal(returned.status, 200);
  assert.deepEqual(await returned.json(), answers[winner]);
});

test("include_usage emits final totals separately from choice chunks", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("stream-usage", {
    stream: true, stream_options: { include_usage: true },
  }));
  const [claim] = await claimRequests(runtime, 1);
  const answer = completion("stream-usage");
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, { id: claim.id, response: answer }));
  const chunks = await readStream(await pending);
  const usageChunks = chunks.filter(chunk => chunk.usage !== null);
  assert.equal(usageChunks.length, 1);
  assert.deepEqual(usageChunks[0].usage, answer.usage);
  assert.deepEqual(usageChunks[0].choices, []);
  assert.equal(chunks.at(-1), usageChunks[0]);
});

test("streaming text includes assistant content, the finish reason, and DONE", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const request = chatRequest("stream-text", { stream: true });
  const pending = startChat(runtime, request);
  const [claim] = await claimRequests(runtime, 1);
  assert.deepEqual(claim.request, request);
  const answer = completion("stream-text");
  answer.choices[0].message.content = "An answer with Zürich, 🌊, and a\nsecond line.";
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, { id: claim.id, response: answer }));

  const chunks = await readStream(await pending);
  for (const chunk of chunks) {
    assert.equal(chunk.id, answer.id);
    assert.equal(chunk.model, answer.model);
    assert.equal(chunk.created, answer.created);
  }
  const choices = chunks.flatMap((chunk) => chunk.choices).filter((choice) => choice.index === 0);
  assert.ok(choices.some((choice) => choice.delta?.role === "assistant"));
  assert.equal(choices.map((choice) => choice.delta?.content ?? "").join(""), answer.choices[0].message.content);
  assert.equal(choices.filter((choice) => typeof choice.finish_reason === "string").at(-1)?.finish_reason, "stop");
});

test("streaming tool calls preserves function names, arguments, and tool-call finish reason", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("stream-tools", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const toolCalls = [
    { id: "call_weather", type: "function", function: { name: "get_weather", arguments: '{"city":"Zürich"}' } },
    { id: "call_time", type: "function", function: { name: "get_time", arguments: '{"zone":"Europe/Zurich"}' } },
  ];
  const answer = completion("stream-tools", {
    choices: [{
      index: 0,
      message: { role: "assistant", content: null, tool_calls: toolCalls },
      finish_reason: "tool_calls",
    }],
  });
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, { id: claim.id, response: answer }));

  const chunks = await readStream(await pending);
  const choices = chunks.flatMap((chunk) => chunk.choices).filter((choice) => choice.index === 0);
  assert.ok(choices.some((choice) => choice.delta?.role === "assistant"));
  const reconstructed = new Map();
  for (const choice of choices) {
    for (const part of choice.delta?.tool_calls ?? []) {
      assert.ok(Number.isInteger(part.index) && part.index >= 0);
      let call = reconstructed.get(part.index);
      if (!call) {
        call = { id: "", type: "", function: { name: "", arguments: "" } };
        reconstructed.set(part.index, call);
      }
      if (part.id !== undefined) call.id += part.id;
      if (part.type !== undefined) call.type = part.type;
      call.function.name += part.function?.name ?? "";
      call.function.arguments += part.function?.arguments ?? "";
    }
  }
  assert.deepEqual([...reconstructed].sort(([left], [right]) => left - right).map(([, call]) => call), toolCalls);
  assert.equal(choices.filter((choice) => typeof choice.finish_reason === "string").at(-1)?.finish_reason, "tool_calls");
});

test("small malformed requests and invalid delivery shapes return 400 without queueing work", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const malformed = await runtime.dispatchFetch("http://cadge.test/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${CLIENT_TOKEN}`, "Content-Type": "application/json" },
    body: "{",
  });
  await assertApiError(malformed, 400);

  for (const body of [
    [],
    { model: "cadge" },
    { model: "cadge", messages: "not-an-array" },
    { model: "cadge", messages: [{ role: "user", content: 123 }] },
  ]) {
    await assertApiError(await post(runtime, "/v1/chat/completions", CLIENT_TOKEN, body), 400);
  }

  const invalidFinish = completion("invalid-finish");
  invalidFinish.choices[0].finish_reason = 123;
  const invalidContent = completion("invalid-content");
  invalidContent.choices[0].message.content = { not: "message content" };
  for (const body of [
    {},
    { id: UNKNOWN_CLAIM_ID },
    { id: UNKNOWN_CLAIM_ID, response: invalidFinish },
    { id: UNKNOWN_CLAIM_ID, response: invalidContent },
    { id: UNKNOWN_CLAIM_ID, error: { message: 123 } },
  ]) {
    await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, body), 400);
  }
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("the 256 KiB request-body limit rejects oversized prompts before queueing", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const body = chatRequest("oversized");
  body.messages[1].content = "x".repeat(256 * 1024);
  await assertApiError(await post(runtime, "/v1/chat/completions", CLIENT_TOKEN, body), 413);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});
