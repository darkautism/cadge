import assert from "node:assert/strict";
import { request as httpRequest } from "node:http";
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

async function openChatSocket(runtime, label, extra = {}) {
  // Miniflare's entry proxy does not forward disconnects; target workerd itself.
  const client = httpRequest(new URL("/v1/chat/completions", await runtime.unsafeGetDirectURL()), {
    method: "POST",
    headers: { Authorization: `Bearer ${CLIENT_TOKEN}`, "Content-Type": "application/json" },
  });
  client.on("error", () => {}); // Resetting an in-flight HTTP request raises ECONNRESET.
  const closed = new Promise(resolve => client.once("close", resolve));
  const response = new Promise(resolve => client.once("response", incoming => {
    incoming.on("error", () => {}); // Midstream RST also closes the response.
    resolve(incoming);
  }));
  const uploaded = new Promise((resolve, reject) => {
    client.once("finish", resolve);
    client.once("error", reject);
  });
  client.end(JSON.stringify(chatRequest(label, extra)));
  await uploaded;
  return { client, closed, response };
}

async function createRuntime(t, requestTimeoutMs = REQUEST_TIMEOUT_MS) {
  const runtime = new Miniflare({
    modules: true,
    scriptPath: fileURLToPath(new URL("../dist/index.js", import.meta.url)),
    compatibilityDate: "2026-07-30",
    compatibilityFlags: ["enable_request_signal"],
    unsafeDirectSockets: [{ host: "127.0.0.1", port: 0 }],
    durableObjects: { BROKER: { className: "Broker", useSQLite: true } },
    bindings: {
      CLIENT_TOKEN,
      AGENT_TOKEN,
      MODEL_ID: "cadge",
      REQUEST_TIMEOUT_MS: String(requestTimeoutMs),
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

const encoder = new TextEncoder();

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

async function beginPackets(t, runtime, id) {
  const abort = new AbortController();
  const finished = deferred();
  let sequence = 0;
  void finished.promise.catch(() => {});
  t.after(() => abort.abort());
  const send = async (body, final) => {
    const response = await fetch(new URL("/deliver", await runtime.unsafeGetDirectURL()), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${AGENT_TOKEN}`,
        "Content-Type": "text/event-stream",
        "X-Cadge-Claim-ID": id,
        "X-Cadge-Stream-Sequence": String(sequence),
        "X-Cadge-Stream-End": final ? "1" : "0",
      },
      body,
      signal: abort.signal,
    });
    if (final) finished.resolve(response);
    else await assertAccepted(response);
    sequence++;
  };
  return {
    delivery: finished.promise,
    write: (chunk) => send(typeof chunk === "string" ? encoder.encode(chunk) : chunk, false),
    close: () => send("", true),
  };
}

function postPacket(runtime, id, body = "data: [DONE]\n\n", sequence = 0, final = true) {
  return runtime.dispatchFetch("http://cadge.test/deliver", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AGENT_TOKEN}`,
      "Content-Type": "text/event-stream",
      "X-Cadge-Claim-ID": id,
      "X-Cadge-Stream-Sequence": String(sequence),
      "X-Cadge-Stream-End": final ? "1" : "0",
    },
    body,
  });
}

function assertStreamHeaders(response) {
  assert.equal(response.status, 200);
  assert.match(response.headers.get("Content-Type") ?? "", /^text\/event-stream;\s*charset=utf-8$/i);
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.equal(response.headers.get("X-Accel-Buffering"), "no");
}

async function readPrefix(reader, expected) {
  const chunks = [];
  let length = 0;
  while (length < expected.byteLength) {
    const { done, value } = await reader.read();
    assert.equal(done, false, "the stream ended before the expected bytes arrived");
    chunks.push(value);
    length += value.byteLength;
  }
  const actual = Buffer.concat(chunks);
  assert.deepEqual(actual, Buffer.from(expected));
  return actual;
}

async function readRemaining(reader) {
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) return Buffer.concat(chunks);
    chunks.push(value);
  }
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

test("a timed-out claim acknowledges late delivery without reviving the request", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("expires"));
  const [claim] = await claimRequests(runtime, 1);

  await assertApiError(await pending, 504, "request_timeout");
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id,
    response: completion("too-late"),
  }));
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("a disconnected HTTP client's late result is acknowledged without exposing cancellation", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const { client, closed } = await openChatSocket(runtime, "disconnected");
  const [claim] = await claimRequests(runtime, 1);
  client.socket.resetAndDestroy();
  await closed;
  // Allow the transport close and its cleanup request to cross the DO boundary.
  await delay(100);
  const delivered = await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, response: completion("must-not-deliver"),
  });
  assert.ok(Date.now() < claim.expires_at, "cancellation must work before timeout cleanup");
  await assertAccepted(delivered);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("an unclaimed request is removed after its HTTP client disconnects", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const { client, closed } = await openChatSocket(runtime, "cancel-before-claim");
  const startedAt = Date.now();
  // The broker is warmed by createRuntime; let the complete upload enqueue.
  await delay(100);
  client.socket.resetAndDestroy();
  await closed;
  await delay(100);
  const next = await post(runtime, "/beg", AGENT_TOKEN);
  assert.ok(Date.now() - startedAt < 5000, "queue removal must not rely on timeout");
  assert.equal(next.status, 204);
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

test("SSE reaches the client before producer EOF and preserves comments, Unicode, tools, usage, and DONE byte-for-byte", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const request = chatRequest("stream-exact", { stream: true, stream_options: { include_usage: true } });
  const pending = startChat(runtime, request);
  const [claim] = await claimRequests(runtime, 1);
  assert.deepEqual(claim.request, request, "stream:true and stream_options must reach the agent unchanged");
  const upload = await beginPackets(t, runtime, claim.id);
  const first = encoder.encode(': heartbeat\r\ndata: {"id":"chatcmpl-wire","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}]}\r\n\r\n');
  const tail = Buffer.from(
    'data: {"id":"chatcmpl-wire","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Zürich 🌊"}}]}\n\n' +
    'data: {"id":"chatcmpl-wire","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_weather","type":"function","function":{"name":"get_weather","arguments":"{\\"city\\":\\""}}]}}]}\n\n' +
    'data: {"id":"chatcmpl-wire","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"Zürich\\"}"}},{"index":1,"id":"call_time","type":"function","function":{"name":"get_time","arguments":"{\\"zone\\":\\"Europe/Zurich\\"}"}}]},"finish_reason":"tool_calls"}]}\n\n' +
    'data: {"id":"chatcmpl-wire","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7},"provider_extra":true}\n\n' +
    ': provider trailer\n\ndata: [DONE]\n\n',
  );
  const firstReceived = deferred();
  let uploadEnded = false;
  let acknowledged = false;
  void upload.delivery.then(() => { acknowledged = true; }, () => {});
  const producer = (async () => {
    await upload.write(first);
    await firstReceived.promise;
    // Split within the UTF-8 encoding of ü, not just at SSE or JSON boundaries.
    const split = tail.indexOf(Buffer.from("ü")) + 1;
    await upload.write(tail.subarray(0, split));
    await upload.write(tail.subarray(split, split + 1));
    await upload.write(tail.subarray(split + 1));
    uploadEnded = true;
    await upload.close();
  })();
  void producer.catch(() => {});
  const response = await pending;
  assertStreamHeaders(response);
  const reader = response.body.getReader();
  await readPrefix(reader, first);
  assert.equal(uploadEnded, false, "the first event must not wait for sender EOF");
  assert.equal(acknowledged, false, "delivery is acknowledged only after sender EOF");
  firstReceived.resolve();
  assert.deepEqual(await readRemaining(reader), tail);
  await producer;
  await assertAccepted(await upload.delivery);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("JSON success cannot consume a streaming claim, but a subsequent SSE upload can", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const pending = startChat(runtime, chatRequest("needs-stream", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, response: completion("not-a-stream"),
  }), 409, "stream_required");
  const upload = await beginPackets(t, runtime, claim.id);
  const bytes = encoder.encode('data: {"choices":[{"delta":{"content":"actual stream"}}]}\n\ndata: [DONE]\n\n');
  await upload.write(bytes);
  await upload.close();
  await assertAccepted(await upload.delivery);
  const response = await pending;
  assertStreamHeaders(response);
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), Buffer.from(bytes));
});

test("SSE cannot consume a nonstream claim, which remains usable for JSON success", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const pending = startChat(runtime, chatRequest("needs-json"));
  const [claim] = await claimRequests(runtime, 1);
  await assertApiError(await postPacket(runtime, claim.id), 409, "stream_not_requested");
  const answer = completion("needs-json");
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, { id: claim.id, response: answer }));
  assert.deepEqual(await (await pending).json(), answer);
});

test("JSON agent errors are accepted for a streaming request before upload starts", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const pending = startChat(runtime, chatRequest("stream-agent-error", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, error: { message: "Provider unavailable" },
  }));
  await assertApiError(await pending, 502);
});

test("an active packet stream excludes duplicate sequences and JSON errors without losing the next packet", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const pending = startChat(runtime, chatRequest("stream-race", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const upload = await beginPackets(t, runtime, claim.id);
  const first = encoder.encode(": active\n\n");
  await upload.write(first);
  const response = await pending;
  const reader = response.body.getReader();
  await readPrefix(reader, first);
  await assertApiError(await postPacket(runtime, claim.id), 409, "stream_sequence_mismatch");
  await assertApiError(await postPacket(runtime, UNKNOWN_CLAIM_ID), 404);
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, error: { message: "racing error" },
  }), 409, "stream_in_progress");
  await upload.write("data: [DONE]\n\n");
  await upload.close();
  assert.equal((await readRemaining(reader)).toString(), "data: [DONE]\n\n");
  await assertAccepted(await upload.delivery);
  await assertApiError(await postPacket(runtime, claim.id), 404);
});

test("a streaming claim canceled before delivery drains and acknowledges a stale upload", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const { client, closed } = await openChatSocket(runtime, "stale-canceled-stream", { stream: true });
  const [claim] = await claimRequests(runtime, 1);
  client.socket.resetAndDestroy();
  await closed;
  // As in the JSON cancellation regression, allow transport cleanup to reach the broker.
  await delay(100);
  const upload = await beginPackets(t, runtime, claim.id);
  let acknowledged = false;
  void upload.delivery.then(() => { acknowledged = true; }, () => {});
  const frame = `: discarded ${"x".repeat(4096)}\n\n`;
  for (let index = 0; index < 80; index++) await upload.write(frame);
  assert.equal(acknowledged, false, "packet ACKs must not finish the producer before explicit EOF");
  await upload.write("data: [DONE]\n\n");
  await upload.close();
  await assertAccepted(await upload.delivery);
  await assertAccepted(await postPacket(runtime, claim.id, "", 987, false));
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, response: completion("repeat-stale-discard"),
  }));
  assert.ok(Date.now() < claim.expires_at, "the acknowledgement must not depend on timeout");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("a stream larger than 256 KiB is relayed in modest frames without a total body cap", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const pending = startChat(runtime, chatRequest("large-stream", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const upload = await beginPackets(t, runtime, claim.id);
  const frame = `data: ${JSON.stringify({ choices: [{ delta: { content: "x".repeat(4096) } }] })}\n\n`;
  const expected = Buffer.from(frame.repeat(80) + "data: [DONE]\n\n");
  assert.ok(expected.byteLength > 256 * 1024);
  const received = pending.then(async response => {
    assertStreamHeaders(response);
    return Buffer.from(await response.arrayBuffer());
  });
  void received.catch(() => {});
  for (let index = 0; index < 80; index++) await upload.write(frame);
  await upload.write("data: [DONE]\n\n");
  await upload.close();
  await assertAccepted(await upload.delivery);
  assert.deepEqual(await received, expected);
});

test("a disconnected midstream client does not abort the agent upload or requeue its job", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const { client, closed, response: responsePromise } = await openChatSocket(runtime, "stream-disconnect", { stream: true });
  const [claim] = await claimRequests(runtime, 1);
  const upload = await beginPackets(t, runtime, claim.id);
  const first = ": before disconnect\n\n";
  await upload.write(first);
  const response = await responsePromise;
  assert.equal(response.statusCode, 200);
  const firstReceived = deferred();
  let received = Buffer.alloc(0);
  response.on("data", chunk => {
    received = Buffer.concat([received, chunk]);
    if (received.byteLength >= Buffer.byteLength(first)) firstReceived.resolve();
  });
  await firstReceived.promise;
  assert.equal(received.toString(), first);
  client.socket.resetAndDestroy();
  await closed;
  // Continue well beyond stream buffering limits after the real TCP RST.
  const frame = `data: ${JSON.stringify({ choices: [{ delta: { content: "x".repeat(4096) } }] })}\n\n`;
  for (let index = 0; index < 160; index++) await upload.write(frame);
  await upload.write("data: [DONE]\n\n");
  await upload.close();
  await assertAccepted(await upload.delivery);
  assert.ok(Date.now() < claim.expires_at, "draining must not wait for the request deadline");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("a timed-out streaming claim acknowledges late packets without reviving the request", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("stale-stream", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  await assertApiError(await pending, 504, "request_timeout");
  const upload = await beginPackets(t, runtime, claim.id);
  let acknowledged = false;
  void upload.delivery.then(() => { acknowledged = true; }, () => {});
  const frame = `: ${"discard".repeat(1024)}\n\n`;
  for (let index = 0; index < 80; index++) await upload.write(frame);
  assert.equal(acknowledged, false);
  await upload.write("data: [DONE]\n\n");
  await upload.close();
  await assertAccepted(await upload.delivery);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("first SSE bytes remove the deadline through idle gaps, alarms, and later queue cleanup", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 1500);
  const pending = startChat(runtime, chatRequest("stream-first-byte-deadline", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const upload = await beginPackets(t, runtime, claim.id);
  const first = encoder.encode('data: {"choices":[{"delta":{"content":"first"}}]}\n\n');
  await upload.write(first);
  const response = await pending;
  assertStreamHeaders(response);
  const reader = response.body.getReader();
  await readPrefix(reader, first);
  await delay(150);
  // Another request keeps an alarm scheduled after this stream's old deadline.
  await assertApiError(await startChat(runtime, chatRequest("still-waiting")), 504, "request_timeout");
  assert.ok(Date.now() > claim.expires_at, "the active stream must outlive its initial deadline");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
  const tail = encoder.encode('data: {"choices":[{"delta":{"content":"late tail"}}]}\n\ndata: [DONE]\n\n');
  const remaining = readRemaining(reader);
  await upload.write(tail);
  await upload.close();
  assert.deepEqual(await remaining, Buffer.from(tail));
  await assertAccepted(await upload.delivery);
});

test("opening SSE upload headers without bytes does not remove the initial deadline", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 1500);
  const pending = startChat(runtime, chatRequest("headers-are-not-first-bytes", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const upload = httpRequest(new URL("/deliver", await runtime.unsafeGetDirectURL()), {
    method: "POST",
    headers: { Authorization: `Bearer ${AGENT_TOKEN}`, "Content-Type": "text/event-stream",
      "X-Cadge-Claim-ID": claim.id, "X-Cadge-Stream-Sequence": "0", "X-Cadge-Stream-End": "0",
      "Transfer-Encoding": "chunked" },
  });
  const acknowledged = new Promise((resolve, reject) => {
    upload.on("error", reject);
    upload.on("response", response => {
      let body = "";
      response.setEncoding("utf8").on("data", chunk => { body += chunk; });
      response.on("end", () => resolve({ status: response.statusCode, body }));
    });
  });
  void acknowledged.catch(() => {});
  t.after(() => upload.destroy());
  upload.flushHeaders();
  await assertApiError(await pending, 504, "request_timeout");
  assert.ok(Date.now() >= claim.expires_at);
  upload.end(": late bytes are discarded\n\ndata: [DONE]\n\n");
  const accepted = await acknowledged;
  assert.equal(accepted.status, 200);
  assert.deepEqual(JSON.parse(accepted.body), { accepted: true });
});

test("a broken later packet fails the stream without fabricating DONE", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const pending = startChat(runtime, chatRequest("stream-producer-failure", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const first = encoder.encode('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n');
  const firstPacket = postPacket(runtime, claim.id, first, 0, false);
  const response = await pending;
  assertStreamHeaders(response);
  const reader = response.body.getReader();
  await readPrefix(reader, first);
  await assertAccepted(await firstPacket);
  const remainder = readRemaining(reader);
  const upload = httpRequest(new URL("/deliver", await runtime.unsafeGetDirectURL()), {
    method: "POST",
    headers: { Authorization: `Bearer ${AGENT_TOKEN}`, "Content-Type": "text/event-stream",
      "X-Cadge-Claim-ID": claim.id, "X-Cadge-Stream-Sequence": "1", "X-Cadge-Stream-End": "0",
      "Transfer-Encoding": "chunked" },
  });
  upload.on("error", () => {});
  t.after(() => upload.destroy());
  upload.write("data: incomplete packet");
  // Let the packet enter the Worker before breaking the HTTP body.
  await delay(100);
  upload.destroy(new Error("Agent input transport failed"));
  const tail = await remainder;
  assert.equal(tail.includes(Buffer.from("[DONE]")), false);
  const terminal = JSON.parse(tail.toString().trim().replace(/^data:\s*/, ""));
  assert.equal(terminal.error.code, "agent_stream_error");
  await assertApiError(await postPacket(runtime, claim.id, "", 1), 404);
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("SSE packet headers are validated at both the public router and Durable Object boundary", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const namespace = await runtime.getDurableObjectNamespace("BROKER");
  const broker = namespace.get(namespace.idFromName("cadge"));
  const valid = {
    "Content-Type": "text/event-stream",
    "X-Cadge-Claim-ID": UNKNOWN_CLAIM_ID,
    "X-Cadge-Stream-Sequence": "0",
    "X-Cadge-Stream-End": "1",
  };
  const invalid = [
    ["X-Cadge-Claim-ID", null], ["X-Cadge-Claim-ID", "not-a-uuid"],
    ["X-Cadge-Stream-Sequence", null], ["X-Cadge-Stream-Sequence", ""],
    ...["-1", "1.5", "1e2", "NaN", "Infinity", "9007199254740992"].map(value => ["X-Cadge-Stream-Sequence", value]),
    ["X-Cadge-Stream-End", null], ["X-Cadge-Stream-End", ""],
    ["X-Cadge-Stream-End", "true"], ["X-Cadge-Stream-End", "2"],
  ];
  for (const send of [
    init => runtime.dispatchFetch("http://cadge.test/deliver", init),
    init => broker.fetch("https://broker/deliver", init),
  ]) {
    for (const [name, value] of invalid) {
      const headers = new Headers({ ...valid, Authorization: `Bearer ${AGENT_TOKEN}` });
      if (value === null) headers.delete(name);
      else headers.set(name, value);
      await assertApiError(await send({ method: "POST", headers, body: "data: test\n\n" }), 400, "invalid_delivery");
    }
    // An omitted body is a valid empty final packet, but does not make an unknown claim valid.
    await assertApiError(await send({ method: "POST", headers: {
      ...valid, Authorization: `Bearer ${AGENT_TOKEN}`,
    } }), 404, "claim_not_found");
  }
});

test("packets are bounded, ordered, byte-transparent, and require explicit EOF without corrupting a rejected claim", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const pending = startChat(runtime, chatRequest("packet-limits", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  let responseStarted = false;
  void pending.then(() => { responseStarted = true; });
  await assertApiError(await postPacket(runtime, claim.id, new Uint8Array(65537), 0, false), 413, "invalid_delivery");
  await assertApiError(await postPacket(runtime, claim.id, "", 0, false), 400, "invalid_delivery");
  await assertApiError(await postPacket(runtime, claim.id, ": skipped\n\n", 1, false), 409, "stream_sequence_mismatch");
  assert.equal(responseStarted, false, "a rejected packet must not begin or write to the client stream");
  const bytes = Uint8Array.from({ length: 65536 }, (_, index) => index % 256);
  let responseEnded = false;
  const received = pending.then(async response => {
    assertStreamHeaders(response);
    const body = Buffer.from(await response.arrayBuffer());
    responseEnded = true;
    return body;
  });
  void received.catch(() => {});
  await assertAccepted(await postPacket(runtime, claim.id, bytes, 0, false));
  assert.equal(responseEnded, false, "a nonfinal ACK must not close the client stream");
  await assertApiError(await postPacket(runtime, claim.id, "duplicate", 0, false), 409, "stream_sequence_mismatch");
  await assertApiError(await postPacket(runtime, claim.id, "skipped", 2, false), 409, "stream_sequence_mismatch");
  await assertAccepted(await postPacket(runtime, claim.id, "", 1));
  assert.deepEqual(await received, Buffer.from(bytes));
  await assertApiError(await postPacket(runtime, claim.id, "", 2), 404, "claim_not_found");
});

test("a packet is fully read before forwarding and excludes overlapping packets without blocking other claims", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const pending = startChat(runtime, chatRequest("packet-overlap", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  let responseStarted = false;
  void pending.then(() => { responseStarted = true; });
  const upload = httpRequest(new URL("/deliver", await runtime.unsafeGetDirectURL()), {
    method: "POST",
    headers: { Authorization: `Bearer ${AGENT_TOKEN}`, "Content-Type": "text/event-stream",
      "X-Cadge-Claim-ID": claim.id, "X-Cadge-Stream-Sequence": "0", "X-Cadge-Stream-End": "0",
      "Transfer-Encoding": "chunked" },
  });
  const accepted = new Promise((resolve, reject) => {
    upload.on("error", reject);
    upload.on("response", response => {
      let body = "";
      response.setEncoding("utf8").on("data", chunk => { body += chunk; });
      response.on("end", () => resolve({ status: response.statusCode, body }));
    });
  });
  void accepted.catch(() => {});
  t.after(() => upload.destroy());
  upload.write(": partial packet");
  await delay(100);
  assert.equal(responseStarted, false, "partial packet bytes must not escape before validation");
  for (const sequence of [0, 1]) {
    await assertApiError(await postPacket(runtime, claim.id, ": overlapping\n\n", sequence, false), 409, "stream_packet_in_progress");
  }
  await assertApiError(await post(runtime, "/deliver", AGENT_TOKEN, {
    id: claim.id, error: { message: "racing error" },
  }), 404);
  const other = startChat(runtime, chatRequest("unrelated-packet-claim"));
  const [otherClaim] = await claimRequests(runtime, 1);
  const answer = completion("unrelated-packet-claim");
  await assertAccepted(await post(runtime, "/deliver", AGENT_TOKEN, { id: otherClaim.id, response: answer }));
  assert.deepEqual(await (await other).json(), answer);
  upload.end(" complete\n\n");
  const response = await pending;
  assertStreamHeaders(response);
  const reader = response.body.getReader();
  await readPrefix(reader, encoder.encode(": partial packet complete\n\n"));
  assert.deepEqual(await accepted, { status: 200, body: '{"accepted":true}' });
  await assertAccepted(await postPacket(runtime, claim.id, "", 1));
  assert.equal((await readRemaining(reader)).byteLength, 0);
});

test("an empty final packet at sequence zero completes an empty stream without starting its deadline exemption", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t);
  const pending = startChat(runtime, chatRequest("empty-packet-stream", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  await assertAccepted(await postPacket(runtime, claim.id, "", 0));
  const response = await pending;
  assertStreamHeaders(response);
  assert.equal((await response.arrayBuffer()).byteLength, 0);
  await delay(REQUEST_TIMEOUT_MS);
  await assertApiError(await postPacket(runtime, claim.id, "", 0), 404, "claim_not_found");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});

test("a paused client backpressures sequential packets and cancellation releases the producer", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 10_000);
  const pending = startChat(runtime, chatRequest("packet-backpressure", { stream: true }));
  const [claim] = await claimRequests(runtime, 1);
  const packet = new Uint8Array(65536).fill(120);
  const firstPacket = postPacket(runtime, claim.id, packet, 0, false);
  const response = await pending;
  assertStreamHeaders(response);
  const reader = response.body.getReader();
  await readPrefix(reader, packet);
  await assertAccepted(await firstPacket);
  let acceptedPackets = 0;
  const producer = (async () => {
    for (let sequence = 1; sequence <= 128; sequence++) {
      await assertAccepted(await postPacket(runtime, claim.id, packet, sequence, false));
      acceptedPackets++;
    }
    await assertAccepted(await postPacket(runtime, claim.id, "", 129));
  })();
  void producer.catch(() => {});
  const completedWhilePaused = await Promise.race([
    producer.then(() => true),
    delay(500).then(() => false),
  ]);
  assert.equal(completedWhilePaused, false, "a paused consumer must not permit unbounded packet buffering");
  const before = acceptedPackets;
  await delay(100);
  assert.equal(acceptedPackets, before, "packet acknowledgements must wait for downstream demand");
  await reader.cancel("client stopped consuming");
  await producer;
  assert.equal(acceptedPackets, 128, "canceled claims must drain and ACK subsequent packets");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
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

test("cancellation arriving before enqueue prevents a ghost job", { timeout: 30_000 }, async (t) => {
  const runtime = await createRuntime(t, 5000);
  const namespace = await runtime.getDurableObjectNamespace("BROKER");
  const broker = namespace.get(namespace.idFromName("cadge"));
  const headers = {
    "X-Cadge-Request-ID": crypto.randomUUID(),
    "X-Cadge-Request-Deadline": String(Date.now() + 3000),
    "Content-Type": "application/json",
  };
  // Exercise the real broker in the network ordering that can precede enqueue.
  const cancelled = await broker.fetch("https://broker/cancel", { method: "POST", headers });
  assert.equal(cancelled.status, 204);
  const delayed = await broker.fetch("https://broker/v1/chat/completions", {
    method: "POST", headers, body: JSON.stringify(chatRequest("already-abandoned")),
  });
  await assertApiError(delayed, 499, "request_cancelled");
  assert.equal((await post(runtime, "/beg", AGENT_TOKEN)).status, 204);
});
