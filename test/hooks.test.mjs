import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { deliverStream } from "../scripts/lib.mjs";

test("a slow SSE source does not open an idle upload before its first nonempty bytes", { timeout: 10_000 }, async (t) => {
  let received = "";
  const packets = [];
  const server = createServer(async (request, response) => {
    try {
      let length = 0;
      for await (const chunk of request) { received += chunk; length += chunk.byteLength; }
      packets.push({ sequence: request.headers["x-cadge-stream-sequence"],
        end: request.headers["x-cadge-stream-end"], length,
        declaredLength: request.headers["content-length"] });
      response.writeHead(200, { "Content-Type": "application/json", Connection: "close" });
      response.end(JSON.stringify({ accepted: true }));
    } catch {
      // The pre-fix upload connects while the producer is still warming up.
    }
  });
  // Model a peer/proxy that closes a connected but inactive upload.
  server.on("connection", socket => socket.setTimeout(200, () => socket.destroy()));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const bytes = 'data: {"choices":[{"delta":{"content":"delayed"}}]}\n\ndata: [DONE]\n\n';
  let timer;
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(0));
      timer = setTimeout(() => { controller.enqueue(new TextEncoder().encode(bytes)); controller.close(); }, 600);
    },
    cancel() { clearTimeout(timer); },
  });
  t.after(() => clearTimeout(timer));
  const accepted = await deliverStream({ url: `http://127.0.0.1:${server.address().port}`, token: "delayed-source-agent" },
    "40da2c48-f18d-4cd7-9e5c-e429eb737672", source);
  assert.deepEqual(JSON.parse(accepted), { accepted: true });
  assert.equal(received, bytes, "prefetch must not drop or duplicate the first bytes");
  assert.deepEqual(packets, [
    { sequence: "0", end: "0", length: Buffer.byteLength(bytes), declaredLength: String(Buffer.byteLength(bytes)) },
    { sequence: "1", end: "1", length: 0, declaredLength: "0" },
  ]);
});

test("SSE packets preserve raw bytes, bound each body, and await every ACK including EOF", { timeout: 5000 }, async (t) => {
  const id = "40da2c48-f18d-4cd7-9e5c-e429eb737672";
  const large = Uint8Array.from({ length: 131_079 }, (_, i) => i % 256);
  const tail = Uint8Array.from([0xff, 0x00, 0xc3, 0x28]);
  const chunks = [new Uint8Array(0), large, tail];
  let reads = 0;
  const source = new ReadableStream({
    pull(controller) {
      if (reads < chunks.length) controller.enqueue(chunks[reads++]);
      else { reads += 1; controller.close(); }
    },
  }, { highWaterMark: 0 });
  const packets = [];
  let firstResponse;
  let finalResponse;
  let resolveFirst;
  let resolveFinal;
  const firstArrived = new Promise(resolve => { resolveFirst = resolve; });
  const finalArrived = new Promise(resolve => { resolveFinal = resolve; });
  const acknowledge = response => {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ accepted: true }));
  };
  const server = createServer(async (request, response) => {
    const buffers = [];
    for await (const chunk of request) buffers.push(chunk);
    packets.push({ headers: request.headers, bytes: Buffer.concat(buffers) });
    if (packets.length === 1) { firstResponse = response; resolveFirst(); }
    else if (request.headers["x-cadge-stream-end"] === "1") { finalResponse = response; resolveFinal(); }
    else acknowledge(response);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  let settled = false;
  const delivery = deliverStream({ url: `http://127.0.0.1:${server.address().port}`, token: "packet-agent" }, id, source);
  delivery.then(() => { settled = true; }, () => { settled = true; });
  await firstArrived;
  assert.equal(packets.length, 1, "do not overlap packets from an oversized producer chunk");
  assert.equal(settled, false);
  acknowledge(firstResponse);
  await finalArrived;
  assert.equal(settled, false, "nonfinal ACKs must not resolve deliverStream");
  for (const [sequence, packet] of packets.entries()) {
    assert.ok(packet.bytes.length <= 65_536);
    assert.equal(packet.headers["content-type"], "text/event-stream");
    assert.equal(packet.headers["x-cadge-claim-id"], id);
    assert.equal(packet.headers["x-cadge-stream-sequence"], String(sequence));
    assert.equal(packet.headers["x-cadge-stream-end"], sequence === packets.length - 1 ? "1" : "0");
    assert.equal(packet.headers["content-length"], String(packet.bytes.length));
    assert.equal(packet.headers["transfer-encoding"], undefined);
  }
  assert.equal(packets.at(-1).bytes.length, 0);
  assert.deepEqual(Buffer.concat(packets.map(packet => packet.bytes)), Buffer.concat([large, tail]));
  acknowledge(finalResponse);
  assert.deepEqual(JSON.parse(await delivery), { accepted: true });
  assert.equal(source.locked, false);
});

test("an empty SSE producer sends only its final packet", { timeout: 5000 }, async (t) => {
  const packets = [];
  const server = createServer(async (request, response) => {
    const buffers = [];
    for await (const chunk of request) buffers.push(chunk);
    packets.push({ sequence: request.headers["x-cadge-stream-sequence"],
      end: request.headers["x-cadge-stream-end"], length: Buffer.concat(buffers).length,
      declaredLength: request.headers["content-length"] });
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ accepted: true }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const source = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(0)); controller.close(); } });
  await deliverStream({ url: `http://127.0.0.1:${server.address().port}`, token: "empty-agent" },
    "40da2c48-f18d-4cd7-9e5c-e429eb737672", source);
  assert.deepEqual(packets, [{ sequence: "0", end: "1", length: 0, declaredLength: "0" }]);
  assert.equal(source.locked, false);
});

test("an unknown packet outcome is not retried and cancels the producer", { timeout: 5000 }, async (t) => {
  let requests = 0;
  const server = createServer(async request => {
    requests += 1;
    for await (const chunk of request) { /* Consume the finite packet before dropping its ACK. */ }
    request.socket.destroy();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  let cancelled = false;
  const source = new ReadableStream({
    pull(controller) { controller.enqueue(Uint8Array.of(0x61)); },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 });
  await assert.rejects(deliverStream({ url: `http://127.0.0.1:${server.address().port}`, token: "failed-agent" },
    "40da2c48-f18d-4cd7-9e5c-e429eb737672", source), /outcome may be unknown\. Not retrying/);
  assert.equal(requests, 1);
  assert.equal(cancelled, true);
  assert.equal(source.locked, false);
});

test("cancelling while awaiting a packet ACK cancels and releases the producer", { timeout: 5000 }, async (t) => {
  const stop = new AbortController();
  const reason = new Error("stop the packet upload");
  let requests = 0;
  const server = createServer(async request => {
    requests += 1;
    for await (const chunk of request) { /* Leave the ACK pending until cancellation. */ }
    stop.abort(reason);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  let cancelledWith;
  const source = new ReadableStream({
    pull(controller) { controller.enqueue(Uint8Array.of(0x61)); },
    cancel(value) { cancelledWith = value; },
  }, { highWaterMark: 0 });
  await assert.rejects(deliverStream({ url: `http://127.0.0.1:${server.address().port}`, token: "cancelled-agent" },
    "40da2c48-f18d-4cd7-9e5c-e429eb737672", source, stop.signal), error => error === reason);
  assert.equal(requests, 1);
  assert.equal(cancelledWith, reason);
  assert.equal(source.locked, false);
});

test("cancelling before first SSE bytes rejects while the source is still waiting", { timeout: 2000 }, async () => {
  const stop = new AbortController();
  const source = new ReadableStream();
  const delivery = deliverStream({ url: "http://127.0.0.1:1", token: "waiting-agent" },
    "40da2c48-f18d-4cd7-9e5c-e429eb737672", source, stop.signal);
  stop.abort();
  await assert.rejects(delivery, { name: "AbortError" });
});

test("the real poller keeps a streaming command alive past expiry after its first bytes", { timeout: 10_000 }, async (t) => {
  const id = "40da2c48-f18d-4cd7-9e5c-e429eb737672";
  const first = 'data: {"choices":[{"delta":{"content":"first"}}]}\n\n';
  const last = 'data: {"choices":[{"delta":{"content":"late"}}]}\n\ndata: [DONE]\n\n';
  let claimed = false;
  let expiresAt;
  let bytes = "";
  let firstAt;
  let sequence = 0;
  let resolveUpload;
  let rejectUpload;
  const uploaded = new Promise((resolve, reject) => { resolveUpload = resolve; rejectUpload = reject; });
  const server = createServer(async (request, response) => {
    if (request.url === "/beg") {
      if (claimed) {
        response.writeHead(204);
        response.end();
        return;
      }
      claimed = true;
      expiresAt = Date.now() + 2_000;
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ id, request: { stream: true }, expires_at: expiresAt }));
      return;
    }
    if (request.url !== "/deliver") {
      response.writeHead(404);
      response.end();
      return;
    }
    try {
      assert.equal(request.headers["content-type"], "text/event-stream");
      assert.equal(request.headers["x-cadge-claim-id"], id);
      assert.equal(request.headers["x-cadge-stream-sequence"], String(sequence++));
      let length = 0;
      for await (const chunk of request) {
        firstAt ??= Date.now();
        length += chunk.byteLength;
        bytes += chunk;
      }
      assert.equal(request.headers["content-length"], String(length));
      const end = request.headers["x-cadge-stream-end"];
      assert.ok(end === "0" || end === "1");
      assert.ok(length <= 65_536);
      if (end === "1") assert.equal(length, 0);
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ accepted: true }));
      if (end === "1") resolveUpload({ bytes, firstAt, finishedAt: Date.now() });
    } catch (error) {
      rejectUpload(error);
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const command = `
    process.stdin.resume();
    process.stdin.once("end", () => {
      process.stdout.write(${JSON.stringify(first)});
      setTimeout(() => process.stdout.end(${JSON.stringify(last)}),
        Math.max(0, Number(process.env.CADGE_EXPIRES_AT) - Date.now() + 500));
    });
  `;
  const poller = spawn(process.execPath, [fileURLToPath(new URL("../scripts/poll.mjs", import.meta.url)),
    "--", process.execPath, "-e", command], {
    env: { ...process.env, CADGE_URL: `http://127.0.0.1:${server.address().port}`,
      CADGE_AGENT_TOKEN: "deadline-agent", CADGE_POLL_INTERVAL_MS: "1000" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  poller.stderr.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
  const exited = once(poller, "exit");
  t.after(async () => {
    poller.kill("SIGTERM");
    const force = setTimeout(() => poller.kill("SIGKILL"), 1_500);
    try { await exited; } finally { clearTimeout(force); }
  });
  const result = await uploaded;
  assert.ok(result.firstAt < expiresAt, "first response bytes must arrive before the initial deadline");
  assert.ok(result.finishedAt >= expiresAt + 400, "the command must survive an idle gap past its original expiry");
  assert.equal(result.bytes, first + last, stderr || "late SSE bytes must not be replaced by an expiry error");
});
