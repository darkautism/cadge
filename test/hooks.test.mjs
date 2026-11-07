import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { deliverStream } from "../scripts/lib.mjs";

test("a slow SSE source does not open an idle upload before its first nonempty bytes", { timeout: 10_000 }, async (t) => {
  let received = "";
  const server = createServer(async (request, response) => {
    try {
      for await (const chunk of request) received += chunk;
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
      let bytes = "";
      let firstAt;
      for await (const chunk of request) {
        firstAt ??= Date.now();
        bytes += chunk;
      }
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ accepted: true }));
      resolveUpload({ bytes, firstAt, finishedAt: Date.now() });
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
