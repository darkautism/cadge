import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
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
