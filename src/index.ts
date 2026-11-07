import { apiError, isObject, type ChatRequest, type Completion, type Env, type JsonObject } from "./protocol";
export { Broker } from "./broker";

const MAX_BODY_BYTES = 256 * 1024;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class BodyError extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}

async function readJson(request: Request): Promise<JsonObject> {
  if (!request.body) throw new BodyError(400, "A JSON body is required.");
  const reader = request.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let bytes = 0;
  let text = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_BODY_BYTES) {
        await reader.cancel();
        throw new BodyError(413, "JSON body exceeds 256 KiB.");
      }
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
    const value: unknown = JSON.parse(text);
    if (!isObject(value)) throw new Error("Not an object");
    return value;
  } catch (error) {
    if (error instanceof BodyError) throw error;
    throw new BodyError(400, "Body must be a valid UTF-8 JSON object.");
  } finally {
    reader.releaseLock();
  }
}

function validChat(body: JsonObject): body is ChatRequest {
  return typeof body.model === "string" && body.model.length > 0
    && Array.isArray(body.messages) && body.messages.length > 0
    && body.messages.every(message => isObject(message) && typeof message.role === "string"
      && (message.content === undefined || message.content === null || typeof message.content === "string"
        || (Array.isArray(message.content) && message.content.every(isObject))))
    && (body.stream === undefined || typeof body.stream === "boolean")
    && (body.stream_options === undefined || (isObject(body.stream_options)
      && (body.stream_options.include_usage === undefined || typeof body.stream_options.include_usage === "boolean")));
}

function validCompletion(value: unknown): value is Completion {
  return isObject(value) && typeof value.id === "string" && value.id.length > 0
    && value.object === "chat.completion" && typeof value.model === "string"
    && typeof value.created === "number" && Number.isFinite(value.created)
    && (value.usage === undefined || isObject(value.usage))
    && Array.isArray(value.choices) && value.choices.length > 0
    && value.choices.every(choice => isObject(choice)
      && Number.isInteger(choice.index) && (choice.index as number) >= 0
      && (choice.finish_reason === null || typeof choice.finish_reason === "string")
      && isObject(choice.message) && choice.message.role === "assistant"
      && (choice.message.content === undefined || choice.message.content === null || typeof choice.message.content === "string")
      && (choice.message.tool_calls === undefined || (Array.isArray(choice.message.tool_calls)
        && choice.message.tool_calls.every(call => isObject(call) && typeof call.id === "string"
          && call.type === "function" && isObject(call.function)
          && typeof call.function.name === "string" && typeof call.function.arguments === "string"))));
}

function forwardStream(response: Response, cancel: () => void, detach: () => void): Response {
  const reader = response.body!.getReader();
  let finished = false;
  const finish = () => {
    if (finished) return;
    finished = true;
    detach();
  };
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const chunk = await reader.read();
        if (finished) return;
        if (chunk.done) {
          reader.releaseLock();
          finish();
          controller.close();
        } else controller.enqueue(chunk.value);
      } catch (error) {
        if (!finished) {
          reader.releaseLock();
          finish();
          controller.error(error);
        }
      }
    },
    async cancel(reason) {
      cancel();
      finish();
      try { await reader.cancel(reason); }
      finally { reader.releaseLock(); }
    },
  });
  return new Response(body, { status: response.status, headers: response.headers });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const path = new URL(request.url).pathname;
    const clientRoute = path === "/v1/models" || path === "/v1/chat/completions";
    const agentRoute = path === "/beg" || path === "/deliver";
    if (!clientRoute && !agentRoute) return apiError(404, "Unknown endpoint.", "not_found");
    if (!env.CLIENT_TOKEN || !env.AGENT_TOKEN || env.CLIENT_TOKEN === env.AGENT_TOKEN) {
      return apiError(503, "Distinct CLIENT_TOKEN and AGENT_TOKEN secrets must be configured.", "configuration_error");
    }
    const token = clientRoute ? env.CLIENT_TOKEN : env.AGENT_TOKEN;
    if (request.headers.get("Authorization") !== `Bearer ${token}`) {
      const response = apiError(401, "Invalid bearer token.", "invalid_api_key");
      response.headers.set("WWW-Authenticate", "Bearer");
      return response;
    }
    const method = path === "/v1/models" ? "GET" : "POST";
    if (request.method !== method) {
      const response = apiError(405, `Use ${method} for this endpoint.`, "method_not_allowed");
      response.headers.set("Allow", method);
      return response;
    }
    const timeout = Number(env.REQUEST_TIMEOUT_MS);
    if (!env.MODEL_ID || !Number.isInteger(timeout) || timeout < 100 || timeout > 600000) {
      return apiError(503, "Configure MODEL_ID and REQUEST_TIMEOUT_MS (100–600000).", "configuration_error");
    }
    if (path === "/v1/models") {
      return Response.json({ object: "list", data: [
        { id: env.MODEL_ID, object: "model", created: 0, owned_by: "cadge" },
      ] }, { headers: { "Cache-Control": "no-store" } });
    }
    let detachAbort: (() => void) | undefined;
    let notifyCancel: (() => void) | undefined;
    try {
      let body: JsonObject | undefined;
      const streamDelivery = path === "/deliver"
        && request.headers.get("Content-Type")?.split(";", 1)[0].trim().toLowerCase() === "text/event-stream";
      if (path !== "/beg" && !streamDelivery) body = await readJson(request);
      const claimId = streamDelivery ? request.headers.get("X-Cadge-Claim-ID") : null;
      if (streamDelivery && (!claimId || !UUID.test(claimId) || !request.body)) {
        return apiError(400, "SSE delivery requires a claim UUID in X-Cadge-Claim-ID and a request body.", "invalid_delivery");
      }
      if (path === "/v1/chat/completions") {
        if (!validChat(body!)) return apiError(400, "Provide model, nonempty messages, and an optional boolean stream.", "invalid_request");
        if (body!.model !== env.MODEL_ID) return apiError(404, "The requested model is not available.", "model_not_found");
      }
      if (path === "/deliver" && !streamDelivery) {
        const hasResponse = Object.hasOwn(body!, "response");
        const hasError = Object.hasOwn(body!, "error");
        if (typeof body!.id !== "string" || !UUID.test(body!.id)
          || hasResponse === hasError
          || (hasResponse && !validCompletion(body!.response))
          || (hasError && (!isObject(body!.error) || typeof body!.error.message !== "string" || !body!.error.message.trim()))) {
          return apiError(400, "Provide a claim UUID and either a full chat.completion response or error.message.", "invalid_delivery");
        }
      }
      // Never forward public Authorization headers, tokens, or correlation IDs into the queue.
      const broker = env.BROKER.get(env.BROKER.idFromName("cadge"));
      const headers = new Headers({ "Content-Type": streamDelivery ? "text/event-stream; charset=utf-8" : "application/json" });
      if (claimId) headers.set("X-Cadge-Claim-ID", claimId);
      if (path === "/v1/chat/completions") {
        const requestId = crypto.randomUUID();
        const deadline = Date.now() + timeout;
        headers.set("X-Cadge-Request-ID", requestId);
        headers.set("X-Cadge-Request-Deadline", String(deadline));
        let notified = false;
        const abort = () => {
          if (notified) return;
          notified = true;
          // Cleanup must outlive the disconnected caller and must not inherit its signal.
          ctx.waitUntil(broker.fetch(new Request("https://broker/cancel", {
            method: "POST",
            headers: {
              "X-Cadge-Request-ID": requestId,
              "X-Cadge-Request-Deadline": String(deadline),
            },
          })).then(() => undefined));
        };
        notifyCancel = abort;
        request.signal.addEventListener("abort", abort, { once: true });
        detachAbort = () => request.signal.removeEventListener("abort", abort);
        // Body parsing may already have observed the caller disconnecting.
        if (request.signal.aborted) return apiError(499, "The caller disconnected.", "request_cancelled");
      }
      const internal = new Request(`https://broker${path}`, {
        method: "POST",
        headers,
        body: streamDelivery ? request.body : body === undefined ? undefined : JSON.stringify(body),
        signal: request.signal,
      });
      const pending = broker.fetch(internal);
      if (path === "/v1/chat/completions") {
        // Keep the enqueue lifecycle alive if the client disconnects while it
        // races cancellation. The awaited branch below handles fetch failures.
        ctx.waitUntil(pending.then(() => undefined, () => undefined));
      }
      const response = await pending;
      if (path === "/v1/chat/completions" && response.ok && response.body
        && response.headers.get("Content-Type")?.startsWith("text/event-stream")) {
        const streamed = forwardStream(response, notifyCancel!, detachAbort!);
        // Cancellation remains attached until the response body ends or is canceled.
        detachAbort = undefined;
        return streamed;
      }
      return response;
    } catch (error) {
      if (request.signal.aborted) return apiError(499, "The caller disconnected.", "request_cancelled");
      if (error instanceof BodyError) return apiError(error.status, error.message, "invalid_request");
      return apiError(503, "The coordinator is unavailable; the request may have been interrupted.", "broker_unavailable");
    } finally {
      detachAbort?.();
    }
  },
} satisfies ExportedHandler<Env>;
