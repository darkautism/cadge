import { apiError, isObject, type ChatRequest, type Completion, type Env, type JsonObject } from "./protocol";
import { completionStream } from "./stream";
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

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
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
    try {
      let body: JsonObject | undefined;
      if (path !== "/beg") body = await readJson(request);
      if (path === "/v1/chat/completions") {
        if (!validChat(body!)) return apiError(400, "Provide model, nonempty messages, and an optional boolean stream.", "invalid_request");
        if (body!.model !== env.MODEL_ID) return apiError(404, "The requested model is not available.", "model_not_found");
      }
      if (path === "/deliver") {
        const hasResponse = Object.hasOwn(body!, "response");
        const hasError = Object.hasOwn(body!, "error");
        if (typeof body!.id !== "string" || !UUID.test(body!.id)
          || hasResponse === hasError
          || (hasResponse && !validCompletion(body!.response))
          || (hasError && (!isObject(body!.error) || typeof body!.error.message !== "string" || !body!.error.message.trim()))) {
          return apiError(400, "Provide a claim UUID and either a full chat.completion response or error.message.", "invalid_delivery");
        }
      }
      // Never forward public Authorization headers or tokens into the queue.
      const internal = new Request(`https://broker${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: request.signal,
      });
      const response = await env.BROKER.get(env.BROKER.idFromName("cadge")).fetch(internal);
      if (path === "/v1/chat/completions" && body!.stream === true && response.ok) {
        const includeUsage = isObject(body!.stream_options) && body!.stream_options.include_usage === true;
        return completionStream(await response.json<Completion>(), includeUsage);
      }
      return response;
    } catch (error) {
      if (error instanceof BodyError) return apiError(error.status, error.message, "invalid_request");
      return apiError(503, "The coordinator is unavailable; the request may have been interrupted.", "broker_unavailable");
    }
  },
} satisfies ExportedHandler<Env>;
