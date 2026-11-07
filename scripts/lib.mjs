import { readFile } from "node:fs/promises";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function validId(value) {
  return typeof value === "string" && UUID.test(value);
}

export function loadConfig() {
  const rawUrl = process.env.CADGE_URL;
  const token = process.env.CADGE_AGENT_TOKEN;
  if (!rawUrl) throw new Error("CADGE_URL is required (the service origin, without /v1).");
  if (!token || /[^\x21-\x7e]/.test(token)) {
    throw new Error("CADGE_AGENT_TOKEN must be a nonempty bearer token without surrounding whitespace.");
  }
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new Error("CADGE_URL must be an absolute HTTP or HTTPS origin.");
  }
  const loopback = url.hostname === "localhost" || url.hostname === "[::1]"
    || /^127(?:\.\d{1,3}){3}$/.test(url.hostname);
  if (!/^https?:\/\/[^/?#\\\s]+\/?$/i.test(rawUrl) || rawUrl.includes("@")
    || url.search || url.hash || url.pathname !== "/"
    || (url.protocol !== "https:" && !(url.protocol === "http:" && loopback))) {
    throw new Error("CADGE_URL must be an HTTPS origin with no credentials, path, query, or fragment; HTTP is allowed only on loopback for development.");
  }
  return { url: rawUrl.endsWith("/") ? rawUrl.slice(0, -1) : rawUrl, token };
}

export function parseJson(text, label) {
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
}

export function validateCompletion(value) {
  if (!(isObject(value) && typeof value.id === "string" && value.id.length > 0
    && value.object === "chat.completion" && typeof value.model === "string"
    && typeof value.created === "number" && Number.isFinite(value.created)
    && (value.usage === undefined || isObject(value.usage))
    && Array.isArray(value.choices) && value.choices.length > 0
    && value.choices.every(choice => isObject(choice)
      && Number.isInteger(choice.index) && choice.index >= 0
      && (choice.finish_reason === null || typeof choice.finish_reason === "string")
      && isObject(choice.message) && choice.message.role === "assistant"
      && (choice.message.content === undefined || choice.message.content === null || typeof choice.message.content === "string")
      && (choice.message.tool_calls === undefined || (Array.isArray(choice.message.tool_calls)
        && choice.message.tool_calls.every(call => isObject(call) && typeof call.id === "string"
          && call.type === "function" && isObject(call.function)
          && typeof call.function.name === "string" && typeof call.function.arguments === "string")))))) {
    throw new Error("Response must be a full non-streaming OpenAI chat.completion object, including id, object, created, model, and choices.");
  }
  return value;
}

export async function readResponse(source) {
  let text;
  try {
    if (source === "-") {
      const chunks = [];
      process.stdin.setEncoding("utf8");
      for await (const chunk of process.stdin) chunks.push(chunk);
      text = chunks.join("");
    } else {
      text = await readFile(source, "utf8");
    }
  } catch {
    throw new Error("Could not read the response JSON from the supplied file or stdin.");
  }
  return validateCompletion(parseJson(text, "Response"));
}

async function post(config, path, body, signal, streamId) {
  let response;
  let text;
  try {
    response = await fetch(`${config.url}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.token}`,
        ...(streamId === undefined
          ? (body === undefined ? {} : { "Content-Type": "application/json" })
          : { "Content-Type": "text/event-stream", "X-Cadge-Claim-ID": streamId }),
      },
      ...(streamId === undefined
        ? (body === undefined ? {} : { body: JSON.stringify(body) })
        : { body, duplex: "half" }),
      redirect: "error",
      signal,
    });
    text = await response.text();
  } catch {
    if (signal?.aborted) throw signal.reason;
    throw new Error(`POST ${path} failed during the HTTP exchange; its outcome may be unknown. Not retrying.`);
  }
  if (!response.ok) {
    let detail = "";
    try {
      const body = JSON.parse(text);
      if (typeof body?.error?.message === "string") detail = `: ${body.error.message}`;
    } catch {
      // Non-JSON error pages are not useful diagnostics and may contain secrets.
    }
    throw new Error(`POST ${path} returned HTTP ${response.status}${detail}`);
  }
  return { status: response.status, text };
}

export async function beg(config, signal) {
  const { status, text } = await post(config, "/beg", undefined, signal);
  if (status === 204) return null;
  if (status !== 200) throw new Error(`POST /beg returned unexpected HTTP ${status}.`);
  const job = parseJson(text, "POST /beg response");
  if (!isObject(job) || !validId(job.id) || !isObject(job.request)
    || !Number.isSafeInteger(job.expires_at) || job.expires_at <= 0) {
    throw new Error("POST /beg returned an invalid job; expected id, request, and expires_at.");
  }
  return { job, text };
}

export async function deliver(config, body, signal) {
  const { status, text } = await post(config, "/deliver", body, signal);
  return acceptedDelivery(status, text);
}

export async function deliverStream(config, id, body, signal) {
  if (!validId(id)) throw new Error("A claim UUID from /beg is required for streaming delivery.");
  // Node fetch consumes Node Readables and web ReadableStreams incrementally,
  // applying backpressure instead of collecting the SSE upload in memory.
  const { status, text } = await post(config, "/deliver", body, signal, id);
  return acceptedDelivery(status, text);
}

function acceptedDelivery(status, text) {
  if (status !== 200) throw new Error(`POST /deliver returned unexpected HTTP ${status}.`);
  const accepted = parseJson(text, "POST /deliver response");
  if (!isObject(accepted) || accepted.accepted !== true) {
    throw new Error("POST /deliver did not acknowledge the delivery with accepted: true.");
  }
  return text;
}

export function reportError(error) {
  const message = error instanceof Error ? error.message : "Unexpected error.";
  const token = process.env.CADGE_AGENT_TOKEN;
  console.error(token ? message.split(token).join("[redacted]") : message);
  process.exitCode = 1;
}
