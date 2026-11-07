export interface Env {
  BROKER: DurableObjectNamespace;
  CLIENT_TOKEN: string;
  AGENT_TOKEN: string;
  MODEL_ID: string;
  REQUEST_TIMEOUT_MS: string;
}

export type JsonObject = Record<string, unknown>;

export interface ChatRequest extends JsonObject {
  model: string;
  messages: JsonObject[];
  stream?: boolean;
}

export interface Completion extends JsonObject {
  id: string;
  object: "chat.completion";
  created: number;
  model: string;
  choices: Array<{
    index: number;
    message: JsonObject;
    finish_reason: string | null;
    logprobs?: unknown;
  }>;
  usage?: JsonObject;
}

export function apiError(status: number, message: string, code: string): Response {
  return Response.json({ error: {
    message,
    type: status >= 500 ? "server_error" : "invalid_request_error",
    param: null,
    code,
  } }, { status, headers: { "Cache-Control": "no-store" } });
}

export function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
