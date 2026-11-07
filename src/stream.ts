import type { Completion, JsonObject } from "./protocol";

// Delivery is a complete response: SSE framing is compatible, not live token generation.
export function completionStream(completion: Completion, includeUsage: boolean): Response {
  const base: JsonObject = {
    id: completion.id,
    object: "chat.completion.chunk",
    created: completion.created,
    model: completion.model,
  };
  for (const key of ["system_fingerprint", "service_tier"]) {
    if (completion[key] !== undefined) base[key] = completion[key];
  }
  const frame = (choices: unknown[], extra: JsonObject = {}) => `data: ${JSON.stringify({
    ...base, choices, ...(includeUsage ? { usage: null } : {}), ...extra,
  })}\n\n`;
  const data = frame(completion.choices.map(choice => {
    const delta = { ...choice.message };
    if (Array.isArray(delta.tool_calls)) {
      delta.tool_calls = delta.tool_calls.map((call, index) => ({ ...(call as JsonObject), index }));
    }
    return { index: choice.index, delta, logprobs: choice.logprobs ?? null, finish_reason: null };
  })) + frame(completion.choices.map(choice => ({
    index: choice.index, delta: {}, logprobs: null, finish_reason: choice.finish_reason,
  }))) + (includeUsage && completion.usage ? frame([], { usage: completion.usage }) : "")
    + "data: [DONE]\n\n";
  return new Response(data, { headers: {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",
  } });
}
