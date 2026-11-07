#!/usr/bin/env python3
"""Emit one OpenAI SSE event line for a Cadge streaming delivery.

The worker generates the answer in parts; each part becomes one SSE data
line, written to stdout so the worker can feed it into a running
`deliver.py <job-id> --stream` process stdin. JSON escaping is done here,
never by hand, so story text with quotes or newlines stays valid.

Usage:
  sse_emit.py --id <chatcmpl-id> --model <model> [--role] < part.txt
      Content chunk: reads the part text from stdin, emits one
      data: {"id":...,"object":"chat.completion.chunk",...} line.
      --role adds "role":"assistant" to the delta (first chunk only).
  sse_emit.py --id <chatcmpl-id> --model <model> --finish
      Finish chunk: empty delta, finish_reason "stop". Ignores stdin.
  sse_emit.py --done
      Emits `data: [DONE]`. Ignores stdin and other flags.
  sse_emit.py --error <message>
      Emits an SSE error event. Ignores stdin. Use only after streaming
      started and the answer cannot be finished; do not add [DONE] after it.
"""
import argparse
import json
import sys
import time


def emit(payload):
    sys.stdout.write(f"data: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n\n")
    sys.stdout.flush()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Emit one OpenAI SSE event line.")
    parser.add_argument("--id", dest="chunk_id", default="")
    parser.add_argument("--model", default="cadge")
    parser.add_argument("--role", action="store_true")
    parser.add_argument("--finish", action="store_true")
    parser.add_argument("--done", action="store_true")
    parser.add_argument("--error", default=None)
    args = parser.parse_args(argv)

    if args.done:
        sys.stdout.write("data: [DONE]\n\n")
        sys.stdout.flush()
        return 0
    if args.error is not None:
        emit({"error": {"message": args.error, "type": "agent_error"}})
        return 0
    if not args.chunk_id or not args.model:
        print("sse_emit.py: --id and --model are required (except --done/--error)", file=sys.stderr)
        return 2
    created = int(time.time())
    if args.finish:
        emit({
            "id": args.chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": args.model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        })
        return 0
    content = sys.stdin.read()
    delta = {"content": content}
    if args.role:
        delta = {"role": "assistant", "content": content}
    emit({
        "id": args.chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": args.model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
