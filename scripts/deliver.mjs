#!/usr/bin/env node
import { createReadStream } from "node:fs";
import { deliver, deliverStream, loadConfig, readResponse, reportError, validId } from "./lib.mjs";

const HELP = `Usage: node scripts/deliver.mjs <uuid> [response-file|-]
       node scripts/deliver.mjs <uuid> --stream [sse-file|-]
       node scripts/deliver.mjs <uuid> --error <message>
       node scripts/deliver.mjs --help

Read a full non-streaming OpenAI chat.completion JSON object from the named
file or stdin (the default; '-' also means stdin). POST {id, response} to
/deliver using the claim UUID returned by /beg, not the completion's id.
With --stream, upload the named file or stdin (the default) as raw OpenAI SSE,
unchanged and without buffering, using text/event-stream and X-Cadge-Claim-ID.
Alternatively --error sends {id, error: {message}}. JSON input, --stream, and
--error are mutually exclusive; quote an error message containing spaces.

After the upload reaches EOF and the server acknowledges it, print the server's
accepted JSON followed by a newline. There are no retries. Invalid CLI/input,
configuration, HTTP, and parse errors go to stderr and exit nonzero. No response
is fabricated or modified; streaming input is not parsed or validated as SSE.

Required environment:
  CADGE_URL          Service origin, without /v1 or any other path.
                     HTTPS in production; HTTP only on loopback for development.
  CADGE_AGENT_TOKEN  Agent bearer token (never placed in the URL).
`;

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(HELP);
    return;
  }
  const [id, mode, value] = args;
  if (!validId(id)) throw new Error("A claim UUID from /beg is required. Use --help for usage.");
  const errorMode = mode === "--error";
  const streamMode = mode === "--stream";
  if (errorMode ? args.length !== 3 || !value.trim()
    : streamMode ? args.length > 3 || (value?.startsWith("-") && value !== "-")
      : args.length > 2 || (mode?.startsWith("-") && mode !== "-")) {
    throw new Error("Use <uuid> [response-file|-], <uuid> --stream [sse-file|-], OR <uuid> --error <message>. See --help.");
  }
  const config = loadConfig();
  let accepted;
  if (streamMode) {
    const source = value === undefined || value === "-" ? process.stdin : createReadStream(value);
    try {
      accepted = await deliverStream(config, id, source);
    } finally {
      source.destroy();
    }
  } else {
    const body = errorMode
      ? { id, error: { message: value } }
      : { id, response: await readResponse(mode ?? "-") };
    accepted = await deliver(config, body);
  }
  process.stdout.write(`${accepted}\n`);
}

main().catch(reportError);
