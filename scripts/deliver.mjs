#!/usr/bin/env node
import { deliver, loadConfig, readResponse, reportError, validId } from "./lib.mjs";

const HELP = `Usage: node scripts/deliver.mjs <uuid> [response-file|-]
       node scripts/deliver.mjs <uuid> --error <message>
       node scripts/deliver.mjs --help

Read a full non-streaming OpenAI chat.completion JSON object from the named
file or stdin (the default; '-' also means stdin). POST {id, response} to
/deliver using the claim UUID returned by /beg, not the completion's id.
Alternatively --error sends {id, error: {message}}. A file and --error are
mutually exclusive; quote an error message containing spaces.

On success, print the server's accepted JSON followed by a newline. There are
no retries. Invalid CLI/input, configuration, HTTP, and parse errors go to
stderr and exit nonzero. No response is fabricated or modified.

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
  const [id, mode, message] = args;
  if (!validId(id)) throw new Error("A claim UUID from /beg is required. Use --help for usage.");
  const errorMode = mode === "--error";
  if (errorMode ? args.length !== 3 || !message.trim()
    : args.length > 2 || (mode?.startsWith("-") && mode !== "-")) {
    throw new Error("Use <uuid> [response-file|-] OR <uuid> --error <message>. See --help.");
  }
  const config = loadConfig();
  const body = errorMode
    ? { id, error: { message } }
    : { id, response: await readResponse(mode ?? "-") };
  process.stdout.write(`${await deliver(config, body)}\n`);
}

main().catch(reportError);
