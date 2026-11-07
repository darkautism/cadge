#!/usr/bin/env node
import { beg, loadConfig, reportError } from "./lib.mjs";

const HELP = `Usage: node scripts/beg.mjs
       node scripts/beg.mjs --help

Poll once for an agent job. On HTTP 200, stdout is the original job JSON
followed by a newline: {id, request, expires_at}. request contains the full
original OpenAI chat request; expires_at is a Unix timestamp in milliseconds.
On HTTP 204, produce no stdout and exit successfully. Do not sleep or retry.

Required environment:
  CADGE_URL          Service origin, without /v1 or any other path.
                     HTTPS in production; HTTP only on loopback for development.
  CADGE_AGENT_TOKEN  Agent bearer token (never placed in the URL).

Configuration, HTTP, and malformed response errors go to stderr and exit nonzero.
`;

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(HELP);
    return;
  }
  if (args.length !== 0) throw new Error("Usage: node scripts/beg.mjs (or --help).");
  const claimed = await beg(loadConfig());
  if (claimed) process.stdout.write(`${claimed.text}\n`);
}

main().catch(reportError);
