#!/usr/bin/env node
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { beg, deliver, deliverStream, loadConfig, parseJson, reportError, validateCompletion } from "./lib.mjs";

const MAX_TIMER_MS = 2_147_483_647;
const KILL_GRACE_MS = 1_000;
const HELP = `Usage: node scripts/poll.mjs -- command [args...]
       node scripts/poll.mjs --help

Poll /beg repeatedly and run the supplied trusted executable once per job,
without a shell. Write the full original job.request JSON plus a newline to
its stdin. All request fields (including stream) are preserved. The command
must read that request. For stream:true, stdout must be raw OpenAI SSE and is
uploaded immediately, byte-for-byte with backpressure. Otherwise stdout must
be exactly one full non-streaming OpenAI chat.completion JSON object. Send logs
to stderr, which is forwarded. No model, provider, or inference implementation
is supplied by these hooks.

The child inherits the environment, with CADGE_JOB_ID set to the claim UUID
and CADGE_EXPIRES_AT set to the Unix deadline in milliseconds. It is terminated
at that deadline. SIGINT/SIGTERM stop polling and terminate the current child;
termination escalates to SIGKILL after 1 second (including the process group
on POSIX). Stopping does not submit a result or start another command.

Non-streaming stdout is delivered as {id, response}; child failure or invalid
JSON stdout is delivered as {id, error: {message}}. For streaming jobs, startup
failure is delivered as JSON error before an upload starts. Failure after the
upload starts appends an SSE error event, finishes that upload, then exits
nonzero; there is no second delivery or fabricated answer/[DONE]. Streaming
stdout is not parsed or validated as SSE. After a successful delivery, poll
again immediately. After HTTP 204, wait CADGE_POLL_INTERVAL_MS. Network/delivery
errors exit without retrying an ambiguous claim or delivery. The poller itself
produces no success stdout.

Required environment:
  CADGE_URL               Service origin, without /v1 or any other path.
                          HTTPS in production; HTTP only on loopback for development.
  CADGE_AGENT_TOKEN       Agent bearer token (never placed in the URL).
Optional environment:
  CADGE_POLL_INTERVAL_MS  Positive finite integer milliseconds (default 5000).
                          For example, 10000 polls every ten seconds when idle.
`;

function runCommand(config, command, args, job, stopSignal) {
  if (stopSignal.aborted) return Promise.resolve(null);
  if (Date.now() >= job.expires_at) {
    return Promise.resolve({ error: { message: "The job expired before the agent command could start." } });
  }
  return new Promise((resolve, reject) => {
    const processGroup = process.platform !== "win32";
    const streaming = job.request.stream === true;
    let child;
    try {
      child = spawn(command, args, {
        shell: false,
        detached: processGroup,
        stdio: ["pipe", "pipe", "inherit"],
        env: {
          ...process.env,
          CADGE_JOB_ID: job.id,
          CADGE_EXPIRES_AT: String(job.expires_at),
        },
      });
    } catch {
      resolve({ error: { message: "Could not start the agent command." } });
      return;
    }
    let closed = false;
    let terminating = false;
    let streamingStarted = false;
    let failure;
    let deadlineTimer;
    let killTimer;
    const chunks = streaming ? null : [];
    let finishChild;
    const childFinished = new Promise(finish => { finishChild = finish; });

    function kill(signal) {
      try {
        if (processGroup && child.pid) process.kill(-child.pid, signal);
        else child.kill(signal);
      } catch {
        // The child or its process group may have exited before the signal.
      }
    }

    function terminate() {
      if (closed || terminating) return;
      terminating = true;
      kill("SIGTERM");
      killTimer = setTimeout(() => {
        kill("SIGKILL");
        // Descendants must not keep our pipes open after forced termination.
        child.stdin.destroy();
        child.stdout.destroy();
      }, KILL_GRACE_MS);
    }

    function armDeadline() {
      const remaining = job.expires_at - Date.now();
      if (remaining <= 0) {
        failure ??= "The agent command exceeded the job expiry deadline.";
        terminate();
      } else {
        deadlineTimer = setTimeout(armDeadline, Math.min(remaining, MAX_TIMER_MS));
      }
    }

    function stop() {
      terminate();
    }

    async function* streamOutput() {
      try {
        for await (const chunk of child.stdout) yield chunk;
      } catch {
        failure ??= "Could not read the agent command's stdout.";
        terminate();
      }
      const result = await childFinished;
      if (!stopSignal.aborted && result.error) {
        // Separate the error event even if the failed producer left a partial frame.
        yield Buffer.from(`\n\nevent: error\ndata: ${JSON.stringify({ error: result.error })}\n\n`);
      }
    }

    if (streaming) {
      child.once("spawn", () => {
        if (stopSignal.aborted) return;
        streamingStarted = true;
        // Starting only after spawn keeps startup errors on the JSON error path.
        deliverStream(config, job.id, streamOutput(), stopSignal).then(async () => {
          const result = await childFinished;
          resolve(stopSignal.aborted ? null : { streamed: true, ...result });
        }, async error => {
          terminate();
          await childFinished;
          if (stopSignal.aborted) resolve(null);
          else reject(error);
        });
      });
    } else {
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", chunk => chunks.push(chunk));
    }
    child.stdout.on("error", () => {
      failure ??= "Could not read the agent command's stdout.";
      terminate();
    });
    child.stdin.on("error", () => {
      failure ??= "Could not write the original request to the agent command's stdin.";
      terminate();
    });
    child.once("error", () => {
      failure ??= "Could not start or communicate with the agent command.";
      terminate();
    });
    child.once("close", (code, signal) => {
      closed = true;
      clearTimeout(deadlineTimer);
      clearTimeout(killTimer);
      if (terminating && processGroup) kill("SIGKILL");
      stopSignal.removeEventListener("abort", stop);
      if (!failure && code !== 0) {
        failure = signal
          ? `The agent command was terminated by ${signal}.`
          : `The agent command exited with code ${code}.`;
      }
      const result = failure ? { error: { message: failure } } : {};
      finishChild(result);
      if (streamingStarted) return;
      if (stopSignal.aborted) {
        resolve(null);
        return;
      }
      if (failure) {
        resolve(result);
        return;
      }
      try {
        const response = validateCompletion(parseJson(chunks.join(""), "Agent command stdout"));
        resolve({ response });
      } catch (error) {
        resolve({ error: { message: error.message } });
      }
    });
    stopSignal.addEventListener("abort", stop, { once: true });
    if (stopSignal.aborted) stop();
    armDeadline();
    child.stdin.end(`${JSON.stringify(job.request)}\n`);
  });
}

async function idle(interval, signal) {
  const until = Date.now() + interval;
  while (!signal.aborted) {
    const remaining = until - Date.now();
    if (remaining <= 0) return;
    await delay(Math.min(remaining, MAX_TIMER_MS), undefined, { signal });
  }
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(HELP);
    return;
  }
  if (args[0] !== "--" || args.length < 2 || !args[1]) {
    throw new Error("Usage: node scripts/poll.mjs -- command [args...] (or --help).");
  }
  const config = loadConfig();
  const interval = process.env.CADGE_POLL_INTERVAL_MS === undefined
    ? 5000 : Number(process.env.CADGE_POLL_INTERVAL_MS);
  if (!Number.isFinite(interval) || !Number.isInteger(interval) || interval <= 0) {
    throw new Error("CADGE_POLL_INTERVAL_MS must be a positive finite integer number of milliseconds.");
  }
  const stop = new AbortController();
  const onSignal = () => stop.abort();
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  try {
    while (!stop.signal.aborted) {
      const claimed = await beg(config, stop.signal);
      if (stop.signal.aborted) break;
      if (!claimed) {
        await idle(interval, stop.signal);
        continue;
      }
      const result = await runCommand(config, args[1], args.slice(2), claimed.job, stop.signal);
      if (stop.signal.aborted || result === null) break;
      if (!result.streamed) await deliver(config, { id: claimed.job.id, ...result }, stop.signal);
      if (result.error) throw new Error(`${result.error.message} The error was delivered to Cadge.`);
    }
  } catch (error) {
    if (!stop.signal.aborted) throw error;
  } finally {
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
  }
}

main().catch(reportError);
