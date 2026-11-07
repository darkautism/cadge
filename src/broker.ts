import { apiError, type ChatRequest, type Completion, type Env } from "./protocol";

const CLEANUP_BATCH_SIZE = 128;
const TOMBSTONE_RETENTION_MS = 5 * 60 * 1000;

interface StreamRelay {
  push: (chunk: Uint8Array) => Promise<void>;
  close: () => void;
  stop: (error: Error, code: string) => void;
}

interface Waiter {
  requestId: string;
  expiresAt: number;
  streaming: boolean;
  streamStarted: boolean;
  relay?: StreamRelay;
  resolve: (response: Response) => void;
  timer: number;
  signal: AbortSignal;
  abort: () => void;
}

type JobRow = {
  id: number;
  claim_id: string | null;
  expires_at: number;
  stream_started: number;
  request_json: string;
  runtime_id: string;
};

type Delivery =
  | { id: string; response: Completion }
  | { id: string; error: { message: string } };

export class Broker {
  private readonly sql: SqlStorage;
  private readonly runtimeId = crypto.randomUUID();
  private readonly waiters = new Map<number, Waiter>();
  private readonly jobsByRequestId = new Map<string, number>();
  private readonly cancelledRequests = new Map<string, number>();
  private readonly delivering = new Set<string>();
  private alarmDeadline: number | null | undefined;
  private alarmWrite: Promise<void> | undefined;

  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {
    this.sql = state.storage.sql;
    state.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        runtime_id TEXT NOT NULL,
        request_json TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        claim_id TEXT UNIQUE,
        stream_started INTEGER NOT NULL DEFAULT 0
      )`);
      const columns = this.sql.exec<{ name: string }>("PRAGMA table_info(jobs)").toArray();
      if (!columns.some(column => column.name === "stream_started")) {
        this.sql.exec("ALTER TABLE jobs ADD COLUMN stream_started INTEGER NOT NULL DEFAULT 0");
      }
      this.sql.exec(`CREATE INDEX IF NOT EXISTS jobs_pending
        ON jobs (runtime_id, id) WHERE claim_id IS NULL`);
      this.sql.exec("DROP INDEX IF EXISTS jobs_deadline");
      this.sql.exec(`CREATE INDEX IF NOT EXISTS jobs_waiting_deadline
        ON jobs (expires_at) WHERE stream_started = 0`);
      this.sql.exec(`CREATE TABLE IF NOT EXISTS expired_claims (
        claim_id TEXT PRIMARY KEY,
        purge_at INTEGER NOT NULL
      )`);
      this.sql.exec(`CREATE INDEX IF NOT EXISTS expired_claims_deadline
        ON expired_claims (purge_at)`);
      // A restarted instance cannot restore the HTTP responses of its predecessor.
      this.cleanup(Date.now());
      await this.scheduleAlarm();
    });
  }

  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return apiError(405, "Only POST is supported.", "method_not_allowed");
    }
    switch (new URL(request.url).pathname) {
      case "/v1/chat/completions":
        return this.enqueue(request);
      case "/beg":
        return this.claim();
      case "/deliver":
        return request.headers.get("Content-Type")?.split(";", 1)[0]?.trim().toLowerCase() === "text/event-stream"
          ? this.deliverStream(request)
          : this.deliver(await request.json<Delivery>());
      case "/cancel":
        return this.cancel(request);
      default:
        return apiError(404, "Not found.", "not_found");
    }
  }

  async alarm(): Promise<void> {
    // The platform consumed this alarm; its previous deadline is no longer set.
    this.alarmDeadline = undefined;
    this.alarmWrite = undefined;
    this.cleanup(Date.now());
    await this.scheduleAlarm();
  }

  private async enqueue(request: Request): Promise<Response> {
    const requestId = request.headers.get("X-Cadge-Request-ID");
    const expiresAt = Number(request.headers.get("X-Cadge-Request-Deadline"));
    if (!requestId || !Number.isSafeInteger(expiresAt) || expiresAt <= 0) {
      return apiError(400, "Missing internal request correlation.", "invalid_request");
    }
    const body = await request.text();
    // An explicit cancellation can beat the enqueue fetch or its body read.
    const cancelled = this.cancelledRequests.delete(requestId);
    if (cancelled || request.signal.aborted) {
      if (cancelled) await this.scheduleAlarm();
      return apiError(499, "The caller disconnected.", "request_cancelled");
    }
    // Once a marker expires, even a very late enqueue cannot revive its job.
    if (expiresAt <= Date.now()) return this.timeoutResponse();
    const { id } = this.sql.exec<{ id: number }>(
      `INSERT INTO jobs (runtime_id, request_json, expires_at)
       VALUES (?, ?, ?) RETURNING id`,
      this.runtimeId, body, expiresAt,
    ).one();
    const { promise: response, resolve } = Promise.withResolvers<Response>();
    const abort = () => {
      this.expire(id, apiError(499, "The caller disconnected.", "request_cancelled"));
      this.state.waitUntil(this.scheduleAlarm());
    };
    const waiter: Waiter = {
      requestId,
      expiresAt,
      resolve,
      streaming: (JSON.parse(body) as ChatRequest).stream === true,
      streamStarted: false,
      signal: request.signal,
      abort,
      timer: setTimeout(() => this.onTimeout(id), Math.max(0, expiresAt - Date.now())),
    };
    this.waiters.set(id, waiter);
    this.jobsByRequestId.set(requestId, id);
    request.signal.addEventListener("abort", abort, { once: true });
    if (request.signal.aborted) abort();
    await this.scheduleAlarm();
    return response;
  }

  private async cancel(request: Request): Promise<Response> {
    const requestId = request.headers.get("X-Cadge-Request-ID");
    const deadline = Number(request.headers.get("X-Cadge-Request-Deadline"));
    if (!requestId || !Number.isSafeInteger(deadline) || deadline <= 0) {
      return apiError(400, "Missing internal request correlation.", "invalid_request");
    }
    const id = this.jobsByRequestId.get(requestId);
    if (id !== undefined) {
      this.expire(id, apiError(499, "The caller disconnected.", "request_cancelled"));
    } else if (deadline > Date.now()) {
      // This can also be a late cancel after delivery: retain only an ephemeral
      // marker, never a new SQL job or prompt. The enqueue deadline bounds it.
      this.cancelledRequests.set(requestId, deadline);
    }
    await this.scheduleAlarm();
    return new Response(null, { status: 204 });
  }

  private async claim(): Promise<Response> {
    const now = Date.now();
    this.cleanup(now);
    // Selection and claim are one SQLite statement: no two agents can win a job.
    const claimed = this.sql.exec<JobRow>(
      `UPDATE jobs SET claim_id = ?
       WHERE id = (
         SELECT id FROM jobs
         WHERE runtime_id = ? AND claim_id IS NULL AND expires_at > ?
         ORDER BY id LIMIT 1
       )
       RETURNING id, claim_id, expires_at, request_json, runtime_id`,
      crypto.randomUUID(), this.runtimeId, now,
    ).toArray()[0];
    await this.scheduleAlarm();
    if (!claimed) {
      return new Response(null, {
        status: 204,
        headers: { "Retry-After": "5", "Cache-Control": "no-store" },
      });
    }
    return Response.json({
      id: claimed.claim_id,
      request: JSON.parse(claimed.request_json) as ChatRequest,
      expires_at: claimed.expires_at,
    }, { headers: { "Cache-Control": "no-store" } });
  }

  private async deliver(delivery: Delivery): Promise<Response> {
    if (this.delivering.has(delivery.id)) {
      return apiError(404, "Unknown or already delivered claim.", "claim_not_found");
    }
    const now = Date.now();
    const job = this.sql.exec<JobRow>(
      `SELECT id, claim_id, expires_at, stream_started, request_json, runtime_id
       FROM jobs WHERE claim_id = ?`, delivery.id,
    ).toArray()[0];
    if (!job) {
      const expired = this.sql.exec<{ claim_id: string }>(
        `SELECT claim_id FROM expired_claims WHERE claim_id = ? AND purge_at > ?`,
        delivery.id, now,
      ).toArray()[0];
      return expired
        ? Response.json({ accepted: true }, { headers: { "Cache-Control": "no-store" } })
        : apiError(404, "Unknown or already delivered claim.", "claim_not_found");
    }
    const waiter = this.waiters.get(job.id);
    if ((!job.stream_started && job.expires_at <= now) || !waiter || waiter.signal.aborted) {
      // The agent need not know its caller left: discard the result and acknowledge it.
      this.expire(job.id, this.timeoutResponse());
      await this.scheduleAlarm();
      return Response.json({ accepted: true }, { headers: { "Cache-Control": "no-store" } });
    }
    if (waiter.streaming && !("error" in delivery)) {
      return apiError(409, "This claim requires an SSE upload.", "stream_required");
    }
    // Delete before resolving, with no await: every subsequent delivery sees 404.
    this.sql.exec("DELETE FROM jobs WHERE id = ?", job.id);
    const result = "error" in delivery
      ? apiError(502, delivery.error.message, "agent_error")
      : Response.json(delivery.response, { headers: { "Cache-Control": "no-store" } });
    this.settle(job.id, result);
    await this.scheduleAlarm();
    return Response.json({ accepted: true }, { headers: { "Cache-Control": "no-store" } });
  }

  private async deliverStream(request: Request): Promise<Response> {
    const claimId = request.headers.get("X-Cadge-Claim-ID");
    if (!claimId || !request.body) {
      return apiError(400, "Missing claim ID or SSE upload body.", "invalid_request");
    }
    if (this.delivering.has(claimId)) {
      return apiError(404, "Unknown or already delivered claim.", "claim_not_found");
    }
    const now = Date.now();
    const job = this.sql.exec<JobRow>(
      `SELECT id, claim_id, expires_at, stream_started, request_json, runtime_id
       FROM jobs WHERE claim_id = ?`, claimId,
    ).toArray()[0];
    let waiter = job ? this.waiters.get(job.id) : undefined;
    if (!job) {
      const expired = this.sql.exec<{ claim_id: string }>(
        `SELECT claim_id FROM expired_claims WHERE claim_id = ? AND purge_at > ?`,
        claimId, now,
      ).toArray()[0];
      if (!expired) {
        return apiError(404, "Unknown or already delivered claim.", "claim_not_found");
      }
    } else if ((!job.stream_started && job.expires_at <= now) || !waiter || waiter.signal.aborted) {
      this.expire(job.id, this.timeoutResponse());
      waiter = undefined;
    } else if (!waiter.streaming) {
      return apiError(409, "This claim did not request streaming.", "stream_not_requested");
    }

    // Reserve before yielding, including discarded uploads. Cancellation removes
    // the SQL job, but this reservation rejects duplicates until the upload ends.
    this.delivering.add(claimId);
    let relay: StreamRelay | undefined;
    if (waiter && job) {
      const downstream = this.createRelay(job.id);
      relay = downstream.relay;
      waiter.relay = relay;
      // Keep the waiter, deadline and request correlation for the body lifetime.
      waiter.resolve(new Response(downstream.body, {
        headers: {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-store",
          "X-Accel-Buffering": "no",
        },
      }));
    }
    const upload = this.pumpUpload(request.body, claimId, job?.id, relay);
    // Client cancellation must not cancel the producer: continue reading to EOF.
    this.state.waitUntil(upload.catch(() => {}));
    try {
      await upload;
      return Response.json({ accepted: true }, { headers: { "Cache-Control": "no-store" } });
    } catch {
      return apiError(502, "The agent stream upload failed.", "agent_stream_error");
    }
  }

  private createRelay(id: number): { body: ReadableStream<Uint8Array>; relay: StreamRelay } {
    let controller: ReadableStreamDefaultController<Uint8Array>;
    let ready = Promise.withResolvers<void>();
    let stopped = false;
    const body = new ReadableStream<Uint8Array>({
      start: (streamController) => { controller = streamController; },
      pull: () => { ready.resolve(); },
      cancel: () => {
        stopped = true;
        ready.resolve();
        this.expire(id, apiError(499, "The caller disconnected.", "request_cancelled"));
        this.state.waitUntil(this.scheduleAlarm());
      },
    }, { highWaterMark: 0 });
    const relay: StreamRelay = {
      push: async (chunk) => {
        if (stopped) return;
        await ready.promise;
        if (stopped) return;
        ready = Promise.withResolvers<void>();
        controller.enqueue(chunk);
      },
      close: () => {
        if (stopped) return;
        stopped = true;
        ready.resolve();
        controller.close();
      },
      stop: (error, code) => {
        if (stopped) return;
        stopped = true;
        // Release forwarding, then expose failure as SSE: HTTP proxies may turn
        // controller.error() into an indistinguishable clean EOF.
        ready.resolve();
        controller.enqueue(new TextEncoder().encode(`\n\ndata: ${JSON.stringify({ error: {
          message: error.message, type: "server_error", param: null, code,
        } })}\n\n`));
        controller.close();
      },
    };
    return { body, relay };
  }

  private async pumpUpload(
    body: ReadableStream<Uint8Array>,
    claimId: string,
    jobId: number | undefined,
    relay: StreamRelay | undefined,
  ): Promise<void> {
    const reader = body.getReader();
    let firstChunk = true;
    try {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        if (chunk.value.byteLength === 0) continue;
        if (firstChunk) {
          firstChunk = false;
          if (jobId !== undefined && relay) this.startStream(jobId);
        }
        // One input chunk at a time; no JSON parsing, decoding or accumulation.
        if (relay) await relay.push(chunk.value);
      }
      relay?.close();
    } catch (error) {
      relay?.stop(new Error("The agent stream upload failed."), "agent_stream_error");
      throw error;
    } finally {
      reader.releaseLock();
      if (jobId !== undefined) {
        this.sql.exec("DELETE FROM jobs WHERE id = ?", jobId);
        this.detachWaiter(jobId);
      }
      this.delivering.delete(claimId);
      await this.scheduleAlarm();
    }
  }

  private startStream(id: number): void {
    const waiter = this.waiters.get(id);
    if (!waiter || waiter.streamStarted) return;
    if (waiter.expiresAt <= Date.now()) {
      this.expire(id, this.timeoutResponse());
      return;
    }
    waiter.streamStarted = true;
    clearTimeout(waiter.timer);
    this.sql.exec("UPDATE jobs SET stream_started = 1 WHERE id = ?", id);
    this.state.waitUntil(this.scheduleAlarm());
  }

  private onTimeout(id: number): void {
    const waiter = this.waiters.get(id);
    if (!waiter || waiter.streamStarted) return;
    const remaining = waiter.expiresAt - Date.now();
    if (remaining > 0) {
      waiter.timer = setTimeout(() => this.onTimeout(id), remaining);
      return;
    }
    this.expire(id, this.timeoutResponse());
    this.state.waitUntil(this.scheduleAlarm());
  }

  private expire(id: number, response: Response): void {
    this.state.storage.transactionSync(() => {
      const job = this.sql.exec<{ claim_id: string | null }>(
        "SELECT claim_id FROM jobs WHERE id = ?", id,
      ).toArray()[0];
      if (job?.claim_id) {
        this.sql.exec(
          "INSERT OR IGNORE INTO expired_claims (claim_id, purge_at) VALUES (?, ?)",
          job.claim_id, Date.now() + TOMBSTONE_RETENTION_MS,
        );
      }
      this.sql.exec("DELETE FROM jobs WHERE id = ?", id);
    });
    this.waiters.get(id)?.relay?.stop(
      new Error(response.status === 504 ? "The request timed out." : "The caller disconnected."),
      response.status === 504 ? "request_timeout" : "request_cancelled",
    );
    this.settle(id, response);
  }

  private settle(id: number, response: Response): void {
    this.detachWaiter(id)?.resolve(response);
  }

  private detachWaiter(id: number): Waiter | undefined {
    const waiter = this.waiters.get(id);
    if (!waiter) return undefined;
    this.waiters.delete(id);
    this.jobsByRequestId.delete(waiter.requestId);
    clearTimeout(waiter.timer);
    waiter.signal.removeEventListener("abort", waiter.abort);
    return waiter;
  }

  private cleanup(now: number): void {
    for (const [requestId, deadline] of this.cancelledRequests) {
      if (deadline <= now) this.cancelledRequests.delete(requestId);
    }
    const expired = this.sql.exec<{ id: number }>(
      `SELECT id FROM jobs WHERE (stream_started = 0 AND expires_at <= ?) OR runtime_id <> ?
       ORDER BY id LIMIT ?`, now, this.runtimeId, CLEANUP_BATCH_SIZE,
    ).toArray();
    for (const job of expired) this.expire(job.id, this.timeoutResponse());
    this.sql.exec(
      `DELETE FROM expired_claims WHERE claim_id IN (
         SELECT claim_id FROM expired_claims WHERE purge_at <= ?
         ORDER BY purge_at LIMIT ?
       )`, now, CLEANUP_BATCH_SIZE,
    );
  }

  private scheduleAlarm(): Promise<void> {
    const orphan = this.sql.exec<{ id: number }>(
      "SELECT id FROM jobs WHERE runtime_id <> ? LIMIT 1", this.runtimeId,
    ).toArray()[0];
    let { deadline } = this.sql.exec<{ deadline: number | null }>(
      `SELECT MIN(deadline) AS deadline FROM (
         SELECT MIN(expires_at) AS deadline FROM jobs WHERE stream_started = 0
         UNION ALL
         SELECT MIN(purge_at) AS deadline FROM expired_claims
       )`,
    ).one();
    for (const cancellationDeadline of this.cancelledRequests.values()) {
      deadline = deadline === null ? cancellationDeadline : Math.min(deadline, cancellationDeadline);
    }
    const target = orphan
      ? Date.now() + 1
      : deadline === null ? null : Math.max(Date.now() + 1, deadline);
    if (target === this.alarmDeadline && this.alarmWrite) return this.alarmWrite;
    // Issue the storage write synchronously with the SQL read. Repeated polls
    // share its promise instead of spending another alarm row write.
    const operation = target === null
      ? this.state.storage.deleteAlarm()
      : this.state.storage.setAlarm(target);
    this.alarmDeadline = target;
    const write = operation.catch((error: unknown) => {
      // An older failed write must not invalidate a newer scheduling decision.
      if (this.alarmWrite === write) {
        this.alarmDeadline = undefined;
        this.alarmWrite = undefined;
      }
      throw error;
    });
    this.alarmWrite = write;
    return write;
  }

  private timeoutResponse(): Response {
    return apiError(504, "The request timed out waiting for an agent.", "request_timeout");
  }
}
