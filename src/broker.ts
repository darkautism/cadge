import { apiError, type ChatRequest, type Completion, type Env } from "./protocol";

const CLEANUP_BATCH_SIZE = 128;
const TOMBSTONE_RETENTION_MS = 5 * 60 * 1000;

interface Waiter {
  expiresAt: number;
  resolve: (response: Response) => void;
  timer: number;
  signal: AbortSignal;
  abort: () => void;
}

type JobRow = {
  id: number;
  claim_id: string | null;
  expires_at: number;
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
        claim_id TEXT UNIQUE
      )`);
      this.sql.exec(`CREATE INDEX IF NOT EXISTS jobs_pending
        ON jobs (runtime_id, id) WHERE claim_id IS NULL`);
      this.sql.exec(`CREATE INDEX IF NOT EXISTS jobs_deadline ON jobs (expires_at)`);
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
        return this.deliver(await request.json<Delivery>());
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
    const body = await request.text();
    if (request.signal.aborted) {
      return apiError(499, "The caller disconnected.", "request_cancelled");
    }
    const expiresAt = Date.now() + Number(this.env.REQUEST_TIMEOUT_MS ?? "120000");
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
      expiresAt,
      resolve,
      signal: request.signal,
      abort,
      timer: setTimeout(() => this.onTimeout(id), Math.max(0, expiresAt - Date.now())),
    };
    this.waiters.set(id, waiter);
    request.signal.addEventListener("abort", abort, { once: true });
    await this.scheduleAlarm();
    return response;
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
    const now = Date.now();
    const job = this.sql.exec<JobRow>(
      `SELECT id, claim_id, expires_at, request_json, runtime_id
       FROM jobs WHERE claim_id = ?`, delivery.id,
    ).toArray()[0];
    if (!job) {
      const expired = this.sql.exec<{ claim_id: string }>(
        `SELECT claim_id FROM expired_claims WHERE claim_id = ? AND purge_at > ?`,
        delivery.id, now,
      ).toArray()[0];
      return expired
        ? apiError(410, "The caller is no longer waiting.", "request_expired")
        : apiError(404, "Unknown or already delivered claim.", "claim_not_found");
    }
    const waiter = this.waiters.get(job.id);
    if (job.expires_at <= now || !waiter || waiter.signal.aborted) {
      this.expire(job.id, this.timeoutResponse());
      await this.scheduleAlarm();
      return apiError(410, "The caller is no longer waiting.", "request_expired");
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

  private onTimeout(id: number): void {
    const waiter = this.waiters.get(id);
    if (!waiter) return;
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
    this.settle(id, response);
  }

  private settle(id: number, response: Response): void {
    const waiter = this.waiters.get(id);
    if (!waiter) return;
    this.waiters.delete(id);
    clearTimeout(waiter.timer);
    waiter.signal.removeEventListener("abort", waiter.abort);
    waiter.resolve(response);
  }

  private cleanup(now: number): void {
    const expired = this.sql.exec<{ id: number }>(
      `SELECT id FROM jobs WHERE expires_at <= ? OR runtime_id <> ?
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
    const { deadline } = this.sql.exec<{ deadline: number | null }>(
      `SELECT MIN(deadline) AS deadline FROM (
         SELECT MIN(expires_at) AS deadline FROM jobs
         UNION ALL
         SELECT MIN(purge_at) AS deadline FROM expired_claims
       )`,
    ).one();
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
