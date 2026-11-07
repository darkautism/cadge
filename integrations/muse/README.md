# Muse Cadge integration

Canonical installable contents adapted from the supplied Cadge hook bundle.
No credential is included. The registered Muse `custom.cadge` connection must
hold the **AGENT_TOKEN**, not the separate client token. Helpers use Muse's real
`dynamic_credentials` surrogate integration; there is no shipped bearer-token
adapter, model/provider call, or automatic claim probe.

## Install

Requirements: Muse's hook runtime and dynamic-credential helper, Python 3,
Bash, and `jq` (polling hook). The file follower also works on macOS; it does
not depend on Linux process-name matching, `tail`, or PID searches.

1. Run `bash /path/to/bundle/install.sh`. It copies the connector to
   `$HOME/workspace/custom-connectors/custom.cadge/`, the polling script to
   `$HOME/hooks/scripts/cadge-beg.sh`, and sets helper executable modes.
   It works from any current directory; source files are resolved relative
   to the installer, while destinations use the actual `$HOME`.
2. Read the installed `custom.cadge/CONNECTOR.md` to register the credential
   using Muse's secure connection flow. Installation never reads or sets it.
3. The installer prints the updated hook JSON path:
   `$HOME/hooks/definitions/hook-cadge-beg.json`. Import/update that JSON
   through Muse's hook configuration to replace the actual runtime prompt.
   It is deliberately **disabled**. Review the delivery destination for the
   target workspace; enable it only by an explicit operator action. No
   remote hook is enabled by installing these files.
4. Do not call `/beg` to check installation: it claims real queued work.
   The parent project owns packaging, verification, and deployment.

All runtime helper paths use the actual `$HOME` or the launcher's own
location; no particular `/home/<user>` is assumed. The existing Muse workspace
layout remains unchanged.

## Files

- `install.sh`: installs local connector/hook files, sets executable modes,
  and prints the hook definition path without enabling a remote hook.
- `hooks/cadge-beg.sh`: one `/beg` per poll, silent on 204, wakes a worker
  with `{id, request, expires_at}` on 200. No upload starts during polling.
- `hooks/hook-cadge-beg.json`: disabled hook and the actual worker prompt.
  The prompt uses the last **user-role** message within the full ordered
  conversation, not the first message or the last array entry. Claims and
  temporary paths stay isolated.
- `custom.cadge/bin/beg.py`: authenticated real claim helper.
- `custom.cadge/bin/deliver.py`: JSON, `--error`, and finite SSE packet modes.
- `custom.cadge/bin/sse_emit.py`: JSON-safe content/finish/error SSE and
  `[DONE]` generation; content comes from stdin.
- `custom.cadge/bin/start_stream.sh`: starts an owned supervisor/follower and
  uploader for one full claim UUID. Does not truncate an existing directory.
- `custom.cadge/bin/stream_session.py`: bounded-memory growing-file follower,
  uploader ownership, completion-marker handling, and final result handoff.
- `custom.cadge/bin/finish_stream.sh`: freezes producer EOF and waits for the
  uploader's actual final ACK without a total-duration deadline.
- `custom.cadge/CONNECTOR.md`, `connector.yaml`: connection and API details.

## Stream workflow

Prepare the actual first answer segment promptly, then start the session:

```sh
BIN="$HOME/workspace/custom-connectors/custom.cadge/bin"
bash "$BIN/start_stream.sh" "$JOB_ID" "$EXPIRES_AT"
```

Save the returned JSON fields: `dir`, `stream_txt`, `eof_marker`,
`finish_helper`, `follower_pid`, `supervisor_pid`, `deliver_pid`, `deliver_log`.
The follower PID is the supervisor PID; the uploader is its actual child.
Directories are `$HOME/hooks/state/stream/<full-claim-uuid>/`, never truncated
or shared based on a short ID prefix.

Append each generated segment immediately. Set `STREAM_TXT` and `STREAM_DIR`
from this session's returned JSON, and `MODEL` from the request (or `cadge`):

```sh
python3 "$BIN/sse_emit.py" --id "chatcmpl-cadge-$JOB_ID" --model "$MODEL" \
  --role < "$FIRST_TEXT_FILE" >> "$STREAM_TXT"
# Subsequent segments use the same invocation without --role.
python3 "$BIN/sse_emit.py" --id "chatcmpl-cadge-$JOB_ID" --model "$MODEL" \
  --finish >> "$STREAM_TXT"
python3 "$BIN/sse_emit.py" --done >> "$STREAM_TXT"
bash "$BIN/finish_stream.sh" "$STREAM_DIR"
```

All append commands must have finished before `finish_stream.sh` is called.
It atomically records the final file byte count at `eof_marker`; the follower
consumes every byte through that offset before closing uploader stdin. Do
not write after the marker, kill the follower to end input, sleep to guess
whether bytes flushed, or pass the growing file directly to `--stream`.
A plain file argument is a finite file, not a file-following interface.

The finish helper prints `{"accepted": true}` only after uploader exit code 0
following the final packet ACK. It returns nonzero on delivery failure;
inspect `deliver_log`. No automatic retry is safe after an ambiguous failure.
After completion remove only this claim's directory and owned text files.
There is no age-based directory deletion: an old directory may still be live.
Abandoned producers must be handled by the operator, not silently timed out.

## Transport and deadlines

Each available nonempty read becomes a finite `POST /deliver` with bytes
unchanged and no batching delay, at most **65536 bytes**. Oversized producer
writes are split. Packets are sequential and never automatically retried:

- `Content-Type: text/event-stream`
- `X-Cadge-Claim-ID: <claim UUID>`
- `X-Cadge-Stream-Sequence: <decimal sequence starting at 0>`
- `X-Cadge-Stream-End: 0` for bytes, `1` for the empty final EOF packet

Each packet receives HTTP 200 `{"accepted": true}`. A nonfinal ACK accepts
only that packet. The uploader prints success only after EOF and final ACK;
`[DONE]` is SSE data, **not** a producer-EOF signal. No SSE is parsed,
rewritten, or supplemented with comments/heartbeats. Memory and pipe buffers
stay bounded; the growing file is on-disk producer storage and can grow while
the network is backpressured.

`start_stream.sh` passes `CADGE_EXPIRES_AT` (Unix milliseconds) to the uploader
as an optional first-byte deadline. The first nonempty packet must reach the
server before the original claim deadline (normally 120 seconds). Then the
server disarms that deadline permanently. There is **no** post-first-byte
producer idle limit or whole-stream duration limit: a 75-second pause and a
stream longer than 120 seconds are allowed. The 30-second network timeout
applies only within each finite HTTP exchange, never while waiting for
producer bytes. It is not a streaming-lifetime workaround.

JSON completion and `--error` modes remain one JSON `POST /deliver`. For JSON
jobs `expires_at` still bounds delivery. After SSE has started, generation
failure should append `sse_emit.py --error <message>` and finish explicitly,
without `[DONE]`; never send a JSON error over an active SSE claim.

## Verification status

This integration's local/live verification and final archive packing are
performed by the parent task after integration. No claims about production
success are inferred from the original bundle's historical notes.
