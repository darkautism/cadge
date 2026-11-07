#!/usr/bin/env python3
"""Deliver a claimed Cadge job with Muse dynamic credentials.

Usage: deliver.py <claim-uuid> <completion.json|->
       deliver.py <claim-uuid> --stream [sse-file|-]
       deliver.py <claim-uuid> --error <message>

Stream bytes are sent unchanged in sequential finite POSTs, at most 65536
bytes each, with no retries. Each available read is sent immediately; EOF
sends an empty final packet. Only the final ACK prints {"accepted": true}.
CADGE_EXPIRES_AT optionally sets the first-byte deadline (Unix milliseconds).
After the first nonempty read there is no producer idle or lifetime timeout.
The 30-second socket timeout applies only during each finite HTTP exchange.
Exit codes: 0 accepted, 1 HTTP rejection, 2 usage/validation, 3 transport failure.
"""
import json
import math
import os
import select
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request, read_response_body


DELIVER_URL = os.environ.get("CADGE_DELIVER_URL", "https://cadge.isekai.date/deliver")
SOCKET_TIMEOUT_SECS = 30
READ_BLOCK_SIZE = 65536
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise RuntimeError("redirect refused, verify the endpoint")


def validate_completion(value):
    if not (isinstance(value, dict) and isinstance(value.get("id"), str) and value.get("id")
            and value.get("object") == "chat.completion" and isinstance(value.get("model"), str)
            and isinstance(value.get("created"), (int, float))
            and isinstance(value.get("choices"), list) and len(value["choices"]) > 0):
        raise ValueError("Response must be a full chat.completion object.")
    for choice in value["choices"]:
        if not (isinstance(choice, dict) and isinstance(choice.get("message"), dict)
                and choice["message"].get("role") == "assistant"):
            raise ValueError("Each choice message must have role assistant.")
    return value


def post(data, headers):
    """Send exactly one finite bytes request. Never retry an ambiguous ACK."""
    request = urllib.request.Request(
        DELIVER_URL, data=data, method="POST",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json", **headers},
    )
    add_surrogate_to_request(
        request, "custom.cadge",
        allowed_hosts=[urllib.parse.urlparse(DELIVER_URL).hostname or "cadge.isekai.date"],
    )
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=SOCKET_TIMEOUT_SECS) as response:
            payload = json.loads(read_response_body(response).decode("utf-8"))
            if response.status != 200 or not isinstance(payload, dict) or payload.get("accepted") is not True:
                print(f"POST /deliver returned HTTP {response.status}: {payload}", file=sys.stderr)
                return 1
            return 0
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            error = json.loads(read_response_body(exc).decode("utf-8")).get("error")
            if isinstance(error, dict):
                detail = f": {error.get('code', '')} {error.get('message', '')}"
        except Exception:
            pass
        print(f"POST /deliver returned HTTP {exc.code}{detail}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"POST /deliver failed: {exc}", file=sys.stderr)
        return 3


def deliver_json(job_id, source):
    try:
        if source == "-":
            text = sys.stdin.read()
        else:
            with open(source, encoding="utf-8") as completion:
                text = completion.read()
        response_obj = validate_completion(json.loads(text))
    except Exception as exc:
        print(f"Invalid completion: {exc}", file=sys.stderr)
        return 2
    return post(json.dumps({"id": job_id, "response": response_obj}).encode("utf-8"),
                {"Content-Type": "application/json"})


def first_byte_deadline():
    expires_at = os.environ.get("CADGE_EXPIRES_AT")
    if expires_at is None:
        return None
    deadline = float(expires_at) / 1000
    if not math.isfinite(deadline):
        raise ValueError("CADGE_EXPIRES_AT must be finite Unix milliseconds")
    return deadline


def deliver_stream(job_id, source):
    raw = None
    try:
        deadline = first_byte_deadline()
        raw = sys.stdin.buffer if source in ("-", None) else open(source, "rb")
        sequence = 0
        while True:
            # Wait only before the first bytes. os.read returns available data
            # without waiting for a full block, unlike a buffered pipe read.
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError("claim first-byte deadline elapsed")
                ready, _, _ = select.select([raw], [], [], remaining)
                if not ready:
                    raise TimeoutError("claim first-byte deadline elapsed")
            data = os.read(raw.fileno(), READ_BLOCK_SIZE)
            if data:
                deadline = None
            result = post(data, {
                "Content-Type": "text/event-stream",
                "X-Cadge-Claim-ID": job_id,
                "X-Cadge-Stream-Sequence": str(sequence),
                "X-Cadge-Stream-End": "0" if data else "1",
            })
            if result != 0:
                return result
            if not data:
                return 0
            sequence += 1
    except Exception as exc:
        print(f"POST /deliver stream failed: {exc}", file=sys.stderr)
        return 3
    finally:
        if raw is not None and raw is not sys.stdin.buffer:
            raw.close()


def deliver_error(job_id, message):
    return post(json.dumps({"id": job_id, "error": {"message": message}}).encode("utf-8"),
                {"Content-Type": "application/json"})


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    job_id = argv[1]
    if len(argv) >= 3 and argv[2] == "--stream":
        if len(argv) > 4 or (len(argv) == 4 and argv[3].startswith("-") and argv[3] != "-"):
            print("Usage: deliver.py <claim-uuid> --stream [sse-file|-]", file=sys.stderr)
            return 2
        result = deliver_stream(job_id, argv[3] if len(argv) == 4 else "-")
    elif len(argv) >= 3 and argv[2] == "--error":
        if len(argv) != 4 or not argv[3].strip():
            print("Usage: deliver.py <claim-uuid> --error <message>", file=sys.stderr)
            return 2
        result = deliver_error(job_id, argv[3])
    elif len(argv) <= 3:
        result = deliver_json(job_id, argv[2] if len(argv) == 3 else "-")
    else:
        print(__doc__, file=sys.stderr)
        return 2
    if result == 0:
        print(json.dumps({"accepted": True}))
    return result


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
