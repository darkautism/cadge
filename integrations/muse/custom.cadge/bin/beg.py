#!/usr/bin/env python3
"""Claim one Cadge job via POST /beg.

Usage: python3 beg.py
Output on stdout: {"status": 204} when no job is waiting,
or {"status": 200, "job": {"id": ..., "request": ..., "expires_at": ...}} when a job is claimed.
Errors go to stderr and exit nonzero. No credential value is printed.
"""
import json
import sys
import urllib.request

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request, read_response_body


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise RuntimeError("redirect refused, verify the endpoint")


def main():
    request = urllib.request.Request("https://cadge.isekai.date/beg", method="POST")
    # Browser Integrity Check blocks Python-urllib/3.12. Use a browser User-Agent.
    request.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
    request.add_header("Accept", "application/json")
    add_surrogate_to_request(request, "custom.cadge", allowed_hosts=["cadge.isekai.date"])
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            body = read_response_body(response)
            if response.status == 204:
                print(json.dumps({"status": 204}))
                return 0
            if response.status != 200:
                print(f"POST /beg returned HTTP {response.status}", file=sys.stderr)
                return 1
            job = json.loads(body.decode("utf-8"))
            print(json.dumps({"status": 200, "job": job}))
            return 0
    except Exception as exc:
        print(f"POST /beg failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
