#!/usr/bin/env python3
"""Own a growing-file follower and uploader, with explicit producer completion.

The supervisor is the follower; its uploader is an actual Popen child. No PID
searches, tail flush guesses, producer idle limit, or total stream deadline.
All state is private to the full claim UUID. A completed marker freezes the
byte count only after the producer has closed its final append.
"""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

BIN = Path(__file__).resolve().parent
BLOCK_SIZE = 65536
POLL_SECONDS = 0.05


def publish(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def start(job_id, expires_at):
    claim = str(uuid.UUID(job_id))
    if not math.isfinite(float(expires_at)):
        raise ValueError("expires_at must be finite Unix milliseconds")
    base = Path.home() / "hooks/state/stream"
    base.mkdir(parents=True, exist_ok=True)
    directory = base / claim
    # A repeated start must not truncate an existing claim or its live stream.
    directory.mkdir(mode=0o700)
    (directory / "stream.txt").touch(mode=0o600)
    environment = os.environ.copy()
    environment["CADGE_EXPIRES_AT"] = expires_at
    with (directory / "deliver.log").open("ab", buffering=0) as log:
        supervisor = subprocess.Popen(
            [sys.executable, str(BIN / "stream_session.py"), "run", str(directory), claim],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            env=environment, start_new_session=True,
        )
    # Ready is published after the supervisor owns a real uploader child,
    # before it follows the growing file.
    ready = directory / "ready.json"
    while not ready.exists():
        if supervisor.poll() is not None:
            raise RuntimeError(f"stream supervisor failed; see {directory / 'deliver.log'}")
        time.sleep(POLL_SECONDS)
    print(json.dumps(read_json(ready)), flush=True)
    return 0


def follow(directory, uploader):
    marker = directory / "producer-complete.json"
    position = 0
    with (directory / "stream.txt").open("rb", buffering=0) as source:
        while True:
            if uploader.poll() is not None:
                raise RuntimeError("uploader exited before explicit producer EOF")
            end = None
            if marker.exists():
                end = read_json(marker)["byte_count"]
                if not isinstance(end, int) or end < position:
                    raise RuntimeError("producer completion byte count is invalid")
            size = BLOCK_SIZE if end is None else min(BLOCK_SIZE, end - position)
            if size == 0:
                return
            data = source.read(size)
            if data:
                # The unbuffered pipe may accept only a prefix. Never drop it.
                view = memoryview(data)
                while view:
                    written = uploader.stdin.write(view)
                    if not written:
                        raise BrokenPipeError("uploader input closed")
                    view = view[written:]
                position += len(data)
                continue
            if source.seek(0, os.SEEK_END) < position:
                raise RuntimeError("stream file was truncated")
            source.seek(position)
            if end is not None:
                raise RuntimeError("completed stream is shorter than its byte count")
            time.sleep(POLL_SECONDS)


def run(directory, claim):
    uploader = None
    code = 3
    try:
        uploader = subprocess.Popen(
            [sys.executable, str(BIN / "deliver.py"), claim, "--stream", "-"],
            stdin=subprocess.PIPE, bufsize=0,
        )
        publish(directory / "ready.json", {
            "dir": str(directory),
            "stream_txt": str(directory / "stream.txt"),
            "eof_marker": str(directory / "producer-complete.json"),
            "finish_helper": str(BIN / "finish_stream.sh"),
            "supervisor_pid": os.getpid(),
            "follower_pid": os.getpid(),
            "deliver_pid": uploader.pid,
            "deliver_log": str(directory / "deliver.log"),
        })
        follow(directory, uploader)
        uploader.stdin.close()
        # EOF causes the empty final POST. There is no total-duration timeout.
        code = uploader.wait()
    except Exception as exc:
        print(f"stream follower failed: {exc}", file=sys.stderr, flush=True)
        if uploader is not None:
            if uploader.poll() is None:
                # A follower failure must not turn a truncated stream into an
                # accepted final packet by gracefully closing the input.
                uploader.terminate()
            uploader.wait()
    finally:
        publish(directory / "result.json", {"accepted": code == 0, "returncode": code})
    return code


def finish(directory):
    ready = read_json(directory / "ready.json")
    marker = directory / "producer-complete.json"
    if not marker.exists():
        # The producer must have closed its final append before calling us.
        publish(marker, {"byte_count": (directory / "stream.txt").stat().st_size})
    result_path = directory / "result.json"
    while not result_path.exists():
        try:
            os.kill(ready["supervisor_pid"], 0)
        except ProcessLookupError:
            # Recheck after the liveness check: result is published before exit.
            if not result_path.exists():
                raise RuntimeError(f"stream supervisor disappeared; see {ready['deliver_log']}")
        time.sleep(POLL_SECONDS)
    result = read_json(result_path)
    if result.get("accepted") is True and result.get("returncode") == 0:
        print(json.dumps({"accepted": True}), flush=True)
        return 0
    print(f"stream delivery failed; see {ready['deliver_log']}", file=sys.stderr)
    return result.get("returncode") or 3


def main(argv):
    try:
        if len(argv) == 4 and argv[1] == "start":
            return start(argv[2], argv[3])
        if len(argv) == 4 and argv[1] == "run":
            return run(Path(argv[2]), argv[3])
        if len(argv) == 3 and argv[1] == "finish":
            return finish(Path(argv[2]))
        raise ValueError("Usage: stream_session.py start <claim> <expires_at_ms> | finish <dir>")
    except Exception as exc:
        print(f"stream session failed: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
