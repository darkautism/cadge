#!/usr/bin/env bash
# Call only after the final append is closed. This freezes the producer byte
# count, drains every byte to the uploader, and waits for its final ACK.
set -euo pipefail
BIN_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec python3 "$BIN_DIR/stream_session.py" finish "${1:?stream directory required}"
