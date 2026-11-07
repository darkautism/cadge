#!/usr/bin/env bash
# Start one claim-isolated growing-file follower and finite-packet uploader.
# Prints paths and real child PIDs. Append SSE to stream_txt, then call
# finish_stream.sh <dir>; never kill the follower to signal EOF.
set -euo pipefail
BIN_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec python3 "$BIN_DIR/stream_session.py" start "${1:?job id required}" "${2:?expires_at ms required}"
