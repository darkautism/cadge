#!/usr/bin/env bash
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

# POST /beg is a real claim, never a health check. Keep polling across
# claims; only the hook's explicit enabled setting controls activation.
# The worker starts a finite-packet uploader when its first SSE is ready.
BEG_PY="$HOME/workspace/custom-connectors/custom.cadge/bin/beg.py"
ERROR_FILE="$(mktemp "${TMPDIR:-/tmp}/cadge-beg.XXXXXXXX")"
trap 'rm -f "$ERROR_FILE"' EXIT

output=""
if ! output="$(python3 "$BEG_PY" 2>"$ERROR_FILE")"; then
  # Do not expose credentials or request payloads in hook logs.
  log "cadge beg failed; check connector access" '{}'
  silent "cadge_beg_error"
fi

status="$(printf '%s' "$output" | jq -r '.status // empty')"
if [ "$status" = "204" ]; then
  silent "no_job"
fi

if [ "$status" = "200" ]; then
  job="$(printf '%s' "$output" | jq -c '.job')"
  job_id="$(printf '%s' "$job" | jq -r '.id // empty')"
  if [ -z "$job_id" ]; then
    log "cadge beg returned 200 without a job id" '{}'
    silent "cadge_beg_bad_job"
  fi
  # Never age-delete stream directories: an active stream has no lifetime
  # limit after its first bytes. Each worker owns its full-UUID directory.
  wake "cadge_job_claimed" "$job"
fi

log "cadge beg returned unexpected status" '{}'
silent "cadge_beg_unexpected"
