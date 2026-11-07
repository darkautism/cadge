#!/usr/bin/env bash
# Install only local files. Never register credentials or enable a remote hook.
set -euo pipefail
BUNDLE_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
CONNECTOR_DIR="$HOME/workspace/custom-connectors/custom.cadge"
HOOK_SCRIPT_DIR="$HOME/hooks/scripts"
HOOK_DEFINITION_DIR="$HOME/hooks/definitions"

mkdir -p "$CONNECTOR_DIR/bin" "$HOOK_SCRIPT_DIR" "$HOOK_DEFINITION_DIR"
cp "$BUNDLE_DIR/custom.cadge/CONNECTOR.md" "$CONNECTOR_DIR/CONNECTOR.md"
cp "$BUNDLE_DIR/custom.cadge/connector.yaml" "$CONNECTOR_DIR/connector.yaml"
for helper in beg.py deliver.py sse_emit.py stream_session.py start_stream.sh finish_stream.sh; do
  cp "$BUNDLE_DIR/custom.cadge/bin/$helper" "$CONNECTOR_DIR/bin/$helper"
  chmod 755 "$CONNECTOR_DIR/bin/$helper"
done
rm -f "$CONNECTOR_DIR/bin/fifo_write.py"
cp "$BUNDLE_DIR/hooks/cadge-beg.sh" "$HOOK_SCRIPT_DIR/cadge-beg.sh"
chmod 755 "$HOOK_SCRIPT_DIR/cadge-beg.sh"
cp "$BUNDLE_DIR/hooks/hook-cadge-beg.json" "$HOOK_DEFINITION_DIR/hook-cadge-beg.json"

printf 'Installed Cadge connector: %s\n' "$CONNECTOR_DIR"
printf 'Updated hook JSON: %s\n' "$HOOK_DEFINITION_DIR/hook-cadge-beg.json"
printf 'Import/update that JSON in Muse to update the runtime prompt. The bundled hook is disabled; no remote hook was enabled.\n'
