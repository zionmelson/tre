#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

REGISTRY_PATH=${TRE_ALT_REGISTRY:-"$REPO_ROOT/alt_registry.json"}
if [[ -f "$REGISTRY_PATH" ]]; then
  timestamp=$(date +%Y%m%d-%H%M%S)
  backup_path="${REGISTRY_PATH}.${timestamp}.bak"
  cp "$REGISTRY_PATH" "$backup_path"
  echo "Backed up ALT registry to $backup_path"
fi

rm -f "$REGISTRY_PATH"

echo "Removed ALT registry snapshot at $REGISTRY_PATH"

echo "Cleaning previous build artifacts"
cargo clean

echo "Rebuilding tre-arb-bot with fresh templates"
cargo build --bin tre-arb-bot

echo "Starting bot with fresh state"
exec "$SCRIPT_DIR/run-bot.sh" "$@"
