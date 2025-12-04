#!/usr/bin/env bash
set -euo pipefail

# Wrapper that keeps the classic run script behavior while always enabling dry mode.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"${SCRIPT_DIR}/run-bot.sh" --dry-run "$@"
