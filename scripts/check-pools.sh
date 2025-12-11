#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

VALIDATIONS=(
  "config_inventory_valid:Config inventory"
  "alt_registry_ready_and_consistent:ALT registry readiness"
  "alt_registry_accounts_exist_on_chain:ALT accounts exist on-chain"
  "idl_program_ids_match_expectations:IDL program IDs"
  "swap_template_sets_cover_both_directions:Instruction template coverage"
)

total=${#VALIDATIONS[@]}
index=1

for entry in "${VALIDATIONS[@]}"; do
  name=${entry%%:*}
  description=${entry#*:}
  echo "==> [${index}/${total}] ${description}"
  cargo test --test check_pools "$name" -- --nocapture
  ((index++))
done

echo "✅ All pool validations passed"
