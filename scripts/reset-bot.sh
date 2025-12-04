#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

DEFAULT_SOLANA_BIN="/home/ubuntu/solana-v1.18.17/bin/solana"
SOLANA_BIN=${TRE_SOLANA_BIN:-$DEFAULT_SOLANA_BIN}
LUT_AUTHORITY=${TRE_LUT_AUTHORITY:-"D3Xwz5TeTPWnDjNMV1xZaW1FSexjiVWVWAEwNm69Pibo"}
LUT_KEYPAIR=${TRE_LUT_KEYPAIR:-"/home/ubuntu/cain/wallets/jito-payer.json"}
LUT_RPC_URL=${TRE_LUT_RPC:-"https://virulent-falling-dew.solana-mainnet.quiknode.pro/a1e8ef59e674a8fddcf6eaa79f437d878cbf139f/"}
LUT_OUTPUT_DIR=${TRE_LUT_OUTPUT_DIR:-"$REPO_ROOT/tmp"}
LUT_CLOSE_COOLDOWN=${TRE_LUT_CLOSE_COOLDOWN:-180}
LUT_PROGRAM_ID="AddressLookupTab1e1111111111111111111111111"

if [[ "${TRE_SKIP_LUT_RESET:-0}" != "1" ]]; then
  if ! command -v "$SOLANA_BIN" >/dev/null 2>&1; then
    echo "solana CLI not found (SOLANA_BIN=$SOLANA_BIN); skipping LUT maintenance"
  elif ! command -v curl >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
    echo "curl and jq are required for LUT maintenance; skipping"
  elif [[ ! -f "$LUT_KEYPAIR" ]]; then
    echo "LUT keypair not found at $LUT_KEYPAIR; skipping"
  else
    mkdir -p "$LUT_OUTPUT_DIR"
    lut_timestamp=$(date +%Y%m%d-%H%M%S)
    LUT_FILE="$LUT_OUTPUT_DIR/luts-$lut_timestamp.txt"
    payload=$(cat <<JSON
{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":["$LUT_PROGRAM_ID",{"encoding":"jsonParsed","filters":[{"memcmp":{"offset":22,"bytes":"$LUT_AUTHORITY"}}]}]}
JSON
)
    echo "Fetching lookup tables owned by $LUT_AUTHORITY"
    if ! curl -s "$LUT_RPC_URL" -H 'Content-Type: application/json' -d "$payload" | jq -r '.result[].pubkey' >"$LUT_FILE"; then
      echo "Failed to fetch lookup tables; removing $LUT_FILE"
      rm -f "$LUT_FILE"
    elif [[ ! -s "$LUT_FILE" ]]; then
      echo "No lookup tables found for $LUT_AUTHORITY"
      rm -f "$LUT_FILE"
    else
      lut_count=$(wc -l <"$LUT_FILE")
      echo "Found $lut_count lookup table(s); list saved to $LUT_FILE"

      echo "Starting lookup table deactivation"
      while IFS= read -r LUT_ADDR; do
        [[ -z "$LUT_ADDR" ]] && continue
        if "$SOLANA_BIN" address-lookup-table deactivate "$LUT_ADDR" -k "$LUT_KEYPAIR" -u "$LUT_RPC_URL" --bypass-warning; then
          echo "Deactivated $LUT_ADDR"
        else
          echo "Failed to deactivate $LUT_ADDR" >&2
        fi
      done <"$LUT_FILE"

      if (( LUT_CLOSE_COOLDOWN > 0 )); then
        echo "Waiting $LUT_CLOSE_COOLDOWN seconds before closing lookup tables"
        sleep "$LUT_CLOSE_COOLDOWN"
      fi

      echo "Closing lookup tables"
      while IFS= read -r LUT_ADDR; do
        [[ -z "$LUT_ADDR" ]] && continue
        if "$SOLANA_BIN" address-lookup-table close "$LUT_ADDR" -k "$LUT_KEYPAIR" -u "$LUT_RPC_URL"; then
          echo "Closed $LUT_ADDR"
        else
          echo "Failed to close $LUT_ADDR" >&2
        fi
      done <"$LUT_FILE"
    fi
  fi
else
  echo "Skipping LUT maintenance because TRE_SKIP_LUT_RESET=1"
fi

REGISTRY_PATH=${TRE_ALT_REGISTRY:-"$REPO_ROOT/alt_registry.json"}
if [[ -f "$REGISTRY_PATH" ]]; then
  timestamp=$(date +%Y%m%d-%H%M%S)
  backup_path="${REGISTRY_PATH}.${timestamp}.bak"
  cp "$REGISTRY_PATH" "$backup_path"
  echo "Backed up ALT registry to $backup_path"
fi

rm -f "$REGISTRY_PATH"

echo "Removed ALT registry snapshot at $REGISTRY_PATH"

echo "Lookup table maintenance complete"
