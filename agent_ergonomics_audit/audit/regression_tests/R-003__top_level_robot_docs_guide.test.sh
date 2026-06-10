#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../../.."

VERIFY_BIN="${VERIFY_BIN:-target/debug/verify}"
if [ ! -x "$VERIFY_BIN" ]; then
  cargo build -p verify-cli >/dev/null
fi

output="$("$VERIFY_BIN" robot-docs guide)"
printf '%s\n' "$output" | grep -F 'verify --robot-triage' >/dev/null
printf '%s\n' "$output" | grep -F 'Outcome contract:' >/dev/null
printf '%s\n' "$output" | grep -F 'Use `assess` after verify' >/dev/null
printf '%s\n' "$output" | grep -F 'Use `pack` to bundle reports and evidence.' >/dev/null
