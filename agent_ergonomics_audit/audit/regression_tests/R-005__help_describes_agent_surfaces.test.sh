#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../../.."

VERIFY_BIN="${VERIFY_BIN:-target/debug/verify}"
if [ ! -x "$VERIFY_BIN" ]; then
  cargo build -p verify-cli >/dev/null
fi

help="$("$VERIFY_BIN" --help)"
printf '%s\n' "$help" | grep -F -- '--robot-triage' >/dev/null
printf '%s\n' "$help" | grep -F 'capabilities' >/dev/null
printf '%s\n' "$help" | grep -F 'robot-docs' >/dev/null
printf '%s\n' "$help" | grep -F 'Emit the machine-readable CLI capabilities contract' >/dev/null
printf '%s\n' "$help" | grep -F 'Print agent-facing operational guidance' >/dev/null
