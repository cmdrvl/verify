#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../../.."

VERIFY_BIN="${VERIFY_BIN:-target/debug/verify}"
if [ ! -x "$VERIFY_BIN" ]; then
  cargo build -p verify-cli >/dev/null
fi

set +e
stderr="$("$VERIFY_BIN" 2>&1 >/dev/null)"
status="$?"
set -e

[ "$status" -eq 2 ]
printf '%s\n' "$stderr" | grep -F 'no command or dataset was provided' >/dev/null
printf '%s\n' "$stderr" | grep -F 'verify --robot-triage' >/dev/null
printf '%s\n' "$stderr" | grep -F 'verify capabilities --json' >/dev/null
printf '%s\n' "$stderr" | grep -F 'verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH>' >/dev/null
printf '%s\n' "$stderr" | grep -F 'For help with verify' >/dev/null
