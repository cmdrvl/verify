#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../../.."

VERIFY_BIN="${VERIFY_BIN:-target/debug/verify}"
if [ ! -x "$VERIFY_BIN" ]; then
  cargo build -p verify-cli >/dev/null
fi

"$VERIFY_BIN" --robot-triage | jq -e '
  .schema == "verify.doctor.triage.v1"
  and .tool == "verify"
  and .version == "0.3.0"
  and .summary.failed_checks == 0
' >/dev/null
