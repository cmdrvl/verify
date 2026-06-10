#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../../.."

VERIFY_BIN="${VERIFY_BIN:-target/debug/verify}"
if [ ! -x "$VERIFY_BIN" ]; then
  cargo build -p verify-cli >/dev/null
fi

"$VERIFY_BIN" capabilities --json | jq -e '
  .schema == "verify.capabilities.v1"
  and .standard_agent_surfaces.robot_triage == "verify --robot-triage"
  and .standard_agent_surfaces.capabilities_json == "verify capabilities --json"
  and .standard_agent_surfaces.robot_docs == "verify robot-docs guide"
  and .protocols.constraint == "verify.constraint.v1"
  and .protocols.report == "verify.report.v1"
  and .commands[3].domain_outcomes["1"] == "FAIL"
' >/dev/null
