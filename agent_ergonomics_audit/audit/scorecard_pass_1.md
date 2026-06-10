# Verify Agent Ergonomics Scorecard, Pass 1

Scope: standard agent-facing discovery surfaces and bare-root recovery behavior.

## Summary

- Surfaces inventoried: 105
- Surfaces scored: 5
- Recommendations applied: 5 of 5
- Regressions detected: 0
- Intent corpus result after changes: 0 silent failures, 0 useless errors, 168 useful hints, 3 inferred-and-acted cases

## Highest-Impact Changes

1. `verify --robot-triage` now works as the top-level read-only triage command.
2. `verify capabilities --json` exposes protocol, command, exit-code, witness, and composition contracts.
3. `verify robot-docs guide` explains safe agent command selection.
4. Bare `verify` now returns actionable next commands instead of generic scaffold text.
5. `verify --help`, README, CI, and release smoke tests advertise the standard agent surfaces.

## Residual Risk

This pass did not implement `verify --describe`; the repo currently documents that flag as scaffold-only. Evaluation commands still append witness records by default, as designed.
