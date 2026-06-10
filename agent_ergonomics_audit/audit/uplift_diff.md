# Uplift Diff

This was the first recorded pass, so there is no previous scorecard baseline in the audit workspace.

Observed uplift from pre-pass runtime checks:

- `verify --robot-triage`: unrecognized argument to successful JSON triage.
- `verify capabilities --json`: arity-1 shortcut misparse/unrecognized intent to successful JSON contract.
- `verify robot-docs guide`: arity-1 shortcut misparse/unrecognized intent to successful operational guide.
- Bare `verify`: generic scaffold-only refusal to actionable discovery and run guidance.

Median estimated uplift across the five scored surfaces: 320 points.
