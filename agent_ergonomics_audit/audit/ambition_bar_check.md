# Ambition Bar Check

Pass 1 meets the focused release bar because it converts the missing standard agent surfaces into tested, documented, release-smoked commands.

The pass deliberately avoided protocol or engine behavior changes:

- `verify.constraint.v1` and `verify.report.v1` are unchanged.
- Portable and batch-only rule semantics are unchanged.
- Witness append behavior for evaluation commands is unchanged.
- `verify --describe` remains scaffold-only, matching current repo documentation.

Next pass focus: decide whether `verify --describe` should become a real spine composition manifest.
