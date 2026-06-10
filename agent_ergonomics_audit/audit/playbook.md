# Verify Agent Playbook

Use these commands in order when approaching an unfamiliar Verify checkout:

1. `verify --robot-triage`
2. `verify capabilities --json`
3. `verify robot-docs guide`
4. `verify compile AUTHORING.yaml --check --json`
5. `verify run COMPILED.verify.json --bind input=data.csv --json --no-witness`

Keep the witness boundary explicit:

- Discovery, schema, validate, and doctor surfaces are read-only.
- Run/evaluate commands append a local witness record unless `--no-witness` is set.
- `0` means PASS, `1` means FAIL, and `2` means REFUSAL.

Composition rule:

- `verify` enforces declared constraints.
- `benchmark` scores correctness against ground truth.
- `assess` decides proceed, escalate, or block.
- `pack` bundles reports and evidence.
