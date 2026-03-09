# verify

**Deterministic constraint evaluation for the epistemic spine and factory runtime.**

`verify` evaluates declared constraints against one or more named relations and
emits a deterministic report of passes, failures, and refusals.

It answers one narrow question:

**Do these declared constraints hold for these bound inputs?**

---

## Quickstart

Build from source:

```bash
cargo build --release -p verify-cli
./target/release/verify --help
```

Arity-1 shortcut (compile and evaluate in one step):

```bash
./target/release/verify fixtures/inputs/arity1/loans.csv \
  --rules fixtures/authoring/arity1/not_null_loans.yaml \
  --json
```

General batch path:

```bash
./target/release/verify run fixtures/constraints/query_rules/orphan_rows.verify.json \
  --bind property=fixtures/inputs/arity_n/property_no_orphans.csv \
  --bind tenants=fixtures/inputs/arity_n/tenants.csv \
  --json
```

Compile authoring to a constraint artifact:

```bash
./target/release/verify compile fixtures/authoring/arity1/not_null_loans.yaml \
  --out /tmp/not_null_loans.verify.json
```

Validate a compiled artifact:

```bash
./target/release/verify validate fixtures/constraints/arity1/not_null_loans.verify.json
```

Inspect the embedded schemas and local witness log:

```bash
./target/release/verify --schema > /tmp/verify.report.v1.schema.json
./target/release/verify compile --schema > /tmp/verify.constraint.v1.schema.json
./target/release/verify witness last --json
```

---

## Why verify exists

The spine and factory both need the same constraint primitive:

- one canonical compiled constraint artifact
- one canonical report artifact
- deterministic evaluation semantics
- reusable execution across batch and embedded contexts

You provide:

- one compiled `verify.constraint.v1` artifact
- one or more named bindings
- optional lockfiles for trusted-input verification

`verify` returns:

- one deterministic `verify.report.v1` report
- localized failures by binding, and when available by key and field
- explicit `PASS`, `FAIL`, or `REFUSAL` outcomes
- one protocol usable in both CLI and embedded runtime contexts

---

## What makes this different

- **One primitive, not two products.** Single-input validation and cross-input
  validation are the same protocol with different arity.
- **One compiled contract.** JSON/YAML authoring and SQL authoring both compile
  into `verify.constraint.v1`.
- **Portable and batch-only rules are explicit.** Portable rules mean the same
  thing in batch and embedded execution. Batch-only query rules never silently
  downgrade.
- **Failure localization is first-class.** Reports identify failing rules,
  implicated bindings, and when possible keys, rows, and fields.
- **Deterministic reports.** Same compiled constraints plus same bound inputs
  yield the same ordered report bytes.
- **Clean spine boundaries.** `verify` checks declared constraints. It does not
  score correctness, choose winners, or make proceed/block decisions.

---

## Where verify fits

`verify` sits in the constraint layer of the spine:

```text
normalize / materialize -> verify -> assess -> pack
```

It also has a factory role:

```text
twinning / decoding -> embedded verify -> assess / routing
```

Related tools:

| If you need... | Use |
|----------------|-----|
| Structural comparability before comparison | [`shape`](https://github.com/cmdrvl/shape) |
| Numeric and content deltas between aligned datasets | [`rvl`](https://github.com/cmdrvl/rvl) |
| Gold-set accuracy scoring | `benchmark` |
| Proceed / escalate / block decisions | `assess` |
| Evidence sealing | [`pack`](https://github.com/cmdrvl/pack) |

`verify` only answers:

**Which declared constraints passed, which failed, and where did they fail?**

---

## The three outcomes

`verify` emits exactly one domain outcome:

| Exit | Outcome | Meaning |
|------|---------|---------|
| `0` | `PASS` | all rules passed |
| `1` | `FAIL` | one or more rules failed |
| `2` | `REFUSAL` | the tool could not evaluate safely |

Outcome discipline:

- `PASS` means every rule result passed and `failed_rules = 0`
- `FAIL` means at least one rule result failed
- `REFUSAL` means no partial "best effort" report masquerades as evaluation

---

## Commands

```text
verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH> [--lock <LOCKFILE>] [--json] [--no-witness]
verify <DATASET> --rules <AUTHORING> [--key <FIELD>] [--json] [--no-witness]
verify compile <AUTHORING> [--out <OUTPUT>] [--check] [--json]
verify compile --schema
verify validate <COMPILED_CONSTRAINTS> [--json]
verify witness [ACTION] [--json]
verify --schema
verify --describe   # currently returns a scaffold refusal
```

---

## Compiled constraint artifact

`verify.constraint.v1` is the compiled artifact:

```json
{
  "version": "verify.constraint.v1",
  "constraint_set_id": "example.not_null_loans",
  "bindings": [
    {
      "name": "input",
      "kind": "relation",
      "key_fields": ["loan_id"]
    }
  ],
  "rules": [
    {
      "id": "INPUT_LOAN_ID_PRESENT",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "not_null",
        "binding": "input",
        "columns": ["loan_id"]
      }
    }
  ]
}
```

Portable rule ops:

- `unique`
- `not_null`
- `predicate`
- `row_count`
- `aggregate_compare`
- `foreign_key`

Batch-only rule op:

- `query_zero_rows`

SQL authoring compiles into batch-only rules. Embedded execution refuses
batch-only rules with explicit refusal semantics.

---

## Report contract

JSON report shape (`verify.report.v1`):

```json
{
  "tool": "verify",
  "version": "verify.report.v1",
  "execution_mode": "batch",
  "outcome": "FAIL",
  "constraint_set_id": "example.not_null_loans",
  "constraint_hash": "sha256:...",
  "bindings": {
    "input": {
      "kind": "relation",
      "source": "tape.csv",
      "content_hash": "sha256:...",
      "input_verification": null
    }
  },
  "summary": {
    "total_rules": 1,
    "passed_rules": 0,
    "failed_rules": 1,
    "by_severity": { "error": 1, "warn": 0 }
  },
  "policy_signals": {
    "severity_band": "ERROR_PRESENT"
  },
  "results": [
    {
      "rule_id": "INPUT_LOAN_ID_PRESENT",
      "severity": "error",
      "status": "fail",
      "violation_count": 1,
      "affected": [
        {
          "binding": "input",
          "key": { "loan_id": "LN-42" },
          "field": "loan_id",
          "value": null
        }
      ]
    }
  ],
  "refusal": null
}
```

Human output:

```text
VERIFY FAIL
constraint_set: example.not_null_loans
binding: input=tape.csv
passed_rules: 0
failed_rules: 1
severity_band: ERROR_PRESENT

FAIL INPUT_LOAN_ID_PRESENT binding=input key.loan_id=LN-42 field=loan_id value=null
```

---

## Execution contexts

One primitive with two execution contexts:

### Batch / CLI

- reads bound inputs from disk (CSV, row-oriented JSON, JSONL, and Parquet)
- evaluates portable and batch-only rules
- can verify bound inputs against lockfiles
- is the reference executor

### Embedded / runtime

- receives already-materialized named relations
- evaluates portable rules only
- refuses batch-only rules with explicit refusal semantics
- exists to support factory-time constraint enforcement

The report contract stays the same across both.

---

## Repository layout

```text
verify/
├── .github/workflows/
│   ├── ci.yml
│   └── release.yml
├── crates/
│   ├── verify-core/        # domain types: constraint, report, refusal, ordering
│   ├── verify-engine/      # portable rule evaluation + embedded executor
│   ├── verify-duckdb/      # batch bindings, query_zero_rows, lock verification
│   └── verify-cli/         # CLI surface: run, compile, validate, witness, render
├── fixtures/
│   ├── authoring/          # YAML and SQL authoring fixtures
│   ├── constraints/        # compiled constraint artifacts
│   ├── inputs/             # CSV test datasets
│   ├── locks/              # lock file fixtures
│   └── reports/            # reference report fixtures
├── schemas/
│   ├── verify.constraint.v1.schema.json
│   └── verify.report.v1.schema.json
├── scripts/
│   └── ubs_gate.sh
├── tests/
│   ├── cli.rs              # CLI exit/output integration tests
│   ├── determinism.rs      # byte-identical report determinism proof
│   ├── embedding_equivalence.rs  # batch/embedded parity
│   ├── lock_integration.rs # lock verification tests
│   ├── portable_rules.rs   # full compile→bind→evaluate pipeline
│   ├── query_rules.rs      # query_zero_rows localization tests
│   ├── refusals.rs         # refusal path coverage
│   ├── schema_contract.rs  # fixture/schema round-trip validation
│   ├── gen_fixtures.rs     # deterministic fixture generators
│   └── perf_smoke.rs       # performance guardrail tests
├── Cargo.toml
└── LICENSE
```

---

## What verify is not

`verify` is not:

- a benchmark scorer
- a policy engine
- a canonicalization system
- an extraction pipeline
- a storage/orchestration layer

Fresh-eyes boundary:

- `verify` enforces constraints
- `benchmark` scores correctness against ground truth
- `assess` decides proceed / escalate / block

---

## Quality gates

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
ubs .
```

---

## Contributing

```bash
cd verify
br ready                          # find available work
bv -robot-alerts -alert-type stale_issue
bv -robot-alerts -alert-type blocking_cascade
br show <bead-id>                 # read the spec
br update <id> --status in_progress
# reserve only exact files, implement, and run the relevant gate
br close <id> --reason "Completed"
br sync --flush-only              # non-invasive; stage .beads/ in normal git workflow
```
