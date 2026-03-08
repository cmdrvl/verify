# verify

**Deterministic constraint evaluation for the epistemic spine and factory runtime.**

`verify` is the spine tool for checking declared constraints against one or more
named relations and emitting a deterministic report of passes, failures, and
refusals.

It answers one narrow question:

**Do these declared constraints hold for these bound inputs?**

Current status:

- repository status: spec-complete, implementation not scaffolded yet
- source of truth: [docs/plan.md](./docs/plan.md)
- current repo contents: plan + Beads execution graph, not a released CLI yet

The examples below describe the target `v0` contract. They are the
implementation target, not a claim that a published binary exists today.

---

## Current quickstart

There is no installable `verify` binary yet. The current quickstart is for
contributors and implementers:

```bash
cd verify
sed -n '1,320p' docs/plan.md
br ready
```

If you are here to use the eventual CLI, read the contract below as the target
surface. If you are here to build it, start with the plan and the Beads graph.

---

## Why verify exists

The spine and factory both need the same constraint primitive:

- one canonical compiled constraint artifact
- one canonical report artifact
- deterministic evaluation semantics
- reusable execution across batch and embedded contexts

Before `verify`, this need was split across old single-file validation framing,
SQL-only cross-check language, and factory-local constraint talk. `verify`
collapses that into one protocol.

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
- **Portable and batch-only rules are explicit.** Portable rules must mean the
  same thing in batch and embedded execution. Batch-only query rules never
  silently downgrade.
- **Failure localization is first-class.** Reports identify failing rules,
  implicated bindings, and when possible keys, rows, and fields.
- **Deterministic reports.** Same compiled constraints plus same bound inputs
  must yield the same ordered report bytes.
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

## How verify compares

| Capability | verify | `benchmark` | `rvl` | ad hoc SQL checks |
|------------|--------|-------------|-------|-------------------|
| Checks declared constraints | ✅ | ❌ | ❌ | ⚠️ inconsistent |
| Emits localized failures | ✅ | ⚠️ gold-set misses only | ⚠️ diffs only | ⚠️ depends |
| Works in batch and embedded modes | ✅ | ❌ | ❌ | ❌ |
| Deterministic structured report | ✅ | ✅ | ✅ | ❌ |
| Chooses winners or policy outcomes | ❌ | ❌ | ❌ | ❌ |

Use `verify` when the question is "did the declared rules hold?" not "did the
output match gold truth?" and not "what changed between old and new?"

---

## What verify is not

`verify` is not:

- a benchmark scorer
- a policy engine
- a canonicalization system
- an extraction pipeline
- a storage/orchestration layer
- a long-term provenance graph

Fresh-eyes boundary that matters:

- `verify` enforces constraints
- `benchmark` scores correctness against ground truth
- `assess` decides proceed / escalate / block

If those blur together, the tool boundary is wrong.

---

## Target v0 workflow

Arity-1 shortcut:

```bash
verify tape.csv --rules rules.yaml --bind-key loan_id --json
```

General batch path:

```bash
verify run compiled.verify.json \
  --bind loans=tape.csv \
  --bind property=property.parquet \
  --lock delivery.lock.json \
  --json
```

Authoring compile path:

```bash
verify compile rules.yaml --output compiled.verify.json
```

Validation path:

```bash
verify validate compiled.verify.json
```

---

## Target compiled artifact

`verify v0` centers on one compiled artifact family:

```json
{
  "version": "verify.constraint.v1",
  "rules": [
    {
      "id": "loans.not_null.loan_id",
      "severity": "critical",
      "portability": "portable",
      "kind": "not_null",
      "binding": "loans",
      "field": "loan_id"
    }
  ],
  "bindings": {
    "loans": {
      "source": "tape.csv",
      "key_fields": ["loan_id"]
    }
  }
}
```

Portable rule kinds in v0:

- `unique`
- `not_null`
- `predicate`
- `row_count`
- `aggregate_compare`
- `foreign_key`

Explicit batch-only rule kind in v0:

- `query_zero_rows`

Important boundary:

- SQL authoring is allowed
- SQL authoring compiles into batch-only rules
- embedded execution must refuse batch-only rules instead of approximating them

---

## Target report contract

Target JSON shape:

```json
{
  "version": "verify.report.v1",
  "tool": "verify",
  "execution_mode": "batch",
  "outcome": "FAIL",
  "constraint_hash": "sha256:1a2b3c...",
  "summary": {
    "total_rules": 3,
    "passed_rules": 2,
    "failed_rules": 1,
    "by_severity": {
      "critical": 1,
      "major": 0,
      "minor": 0
    }
  },
  "policy_signals": {
    "severity_band": "critical"
  },
  "results": [
    {
      "rule_id": "loans.not_null.loan_id",
      "status": "FAIL",
      "severity": "critical",
      "binding": "loans",
      "field": "loan_id",
      "violations": [
        {
          "key": {
            "loan_id": "LN-42"
          },
          "field": "loan_id"
        }
      ]
    }
  ],
  "input_verification": null,
  "refusal": null
}
```

Target human output:

```text
VERIFY FAIL
rules: 3
passed: 2
failed: 1
severity_band: critical
top_failure: loans.not_null.loan_id
```

---

## The three outcomes

`verify` should emit exactly one domain outcome:

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

## Execution contexts

This is one primitive with two execution contexts:

### Batch / CLI

- reads bound inputs from disk
- can evaluate portable and batch-only rules
- can verify bound inputs against lockfiles
- is the reference executor

### Embedded / runtime

- receives already-materialized named relations
- can evaluate portable rules only
- must refuse batch-only rules with explicit refusal semantics
- exists to support factory-time constraint enforcement, not winner selection

The report contract stays the same across both.

---

## Supported v0 input discipline

Planned supported batch binding formats:

| Format | Status in v0 |
|--------|--------------|
| CSV | supported target |
| JSON | supported target if row-oriented |
| JSONL | supported target |
| Parquet | supported target |
| nested / document-shaped JSON | out of scope for v0 |

Important non-goals:

- no arbitrary backend plugins
- no heuristic flattening of nested document structures
- no silent format guessing when extension-based detection is ambiguous

---

## Lock and witness participation

`verify` participates in two spine evidence surfaces:

- **lock**: optional trusted-input verification before evaluation
- **witness**: local run receipt logging only

Important boundary:

- `lock` and `pack` remain portable evidence artifacts
- `witness` is supplemental local context only
- `verify` should not invent its own provenance system

---

## Planned repository shape

Target implementation layout:

```text
verify/
├── docs/
│   ├── plan.md
│   └── PLAN_VERIFY.md
├── schemas/
│   ├── verify.constraint.v1.schema.json
│   └── verify.report.v1.schema.json
├── fixtures/
│   ├── authoring/
│   ├── constraints/
│   ├── inputs/
│   ├── locks/
│   └── reports/
├── crates/
│   ├── verify-core/
│   ├── verify-engine/
│   ├── verify-duckdb/
│   └── verify-cli/
├── tests/
│   ├── schema_contract.rs
│   ├── portable_rules.rs
│   ├── query_rules.rs
│   ├── refusals.rs
│   ├── lock_integration.rs
│   ├── cli.rs
│   ├── embedding_equivalence.rs
│   └── determinism.rs
└── Cargo.toml
```

The first implementation bead creates this skeleton so later agents can work in
parallel without fighting over structure.

---

## Contributing right now

Until the crate lands, the useful work in this repo is:

- tightening the plan in [docs/plan.md](./docs/plan.md)
- improving the execution graph in [.beads/issues.jsonl](./.beads/issues.jsonl)
- keeping future-facing docs aligned with the actual implementation target

Contributor loop:

```bash
cd verify
br ready
br show bd-3fq
```

When code lands, the standard Rust gate will apply:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
ubs .
```
