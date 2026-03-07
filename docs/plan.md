# verify

This is the implementation-direction plan for the repository.

`docs/PLAN_VERIFY.md` captures the earlier feature-oriented framing inherited
from the broader spine plan. Where the two documents differ, this file should
govern repository structure and protocol boundaries.

## Core definition

`verify` is the epistemic spine's constraint primitive.

Given a versioned constraint set and a set of named relation bindings, `verify`
deterministically evaluates the constraints and emits a structured report.

This is one primitive with two execution contexts:

- **Spine / CLI context**: batch validation over locked artifacts on disk
- **Factory / runtime context**: embedded validation over materialized candidate
  state inside `twinning` and `decoding`

`verify` is not a conflict resolver, not a benchmark scorer, not a policy
engine, and not a storage system.

## Why this repo exists

The spine and factory both need the same thing:

- a canonical constraint artifact
- a canonical violation/report artifact
- deterministic evaluation semantics
- a reusable execution engine

The CLI is the reference executor. The factory embeds the same protocol.

## Hard decisions

### 1. No separate `verify cross` product

There is one `verify` primitive.

Single-artifact validation is just the arity-1 case:

```text
--bind input=tape.csv
```

Cross-artifact validation is the arity-N case:

```text
--bind property=property.json --bind tenants=tenants.csv --bind escalations=escalations.parquet
```

We may keep a `cross` alias later if it helps operator ergonomics, but it is not
a second conceptual tool and not a second protocol.

### 2. One canonical report contract

Every execution mode emits the same report shape:

- same summary fields
- same result ordering
- same refusal envelope
- same binding identity surface

Batch CLI and embedded runtime should differ only in invocation, not in meaning.

### 3. One constraint protocol, two portability tiers

The protocol must support both:

- **portable rules** that can run in CLI and factory runtime
- **batch-only query rules** that are valid in the spine batch executor but not
  embeddable in the factory runtime without lowering/translation

This is stricter than pretending every SQL check is automatically factory-grade.

### 4. `verify` enforces constraints; it does not decide winners

`verify` answers:

- which constraints passed
- which constraints failed
- which entities/rows/fields are implicated
- whether execution refused

`verify` does not answer:

- which claim should win
- whether the output is correct against ground truth
- whether the pipeline should proceed

Those belong to:

- `decoding` for winner selection
- `benchmark` for gold-set accuracy
- `assess` for proceed/escalate/block decisions

## Non-goals

`verify` will not:

- resolve canonical entities
- perform extraction from source documents
- own the tournament scorer
- replace `benchmark`
- store long-term lineage beyond normal witness/pack participation
- become a general-purpose arbitrary rules engine

## Repo shape

Initial repository layout:

```text
verify/
├── docs/
│   └── plan.md
├── schemas/
│   ├── verify.constraint.v1.schema.json
│   └── verify.report.v1.schema.json
├── fixtures/
│   ├── constraints/
│   ├── inputs/
│   └── reports/
├── crates/
│   ├── verify-core/
│   ├── verify-engine/
│   ├── verify-duckdb/
│   └── verify-cli/
└── Cargo.toml
```

### `verify-core`

Owns domain types and protocol contracts:

- constraint artifact types
- report artifact types
- refusal types
- stable sorting and canonical serialization helpers
- schema tests against `schemas/`

No file IO. No DuckDB. No CLI parsing.

### `verify-engine`

Owns deterministic evaluation over bound relations:

- rule execution for portable rules
- violation collection
- summary aggregation
- stable ordering of results and affected records

No filesystem concerns. No command-line concerns.

### `verify-duckdb`

Owns batch bindings and query-backed execution:

- CSV / JSON / JSONL / Parquet bindings
- DuckDB-backed relation materialization
- batch-only query rule execution

This crate is the bridge from on-disk artifacts into `verify-engine`.

### `verify-cli`

Owns user-facing command surface:

- `run`
- `compile`
- `validate`
- `--describe`
- `--schema`

It should stay thin. It wires together `verify-core`, `verify-engine`, and
`verify-duckdb`.

## Core artifacts

### `verify.constraint.v1`

This is the real center of the repo.

The constraint artifact is relation-oriented, not CSV-oriented. Files are only
one way of binding relations at execution time.

Minimum shape:

```json
{
  "version": "verify.constraint.v1",
  "constraint_set_id": "loan_tape.monthly.v1",
  "bindings": [
    { "name": "input", "kind": "relation" }
  ],
  "rules": [
    {
      "id": "UNIQUE_LOAN_ID",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "unique",
        "binding": "input",
        "columns": ["loan_id"]
      }
    },
    {
      "id": "POSITIVE_BALANCE",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "predicate",
        "binding": "input",
        "expr": {
          "gt": [
            { "column": "balance" },
            0
          ]
        }
      }
    },
    {
      "id": "TOTAL_BALANCE",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "aggregate_compare",
        "binding": "input",
        "aggregate": { "sum": "balance" },
        "compare": { "eq": 1500000000.0, "tolerance": 0.01 }
      }
    }
  ]
}
```

### Minimum v1 rule ops

Portable:

- `unique`
- `not_null`
- `predicate`
- `row_count`
- `aggregate_compare`
- `foreign_key`

Batch-only:

- `query_zero_rows`

`query_zero_rows` exists so the batch spine executor can support SQL-heavy
relational checks without pretending those rules are automatically usable inside
the factory runtime. If the factory needs one of those rules, it should be
lowered into portable ops or implemented as a dedicated portable rule kind.

### Rule identity and determinism

Every rule must have:

- stable `id`
- declared `severity`
- declared `portability`

Result ordering must be deterministic:

1. sort by `rule.id`
2. then by affected binding name
3. then by key tuple
4. then by field name

### `verify.report.v1`

This is the single output contract for both execution contexts.

Minimum shape:

```json
{
  "version": "verify.report.v1",
  "outcome": "FAIL",
  "constraint_set_id": "loan_tape.monthly.v1",
  "constraint_hash": "sha256:...",
  "bindings": {
    "input": {
      "kind": "relation",
      "source": "tape.csv",
      "content_hash": "sha256:...",
      "input_verification": {
        "status": "VERIFIED",
        "locks": ["dec.lock.json"]
      }
    }
  },
  "summary": {
    "total_rules": 3,
    "passed_rules": 2,
    "failed_rules": 1,
    "by_severity": {
      "error": 1,
      "warn": 0
    }
  },
  "results": [
    {
      "rule_id": "POSITIVE_BALANCE",
      "severity": "error",
      "status": "fail",
      "violation_count": 1,
      "affected": [
        {
          "binding": "input",
          "key": { "loan_id": "LN-00421" },
          "field": "balance",
          "value": -500.0
        }
      ]
    }
  ],
  "refusal": null
}
```

### Required report properties

Every report must include:

- constraint set identity
- exact binding identity
- exact rule results
- exact refusal, if any

For factory use, the report must also preserve enough structure to map a failed
constraint back to affected entity/bucket candidates. That means `affected`
records are part of the core contract, not an optional pretty-print detail.

## CLI shape

### Primary command

```text
verify run <CONSTRAINTS> --bind <NAME=PATH>... [--lock <LOCKFILE>]... [--json]
```

Examples:

```bash
verify run constraints/loan_tape.monthly.v1.json \
  --bind input=tape.csv \
  --lock dec.lock.json \
  --json

verify run constraints/lease_abstract.v1.json \
  --bind property=property.json \
  --bind tenants=tenants.jsonl \
  --bind escalations=escalations.csv \
  --json
```

### Compile step

```text
verify compile <SOURCE> --out <CONSTRAINTS>
```

Authoring inputs may include:

- simple JSON/YAML rule authoring format
- SQL assertion file for `query_zero_rows`

The compile step exists to make the protocol artifact explicit. We do not want
raw authoring files to silently double as the runtime contract forever.

### Validation and discovery

```text
verify validate <CONSTRAINTS>
verify --schema
verify --describe
```

## Execution contexts

### Spine batch executor

The spine batch executor:

- binds named relations from on-disk files
- verifies lock membership when requested
- evaluates the constraint set
- emits `verify.report.v1`
- appends a normal witness record

This is the reference implementation for deterministic behavior.

### Factory runtime executor

The factory runtime executor is an embedded use of the same protocol:

- `twinning` materializes candidate state as named relations
- the runtime loads `verify.constraint.v1`
- portable rules are evaluated incrementally
- failures map back to affected buckets/entities/fields
- batch-only rules are rejected in embedded mode unless they have been lowered

This is the critical boundary:

- `verify` owns rule semantics and report semantics
- `twinning` owns fast incremental execution strategy
- `decoding` owns winner selection subject to those constraints

## Factory role

In factory terms, `verify` is the constraint oracle and factor surface.

It is used in three places:

1. **Preflight validation** of extracted/intermediate artifacts
2. **Incremental mutation checking** inside `twinning`
3. **Publish gating inputs** for `assess` and factory release criteria

What `verify` contributes to the factory:

- explicit structural constraints
- deterministic failure surfaces
- affected-bucket localization
- consistency metrics

What it does not contribute:

- truth against gold
- decode cascade policy
- escalation routing

## Tournament role

In tournament terms, `verify` is not the winner selector.

It answers:

- is this candidate internally consistent?
- how many structural rules failed?
- how severe were those failures?
- which parts of the output are implicated?

`benchmark` answers correctness against the gold set.

Tournament logic should use `verify` as:

- a hard gate for fatal structural failures
- a penalty signal for lower-severity failures
- an explanatory artifact in evidence packs

Tournament logic should not use `verify` as a substitute for `benchmark`.

A self-consistent answer can still be wrong.

## Build order

### Phase 1: lock the protocol

- write `verify.constraint.v1.schema.json`
- write `verify.report.v1.schema.json`
- implement domain types in `verify-core`
- add canonical serialization and ordering tests

### Phase 2: portable engine

- implement portable rule ops in `verify-engine`
- add golden fixtures
- prove determinism across repeated runs

### Phase 3: batch executor

- implement file bindings in `verify-duckdb`
- implement `verify run`
- implement lock verification surface in reports

### Phase 4: compile surface

- implement `verify compile` for simple authoring files
- add SQL-backed `query_zero_rows` support

### Phase 5: factory embedding contract

- expose embedding API for named in-memory relations
- reject batch-only rules in embedded mode
- prove CLI and embedded mode emit identical results for the same portable rules

## Acceptance criteria for v0

`verify` is ready for first real use when all of this is true:

- one constraint artifact works for arity-1 and arity-N cases
- there is one report contract for batch and embedded execution
- portable rules run identically in CLI and embedded contexts
- batch-only rules are clearly marked and refused in embedded mode
- reports localize failures to affected bindings/keys/fields
- tournament code can consume the summary without custom adapters
- evidence packs can include the constraint artifact and report artifact directly

## The sentence to keep fixed

`verify` is the canonical constraint protocol for the epistemic spine; the CLI
is the reference executor, and the factory embeds the same protocol as its
constraint engine.
