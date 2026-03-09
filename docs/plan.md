# verify — Constraint Protocol

This is the implementation-direction plan for the repository.

`docs/PLAN_VERIFY.md` captures the earlier feature-oriented framing inherited
from the broader spine plan. Where the two documents differ, this file should
govern repository structure and protocol boundaries.

## One-line promise

**Evaluate a versioned constraint set against one or more named relations and
emit a deterministic constraint report with localized failures.**

## Decision

`verify` is the epistemic spine's constraint primitive.

It evaluates declared constraints. It does not select winners, score gold-set
accuracy, or decide whether a pipeline should proceed.

## Problem

The spine and factory both need the same thing:

- a canonical constraint artifact
- a canonical violation/report artifact
- deterministic evaluation semantics
- a reusable execution engine

Today that need is scattered across older single-file rule framing, SQL-backed
cross-artifact checks, and factory-local constraint talk. `verify` exists to
collapse those into one explicit protocol that works in both the spine and the
factory.

## V0 scope discipline

V0 is intentionally narrow:

- one canonical compiled constraint artifact: `verify.constraint.v1`
- one canonical report artifact: `verify.report.v1`
- one portable rule family plus one explicit batch-only rule family
- one batch executor over files and one embedded executor over named relations

Deferred beyond v0:

- arbitrary user-defined execution backends
- a general-purpose policy language
- graph-shaped constraint validation
- hosted orchestration concerns
- rules lifecycle machinery beyond content hashing and evidence packing

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

## Non-negotiables

These are engineering contracts, not aspirations. If any are violated,
`verify` is not `verify` yet.

1. One primitive only. Arity-1 and arity-N are execution cases of the same
   protocol, not separate products.
2. No hidden semantics split. JSON/YAML authoring and SQL authoring compile into
   one compiled constraint artifact family.
3. Portable rules are truly portable. A rule marked `portable` must evaluate
   with the same meaning in batch and embedded execution.
4. Batch-only rules stay explicit. Embedded execution must refuse them rather
   than silently ignoring or approximating them.
5. Failure localization is first-class. Failed results must identify affected
   bindings and, when available, keys and fields.
6. Reports are deterministic. Same bindings + same compiled constraint bytes
   produce the same ordered report bytes.
7. `verify` never becomes a correctness scorer. Gold truth belongs to
   `benchmark`, and policy decisions belong to `assess`.

## Non-goals

`verify` will not:

- resolve canonical entities
- perform extraction from source documents
- own the tournament scorer
- replace `benchmark`
- store long-term lineage beyond normal witness/pack participation
- become a general-purpose arbitrary rules engine

## Tool category

`verify` is a **report tool**.

- default stdout: human-readable summary
- `--json`: machine-readable full report
- stderr: process diagnostics only, never evidence

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
- `witness`
- `--describe`
- `--schema`
- `--version`

It should stay thin. It wires together `verify-core`, `verify-engine`, and
`verify-duckdb`.

### Dependency direction

- `verify-core` -> no internal crate dependencies
- `verify-engine` -> `verify-core`
- `verify-duckdb` -> `verify-core`, `verify-engine`
- `verify-cli` -> `verify-core`, `verify-engine`, `verify-duckdb`

`verify-cli` should only map command inputs and exit codes. Rule semantics,
report construction, and deterministic ordering must live below the CLI layer.

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
    { "name": "input", "kind": "relation", "key_fields": ["loan_id"] }
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

Compiled constraint artifacts are the runtime contract. They are what gets
validated, hashed, packed, and embedded.

#### Constraint top-level fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `version` | string | yes | Must be `verify.constraint.v1` |
| `constraint_set_id` | string | yes | Stable logical identifier for the constraint set |
| `bindings` | array | yes | Declared named relations required by the constraint set |
| `rules` | array | yes | Ordered rule declarations; rule IDs must be unique |

#### Binding fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | Logical relation name used by rules |
| `kind` | string | yes | V0 only allows `relation` |
| `key_fields` | string[] | no | Canonical localization key for failed rows |

#### Rule fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | string | yes | Stable rule identifier, unique within the set |
| `severity` | string | yes | `error` or `warn` |
| `portability` | string | yes | `portable` or `batch_only` |
| `check` | object | yes | Rule payload; shape depends on `op` |

### Authoring inputs and compile contract

V0 may accept two authoring families:

- simple JSON/YAML authoring for portable rules
- SQL assertion files for `query_zero_rows`

Those are authoring surfaces, not the canonical runtime contract. `verify
compile` must normalize them into `verify.constraint.v1` so that:

- rule IDs, severities, and portability are explicit
- bindings are declared before execution
- the runtime never has to guess which semantics a source file implied

For SQL-backed authoring, each named assertion compiles into one rule entry in
`rules` with `portability = "batch_only"` and the stored query payload required
for `query_zero_rows`.

For operator ergonomics, an arity-1 CLI shortcut may still accept
`verify <DATASET> --rules <SOURCE>`, but that path must be equivalent to
compiling the source and then executing a compiled constraint artifact against a
single `input` binding.

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

#### Rule op contract

| `op` | Portability | Required fields | Meaning |
|------|-------------|-----------------|---------|
| `unique` | portable | `binding`, `columns[]` | No two rows may share the same tuple across the named columns |
| `not_null` | portable | `binding`, `columns[]` | Named columns must be present and non-null for every row |
| `predicate` | portable | `binding`, `expr` | Row-level boolean expression must evaluate true for every row |
| `row_count` | portable | `binding`, `compare` | Relation row count must satisfy the declared comparison |
| `aggregate_compare` | portable | `binding`, `aggregate`, `compare` | Aggregate over one binding must satisfy the declared comparison |
| `foreign_key` | portable | `binding`, `columns[]`, `ref_binding`, `ref_columns[]` | Referencing rows must resolve against the referenced relation key |
| `query_zero_rows` | batch_only | `bindings[]`, `query` | Query returns violating rows; zero rows means PASS |

Portable op semantics must be executable without DuckDB-specific query text.

#### `not_null` missingness semantics

V0 must pin missingness semantics for string-like batch inputs explicitly.

For CSV / JSON / JSONL / Parquet bindings:

- `null` is missing
- empty string is blank
- whitespace-only string is blank

For `not_null`, null and blank both fail.

This is deliberate. For the spine's batch surfaces, operators use `not_null`
when they mean "present with substantive content", not merely "column key
exists". If a future version needs a distinction between null-only and
blank-aware presence checks, it should add a new op rather than weakening
`not_null` silently.

#### `predicate` expression contract

`predicate` needs a real v0 grammar, not just the idea of "some expression".

Minimum expression forms:

- comparison:
  - `eq`
  - `ne`
  - `gt`
  - `gte`
  - `lt`
  - `lte`
- boolean composition:
  - `and`
  - `or`
  - `not`
- set membership:
  - `in`
- presence checks:
  - `is_null`
  - `is_blank`
- value access:
  - `{ "column": "<NAME>" }`

Examples:

```json
{
  "op": "predicate",
  "binding": "candidate",
  "expr": {
    "eq": [
      { "column": "row_type" },
      "holding"
    ]
  }
}
```

```json
{
  "op": "predicate",
  "binding": "alignment",
  "expr": {
    "in": [
      { "column": "match_status" },
      ["MATCHED", "UNMATCHED_GOLD", "UNMATCHED_CANDIDATE", "AMBIGUOUS"]
    ]
  }
}
```

Implication is expressed through normal boolean form, not a dedicated `if`
operator in v0. Example:

```json
{
  "op": "predicate",
  "binding": "alignment",
  "expr": {
    "or": [
      {
        "ne": [
          { "column": "match_status" },
          "MATCHED"
        ]
      },
      {
        "and": [
          {
            "not": {
              "or": [
                { "is_null": { "column": "benchmark_entity_key" } },
                { "is_blank": { "column": "benchmark_entity_key" } }
              ]
            }
          },
          {
            "not": {
              "or": [
                { "is_null": { "column": "candidate_row_id" } },
                { "is_blank": { "column": "candidate_row_id" } }
              ]
            }
          }
        ]
      }
    ]
  }
}
```

This is the minimum needed to express structural tournament rules without
smuggling hidden semantics into the evaluator.

#### `query_zero_rows` localization contract

`query_zero_rows` must not stop at row counting. It needs a deterministic map
from query output rows into `results[].affected[]`.

Reserved result columns for query-backed failures:

- `binding` — required binding name implicated by the violating row
- `field` — optional implicated field/column name
- `value` — optional observed value
- `key__<COLUMN>` — optional key component for the affected row

Rules:

- every returned row becomes one `affected` entry
- if `binding` is absent, the rule's first declared binding is used
- `key__<COLUMN>` columns are collected into the `affected.key` object with the
  `key__` prefix stripped
- all non-reserved columns are ignored for the portable report surface unless a
  future version promotes them explicitly

This keeps batch-only SQL checks compatible with the core localization contract
instead of turning them into opaque failure counts.

### Binding contract

Bindings are named relations, not "files". Batch execution happens to satisfy
bindings from files; embedded execution satisfies bindings from in-memory
relations.

Bindings may optionally declare `key_fields`. These are not required for rule
evaluation, but they are the canonical localization surface for failed rows in
reports when the relation has a stable entity key. This matters because
localized failures are part of the protocol, not just CLI sugar.

For batch-loaded string fields, the executor must preserve raw scalar content
for reporting but also apply the v0 missingness rules consistently:

- null stays null
- empty string counts as blank
- whitespace-only string counts as blank

Rule evaluation must not depend on DuckDB's incidental distinction between
`''`, `'   '`, and `NULL` for presence-sensitive checks.

For batch execution, v0 supports:

- CSV
- JSON
- JSONL
- Parquet

Format detection follows the spine-era DuckDB assumptions:

| Extension | Reader |
|-----------|--------|
| `.csv` | `read_csv_auto` |
| `.json` | `read_json` |
| `.jsonl` | `read_json(..., format='newline_delimited')` |
| `.parquet` | `read_parquet` |

Unknown or unsupported binding formats refuse before rule evaluation.

### Rule identity and determinism

Every rule must have:

- stable `id`
- declared `severity`
- declared `portability`

Rule IDs must be unique within a constraint set.

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
  "tool": "verify",
  "version": "verify.report.v1",
  "execution_mode": "batch",
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
  "policy_signals": {
    "severity_band": "ERROR_PRESENT"
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

#### Report top-level fields

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| `tool` | string | no | Must be `verify` |
| `version` | string | no | Must be `verify.report.v1` |
| `execution_mode` | string | no | `batch` or `embedded` |
| `outcome` | string | no | `PASS`, `FAIL`, or `REFUSAL` |
| `constraint_set_id` | string | no | Logical identifier of the applied constraint set |
| `constraint_hash` | string | no | Content hash of the compiled constraint artifact |
| `bindings` | object | no | Binding identities keyed by binding name |
| `summary` | object | no | Aggregate rule counts |
| `policy_signals` | object | no | Narrow discrete signals for downstream policy |
| `results` | array | no | One result entry per rule |
| `refusal` | object | yes | Populated only for `REFUSAL` |

#### Binding report fields

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| `kind` | string | no | V0 only allows `relation` |
| `source` | string | no | Path-like label in batch; stable executor label in embedded |
| `content_hash` | string | no | Content hash of the bound relation input |
| `input_verification` | object | yes | Present when `--lock` verification was requested |

#### Rule result fields

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| `rule_id` | string | no | Stable rule identifier |
| `severity` | string | no | `error` or `warn` |
| `status` | string | no | `pass` or `fail` |
| `violation_count` | integer | no | `0` for PASS, `>0` for FAIL |
| `affected` | array | no | Localized failure details; empty for PASS |

#### Affected-entry fields

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| `binding` | string | no | Binding name implicated by the failure |
| `key` | object | yes | Key tuple when the binding exposes `key_fields` |
| `field` | string | yes | Field/column implicated by the failure |
| `value` | any | yes | Observed value that caused the failure |

For `query_zero_rows`, `affected` entries are populated from the reserved query
output columns described above. That mapping is part of the protocol contract,
not implementation-local convenience behavior.

### Required report properties

Every report must include:

- tool identity
- constraint set identity
- exact binding identity
- exact rule results
- exact refusal, if any

For batch runs, reports must also preserve:

- exact binding source labels
- exact binding content hashes
- lock verification status when `--lock` was used

For embedded runs, `bindings.<name>.source` is an executor-supplied stable label
rather than a filesystem path.

For factory use, the report must also preserve enough structure to map a failed
constraint back to affected entity/bucket candidates. That means `affected`
records are part of the core contract, not an optional pretty-print detail.

Every rule emits exactly one result entry.

- PASS results carry `status = "pass"`, `violation_count = 0`, and
  `affected = []`.
- FAIL results carry `status = "fail"`, `violation_count > 0`, and one or more
  localized `affected` entries.

`summary.by_severity` counts failing rules by severity, not all declared rules.

`policy_signals.severity_band` should stay narrow and discrete:

- `CLEAN` — no failing rules
- `WARN_ONLY` — one or more failures, but all failing rules are `warn`
- `ERROR_PRESENT` — at least one failing rule is `error`

Outcome semantics are exact:

- `PASS` — zero failed rules
- `FAIL` — one or more failed rules
- `REFUSAL` — execution did not complete and `refusal` is populated

### Output (human)

Default stdout should be a compact operator summary:

```text
VERIFY FAIL
constraint_set: loan_tape.monthly.v1
binding: input=tape.csv
passed_rules: 2
failed_rules: 1
severity_band: ERROR_PRESENT

FAIL POSITIVE_BALANCE binding=input key.loan_id=LN-00421 field=balance value=-500.0
```

Human mode is a rendering of the same report contract, not a separate semantics
path.

## CLI shape

### Primary command

```text
verify run <CONSTRAINTS> --bind <NAME=PATH>... [--lock <LOCKFILE>]... [--json]
```

### Arity-1 ergonomic shortcut

```text
verify <DATASET> --rules <SOURCE> [OPTIONS]
```

This is a convenience surface only. It is semantically equivalent to:

1. compile `<SOURCE>` into a temporary `verify.constraint.v1` artifact
2. execute `verify run <COMPILED>` with `--bind input=<DATASET>`

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

verify tape.csv \
  --rules authoring/loan_tape.rules.yaml \
  --lock dec.lock.json \
  --json
```

### Flags

`verify run` should support:

- `--bind <NAME=PATH>` repeatable, required unless using the arity-1 shortcut
- `--lock <LOCKFILE>` repeatable
- `--max-rows <N>` refuse if any bound relation exceeds `N` rows
- `--max-bytes <N>` refuse if any bound file exceeds `N` bytes before loading
- `--json`
- `--no-witness`
- `--describe`
- `--schema`
- `--version`

The arity-1 shortcut should support:

- `--rules <SOURCE>` required
- `--key <COLUMN>` optional convenience for arity-1 inputs; it supplies
  `bindings[0].key_fields = [<COLUMN>]` during the compile+run shortcut when the
  authoring source does not already declare key fields
- if the authoring source already declares `key_fields` for the single `input`
  binding, a conflicting `--key` must refuse rather than silently override the
  compiled contract
- the same `--lock`, `--max-rows`, `--max-bytes`, `--json`, `--no-witness`,
  `--describe`, `--schema`, and `--version` flags

### Exit codes

`0` PASS | `1` FAIL | `2` refusal

### Streams

- human mode: PASS / FAIL summary to stdout; refusal to stderr
- `--json` mode: exactly one JSON object on stdout for PASS, FAIL, or refusal
- stderr: process diagnostics only

### Compile step

```text
verify compile <SOURCE> --out <CONSTRAINTS>
verify compile <SOURCE> --check
verify compile --schema
```

Authoring inputs may include:

- simple JSON/YAML rule authoring format
- SQL assertion file for `query_zero_rows`

The compile step exists to make the protocol artifact explicit. We do not want
raw authoring files to silently double as the runtime contract forever.

`verify compile --check` validates authoring inputs and the compiled
`verify.constraint.v1` output shape without writing an artifact.

`verify compile --schema` should print the compiled constraint schema
(`verify.constraint.v1.schema.json`).

### Validation and discovery

```text
verify validate <CONSTRAINTS>
verify --schema
verify --describe
verify witness <query|last|count>
```

`verify validate` validates compiled `verify.constraint.v1` artifacts only.
Authoring sources are validated through `verify compile --check`.

`verify --schema` should print the primary report schema
(`verify.report.v1.schema.json`). The compiled constraint schema belongs on the
compile surface because it is the output contract of `verify compile`.

`verify witness` is read/query-only. It participates in the same local receipt
log pattern as the other spine tools, but witness remains supplemental local
context rather than portable evidence.

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

## Data model invariants

- `I01` Primitive invariant: arity-1 and arity-N executions emit the same report
  family and use the same compiled constraint artifact family.
- `I02` Binding declaration invariant: every binding referenced by a rule must be
  declared in `bindings`.
- `I03` Binding satisfaction invariant: every declared binding required for
  execution must be provided exactly once by the executor.
- `I04` Binding key invariant: when a binding declares `key_fields`, those fields
  are the canonical row-localization surface for that binding's failed results.
- `I05` Shortcut-key invariant: the arity-1 shortcut may supply `key_fields`
  only when the authored contract does not already declare a conflicting key.
- `I06` Rule identity invariant: rule IDs are unique within a constraint set.
- `I07` Portability invariant: `portable` rules cannot depend on batch-only
  query execution semantics.
- `I08` Embedded refusal invariant: embedded execution refuses any batch-only
  rule that has not been lowered explicitly.
- `I09` Localization invariant: every failing rule result carries
  `violation_count`, and failure details localize to affected bindings plus keys
  and fields when available.
- `I10` Summary invariant: `total_rules = passed_rules + failed_rules`.
- `I11` Rule-result invariant: every rule emits exactly one result entry, and
  PASS results always carry `violation_count = 0`.
- `I12` Policy-band invariant: `severity_band` is derived from failing rule
  severities only and has exactly three values: `CLEAN`, `WARN_ONLY`,
  `ERROR_PRESENT`.
- `I13` Input integrity invariant: when `--lock` is provided, all referenced
  bound inputs must verify before rule evaluation proceeds.
- `I14` Determinism invariant: same compiled constraint bytes and same bound
  relation contents produce the same ordered report bytes.
- `I15` Blank semantics invariant: `not_null` fails on null, empty string, and
  whitespace-only string for string-like batch inputs.
- `I16` Predicate grammar invariant: all portable predicate expressions reduce
  to the declared v0 grammar; no executor-specific hidden operators are allowed.
- `I17` Query localization invariant: every `query_zero_rows` failure row maps
  deterministically into one `affected` entry via the reserved output-column
  contract.

## Refusal codes

### Internal error taxonomy

`verify` should keep internal failures explicit and map them deterministically to
refusal codes:

| Internal error variant | Maps to | Notes |
|------------------------|---------|-------|
| `VerifyError::ConstraintIo` | `E_IO` | Constraint artifact unreadable |
| `VerifyError::AuthoringIo` | `E_IO` | Authoring source unreadable during compile |
| `VerifyError::BindingIo` | `E_IO` | Bound file unreadable |
| `VerifyError::BadConstraint` | `E_BAD_CONSTRAINTS` | Invalid compiled artifact shape or unsupported version |
| `VerifyError::BadAuthoring` | `E_BAD_AUTHORING` | Invalid JSON/YAML/SQL authoring input |
| `VerifyError::DuplicateBinding` | `E_DUPLICATE_BINDING` | Same binding name provided twice |
| `VerifyError::MissingBinding` | `E_MISSING_BINDING` | Declared binding not supplied |
| `VerifyError::UndeclaredBinding` | `E_UNDECLARED_BINDING` | Extra binding name not declared by the constraint set |
| `VerifyError::FormatDetect` | `E_FORMAT_DETECT` | Unsupported or ambiguous file format |
| `VerifyError::FieldReference` | `E_FIELD_NOT_FOUND` | Rule references a field missing from a bound relation |
| `VerifyError::BadExpression` | `E_BAD_EXPR` | Invalid predicate or aggregate compare expression |
| `VerifyError::SqlExecution` | `E_SQL_ERROR` | `query_zero_rows` failed in DuckDB |
| `VerifyError::EmbeddedUnsupported` | `E_BATCH_ONLY_RULE` | Batch-only rule used in embedded execution |
| `VerifyError::KeyOverrideConflict` | `E_KEY_CONFLICT` | Shortcut `--key` conflicts with authored `key_fields` |
| `VerifyError::InputNotLocked` | `E_INPUT_NOT_LOCKED` | Bound input missing from provided locks |
| `VerifyError::InputDrift` | `E_INPUT_DRIFT` | Bound input hash differs from lock member |
| `VerifyError::TooLarge` | `E_TOO_LARGE` | Bound input exceeds `--max-rows` or `--max-bytes` |

### Refusal table

| Code | Trigger | Next step |
|------|---------|-----------|
| `E_IO` | Can't read a constraint source, compiled artifact, or bound input | Check paths and file permissions |
| `E_BAD_CONSTRAINTS` | Compiled artifact invalid or unrecognized version | Recompile or fix the constraint artifact |
| `E_BAD_AUTHORING` | JSON/YAML/SQL authoring source invalid | Fix the authoring file, then re-run `verify compile` |
| `E_DUPLICATE_BINDING` | A binding name was supplied more than once | Remove duplicate `--bind` inputs |
| `E_MISSING_BINDING` | A declared binding was not provided | Add the missing `--bind` |
| `E_UNDECLARED_BINDING` | An unknown binding name was provided | Remove or rename the extra `--bind` |
| `E_FORMAT_DETECT` | Bound input format cannot be loaded | Use CSV, JSON, JSONL, or Parquet |
| `E_FIELD_NOT_FOUND` | Rule references a missing field | Fix the constraint set or input schema |
| `E_BAD_EXPR` | Predicate or aggregate expression is invalid | Fix the rule expression |
| `E_SQL_ERROR` | `query_zero_rows` failed during batch execution | Fix the query-backed rule |
| `E_BATCH_ONLY_RULE` | Embedded execution received a batch-only rule | Lower the rule or run in batch mode |
| `E_KEY_CONFLICT` | Shortcut `--key` disagrees with authored `key_fields` | Remove the CLI override or fix the authored binding key |
| `E_INPUT_NOT_LOCKED` | Bound input not present in any provided lockfile | Lock the input or provide the correct lock |
| `E_INPUT_DRIFT` | Bound input hash differs from the lock member | Use the locked artifact or regenerate the lock intentionally |
| `E_TOO_LARGE` | A bound input exceeds the configured size limit | Increase the limit or split the input |

### Refusal JSON envelope

```json
{
  "tool": "verify",
  "version": "verify.report.v1",
  "execution_mode": "batch",
  "outcome": "REFUSAL",
  "constraint_set_id": "loan_tape.monthly.v1",
  "constraint_hash": "sha256:...",
  "bindings": {},
  "summary": {
    "total_rules": 0,
    "passed_rules": 0,
    "failed_rules": 0,
    "by_severity": {
      "error": 0,
      "warn": 0
    }
  },
  "policy_signals": {
    "severity_band": "CLEAN"
  },
  "results": [],
  "refusal": {
    "code": "E_FIELD_NOT_FOUND",
    "message": "Rule POSITIVE_BALANCE references field balance, which is not present in binding input",
    "detail": {
      "rule_id": "POSITIVE_BALANCE",
      "binding": "input",
      "field": "balance"
    },
    "next_step": "Fix the constraint set or bind an input that exposes the required field."
  }
}
```

### Refusal detail schemas

```text
E_DUPLICATE_BINDING:
  { "binding": "input" }

E_MISSING_BINDING:
  { "binding": "tenants" }

E_UNDECLARED_BINDING:
  { "binding": "options" }

E_FIELD_NOT_FOUND:
  { "rule_id": "POSITIVE_BALANCE", "binding": "input", "field": "balance" }

E_KEY_CONFLICT:
  {
    "binding": "input",
    "authored_key_fields": ["loan_identifier"],
    "cli_key_field": "loan_id"
  }

E_INPUT_NOT_LOCKED:
  { "binding": "input", "path": "tape.csv", "locks_checked": ["dec.lock.json"] }

E_INPUT_DRIFT:
  {
    "binding": "input",
    "path": "tape.csv",
    "expected_hash": "sha256:...",
    "observed_hash": "sha256:..."
  }

E_TOO_LARGE:
  {
    "binding": "input",
    "limit_kind": "max_rows | max_bytes",
    "limit": 1000000,
    "observed": 1250344
  }
```

## Test matrix

Named test suites should exist before calling v0 complete:

- `schema_contract` — compiled artifact and report schemas round-trip and reject
  invalid fixtures
- `compile_contract` — JSON/YAML and SQL authoring inputs compile deterministically
  into `verify.constraint.v1`, including `--check` and `compile --schema`
- `portable_rules` — `unique`, `not_null`, `predicate`, `row_count`,
  `aggregate_compare`, and `foreign_key`
- `query_rules` — `query_zero_rows` happy path, failing path, and SQL refusal
  path
- `batch_missingness` — null, empty string, and whitespace-only string behave
  identically for `not_null`
- `predicate_grammar` — equality, membership, boolean composition, and
  null/blank checks round-trip through authoring and execute deterministically
- `query_localization` — reserved SQL result columns map into `affected`
  bindings / keys / fields / values deterministically
- `refusals` — bad authoring, bad compiled artifacts, missing fields, missing
  bindings, bad locks, and oversize inputs
- `lock_integration` — `--lock` success, `E_INPUT_NOT_LOCKED`, and
  `E_INPUT_DRIFT`
- `cli` — human mode, `--json`, arity-1 shortcut, and exit code mapping
- `cli_key_conflict` — conflicting authored `key_fields` and shortcut `--key`
  refuse with `E_KEY_CONFLICT`
- `embedding_equivalence` — portable rules emit identical results in batch and
  embedded execution
- `determinism` — repeated runs keep report ordering and serialization stable

## Quality gates

Before release or major refactors, `verify` must prove:

- schema validation passes for compiled constraints and reports
- portable and batch-only rule fixtures pass
- batch and embedded parity holds for portable rules
- refusal envelopes are stable and snapshot-tested
- CLI exit codes match PASS / FAIL / refusal semantics

Exact commands:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

## Implementation sequence

### D1. Lock the protocol surface

- write `verify.constraint.v1.schema.json`
- write `verify.report.v1.schema.json`
- implement domain and refusal types in `verify-core`
- add canonical serialization and ordering tests

### D2. Build the portable evaluator

- implement `unique`
- implement `not_null`
- implement `predicate`
- implement `row_count`
- implement `aggregate_compare`
- implement `foreign_key`
- freeze the v0 predicate grammar before calling portable evaluation done

### D3. Add report construction and human rendering

- materialize summary math and `severity_band`
- render deterministic human output from the same report model
- snapshot PASS / FAIL / refusal outputs

### D4. Add batch bindings

- implement CSV / JSON / JSONL / Parquet loading in `verify-duckdb`
- enforce `--max-bytes` before loading
- enforce `--max-rows` after relation materialization

### D5. Add CLI surfaces

- implement `verify run`
- implement the arity-1 shortcut `verify <DATASET> --rules <SOURCE>`
- implement `verify validate`, `--schema`, and `--describe`
- implement shortcut conflict handling for authored `key_fields` vs `--key`

### D6. Add lock verification

- verify bound inputs against repeatable `--lock`
- materialize `input_verification` into reports
- add `E_INPUT_NOT_LOCKED` and `E_INPUT_DRIFT` fixtures

### D7. Add compile and query-backed support

- implement `verify compile` for JSON/YAML authoring
- implement `verify compile --check` and `verify compile --schema`
- compile SQL-backed assertions into `query_zero_rows`
- implement `query_zero_rows` batch execution
- implement reserved-column mapping from `query_zero_rows` results into
  `affected` entries

### D8. Add embedded parity contract

- expose an embedding API for named in-memory relations
- reject batch-only rules in embedded mode
- prove portable-rule parity between batch and embedded execution

### D9. Close determinism and release gates

- add determinism suite across repeated runs
- freeze refusal snapshots
- run the full quality gate on a representative fixture corpus

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
