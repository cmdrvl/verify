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

The first post-v0 extension admits binding-qualified predicates in the batch
executor. It preserves the v0 artifact families and predicate grammar while
adding an explicit batch-only execution case; it does not retroactively expand
the completed v0 scope.

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
- **batch-only rules** that are valid in the spine batch executor but not
  embeddable in the factory runtime without lowering/translation; these include
  query-backed rules and structured predicates with cross-binding references

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
6. Reports are deterministic. Same compiled constraint bytes plus same bound
   relation bytes produce the same ordered report bytes.
7. `verify` never becomes a correctness scorer. Gold truth belongs to
   `benchmark`, and policy decisions belong to `assess`.

## Non-goals

`verify` will not:

- resolve canonical entities
- load profiles, resolve `--profile-id`, expand column registries, or rewrite
  raw input headers
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

Owns batch bindings and batch-only execution:

- CSV / JSON / JSONL / Parquet bindings
- DuckDB-backed relation materialization
- batch-only query rule execution
- deterministic lowering and execution of binding-qualified predicates

This crate is the bridge from on-disk artifacts into `verify-engine` and the
explicit home of semantics that are valid only in batch execution.

### `verify-cli`

Owns user-facing command surface:

- `run`
- `compile`
- `validate`
- `witness`
- `doctor`
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

The authoring surface accepts two families:

- structured JSON/YAML authoring for portable rules and binding-qualified,
  batch-only predicates
- SQL assertion files for `query_zero_rows`

Those are authoring surfaces, not the canonical runtime contract. `verify
compile` must normalize them into `verify.constraint.v1` so that:

- rule IDs, severities, and portability are explicit
- bindings are declared before execution
- the runtime never has to guess which semantics a source file implied

For structured authoring, the compiler derives predicate portability from the
expression. A predicate whose column references all resolve to its anchor
binding compiles as `portable`. A predicate with at least one reference to a
different binding compiles as `batch_only`. If authoring declares portability
explicitly, it must equal the derived value; the compiler refuses a mismatch
rather than trusting an assertion that could route the rule to the wrong
executor.

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
- `predicate` when any column reference names a binding other than the rule's
  anchor binding

`query_zero_rows` exists so the batch spine executor can support SQL-heavy
relational checks without pretending those rules are automatically usable inside
the factory runtime. If the factory needs one of those rules, it should be
lowered into portable ops or implemented as a dedicated portable rule kind.

#### Rule op contract

| `op` | Portability | Required fields | Meaning |
|------|-------------|-----------------|---------|
| `unique` | portable | `binding`, `columns[]` | No two rows may share the same tuple across the named columns |
| `not_null` | portable | `binding`, `columns[]` | Named columns must be present and non-null for every row |
| `predicate` | portable or batch_only | `binding`, `expr` | Expression must evaluate true for every anchor row; references to another binding require batch execution |
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
  - `{ "binding": "<NAME>", "column": "<NAME>" }`

`Check::Predicate.binding` is the rule's **anchor binding**. An omitted
`ColumnReference.binding` resolves to that anchor. An explicit binding equal to
the anchor has the same meaning. A reference to any other declared binding is a
binding-qualified reference and makes the rule batch-only.

`binding` is optional on `ColumnReference` rather than represented by a second
expression or rule family. Serializers omit it when absent, so every existing
compiled artifact retains its bytes and meaning. This is an additive extension
to `verify.constraint.v1`, not a second compiled contract.

A verifier built before this extension will reject the new field through its
closed-world parser. That fail-closed behavior is intentional: an older binary
must never discard `binding` and execute a cross-binding rule as a tautological
single-binding predicate.

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

#### Predicate scalar comparability

Predicate comparisons are type-strict. Executors must not inherit coercion
behavior from an input reader, SQL engine, or host language.

- number compares with number
- string compares with string
- boolean compares with boolean
- `eq`, `ne`, and `in` treat null as an explicit absence value: null equals
  null, while null and a present scalar are unequal
- ordered comparisons (`gt`, `gte`, `lt`, `lte`) involving null refuse because
  absence has no ordering
- arrays and objects are not scalar predicate literals and must be rejected at
  authoring or compiled-artifact parsing boundaries
- any other cross-type comparison refuses with `E_BAD_EXPR`; it must not become
  an ordinary true or false predicate result
- boolean composition checks comparability in every branch even when normal
  boolean short-circuiting could determine the row verdict earlier

An incomparability refusal identifies the rule, operator, operand categories,
binding, and available key/field context. Batch and embedded execution must
produce the same refusal semantics. In particular, a numeric cell compared with
the string literal `"0"` refuses rather than relying on implicit coercion or
evaluating as unequal.

#### Binding-qualified predicate contract

Binding-qualified predicates extend the same structured predicate grammar over
key-aligned named relations. They do not admit raw SQL, new domain operators, or
a second predicate language.

Example:

```json
{
  "id": "MATURITY_DATE_IMMUTABLE",
  "severity": "error",
  "portability": "batch_only",
  "check": {
    "op": "predicate",
    "binding": "current",
    "expr": {
      "eq": [
        { "binding": "current", "column": "maturity_date" },
        { "binding": "prior", "column": "maturity_date" }
      ]
    }
  }
}
```

The executor derives the complete participating-binding set from the anchor and
every column reference in the expression before evaluation. References in all
boolean branches participate even if ordinary short-circuit evaluation could
skip a branch. Every referenced binding must be declared.

##### Anchor row domain

- `Check::Predicate.binding` is the anchor.
- Every anchor row is evaluated exactly once.
- Each distinct non-anchor binding contributes exactly one counterpart row for
  the anchor row's key tuple.
- A row present only in a non-anchor binding is outside this rule invocation. It
  neither fails the predicate nor produces an affected entry.
- An anchor row without exactly one counterpart in every referenced non-anchor
  binding refuses the run. Missing counterparts are not predicate failures.

This asymmetric row domain is intentional. It supports rules such as "fields on
the current relation must agree with the prior relation" without silently
turning the predicate into a full outer reconciliation primitive. Symmetric
coverage belongs in a separately declared rule in the opposite direction or a
dedicated relation rule.

##### Key alignment and identity

Every participating binding must declare a non-empty `key_fields` tuple.
Tuples align positionally, so physical key names may differ:

```text
current.key_fields = [loan_id, tranche_id]
prior.key_fields   = [asset_number, class_code]
                         |              |
                         + position 0   + position 1
```

All participating tuples must have the same arity. A missing declaration, an
empty tuple, or incompatible arity makes the compiled rule invalid and maps to
`E_BAD_CONSTRAINTS`. The schema should reject an explicitly empty `key_fields`
array; semantic validation checks the cross-binding arity constraint.

For JSON/YAML authoring, portability, declaration, and key-shape defects are
`E_BAD_AUTHORING`. The same defects in an already compiled artifact are
`E_BAD_CONSTRAINTS`. Their stable `reason` values are
`portability_mismatch`, `undeclared_reference`, `missing_key_fields`,
`empty_key_fields`, and `key_arity_mismatch`; detail includes `rule_id`, the
participating bindings, and each binding's declared key fields when relevant.

Before predicate evaluation, the batch executor validates the full key surface
of every participating relation:

- every declared key field must exist, otherwise `E_FIELD_NOT_FOUND`
- every key component must be a non-null protocol scalar; a null, array, object,
  or unrepresentable component refuses with `E_KEY_INVALID`
- temporal key components (`date`, `timestamp`, `time`, `interval`) remain
  unrepresentable for key identity and refuse with `E_KEY_INVALID`; the batch
  temporal comparison exception below does not apply to keys because joining,
  dedupe, and report localization require protocol `Value` keys
- corresponding components compare with the same type-strict scalar equality
  contract as predicate `eq`; incompatible component categories refuse with
  `E_KEY_INVALID`, and no string/number/boolean coercion is permitted
- string keys compare exactly, without trimming, case folding, or collation
  changes
- every key tuple must be unique within its binding; a duplicate makes lookup
  ambiguous and refuses with `E_KEY_AMBIGUOUS`

Key uniqueness is validated across the full participating relation, including
non-anchor rows that are otherwise outside the rule's row domain. This makes
relation identity a precondition rather than allowing ambiguous data to become
conditionally acceptable based on which rows happen to be referenced.

The internal join key is an ordered sequence of tagged scalar values. Physical
key-field names are retained for diagnostics and affected entries but are not
part of cross-binding equality, because positionally aligned bindings may use
different names.

##### Field resolution and scalar semantics

All column references are resolved and checked before the first row verdict is
emitted. A missing referenced field refuses with `E_FIELD_NOT_FOUND`, including
the referenced binding and field in the detail.

The DuckDB lane must reuse the protocol scalar categories and comparison
semantics defined above for every protocol-representable operand. In
particular:

- DuckDB implicit casts are never allowed to decide a predicate verdict
- the DuckDB-to-protocol scalar classification is shared with the existing
  batch portable-materialization path rather than duplicated; the converter
  lives below the CLI in `verify-duckdb` and is called by both paths
- numeric DuckDB families enter the protocol `number` category; boolean and
  string families remain distinct
- null equality, null ordering, heterogeneous membership, and full-branch
  comparability behave exactly as they do for portable predicates
- same-family temporal column-to-column comparisons (`date`, `timestamp`,
  `time`, `interval`) may evaluate `eq`, `ne`, `gt`, `gte`, `lt`, and `lte`
  inside DuckDB without first representing the operands as protocol `Value`s;
  this exception applies only after explicit type checks prove both operands are
  columns in the same temporal family
- temporal equality uses the protocol null contract: null equals null, and null
  differs from a present temporal value; ordered comparisons involving null
  still refuse with `E_BAD_EXPR`
- when a failed temporal comparison localizes to one anchor column, the affected
  value is rendered only for the report by an explicit DuckDB `CAST(... AS
  VARCHAR)`; that rendered string is not used for predicate truth
- any DuckDB type that cannot be represented as a declared protocol scalar and
  is not admitted by the same-family temporal column comparison exception
  refuses rather than being compared through engine-specific behavior

An incomparable expression refuses with `E_BAD_EXPR`. Its detail preserves the
existing `rule_id`, `operator`, `left_type`, `right_type`, anchor `binding`, and
anchor `key` fields and additionally names `left_binding` / `left_field` and
`right_binding` / `right_field` when those operands are column references.

`E_KEY_INVALID.detail.reason` is one of `null_component`,
`non_scalar_component`, `unrepresentable_component`, or `type_mismatch`.
`type_mismatch` detail names both participating bindings, physical key fields,
and protocol scalar categories for the mismatched tuple position.

##### Batch execution and lowering boundary

All bound relations are already loaded as temporary tables in one
`verify_duckdb::BatchContext`. The executor must use that existing connection.
It must not open a second database, reload inputs, or create durable state.

The structured predicate AST is lowered deterministically inside
`verify-duckdb`. Binding and column names are identifiers taken from the
validated compiled artifact and are quoted as identifiers; literal values are
bound or rendered through one canonical literal encoder. Generated SQL remains
an implementation detail and never becomes authoring or report evidence.

Lowering must preserve AST order for diagnostics but may share repeated joins
and projections. It must perform type checks explicitly before issuing semantic
comparisons. Direct DuckDB comparison is allowed only for the same-family
temporal column-to-column exception above; DuckDB implicit coercion across
protocol scalar categories or temporal families is not an acceptable
implementation.

Each distinct non-anchor binding is joined to the anchor once per rule through
the positionally aligned key tuple. The executor must not issue one query per
anchor row or construct a Cartesian product and filter it afterward. Key
validation and predicate execution should remain set-oriented in DuckDB; only
result and refusal data needed for the report crosses back into Rust.

Embedded execution does not approximate this rule. It sees
`portability = "batch_only"` and refuses with `E_BATCH_ONLY_RULE` before portable
evaluation. A future portable lowering may reclassify the same structured rule
only after batch/embedded differential conformance is proved.

##### Failure localization

A false predicate produces one affected entry per failed anchor row:

- `binding` is always the anchor binding
- `key` is the complete anchor key tuple
- `field` and `value` are populated only when the failing expression localizes
  unambiguously to one anchor column and its observed value
- comparisons involving multiple columns, or boolean expressions with multiple
  implicated leaves, leave `field` and `value` absent rather than choosing one
  arbitrarily

`violation_count` is therefore the number of failed anchor rows, not the number
of false leaves or joined rows. A refusal aborts the run and does not emit a
partial rule result.

##### Deterministic validation and refusal precedence

When more than one defect exists, evaluation selects the first refusal in this
stable order:

1. compiled-contract checks: declarations, portability, and key arity
2. referenced fields, ordered by binding name and then field name
3. invalid or duplicate keys, ordered by binding name and canonical key tuple
4. unmatched counterpart keys, ordered by canonical anchor key tuple and then
   referenced binding name
5. expression evaluation, in canonical anchor-key order and AST order

Canonical tuple ordering uses the same canonical JSON scalar ordering helpers as
report ordering over the ordered key-value sequence; it must not inherit scan
order from DuckDB. Failed affected entries are sorted by the normal report
ordering contract.

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

`verify` evaluates relations after upstream profile materialization has already
made them canonical. Column alias resolution, column registry lookup, and raw
header rewriting belong to the profile/materialize stage before `verify` runs.
The runtime never loads profile YAML, resolves `--profile-id`, reads registry
files, or translates provider-specific headers. A bound relation's exposed field
names are the field names in the constraint contract; if a rule names a missing
field, the normal `E_FIELD_NOT_FOUND` refusal applies.

Compiled constraints therefore name canonical fields rather than servicer- or
format-specific source headers. The same `verify.constraint.v1` artifact can be
reused across raw schemas once each input has been materialized into the
canonical relation shape upstream. In arity-N runs, binding identity remains
per named binding: each `bindings.<name>` report entry carries its own source
label, content hash, and optional lock verification status so different raw
schemas, materialized outputs, or profile bundles cannot be conflated.

Bindings may optionally declare `key_fields`. When present, the tuple must be
non-empty and contain unique field names. Most rule kinds do not require a key,
but binding-qualified predicates require one on every participating binding.
Key fields are the canonical localization surface for failed rows in reports
when the relation has a stable entity key. This matters because localized
failures are part of the protocol, not just CLI sugar.

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

These are the complete binding-level provenance fields in `verify.report.v1`.
`verify` reports the executor source label, the bound relation content hash, and
when lock checking was requested, `input_verification.status` plus the lock
names that verified the input. CRV1 profile provenance is deliberately not a
report field: `lock.v0` owns canonical input byte hashes and `profiles[]`
metadata, including `profile_sha256` and any `column_registry_hash`, while
`pack` owns the final membership relationship among profiles, locks, canonical
inputs, constraints, and reports. `verify` consumes lock membership only to
prove the named binding bytes it is evaluating; it does not interpret profile
fields or copy them into a per-tool receipt.

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
rather than a filesystem path. Embedded callers provide the same structured
binding identity and already-canonical relation contents directly; embedded
execution never performs filesystem profile resolution.

Receipt, input, or profile mismatches are refused by the layer that validates
that evidence. When `verify` checks locks, an unreadable or unsupported lock
remains an `E_IO` lock refusal, a missing bound input member remains
`E_INPUT_NOT_LOCKED`, and a content-hash mismatch remains `E_INPUT_DRIFT`.
Unsupported profile versions, profile/registry mismatches, and profile-pack
membership defects belong to `profile`, `lock`, or `pack` unless they alter the
canonical binding bytes or lock membership that `verify` actually evaluates.

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
verify doctor health [--json]
verify doctor capabilities --json
verify doctor robot-docs
verify doctor --robot-triage
```

`verify validate` validates compiled `verify.constraint.v1` artifacts only.
Authoring sources are validated through `verify compile --check`.

`verify --schema` should print the primary report schema
(`verify.report.v1.schema.json`). The compiled constraint schema belongs on the
compile surface because it is the output contract of `verify compile`.

`verify witness` is read/query-only. It participates in the same local receipt
log pattern as the other spine tools, but witness remains supplemental local
context rather than portable evidence.

The implicit witness ledger path is `~/.cmdrvl/state/witness/witness.jsonl`.
`EPISTEMIC_WITNESS` remains an explicit operator override. On first use without
that override, a legacy `~/.epistemic/witness.jsonl` or
`.epistemic/witness.jsonl` ledger is copied into the canonical location, the
legacy file is left in place, and path-only migration/deprecation records are
written under `~/.cmdrvl/migrations/applied.jsonl` and
`~/.cmdrvl/notices/deprecated-paths.jsonl`.

`verify doctor` is an agent-friendly, read-only inspection surface. It reports
embedded schema health, available doctor capabilities, compact robot docs, and
machine-readable triage without reading datasets, loading DuckDB, executing
rules, opening the witness ledger, appending witness records, creating
directories, writing output artifacts, or offering `doctor --fix`.

## Execution contexts

### Spine batch executor

The spine batch executor:

- binds named relations from on-disk files
- verifies lock membership when requested
- materializes only the columns portable rules read — their declared columns,
  their predicate operand columns, and the binding's `key_fields` — so a column
  no rule references can never fail the load, and a binding no portable rule
  reads is not materialized at all
- evaluates portable rules through `verify-engine`
- evaluates binding-qualified batch-only predicates and `query_zero_rows`
  through the existing DuckDB batch context
- emits `verify.report.v1`
- appends a normal witness record

This is the reference implementation for deterministic behavior.

In a CRV1 replay, the profile stage freezes a profile whose canonical bytes
include `column_registry_hash` when a column registry is configured, then
materializes a raw servicer file into a canonical relation such as
`canonical/loans.csv`. `shape` and `rvl` compare canonical relations, while
`verify` evaluates constraints whose fields already match that canonical schema:

```bash
verify run constraints/loan_tape.monthly.v1.verify.json \
  --bind current=canonical/current_loans.csv \
  --bind prior=canonical/prior_loans.csv \
  --lock canonical.lock.json \
  --json
```

The resulting report records `current` and `prior` as separate binding
identities and records only their source labels, content hashes, and lock
verification status. The profile bundle and registry hash are carried by
`lock.v0 profiles[]` and by the eventual pack membership, so replay can prove
which canonicalization inputs produced the bound relation without making
`verify` profile-aware. Companion CRV1 work is tracked in profile `bd-3bg`,
`bd-1ag`, `bd-390`, and `bd-2w7`, shape `bd-1y1d`, rvl `bd-3qg`, and the
closed won't-do verify receipt bead `bd-1bh`; this design amendment is
`bd-21h`.

### Factory runtime executor

The factory runtime executor is an embedded use of the same protocol:

- `twinning` materializes already-canonical candidate state as named relations
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
  relation bytes produce the same ordered report bytes. Profile receipts and
  pack membership do not alter report identity unless they change the
  canonical binding bytes or lock verification surface that `verify` consumes.
- `I15` Blank semantics invariant: `not_null` fails on null, empty string, and
  whitespace-only string for string-like batch inputs.
- `I16` Predicate grammar invariant: all portable predicate expressions reduce
  to the declared v0 grammar; no executor-specific hidden operators are allowed.
- `I17` Query localization invariant: every `query_zero_rows` failure row maps
  deterministically into one `affected` entry via the reserved output-column
  contract.
- `I18` Predicate-anchor invariant: every predicate has one anchor binding;
  omitted column-reference bindings resolve to it, and only distinct binding
  references change portability.
- `I19` Predicate-portability invariant: a predicate is `batch_only` if and only
  if its expression references a binding other than its anchor; declared and
  derived portability must agree.
- `I20` Key-alignment invariant: every binding participating in a
  binding-qualified predicate declares a non-empty, unique key tuple of the same
  arity, aligned positionally.
- `I21` Key-identity invariant: participating key tuples contain non-null,
  type-compatible protocol scalars and are unique within each relation;
  temporal key components are refused as unrepresentable key identity.
- `I22` Anchor-domain invariant: binding-qualified predicates evaluate exactly
  the anchor rows; non-anchor-only rows are outside the invocation, while a
  missing anchor counterpart refuses.
- `I23` Comparison invariant: DuckDB coercion, collation, debug formatting, and
  scan order cannot change predicate meaning, refusal choice, or output order;
  temporal sort keys are explicit and chronological rather than derived from
  third-party debug representations.
- `I24` Cross-binding localization invariant: each false predicate produces one
  affected anchor row, with field/value included only when localization to one
  anchor column is unambiguous.

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
| `VerifyError::InvalidKey` | `E_KEY_INVALID` | A join key is null, non-scalar, unrepresentable, or type-incompatible |
| `VerifyError::AmbiguousKey` | `E_KEY_AMBIGUOUS` | A participating binding contains a duplicate key tuple |
| `VerifyError::UnmatchedKey` | `E_KEY_UNMATCHED` | An anchor row has no counterpart in a referenced binding |
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
| `E_KEY_INVALID` | A participating join key cannot establish type-strict identity | Fix the key data or correct the binding's `key_fields` |
| `E_KEY_AMBIGUOUS` | A participating relation contains a duplicate key tuple | Deduplicate the keyed relation or correct `key_fields` |
| `E_KEY_UNMATCHED` | An anchor key has no row in a referenced binding | Supply the counterpart row or correct key alignment |
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

E_BAD_EXPR (runtime scalar incomparability):
  {
    "rule_id": "POSITIVE_BALANCE",
    "operator": "gt",
    "left_type": "number",
    "right_type": "string",
    "binding": "input",
    "key": { "loan_id": "LN-00421" },
    "field": "balance"
  }

E_BAD_EXPR (binding-qualified operand incomparability):
  {
    "rule_id": "MATURITY_DATE_IMMUTABLE",
    "operator": "eq",
    "left_type": "string",
    "right_type": "number",
    "binding": "current",
    "key": { "loan_id": "LN-00421" },
    "left_binding": "current",
    "left_field": "maturity_date",
    "right_binding": "prior",
    "right_field": "maturity_date"
  }

E_KEY_INVALID:
  {
    "rule_id": "MATURITY_DATE_IMMUTABLE",
    "binding": "prior",
    "key_fields": ["asset_number"],
    "field": "asset_number",
    "value_type": "null",
    "reason": "null_component"
  }

E_KEY_AMBIGUOUS:
  {
    "rule_id": "MATURITY_DATE_IMMUTABLE",
    "binding": "prior",
    "key": { "asset_number": "LN-00421" },
    "occurrences": 2
  }

E_KEY_UNMATCHED:
  {
    "rule_id": "MATURITY_DATE_IMMUTABLE",
    "binding": "current",
    "key": { "loan_id": "LN-00421" },
    "missing_binding": "prior",
    "missing_key_fields": ["asset_number"]
  }

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
- `binding_qualified_predicates` — compile/validate portability inference,
  positionally aligned composite keys with different physical names, full
  predicate grammar PASS/FAIL behavior, anchor-only row domain, localization,
  and embedded `E_BATCH_ONLY_RULE`
- `binding_qualified_refusals` — absent/empty/incompatible keys, missing key or
  operand fields, duplicate keys, unmatched anchor keys, invalid key scalars,
  and incomparable operands produce the pinned refusal codes and detail
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
  embedded execution; legacy single-binding predicates remain portable after
  the binding-qualified extension
- `determinism` — repeated runs keep report ordering and serialization stable
  for identical artifact/input bytes; row-order permutations select the same
  semantic results and first refusal after identity hashes are excluded,
  including binding-qualified PASS, FAIL, and refusal cases

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

### D10. Add binding-qualified batch predicates (post-v0)

This extension lands in dependency order and must not be implemented as one
cross-crate file grab:

1. **Protocol and core types** — amend this plan; add optional
   `ColumnReference.binding`, schema support, new refusal codes, and stable
   serialization tests. Omission must retain existing artifact bytes.
2. **Structured authoring and validation** — collect every binding reference,
   derive portability, reject declared/derived mismatches, validate key
   declarations and arity, and keep SQL authoring scoped to `query_zero_rows`.
3. **DuckDB lowering and execution** — add a dedicated batch predicate executor
   over the existing `BatchContext`, including field/type preflight, key
   uniqueness, counterpart resolution, deterministic AST lowering, and
   localized results.
4. **CLI integration and conformance fixtures** — route portable predicates to
   `verify-engine`, route binding-qualified predicates to `verify-duckdb`, map
   every new error to its refusal envelope, and prove PASS/FAIL/refusal plus
   deterministic ordering end to end.

Steps 2 and 3 depend on step 1. Step 4 depends on both. Portable/embedded
lowering is explicitly outside D10 and remains a later feature guarded by a
batch-versus-embedded differential conformance suite.

## Acceptance criteria for binding-qualified batch predicates

The post-v0 extension is complete only when all of the following hold:

- existing constraint fixtures serialize byte-identically and retain portable
  behavior
- optional `ColumnReference.binding` round-trips through schema, core types,
  JSON/YAML authoring, compile, validate, and run
- declared bindings and derived portability are validated recursively across
  the full expression AST
- the complete v0 predicate grammar executes over binding-qualified operands in
  one existing DuckDB batch context without exposing SQL authoring
- every key, field, counterpart, and scalar-comparability precondition has a
  stable refusal code and actionable detail
- PASS and FAIL results use the anchor row domain and produce deterministic,
  anchor-localized affected entries
- embedded execution refuses the new batch-only form with
  `E_BATCH_ONLY_RULE`, while existing single-binding predicates preserve
  batch/embedded parity
- repeated execution over identical compiled and bound bytes is byte-identical;
  physical row-order permutations preserve semantic results and refusal choice
  after the intentionally different input hashes are excluded
- schema, compile, validate, DuckDB executor, CLI, refusal, embedded, and
  determinism tests cover the extension before it is advertised

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
