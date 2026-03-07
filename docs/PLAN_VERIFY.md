# verify — Invariant Checks

This document preserves the earlier feature-oriented framing of `verify`.

The repository's current implementation-direction plan is
`docs/plan.md`. Where this file and `docs/plan.md` disagree, prefer
`docs/plan.md` for repo structure, protocol boundaries, and factory/tournament
role definition.

In particular: `verify cross` in this document is historical framing, not a
separate product boundary. The current plan treats cross-artifact validation as
the multi-binding / arity-N case of one `verify` primitive.

## One-line promise
**Deterministic invariant checks against declared rules — PASS or FAIL with violated invariant IDs.**

Three modes: **single-artifact** (JSON rules against one dataset), **multi-binding** (historically called `verify cross`, using SQL constraints across multiple files via embedded DuckDB), and **infer** (discover invariants from data and emit them as a standard rules file).

---

## Problem

Finance teams need to validate data before acting on it:
- Is the key column actually unique?
- Are all balances positive?
- Does the total match expectations?
- Do cross-file relationships hold? (tenant areas sum to property total, every escalation references a real tenant)

Today this is ad-hoc scripts, manual spot-checks, or nothing. `verify` replaces that with declared, versioned, deterministic rules that produce auditable evidence.

Worse: even teams that want rules don't write them. The activation energy is too high. `verify infer` solves the cold-start problem — hand it a known-good dataset and it discovers the invariants for you.

---

## Non-goals

`verify` is NOT:
- Analytics or profiling (that's `shape`)
- Change detection (that's `rvl` or `compare`)
- Decision framing (that's `assess`)
- Schema migration or data transformation

It does not fix data. It tells you whether data satisfies declared invariants.

---

## Single-artifact mode

### CLI

```
verify <DATASET> --rules <RULES> [OPTIONS]

Arguments:
  <DATASET>              CSV file to verify

Options:
  --rules <RULES>        Rules file (JSON)
  --key <COLUMN>         Key column (if rules reference keys)
  --lock <LOCKFILE>      Verify dataset is a member of these lockfiles (repeatable)
  --max-rows <N>         Refuse if input exceeds N rows (default: unlimited)
  --max-bytes <N>        Refuse if input file exceeds N bytes (default: unlimited)
  --json                 JSON output
```

### Exit codes

`0` PASS | `1` FAIL | `2` refusal

### Lock verification

When one or more `--lock` files are provided, `verify` hashes `<DATASET>` and verifies it is present as a member of at least one provided lockfile. On success, JSON output includes an `input_verification` block.

### Rules file schema (`verify.rules.v0`)

```json
{
  "version": "verify.rules.v0",
  "rules": [
    { "id": "UNIQUE_LOAN_ID", "type": "unique", "column": "loan_id" },
    { "id": "POSITIVE_BALANCE", "type": "domain", "column": "balance", "constraint": { "gt": 0 } },
    { "id": "TOTAL_BALANCE", "type": "sum", "column": "balance", "expected": 1500000000.00, "tolerance": 0.01 },
    { "id": "EXPECTED_ROW_COUNT", "type": "row_count", "expected": 4183 }
  ]
}
```

### Rule types (v0)

| Type | Description | Required fields |
|------|-------------|-----------------|
| `unique` | Column values must be unique | `column` |
| `not_null` | No missing values in column | `column` |
| `domain` | Values satisfy a constraint | `column`, `constraint` (`gt`, `gte`, `lt`, `lte`, `in`, `not_in`) |
| `sum` | Column total matches expected | `column`, `expected`, `tolerance` (optional) |
| `row_count` | Dataset has expected row count | `expected`, `tolerance` (optional, integer) |

### Output (JSON)

```json
{
  "version": "verify.v0",
  "outcome": "FAIL",
  "input_verification": null,
  "file": "tape.csv",
  "rules_file": "loan-tape-rules.json",
  "rules_version": "verify.rules.v0",
  "rules_hash": "sha256:9f86d08...",
  "results": [
    { "rule_id": "UNIQUE_LOAN_ID", "status": "pass" },
    {
      "rule_id": "POSITIVE_BALANCE",
      "status": "fail",
      "violations": [
        { "row": 184, "key": "u8:LN-00421", "column": "u8:balance", "value": -500.00 }
      ],
      "violation_count": 1
    },
    {
      "rule_id": "TOTAL_BALANCE",
      "status": "pass",
      "actual": 1500000023.45,
      "expected": 1500000000.00,
      "delta": 23.45
    }
  ],
  "pass_count": 2,
  "fail_count": 1,
  "refusal": null
}
```

### Rules integrity

`verify` computes SHA256 of the rules file and records it as `rules_hash`. This pins the exact rules applied without requiring a full lifecycle for rules files. When a verify report is included in a `pack`, the rules file should also be included — together they form complete evidence of what was checked and what the rules said at the time.

Rules files don't need profiles' draft/frozen lifecycle. They're just JSON. Hash them, pack them, done.

### Refusal codes (single-artifact)

| Code | Trigger | Next step |
|------|---------|-----------|
| `E_IO` | Can't read dataset or rules file | Check paths |
| `E_BAD_RULES` | Rules file invalid or unrecognized version | Fix rules file |
| `E_COLUMN_NOT_FOUND` | Rule references nonexistent column | Fix column name |
| `E_CSV_PARSE` | Can't parse dataset | Check format |
| `E_INPUT_NOT_LOCKED` | Dataset not present in any provided lockfile | Re-run with correct `--lock` or lock the dataset first |
| `E_INPUT_DRIFT` | Dataset hash doesn't match the referenced lock member | Use the locked file; regenerate lock if expected |
| `E_TOO_LARGE` | Input exceeds `--max-rows` or `--max-bytes` | Increase limit or split input |

---

## Cross-artifact mode (`verify cross`)

Cross-artifact constraint validation using SQL via embedded DuckDB. Validates relationships, aggregations, and business rules that span multiple files — constraints that single-artifact mode cannot express.

### Why SQL

Cross-artifact constraints are naturally relational — foreign keys, aggregation checks, join-based validation. SQL is the most widely understood language for expressing these constraints. DuckDB evaluates SQL directly against CSV, JSON, JSONL, and Parquet files with zero setup. No custom DSL — the constraint language is standard SQL, standing on 50 years of relational theory and a proven execution engine.

### CLI

```
verify cross <CONSTRAINTS> [OPTIONS]

Arguments:
  <CONSTRAINTS>          SQL constraint file (.sql)

Options:
  --bind <NAME=PATH>     Bind a logical table name to a physical file (repeatable)
  --lock <LOCKFILE>      Verify bound files are members of these lockfiles (repeatable)
  --json                 JSON output
```

### Exit codes

`0` PASS (all assertions pass) | `1` FAIL (one or more assertions fail) | `2` refusal

### Why `--bind`

Constraint files reference logical table names, not physical file paths or formats. `--bind` maps logical names to physical files. DuckDB auto-detects the format (CSV, JSON, JSONL, Parquet). This makes constraint files **format-agnostic and portable** — the same SQL works whether a partner delivers CSV and another delivers JSON. The constraint file is reusable across projects; only the `--bind` arguments change.

### Constraint file format

Standard SQL with assertion metadata in structured comments. Each assertion is a SELECT that returns rows that **violate** the constraint. Zero rows = PASS. Non-zero rows = FAIL (violation rows captured in the report). This is the dbt-style test pattern used by thousands of data teams.

```sql
-- @name area_sum
-- @severity error
-- Tenant areas must equal property total
SELECT p.total_area, t.sum_area
FROM property p
CROSS JOIN (SELECT SUM(area_sf) AS sum_area FROM tenants) t
WHERE p.total_area != t.sum_area;

-- @name tenant_fk
-- @severity error
-- Every escalation must reference a real tenant
SELECT tenant_id FROM escalations
WHERE tenant_id NOT IN (SELECT tenant_id FROM tenants);

-- @name date_bounds
-- @severity warn
-- Escalation dates must fall within lease term
SELECT e.tenant_id, e.start_date, t.lease_start
FROM escalations e
JOIN tenants t USING (tenant_id)
WHERE e.start_date < t.lease_start;

-- @name escalation_monotonic
-- @severity warn
-- Rent escalations should be non-decreasing
SELECT e1.tenant_id, e1.year, e1.psf, e2.psf AS next_psf
FROM escalations e1
JOIN escalations e2 ON e1.tenant_id = e2.tenant_id AND e2.year = e1.year + 1
WHERE e2.psf < e1.psf;
```

### `--bind` mechanics

Each `--bind NAME=PATH` registers `NAME` as a DuckDB view over `PATH`. Format is auto-detected from extension:

| Extension | DuckDB reader |
|-----------|--------------|
| `.csv` | `read_csv_auto('PATH')` |
| `.parquet` | `read_parquet('PATH')` |
| `.json` | `read_json('PATH')` |
| `.jsonl` | `read_json('PATH', format='newline_delimited')` |

The constraint SQL references logical names (`FROM property`, `FROM tenants`), never physical paths.

### Output (JSON)

```json
{
  "version": "verify_cross.v0",
  "outcome": "FAIL",
  "constraint_file": "lease_abstract.v1.sql",
  "constraint_hash": "sha256:d4e5f6...",
  "bindings": {
    "property": { "path": "property.json", "format": "json", "bytes_hash": "sha256:a1b2..." },
    "tenants": { "path": "tenants.jsonl", "format": "jsonl", "bytes_hash": "sha256:c3d4..." },
    "escalations": { "path": "escalations.csv", "format": "csv", "bytes_hash": "sha256:e5f6..." }
  },
  "input_verification": { "lock": "spring11.lock.json", "status": "VERIFIED" },
  "summary": {
    "total": 4,
    "passed": 3,
    "failed": 1,
    "by_severity": { "error": 1, "warn": 0 }
  },
  "results": [
    { "name": "area_sum", "severity": "error", "outcome": "PASS", "violations": 0, "rows": [] },
    {
      "name": "tenant_fk",
      "severity": "error",
      "outcome": "FAIL",
      "violations": 1,
      "rows": [ { "tenant_id": "T-099" } ]
    },
    { "name": "date_bounds", "severity": "warn", "outcome": "PASS", "violations": 0, "rows": [] },
    { "name": "escalation_monotonic", "severity": "warn", "outcome": "PASS", "violations": 0, "rows": [] }
  ],
  "refusal": null
}
```

### Refusal codes (cross-artifact)

| Code | Trigger | Next step |
|------|---------|-----------|
| `E_IO` | Can't read constraint file or bound file | Check paths |
| `E_BAD_CONSTRAINTS` | Constraint file has no parseable `@name` sections | Fix constraint file |
| `E_SQL_ERROR` | DuckDB SQL execution error | Fix SQL in constraint file |
| `E_UNBOUND_TABLE` | SQL references a table name not provided via `--bind` | Add missing `--bind` |
| `E_FORMAT_DETECT` | Can't detect format of bound file | Use a supported extension (.csv, .json, .jsonl, .parquet) |
| `E_INPUT_NOT_LOCKED` | Bound file not present in any provided lockfile | Re-run with correct `--lock` or lock the file first |
| `E_INPUT_DRIFT` | Bound file hash doesn't match the referenced lock member | Use the locked file; regenerate lock if expected |

---

## Usage examples

```bash
# Check a loan tape against declared rules
verify tape.csv --rules loan-tape-rules.json --json

# With key column for violation reporting
verify tape.csv --rules loan-tape-rules.json --key loan_id --json

# verify -> rvl: validate the new file, then explain what changed
verify dec.csv --rules rules.json --json > verify.report.json \
  && rvl nov.csv dec.csv --key loan_id --json > rvl.report.json

# verify -> pack: validate and seal as evidence
verify tape.csv --rules rules.json --json > verify.report.json
pack seal verify.report.json dec.lock.json --note "Q4 validation" --output evidence/q4/

# Cross-artifact validation with logical name bindings
verify cross lease_abstract.v1.sql \
  --bind property=property.json \
  --bind tenants=tenants.jsonl \
  --bind escalations=escalations.csv \
  --lock spring11.lock.json \
  --json

# Same constraint file, different formats from a different partner
verify cross lease_abstract.v1.sql \
  --bind property=newmark_property.csv \
  --bind tenants=newmark_tenants.csv \
  --bind escalations=newmark_escalations.csv \
  --lock newmark.lock.json \
  --json

# Discover rules from a known-good dataset
verify infer tape.csv --key loan_id --json > rules.json

# Discover temporal rules from 3 months of data
verify infer jan.csv feb.csv mar.csv --key loan_id --json > rules.json

# Full workflow: infer -> verify -> pack
verify infer good_tape.csv --key loan_id --json > rules.json
verify new_tape.csv --rules rules.json --key loan_id --json > verify.report.json
pack seal verify.report.json rules.json --note "Inferred contract check" --output evidence/
```

---

## Contract discovery (`verify infer`)

The fundamental problem with data validation is that nobody writes the rules. The activation energy to go from "no validation" to "declared rules" is enormous. `verify infer` eliminates that barrier: hand it a known-good dataset, and it emits the invariants the data currently satisfies as a standard `verify.rules.v0` JSON file.

The same tool that *checks* invariants also *discovers* them.

### CLI

```
verify infer <DATASET>... [OPTIONS]

Arguments:
  <DATASET>...           One or more CSV files to infer rules from

Options:
  --key <COLUMN>         Key column (infers uniqueness/not_null for this column)
  --include-measured     Also emit measured rules (row_count, sum) — see rule classification below
  --json                 JSON output (default: the rules file itself)
```

### Exit codes

`0` rules inferred | `2` refusal

`verify infer` always exits `0` on success — there is no pass/fail. The output is a rules file, not a report.

### Structural vs measured rules

From a single file, some inferred properties are **structural** (true of all valid files of this type) and some are **measured** (true of this particular instance). A single file cannot distinguish between the two. `verify infer` handles this by classifying rule types:

| Classification | Rule types | Single-file behavior | Multi-file behavior |
|---|---|---|---|
| **Structural** | `unique`, `not_null`, `domain` (with `in`) | Always emitted | Emitted if true in ALL files |
| **Measured** | `row_count`, `sum`, `domain` (with `gt`/`lt` from observed bounds) | Only with `--include-measured` | Emitted with tolerance derived from observed range |

The default (no flag) emits only structural rules. This is the honest answer: from one file, you can discover what the data promises about its own shape. You cannot discover what it promises about its behavior over time.

### Single-file inference

Given one dataset, `verify infer` performs a single pass and discovers:

| What it checks | Rule emitted | Classification |
|---|---|---|
| Column has zero nulls | `not_null` | structural |
| Column has all unique values | `unique` | structural |
| All values in a column belong to a small finite set (≤ 20 distinct values relative to row count) | `domain` with `in: [...]` | structural |
| `--key` column specified | `unique` + `not_null` for that column | structural |
| All numeric values are positive | `domain` with `gt: 0` | structural |
| Row count | `row_count` | measured |
| Numeric column total | `sum` | measured |

**Rule ID generation:** Inferred rules get deterministic IDs: `INFERRED_<TYPE>_<COLUMN>` (e.g., `INFERRED_UNIQUE_loan_id`, `INFERRED_NOT_NULL_balance`). Deterministic naming means the same dataset always produces the same rule IDs.

**Output (single file):**

```bash
verify infer tape.csv --key loan_id --json > rules.json
```

```json
{
  "version": "verify.rules.v0",
  "rules": [
    { "id": "INFERRED_UNIQUE_loan_id", "type": "unique", "column": "loan_id" },
    { "id": "INFERRED_NOT_NULL_loan_id", "type": "not_null", "column": "loan_id" },
    { "id": "INFERRED_NOT_NULL_balance", "type": "not_null", "column": "balance" },
    { "id": "INFERRED_NOT_NULL_status", "type": "not_null", "column": "status" },
    { "id": "INFERRED_DOMAIN_status", "type": "domain", "column": "status", "constraint": { "in": ["current", "delinquent", "default"] } },
    { "id": "INFERRED_DOMAIN_balance_positive", "type": "domain", "column": "balance", "constraint": { "gt": 0 } }
  ],
  "inferred_from": {
    "files": ["tape.csv"],
    "file_hashes": ["sha256:abc123..."],
    "mode": "structural"
  }
}
```

The output is a standard rules file. Edit it, tighten it, and use it:

```bash
# Infer from known-good data
verify infer tape.csv --key loan_id --json > rules.json

# Review and edit (remove false positives, adjust constraints)
# ...

# Enforce on next delivery
verify new_tape.csv --rules rules.json --json
```

### Multi-file inference

Given N datasets, `verify infer` discovers **temporal invariants** — rules that hold across all files. Measured rules become legitimate because they're derived from observed ranges, not single snapshots.

```bash
verify infer jan.csv feb.csv mar.csv --key loan_id --json > rules.json
```

**Multi-file logic:**

| Property | Behavior |
|---|---|
| Structural rule holds in ALL files | Emitted |
| Structural rule fails in ANY file | Not emitted |
| `row_count` across N files | Emitted with `expected` = mean, `tolerance` = max observed deviation |
| `sum` across N files | Emitted with `expected` = mean, `tolerance` = max observed deviation |
| `domain` with `in` | Union of observed values across all files |
| `domain` numeric bounds | `gt`/`lt` from the widest observed range |

**Output (multi-file):**

```json
{
  "version": "verify.rules.v0",
  "rules": [
    { "id": "INFERRED_UNIQUE_loan_id", "type": "unique", "column": "loan_id" },
    { "id": "INFERRED_NOT_NULL_balance", "type": "not_null", "column": "balance" },
    { "id": "INFERRED_DOMAIN_status", "type": "domain", "column": "status", "constraint": { "in": ["current", "delinquent", "default", "prepaid"] } },
    { "id": "INFERRED_ROW_COUNT", "type": "row_count", "expected": 4193, "tolerance": 15 },
    { "id": "INFERRED_SUM_balance", "type": "sum", "column": "balance", "expected": 1500000000.00, "tolerance": 30000000.00 }
  ],
  "inferred_from": {
    "files": ["jan.csv", "feb.csv", "mar.csv"],
    "file_hashes": ["sha256:aaa...", "sha256:bbb...", "sha256:ccc..."],
    "mode": "temporal"
  }
}
```

Note: `status` domain is the union (`prepaid` appeared in one month). `row_count` tolerance is 15 (observed range: 4178-4208). `sum` tolerance is 30M (observed range: 1.47B-1.53B). `--include-measured` is implicit in multi-file mode — the whole point of N files is to make measured rules legitimate.

### Refusal codes (infer)

| Code | Trigger | Next step |
|------|---------|-----------|
| `E_IO` | Can't read dataset | Check path |
| `E_CSV_PARSE` | Can't parse dataset | Check format |
| `E_EMPTY` | Dataset has zero rows | Provide a non-empty dataset |
| `E_NO_COMMON_COLUMNS` | Multi-file: files share no column names | Check that files are the same type |

### Why this matters

1. **Zero activation energy.** Go from "no validation" to "full rule set" in one command. No JSON authoring, no documentation spelunking.
2. **Agent-native.** An agent can `verify infer` on a known-good dataset, store the rules in a `pack`, and enforce them on every subsequent delivery — without a human writing a single rule.
3. **Flywheel.** One file gives you structural contracts. Three files give you temporal contracts. Twelve files give you high-confidence contracts with tight tolerances. The rules get better with every dataset you feed it.
4. **No new format.** The output is `verify.rules.v0` — the same JSON that `verify` already consumes. It feeds `assess`, lands in `pack`, composes with `--lock`. The protocol just works.
5. **Deterministic.** Same data always infers the same rules. Not ML, not probabilistic. Single-pass, ~200 LOC.

---

## Relationship to other tools

- **Single-artifact verify** validates one file against JSON rules (column-level: uniqueness, domains, sums, row counts).
- **Cross-artifact verify** validates relationships across files via SQL (relational: foreign keys, aggregation consistency, cross-file business rules).
- They are complementary — a typical pipeline runs both: single-artifact verify per file, then cross-artifact verify across the set.
- **RDF/SHACL** handles graph-shaped constraint validation — that belongs in data-fabric (Neo4j), not the spine. The spine stays tabular; data-fabric handles graph reasoning.

---

## Implementation notes

### Single-artifact

The five v0 rule types (`unique`, `not_null`, `domain`, `sum`, `row_count`) are each a single-pass column operation — 10-30 LOC each. `jsonschema` for meta-validation of the rules file against `verify.rules.v0.schema.json`.

### Cross-artifact

Rust binary with `duckdb-rs` (bundled DuckDB). Parses constraint file by splitting on `-- @name` markers (~50 LOC regex). Creates in-memory DuckDB instance. Registers each `--bind` as a view. Executes each assertion query. Collects results. No external database, no server, no state.

### Candidate crates

| Need | Crate | Notes |
|------|-------|-------|
| Embedded SQL engine | `duckdb` (bundled) | Multi-format query engine for cross-artifact mode |
| JSON Schema validation | `jsonschema` | Meta-validation of rules files and tool outputs |
| Regex for constraint parsing | `regex` | Splitting SQL on `-- @name` markers |
| CSV parsing | `csv` (BurntSushi) | Single-artifact mode input |
| Content hashing | `sha2` | Rules hash, input verification |

### Supported input formats (cross-artifact, via DuckDB)

| Format | DuckDB reader | Auto-detected by |
|--------|--------------|------------------|
| CSV | native | `.csv` extension |
| Parquet | native | `.parquet` extension |
| JSON | `read_json` | `.json` extension |
| JSONL | `read_json` (newline-delimited) | `.jsonl` extension |

---

## Determinism

Same files + same rules/SQL = same report. DuckDB is deterministic. SQL is deterministic (no RANDOM(), no NOW()). The rules/constraint file is content-hashed and included in the report.
