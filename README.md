# verify

![verify: deterministic constraint evaluation. A painterly dashboard with a RULES (constraints.yaml) panel on the left declaring four constraints (not_null, unique, in_range, foreign_key), and a RULE EVALUATION RESULTS table on the right showing per-rule verdicts: two pass, one fail (with the specific violating rows: row 1417 rate=0.42, row 2208 rate=0.38), and one refused (counterparties relation not bound). A VERDICT panel reports outcome FAIL with exit_code 1 and refusal_codes E_UNBOUND_RELATION. A ring-bound Constraint Rules Handbook on the desk has index tabs for each rule; an open page shows the verdicts with a handwritten margin note questioning the rate covenant breach. A legend card pairs the three verdict states with their icons.](docs/images/verify.webp)

> *Do these constraints hold for this data? If not, here are the rows that broke them.*

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

Binding-qualified batch predicate over positionally aligned composite keys:

```bash
./target/release/verify run \
  fixtures/constraints/binding_qualified/maturity_date_immutable.verify.json \
  --bind current=fixtures/inputs/binding_qualified/current.csv \
  --bind prior=fixtures/inputs/binding_qualified/prior_matching.csv \
  --json
```

Preview the first few localized failures directly in human output:

```bash
./target/release/verify run fixtures/constraints/query_rules/orphan_rows.verify.json \
  --bind property=fixtures/inputs/arity_n/property.csv \
  --bind tenants=fixtures/inputs/arity_n/tenants.csv \
  --sample-affected 3
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

Inspect the read-only doctor surface for agents and automation:

```bash
./target/release/verify doctor triage --json
./target/release/verify capabilities --json
./target/release/verify robot-docs guide
./target/release/verify --describe
./target/release/verify doctor health
./target/release/verify doctor capabilities --json
./target/release/verify doctor robot-docs
```

`verify doctor triage --json` is the fastest read-only machine entrypoint for
automation. `verify capabilities --json` prints the `operator.v1` contract:
the same document as `verify --describe` (exit codes, witness behavior, formats,
side effects, pipeline, CRV1 canonical relation boundary). `verify robot-docs
guide` prints the agent-facing quick guide. Every doctor report is
`cmdrvl.doctor.v1`; the doctor exits `0` when healthy, `1` when unhealthy, and
`2` only for usage errors.

Deprecated aliases, still working: `verify --robot-triage` and
`verify doctor --robot-triage` (both mean `doctor triage --json`). The old
per-tool schema ids (`verify.doctor.{health,capabilities,triage}.v1`,
`verify.capabilities.v1`) are replaced by `cmdrvl.doctor.v1` and `operator.v1`;
key on `schema_version`.

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
  thing in batch and embedded execution. Batch-only query rules and
  binding-qualified predicates never silently downgrade.
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

Other exit codes, per the family contract (`spine-rules/docs/exit-codes.md`):

- `verify doctor` exits `1` when it finds an unhealthy diagnostic (this was `2`
  before 0.5.2; `2` now means a usage error only). Under the contract `1` is
  therefore "FAIL (doctor: unhealthy)".
- Read commands never exit `1` for an empty result. `verify witness query`
  prints `[]`, `witness count` prints `{"count":0}` and `witness last` prints
  `null`, all with exit `0` (this was exit `1` before 0.5.2). Narrow a query with
  `--outcome PASS|FAIL|REFUSAL`, `--command run|compile|...` and `--limit N`.

`set -e` scripts: a constraint `FAIL` is exit `1` by design and aborts the
script. Guard it when you want to read the report afterwards:

```bash
verify run constraints.verify.json --bind input=data.csv --json > report.json || [ $? -eq 1 ]
```

A `witness query` on an empty ledger no longer needs that guard. A mistyped
subcommand (`verify docter`) is refused with exit `2` and a "did you mean
`doctor`" hint instead of being parsed as a dataset path.

---

## Commands

```text
verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH> [--lock <LOCKFILE>] [--json] [--sample-affected <N>] [--explicit] [--no-witness]
verify <DATASET> --rules <AUTHORING> [--key <FIELD>] [--json] [--sample-affected <N>] [--explicit] [--no-witness]
verify ground <CLAIMS_JSONL> --bind <NAME=DOCUMENT> --lock <LOCKFILE> [--json] [--explicit] [--no-witness]
verify compile <AUTHORING> [--out <OUTPUT>] [--check] [--json]
verify compile --schema
verify validate <COMPILED_CONSTRAINTS> [--json]
verify witness <query|last|count> [--outcome <OUTCOME>] [--command <COMMAND>] [--limit <N>] [--json]
verify doctor triage --json
verify capabilities --json
verify robot-docs guide
verify doctor health [--json]
verify doctor capabilities --json
verify doctor robot-docs
verify --robot-triage          # deprecated alias of `doctor triage --json`
verify doctor --robot-triage   # deprecated alias of `doctor triage --json`
verify --schema
verify --describe
```

The doctor surface is read-only. It does not read bound inputs, execute rules,
load DuckDB, write output artifacts, append witness records, create witness
directories, or expose `doctor --fix`.

Configuration footprint:

- Shared witness ledger fallback: `~/.cmdrvl/state/witness/witness.jsonl`
- Explicit override: `EPISTEMIC_WITNESS` for an operator-supplied ledger path
- First use copies legacy `~/.epistemic/witness.jsonl` or
  `.epistemic/witness.jsonl` into the canonical location, leaves legacy files
  in place, and writes path-only records to
  `~/.cmdrvl/migrations/applied.jsonl` and
  `~/.cmdrvl/notices/deprecated-paths.jsonl`

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

Batch-only execution cases:

- `query_zero_rows`
- `predicate` when any column reference names a binding other than its anchor

A predicate column reference may optionally include `binding`:

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
        { "column": "maturity_date" },
        { "binding": "prior", "column": "maturity_date" }
      ]
    }
  }
}
```

`check.binding` remains the anchor. Omitted reference bindings resolve to that
anchor. Every participating binding must declare a non-empty `key_fields`
tuple of equal arity; physical key names may differ because tuples align by
position. The batch executor evaluates each anchor row once, ignores rows found
only in non-anchor bindings, and refuses ambiguous or missing counterparts.

Structural and scalar defects use stable refusal codes:

- `E_FIELD_NOT_FOUND` for missing key or operand fields
- `E_KEY_INVALID` for null, non-scalar, unrepresentable, or type-incompatible keys
- `E_KEY_AMBIGUOUS` for duplicate participant keys
- `E_KEY_UNMATCHED` for an anchor key without a counterpart
- `E_BAD_EXPR` for incomparable predicate operands

Same-family temporal column-to-column predicates over `date`, `timestamp`,
`time`, and `interval` values use DuckDB-native comparison after explicit type
checks, without widening `verify.report.v1` values. Temporal fields remain
invalid as `key_fields` because join identity and localization keys must be
protocol scalars. A failing temporal predicate renders an unambiguous anchor
field value through an explicit report-only cast.

SQL authoring compiles into batch-only rules. Embedded execution refuses
all batch-only rules with `E_BATCH_ONLY_RULE`; it does not approximate
binding-qualified predicates. Portable cross-binding lowering is deferred.

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

When a failing rule has many localized rows, `--sample-affected <N>` limits the human output to the first `N` affected entries per failing rule and adds a preview note when additional entries were omitted. JSON output is unchanged.

### Excel inputs (`.xlsx`, `.xlsm`, `.xls`)

`--bind tape=report.xlsx#loans` binds one worksheet, by name or 0-based index (digits select an index unless a sheet has that exact name). The worksheet is read by the shared [`cmdrvl-tabular`](https://github.com/cmdrvl/tabular) reader and loaded exactly like a CSV of the same data, so identical rules give identical results over CSV, Parquet and XLSX (golden tests in `tests/xlsx_bindings.rs`). A workbook with more than one sheet and no `#Sheet` refuses with `E_FORMAT_DETECT` (the detail names `E_AMBIGUOUS_SHEET` and lists the sheet names); an unknown sheet refuses the same way (`E_SHEET_NOT_FOUND`). Cells are rendered as canonical text (numbers `2000.0` and `2000` both become `2000`; date cells render as `YYYY-MM-DD` or a datetime); duration cells, error cells and some out-of-range values refuse. The exact rules are in the tabular README. The sheet selector works with `--bind` only: the `verify DATASET --rules` shortcut can open a one-sheet workbook but cannot select a sheet. Locks hash the `.xlsx` file bytes, not the worksheet. Not covered: the temporal and decimal behaviour of the tabular v1 reader beyond what its own tests prove, and workbooks whose values depend on volatile formulas.

### Input locks (`--lock`)

`--lock <LOCKFILE>` (repeatable) checks each bound input against a lockfile before any rule runs. It reads the canonical `lock.v0` manifest that `vacuum | hashbytes | lock` writes: each `members[].bytes_hash` is `sha256:<hex>` or `blake3:<hex>` of the raw file bytes, and verify hashes the bound file with the member's own algorithm. A member matches by exact path, otherwise by a unique basename (real locks store paths relative to the vacuum root). A basename is ambiguous, and counts as not locked, when it is found on two or more *different* member paths across all the `--lock` files. Entries with the same member path are not ambiguous, even in different lock files: the input passes if it matches any of their hashes (see the warning below). Different bytes refuse with `E_INPUT_DRIFT`, a bound input with no member refuses with `E_INPUT_NOT_LOCKED`, and an unknown hash algorithm, a member with no `bytes_hash`, or an unknown lock version refuses with `E_IO` (exit 2). The old verify-private `lock.input.v1` format (`content_hash`, sha256 only) is still read and is deprecated. Not checked: the lock's own `lock_hash` self-hash.

> **Warning: with two locks that list the same member path, `PASS` does not show that each input is the right one.** `verify run` accepts either lock's hash for either input. Reproduced on released 0.7.6: `dec.lock` and `nov.lock` each list `loans.csv`, with different hashes, and `current=dec/loans.csv` and `prior=nov/loans.csv` are bound with both locks. The run passes with the files in their right places, with the two files exchanged, and with the same period's file in both places; a file that is in neither lock refuses with `E_INPUT_DRIFT`. `verify ground` 0.7.6 also passed with two documents exchanged between two locks that list the same member path (every claim quoted text found in both documents). In the JSON report, each binding's `input_verification.locks` names the lock whose hash matched; after an exchange it names the other period's lock.

### Document grounding (`ground`)

`verify ground` checks citations against raw source-document bytes that are pinned by a required, repeatable `--lock` (`lock.v0`) argument. The claims input is JSONL; each line requires `claim_id`, `source`, and `quote`, with an optional half-open raw byte span and optional numeric `value`, `unit`, and `scale`:

```json
{"claim_id":"loan-amount","source":"filing","quote":"95000000.00000000","byte_span":{"start":123,"end":140},"value":95,"scale":"millions"}
```

The binary describes its own inputs, with no other arguments needed: `verify ground --schema` (or `--describe`, `--input-schema`) prints the JSON Schema of one claims line (`verify.claims.v1`), `verify compile --input-schema` the schema of YAML or JSON rule authoring (`verify.authoring.v1`), and `verify compile --schema` the compiled artifact's schema. `verify ground --help` and `verify robot-docs guide` show the fields with copy-paste examples.

> The rules below are stricter than released 0.7.6, which passes many unsupported claims, including a quote that cuts a word or number and a unit the document never states. They apply from the first release that includes them.

**Text.** The pure-Rust engine drops HTML tags and comments and the contents of `script` and `style`, decodes common named entities and numeric character references, removes soft hyphens and zero-width characters, and collapses whitespace to one space. Styling tags (`span`, `b`, `i`, `font`, `a`, inline XBRL and the like) join the text around them; every other tag, including table cells, `br`, `sup` and XML elements, separates it. A `<` that does not start a tag name is ordinary text.

**Quotes.** A quote must stand on its own: it may not start or end inside a word (including hyphenated words and contractions) or inside a number, and may not leave out a sign, parenthesis, scale word or percent sign that belongs to a number it quotes. It also may not start or end inside a currency marker: letters joined to a currency sign (`C$`, `US$`, `CN¥`, `E£`, `Cdn.$`, `U.S.$`; up to three letters per part) or a currency name written after a number (`Canadian dollars`). `US`, `U.S.`, `CA`, `Cdn`, `Cdn.`, `CDN`, `HK`, `NZ` and `AU`, written in exactly those letters one space before a `$` (including a line break, a non-breaking space, a tag or a table cell), are part of the marker too. A sentence that ends in `U.S.` right before an amount cannot be told apart from that prefix, so it is treated as one. A quote may still leave out a currency altogether. Other prefixes typed apart from the sign, including single letters (`C $`, `A $`), are not recognised: they read as a bare `$`. Without `byte_span`, exactly one such match is required. Zero is `E_QUOTE_NOT_FOUND`; several refuse with `E_BAD_AUTHORING` and detail code `E_QUOTE_AMBIGUOUS` plus `match_count`. A `byte_span` is held to the same rules, and the bytes in it must read on their own as they do in the document.

**Numbers.** A numeric claim passes only when all of these hold:

- The quote contains exactly one number. Digits that are part of a name (`Q3`, `10-K`, `10-year`, `3.5x`) do not count. No number is `E_NUMBER_NOT_IN_SPAN`; several, including a date beside an amount, is `E_NUMBER_UNBOUND`. Quote less text.
- Its value, sign and scale can be read from the document around it, not just from the quote. Otherwise it is `E_NUMBER_UNBOUND`. This covers, among others: parentheses; a spaced or ambiguous dash; `MM`, `mil`, `hundred` and other magnitudes that no claim scale expresses; a lone `M` (`$8M`: thousands to some writers, millions to others), `B` or `T`, or `k` without a currency; `tn` without a currency (it may be tonnes); billions and trillions in a document that also uses long-scale words such as `Milliarden`; ranges, lists that share a scale, dates, ratios and fractions; unusual digit grouping; a footnote or exponent beside the digits; a scale in another HTML element; and, for a number with no scale of its own, any document that declares one ("in thousands", "US$'000").
- The value matches in exact decimal arithmetic, and a percentage or basis-point figure is claimed as `percent` or `bps` (a plain quantity is not). Otherwise `E_SCALE_MISMATCH`. The claim's `scale` is `units`, `thousands`, `millions`, `billions`, `trillions`, `percent` or `bps`, and any equivalent one matches: `$1.3 billion` passes as 1300 `millions`. The document's `billion`, `billions`, `bn`, `trillion`, `trillions` and `trn` are read as the English short scale, and `tn` only next to a currency.
- If the claim has a `unit`, the document states that unit on the number itself: otherwise `E_UNIT_NOT_ESTABLISHED`. A currency code or `US$` establishes `USD`; `€`, `£`, `₹` and the currency names next to a number establish theirs. **A bare `$` does not establish `USD`** (or any other dollar): claim `"unit":"$"` for it. `¥` works the same way. A sign with other letters joined to it (`E£`, `CN¥`, `Cdn.$`, `U.S.$`) is not the plain currency and establishes none of these. A spaced prefix (`US $`, `CA $`) establishes no unit by itself, neither a bare `$` nor a code, because the words in front of a `$` may be prose; with a matching code on the same number (`CA $41.2 million CAD`) that code is established. Besides currencies, only `percent`/`%` and `bps` can be established; other units, such as `shares`, do not pass.

#### Behaviour changes from released 0.7.6

Call these out in the release notes. Released 0.7.6 passes each of these claims; they no longer pass:

- **A bare `$` claimed as `USD`.** Claim `"unit":"$"`, or use a document that states `USD` or `US$`.
- **Any unit besides a currency, `percent` or `bps`,** such as `shares`.
- **A lone `M`, with or without a currency.** `$8M` claimed as 8 millions passed in 0.7.6 and is now `E_NUMBER_UNBOUND`, because some writers use `M` for thousands. `$8K` is still thousands.
- **A quote with more than one number,** including a date beside the amount. Quote only the text around the amount.
- **A number in parentheses.** Its sign is not established.
- **A number without its own scale word in a document that declares a scale** ("in thousands", "US$'000").

The rules above refuse more than these; this list is the choices most likely to affect claims that look right.

#### Known limits

A `PASS` does not show any of the following.

Currency:

- **A single letter before a spaced `$` can be cut off.** From `C $41.2 million` (or `A $`, `S $`), the quote `$41.2 million` passes, even with unit `$`. These are left alone because `Class A $5 million` would otherwise be refused.
- **A quote may leave the currency out when the claim states no unit.** `41.2 million` quoted from `C$41.2 million` passes. A claimed unit is still checked against the document.
- **Other spellings of a spaced prefix are not recognised** and can be cut off the same way: for example `U.S` without its last dot, `A.U.`, `(US)`, `US:`, `US-`, `Can`, `NT`, or a footnote marker between the prefix and the `$`.
- **A quote with no digits may end just after the `$`** in `$US` or `$CAD`.
- **`Rs.` is not recognised as a currency,** so `41.2 million` can be quoted from `Rs.41.2 million`.
- **A currency name that does not follow a number can be cut:** `Amounts are in Canadian` passes against `Amounts are in Canadian dollars.`

Words and meaning:

- **A scale phrase can be cut in a quote with no digits,** such as `cent applied` from `per cent applied`. No numeric claim can bind to such a quote.
- **Meaning outside the quote is not read:** "not", "up to" or "more than" left outside the quote are not checked, so quote enough text to carry them.
- **Text hidden by CSS is still searched.** A quote may run across adjacent table cells or blocks. In a plain-text file, anything shaped like a tag is removed.
- **`billion` is read as a thousand million.** In a long-scale language it means a million millions; verify cannot tell from a document that uses no other long-scale word, such as a German text with only `1 Billion`.
- **Scale words are recognised in English and a few other forms only.** Text written without spaces between words, such as Chinese or Japanese, can only be quoted in whole runs.

The command reuses `verify.report.v1` and the existing redaction: failing quote and value details are ordinal refs by default and raw only with `--explicit`.

### Redaction (`--explicit`)

Default `verify run` output is redacted: the raw key values and cell values of failing rows are replaced by deterministic ordinal refs, so a failing rule cannot print bound data into a log or a model's context. Keys become `redacted:ref:k1`, `redacted:ref:k2`, ... and cell values `redacted:ref:v1`, `redacted:ref:v2`, ... in two separate namespaces (a key and a cell holding equal bytes do not share a ref). Numbers follow first appearance over the entries that are actually printed, so `--sample-affected` cannot leak anything about unprinted rows, and rerunning the same input with the same flags gives byte-identical output. No hash of any value appears anywhere. Rule ids, severities, counts, binding names, key column names, field names, and the JSON shape are unchanged. DuckDB error text on a `SQL_ERROR` refusal (which can quote the offending cell) is withheld too. Pass `--explicit` to get the raw values back, for example `verify run c.verify.json --bind input=data.csv --json --explicit`.

What a ref reveals: which printed entries share a value and how many distinct values there are, never the content. Refs are not comparable across runs on different inputs or with different sampling.

**Stable markers (opt-in).** Set `VERIFY_REDACTION_KEY` to a secret of at least 16 bytes and every key and cell value becomes `redacted:hmac-sha256:<32 hex>`: HMAC-SHA256 (RustCrypto `hmac` 0.12 with `sha2` 0.10) over the canonical JSON of the value, truncated to 128 bits. Markers are stable across runs and inputs for whoever holds the key and cannot be guessed without it. A set-but-empty or shorter key refuses with `E_REDACTION_KEY` (exit 2) before any input is read; it never falls back to ordinal refs. The key is never printed, logged or written to the witness ledger. `--explicit` ignores the key but a bad key is still refused. There is deliberately no random per-run salt: it would make default output non-reproducible.

What is **not** redacted, with or without `--explicit`: column and binding names, rule ids, the rule's own SQL or predicate text, file paths, and counts. Equality between values is visible inside one output (and, in keyed mode, across outputs for the key holder). Treat redaction as "no raw values in the output", not as anonymization.

Changed in this release: outputs that used to show raw `key` and `value` fields now show refs unless `--explicit` is passed, and the short-lived `redacted:sha256:<16 hex>` token is gone (its only consumers were verify's own docs and tests). Exit codes and the report shape are unchanged; `E_REDACTION_KEY` is a new refusal.

---

## Execution contexts

One primitive with two execution contexts:

### Batch / CLI

- reads bound inputs from disk (CSV, row-oriented JSON, JSONL, Parquet, and Excel worksheets; see below)
- evaluates portable and batch-only rules
- loads every named relation once into one in-memory `BatchContext`
- materializes only the columns portable rules read, so a column no rule
  references cannot fail the load
- evaluates binding-qualified predicates through deterministic keyed joins
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
│   ├── verify-duckdb/      # batch bindings, keyed predicates, query rules, locks
│   └── verify-cli/         # CLI surface: run, compile, validate, witness, doctor, render
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
│   ├── binding_qualified_predicates.rs  # cross-binding PASS/FAIL conformance
│   ├── binding_qualified_refusals.rs    # keyed predicate refusal contract
│   ├── binding_qualified_determinism.rs # row-order and byte determinism
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

AI coding agents should read [`AGENTS.md`](./AGENTS.md) before making changes.
Claude Code users should also read [`CLAUDE.md`](./CLAUDE.md) for the
machine-local permission and MCP availability boundary; shared project policy
remains in `AGENTS.md`.

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

---

*`verify` is part of the open-source toolchain from the [CMD+RVL](https://cmdrvl.com) lineage and AI enablement practice. MIT-licensed. Contributions welcome from any practice or stack.*
