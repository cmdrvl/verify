# AGENTS.md — verify

> Repo-specific guidance for AI coding agents working in `verify`.

This file adds repo-specific instructions on top of the shared monorepo rules
when you are working inside the full `cmdrvl` workspace. In the standalone
`verify` repo, treat this file and [docs/plan.md](./docs/plan.md) as the local
source of truth.

---

## verify — What This Project Does

`verify` is the epistemic spine's **constraint primitive**.

It evaluates a versioned constraint set against one or more named relations and
emits a deterministic constraint report with localized failures.

Pipeline position:

```text
normalize / materialize -> verify -> assess -> pack
```

Factory position:

```text
twinning / decoding -> embedded verify -> assess / routing
```

What `verify` owns:

- compiled constraint artifacts
- deterministic rule evaluation
- localized failures and refusal envelopes
- batch and embedded execution over the same protocol
- optional lock verification for bound inputs

What `verify` does not own:

- entity resolution (`canon`)
- structural comparability (`shape`)
- delta analysis (`rvl`)
- gold-set scoring (`benchmark`)
- policy decisions (`assess`)
- winner selection or factory orchestration

---

## Current Repository State

This repo is **pre-implementation but plan-complete**.

Current contents:

- [docs/plan.md](./docs/plan.md) — full implementation-grade spec
- [docs/PLAN_VERIFY.md](./docs/PLAN_VERIFY.md) — legacy feature framing only
- [.beads/issues.jsonl](./.beads/issues.jsonl) — execution graph for the swarm
- [README.md](./README.md) — operator-facing contract and project framing

There is no Rust workspace yet. The first implementation task is the scaffold
bead.

Implication:

- do not invent architecture beyond the plan
- do not collapse future crate boundaries into one temporary crate
- do not add behavior just because it seems reasonable if the plan does not say
  to do it

---

## Quick Reference

```bash
# Read the spec first
sed -n '1,360p' docs/plan.md

# See the execution graph
br ready
br blocked

# Current docs-only verification
git diff --check
ubs --diff

# Mandatory gate once the workspace exists
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
ubs .
```

---

## Source of Truth

- **Spec:** [docs/plan.md](./docs/plan.md)
- **Execution graph:** [.beads/issues.jsonl](./.beads/issues.jsonl)

If code, README, and plan disagree, the plan wins.

Do not revive the old split-product framing from `PLAN_VERIFY.md`. The current
repo direction is one `verify` primitive with two execution contexts.

---

## Planned File Map

The intended implementation structure is:

| Path | Purpose |
|------|---------|
| `Cargo.toml` | workspace root |
| `crates/verify-core/src/lib.rs` | shared domain types and protocol exports |
| `crates/verify-core/src/constraint.rs` | `verify.constraint.v1` types |
| `crates/verify-core/src/report.rs` | `verify.report.v1` types |
| `crates/verify-core/src/refusal.rs` | refusal codes and detail schemas |
| `crates/verify-core/src/order.rs` | canonical ordering helpers |
| `crates/verify-engine/src/lib.rs` | portable and embedded execution surface |
| `crates/verify-engine/src/portable_row.rs` | `unique`, `not_null`, `predicate` |
| `crates/verify-engine/src/portable_relation.rs` | `row_count`, `aggregate_compare`, `foreign_key` |
| `crates/verify-engine/src/summary.rs` | summary math and policy signals |
| `crates/verify-engine/src/embedded.rs` | embedded portable executor |
| `crates/verify-duckdb/src/lib.rs` | batch binding and query-rule surface |
| `crates/verify-duckdb/src/bindings.rs` | CSV/JSON/JSONL/Parquet loading |
| `crates/verify-duckdb/src/query_rules.rs` | `query_zero_rows` executor |
| `crates/verify-duckdb/src/lock_check.rs` | lock verification |
| `crates/verify-cli/src/main.rs` | thin binary entrypoint only |
| `crates/verify-cli/src/run.rs` | run command shell and exit plumbing |
| `crates/verify-cli/src/compile/mod.rs` | compile command shell |
| `crates/verify-cli/src/compile/portable.rs` | JSON/YAML portable compilation |
| `crates/verify-cli/src/compile/query.rs` | SQL authoring compilation |
| `crates/verify-cli/src/validate.rs` | compiled-artifact validation |
| `crates/verify-cli/src/witness.rs` | local witness receipt plumbing |
| `crates/verify-cli/src/render/mod.rs` | render module root |
| `crates/verify-cli/src/render/json.rs` | JSON report rendering |
| `crates/verify-cli/src/render/human.rs` | human report rendering |
| `schemas/*.json` | schema contracts |
| `fixtures/**` | authoring, inputs, locks, reports |
| `tests/*.rs` | named behavior suites from the plan |
| `tests/support/**` | shared fixture helpers only |

Critical structural rules:

- `main.rs` stays thin
- protocol types live in `verify-core`, not in CLI code
- portable rule semantics live in `verify-engine`, not in DuckDB code
- batch file loading and query-rule execution live in `verify-duckdb`
- compile and render surfaces stay pre-split for swarm parallelism

---

## Output Contract (Critical)

Target domain outcomes:

| Exit | Outcome | Meaning |
|------|---------|---------|
| `0` | `PASS` | all rules passed |
| `1` | `FAIL` | one or more rules failed |
| `2` | `REFUSAL` | verify could not evaluate safely |

Target output modes:

- default stdout: compact human-readable report
- `--json`: machine-readable full `verify.report.v1`
- stderr: process diagnostics only

Target command family:

- `verify run <COMPILED_CONSTRAINTS> --bind ...`
- `verify <DATASET> --rules <AUTHORING>` for the arity-1 shortcut
- `verify compile`
- `verify validate`
- `verify --schema`
- `verify --describe`
- `verify witness`

Refusal envelopes are part of the protocol. Do not replace them with ad hoc
text or mix diagnostics into stdout evidence.

---

## Core Invariants (Do Not Break)

### 1. One primitive only

Arity-1 and arity-N are execution cases of the same protocol.

Do not create a separate conceptual engine for "cross" validation.

### 2. One compiled contract

There is one compiled artifact family: `verify.constraint.v1`.

- portable JSON/YAML authoring compiles into it
- SQL authoring compiles into it as batch-only rules

Do not let authoring format imply a second protocol.

### 3. Portable means portable

Portable rules must evaluate with the same meaning in batch and embedded modes.

Embedded mode may refuse unsupported rule tiers, but it may not silently
approximate or reinterpret them.

### 4. Batch-only rules stay explicit

`query_zero_rows` is batch-only in v0.

Embedded execution must refuse it with explicit refusal semantics.

### 5. Failure localization is first-class

Failures must identify:

- the failing rule
- the implicated binding
- keys and fields when available

Do not collapse failures into summary counts only.

### 6. Determinism is mandatory

Same compiled constraint bytes plus same bound input bytes must yield the same
ordered JSON report bytes.

That includes:

- stable rule ordering
- stable violation ordering
- stable summary math

### 7. Lock and witness boundaries stay clean

- `lock` verifies trusted inputs
- `witness` is a local receipt log only

Do not turn witness into portable evidence or try to make it authoritative over
lock or pack.

### 8. verify is not a scorer or policy engine

Do not implement:

- gold-set correctness scoring
- proceed / escalate / block decisions
- winner selection logic

Those belong elsewhere in the spine.

---

## Rule-Tier Discipline

Portable v0 rule kinds:

- `unique`
- `not_null`
- `predicate`
- `row_count`
- `aggregate_compare`
- `foreign_key`

Batch-only v0 rule kind:

- `query_zero_rows`

This division is constitutional for the implementation. If a new rule does not
fit either tier cleanly, stop and update the plan first.

---

## Toolchain

Target implementation assumptions:

- language: Rust
- package manager: Cargo only
- edition: 2024
- unsafe code: forbidden

Expected dependency profile:

- `clap` for CLI parsing
- `serde` and `serde_json` for structured artifacts
- `duckdb` for batch relation loading and query execution
- `sha2` or equivalent for content hashing
- small, pinned dependencies only

Do not add large convenience layers or alternate execution runtimes casually.

---

## Quality Gates

### Current docs-only state

Run this after doc-only changes:

```bash
git diff --check
ubs --diff
```

### After scaffold lands

Run this after substantive code changes:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
ubs .
```

### Stop-ship verification target

The final implementation must have named coverage for:

- schema contracts
- portable rule behavior
- query-rule behavior
- refusal paths
- lock integration
- CLI exit/output behavior
- embedded parity
- determinism
- perf smoke guardrails

---

## MCP Agent Mail — Multi-Agent Coordination

Agent Mail is the coordination layer for multi-agent sessions in this repo:
identities, inbox/outbox, thread history, and advisory file reservations.

### Session Baseline

1. If direct MCP Agent Mail tools are available in this harness, ensure project
   and reuse your identity:
   - `ensure_project(project_key=<abs-path>)`
   - `whois(project_key, agent_name)` or `register_agent(...)` only if identity
     does not exist
2. Reserve only exact files you will edit:
   - Allowed: `crates/verify-engine/src/portable_row.rs`,
     `tests/query_rules.rs`
   - Not allowed: `crates/verify-engine/**`, `tests/**`, whole directories
3. Send a short start message and finish message for each bead, reusing the
   bead ID as the thread when practical.
4. Check inbox at moderate cadence (roughly every 2-5 minutes), not
   continuously.

### Important `ntm` Boundary

When this repo is worked via `ntm`, the session may be connected to Agent Mail
even if the spawned Codex or Claude harness does **not** expose direct
`mcp__mcp-agent-mail__...` tools.

If direct MCP Agent Mail tools are unavailable:

- do **not** stop working just because mail tools are absent
- continue with `br`, exact file reservations via the available coordination
  surface, and overseer instructions
- treat Beads + narrow file ownership as the minimum coordination contract

### Stability Rules

- Do not run retry loops for `register_agent`, `create_agent_identity`, or
  `macro_start_session`.
- If a call fails with a transient DB/SQLite lock error, back off for 90
  seconds before retrying.
- Continue bead work while waiting for retry windows; do not block all progress
  on mail retries.

### Communication Rules

- If a message has `ack_required=true`, acknowledge it promptly.
- Keep bead updates short and explicit: start message, finish message, blocker
  message.
- Reuse a stable bead thread when possible for searchable history.

### Reservation Rules

- Reserve only specific files you are actively editing.
- Never reserve entire directories or broad patterns.
- If a reservation conflict appears, pick another unblocked bead or a
  non-overlapping file.

---

## CI / Release Target State

`verify` does not have CI or release automation yet, but the implementation
must land with the same release-grade surface as the stronger spine tools.

Target release surface:

- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `scripts/ubs_gate.sh`
- `LICENSE`
- cross-target packaged archives
- `SHA256SUMS`, signing, SBOM, and provenance output
- Homebrew tap update workflow parity

Do not treat release automation as optional cleanup after the implementation is
"basically done." It is part of the repo contract.

---

## Beads (br) — Execution Shape

Beads is the execution source of truth in this repo.

- Beads = task graph, state, priorities, dependencies
- Agent Mail = coordination, reservations, audit trail

Core commands:

```bash
br ready
br show <id>
br update <id> --status in_progress
br close <id> --reason "Completed"
br sync --flush-only
```

Conventions:

- include bead IDs in coordination subjects, for example
  `[bd-18u] Start schema and core types`
- use the bead ID in reservation reasons when the tool supports it
- prefer concrete ready beads over the epic tracker

Workflow:

1. Start with `br ready`.
2. Mark the bead `in_progress` before editing.
3. Reserve exact files and send a short start update when coordination tools are
   available.
4. Implement and run the relevant quality gate.
5. Close the bead, send a completion note, and release reservations.

This repo already has a swarm-oriented Beads graph.

Current first task:

- `bd-3fq` — bootstrap verify workspace, crates, and swarm-safe file skeleton

After that closes, the immediate parallel lanes are:

- `bd-18u` core schemas/types
- `bd-p82` fixtures/support
- `bd-254` run CLI shell
- `bd-21w` compile/validate/schema shell
- `bd-12i` DuckDB bindings/size guards
- `bd-rld` witness surface

Implication for agents:

- do not freeload architecture into your bead
- do not reopen files assigned to another lane unless the user explicitly wants
  a replan
- use the pre-split file map to avoid collisions

---

## File Reservation Guidance

When working in a multi-agent session, reserve only exact files.

Good reservations:

- `crates/verify-engine/src/portable_row.rs`
- `crates/verify-cli/src/compile/query.rs`
- `tests/query_rules.rs`
- `README.md`

Bad reservations:

- `crates/verify-engine/**`
- `crates/verify-cli/src/compile/`
- `tests/**`

Practical rule:

- foundation creates the layout
- later beads fill one or two files each
- do not reshape the tree once swarm work starts unless the user explicitly
  asks for a replan

---

## Editing Rules

- No file deletion without explicit written user permission.
- No destructive git commands without explicit authorization.
- No scripted mass edits; keep patches small and reviewable.
- No backwards-compatibility shims.
- No hidden semantics in CLI-only flags that bypass the compiled protocol.
- No second engine for the arity-1 shortcut; it must be compile+run over the
  same core implementation.

---

## Current Contributor Loop

```bash
cd verify
br ready
br show bd-3fq
git diff --check
```

If you are implementing:

1. read `docs/plan.md`
2. claim a ready bead
3. reserve exact files
4. implement only that slice
5. run the relevant quality gate
6. sync Beads, commit, pull --rebase, push `main`, then push `main:master`
