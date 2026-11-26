use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};
use verify_core::{CONSTRAINT_VERSION, REPORT_VERSION, TOOL_NAME};

use crate::{paths, render, witness};

/// Family doctor schema (spine-rules `cmdrvl.doctor.v1`). Replaces the old
/// per-tool `verify.doctor.{health,capabilities,triage}.v1` and
/// `verify.capabilities.v1` ids; consumers key on `schema_version`.
const DOCTOR_SCHEMA: &str = "cmdrvl.doctor.v1";
const DOCTOR_CONTRACT: &str = "cmdrvl.read_only_doctor.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Health,
    Capabilities,
    Triage,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Capabilities => "capabilities",
            Self::Triage => "triage",
        }
    }
}

const SIDE_EFFECTS: &[&str] = &[
    "reads_stdin",
    "reads_constraint_files",
    "reads_authoring_files",
    "reads_bindings",
    "loads_duckdb",
    "executes_rules",
    "verifies_locks",
    "opens_witness_ledger",
    "appends_witness_ledger",
    "creates_witness_directory",
    "writes_migration_logs",
    "writes_deprecation_notices",
    "writes_outputs",
    "writes_doctor_artifacts",
    "uses_network",
    "changes_cwd",
    "rewrites_schema",
];

#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    #[arg(long = "robot-triage")]
    pub robot_triage: bool,
    #[arg(long)]
    pub json: bool,
    #[command(subcommand)]
    pub action: Option<DoctorAction>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum DoctorAction {
    Health(DoctorJsonArgs),
    Capabilities(DoctorJsonArgs),
    /// Canonical spelling of the triage report (`--robot-triage` stays as an alias).
    Triage(DoctorJsonArgs),
    RobotDocs,
}

#[derive(Debug, Clone, Args)]
pub struct DoctorJsonArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct TopLevelCapabilitiesArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct TopLevelRobotDocsArgs {
    #[command(subcommand)]
    pub action: Option<TopLevelRobotDocsAction>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum TopLevelRobotDocsAction {
    #[command(about = "Print the agent quick-start guide")]
    Guide,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCommandResult {
    pub exit_code: u8,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

#[derive(Debug, Clone)]
struct Check {
    name: &'static str,
    ok: bool,
    detail: String,
}

#[derive(Debug, Clone)]
struct Diagnostics {
    report_schema_title: String,
    constraint_schema_title: String,
    witness_ledger_path: String,
    checks: Vec<Check>,
}

impl Diagnostics {
    fn ok(&self) -> bool {
        self.checks.iter().all(|check| check.ok)
    }

    fn status(&self) -> &'static str {
        if self.ok() { "healthy" } else { "unhealthy" }
    }

    /// Family contract: `0` healthy, `1` unhealthy. `2` is reserved for
    /// CLI/usage errors and is never returned for a parsed unhealthy report.
    fn exit_code(&self) -> u8 {
        if self.ok() { 0 } else { 1 }
    }
}

pub fn execute(args: DoctorArgs) -> DoctorCommandResult {
    render(args, &collect_diagnostics())
}

/// Turn diagnostics into the requested output and exit code. Split from
/// `execute` so an unhealthy run can be exercised without breaking the install.
fn render(args: DoctorArgs, diagnostics: &Diagnostics) -> DoctorCommandResult {
    if args.robot_triage {
        return json_result(
            report_value(Mode::Triage, diagnostics),
            diagnostics.exit_code(),
        );
    }

    let action = args
        .action
        .unwrap_or(DoctorAction::Health(DoctorJsonArgs { json: args.json }));

    match action {
        DoctorAction::Health(format) => {
            if args.json || format.json {
                json_result(
                    report_value(Mode::Health, diagnostics),
                    diagnostics.exit_code(),
                )
            } else {
                text_result(human_health(diagnostics), diagnostics.exit_code())
            }
        }
        DoctorAction::Capabilities(format) => {
            if args.json || format.json {
                json_result(
                    report_value(Mode::Capabilities, diagnostics),
                    diagnostics.exit_code(),
                )
            } else {
                text_result(human_capabilities(), diagnostics.exit_code())
            }
        }
        DoctorAction::Triage(format) => {
            if args.json || format.json {
                json_result(
                    report_value(Mode::Triage, diagnostics),
                    diagnostics.exit_code(),
                )
            } else {
                text_result(human_health(diagnostics), diagnostics.exit_code())
            }
        }
        DoctorAction::RobotDocs => text_result(robot_docs(), 0),
    }
}

pub fn execute_top_level_robot_triage() -> DoctorCommandResult {
    let diagnostics = collect_diagnostics();
    json_result(
        report_value(Mode::Triage, &diagnostics),
        diagnostics.exit_code(),
    )
}

/// `verify capabilities --json` is the `operator.v1` document, byte-identical to
/// `verify --describe`; both come from the embedded `operator.json`.
pub fn execute_top_level_capabilities(args: TopLevelCapabilitiesArgs) -> DoctorCommandResult {
    if args.json {
        text_result(render::operator_contract().to_owned(), 0)
    } else {
        text_result(human_top_level_capabilities(), 0)
    }
}

pub fn execute_top_level_robot_docs(_args: TopLevelRobotDocsArgs) -> DoctorCommandResult {
    text_result(top_level_robot_docs(), 0)
}

fn text_result(stdout: String, exit_code: u8) -> DoctorCommandResult {
    DoctorCommandResult {
        exit_code,
        stdout: Some(stdout),
        stderr: None,
    }
}

fn json_result(value: Value, exit_code: u8) -> DoctorCommandResult {
    match serde_json::to_string_pretty(&value) {
        Ok(stdout) => text_result(stdout, exit_code),
        Err(error) => DoctorCommandResult {
            exit_code: 2,
            stdout: None,
            stderr: Some(format!("verify doctor: failed to render JSON: {error}")),
        },
    }
}

fn collect_diagnostics() -> Diagnostics {
    let report_schema = serde_json::from_str::<Value>(render::report_schema());
    let constraint_schema = serde_json::from_str::<Value>(render::constraint_schema());
    let report_schema_title = schema_title(&report_schema);
    let constraint_schema_title = schema_title(&constraint_schema);
    let witness_ledger_path = witness::witness_ledger_path().display().to_string();

    let checks = vec![
        Check {
            name: "report_schema_embedded_json",
            ok: report_schema.is_ok(),
            detail: parse_detail(&report_schema),
        },
        Check {
            name: "report_schema_title",
            ok: report_schema_title == REPORT_VERSION,
            detail: format!("title={report_schema_title}"),
        },
        Check {
            name: "report_schema_required_fields",
            ok: required_fields_present(
                &report_schema,
                &[
                    "tool",
                    "version",
                    "execution_mode",
                    "outcome",
                    "summary",
                    "policy_signals",
                    "results",
                    "refusal",
                ],
            ),
            detail: "required report contract fields are embedded".to_owned(),
        },
        Check {
            name: "constraint_schema_embedded_json",
            ok: constraint_schema.is_ok(),
            detail: parse_detail(&constraint_schema),
        },
        Check {
            name: "constraint_schema_title",
            ok: constraint_schema_title == CONSTRAINT_VERSION,
            detail: format!("title={constraint_schema_title}"),
        },
        Check {
            name: "doctor_dispatch_read_only",
            ok: true,
            detail: "doctor dispatch does not call run, compile, validate, or witness append paths"
                .to_owned(),
        },
        Check {
            name: "witness_path_resolves_without_open",
            ok: true,
            detail: witness_ledger_path.clone(),
        },
        Check {
            name: "config_footprint_declared",
            ok: true,
            detail: "implicit witness state is rooted under ~/.cmdrvl/state/witness".to_owned(),
        },
        Check {
            name: "fix_mode_disabled",
            ok: true,
            detail: "no doctor --fix argument or fixer registry is exposed".to_owned(),
        },
    ];

    Diagnostics {
        report_schema_title,
        constraint_schema_title,
        witness_ledger_path,
        checks,
    }
}

fn schema_title(schema: &Result<Value, serde_json::Error>) -> String {
    match schema {
        Ok(value) => value
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("<missing title>")
            .to_owned(),
        Err(error) => format!("parse error: {error}"),
    }
}

fn parse_detail(schema: &Result<Value, serde_json::Error>) -> String {
    match schema {
        Ok(_) => "embedded schema parses".to_owned(),
        Err(error) => format!("embedded schema parse failed: {error}"),
    }
}

fn required_fields_present(schema: &Result<Value, serde_json::Error>, fields: &[&str]) -> bool {
    let Ok(value) = schema else {
        return false;
    };

    let Some(required) = value.get("required").and_then(Value::as_array) else {
        return false;
    };

    fields.iter().all(|field| {
        required
            .iter()
            .any(|candidate| candidate.as_str() == Some(*field))
    })
}

/// One `cmdrvl.doctor.v1` report. `findings` holds every check (passing checks
/// as `info`, failing ones as `error`); `healthy` is exactly "no error finding".
/// Extra read-only facts (`witness`, `config_footprint`, `side_effects`,
/// `fixers`) ride alongside the contract fields.
fn report_value(mode: Mode, diagnostics: &Diagnostics) -> Value {
    let mut report = Map::new();
    report.insert("schema_version".to_owned(), json!(DOCTOR_SCHEMA));
    report.insert("mode".to_owned(), json!(mode.as_str()));
    report.insert("tool".to_owned(), json!(TOOL_NAME));
    report.insert("tool_version".to_owned(), json!(env!("CARGO_PKG_VERSION")));
    report.insert("healthy".to_owned(), json!(diagnostics.ok()));
    report.insert("findings".to_owned(), findings_value(&diagnostics.checks));
    match mode {
        Mode::Health => {}
        Mode::Capabilities => {
            // The same document as `verify capabilities --json` / `--describe`.
            let contract = serde_json::from_str::<Value>(render::operator_contract())
                .unwrap_or_else(
                    |error| json!({"error": format!("embedded operator.json: {error}")}),
                );
            report.insert("contract".to_owned(), contract);
        }
        Mode::Triage => {
            let next_actions = if diagnostics.ok() {
                Value::Array(Vec::new())
            } else {
                json!(["inspect embedded schema wiring and doctor dispatch checks"])
            };
            report.insert("next_actions".to_owned(), next_actions);
        }
    }
    report.insert("doctor_contract".to_owned(), json!(DOCTOR_CONTRACT));
    report.insert(
        "witness".to_owned(),
        json!({
            "ledger_path": diagnostics.witness_ledger_path,
            "opened": false,
            "appended": false,
            "directory_created": false
        }),
    );
    report.insert("config_footprint".to_owned(), paths::config_footprint());
    report.insert("side_effects".to_owned(), side_effects_value());
    report.insert("fixers".to_owned(), json!([]));
    Value::Object(report)
}

fn findings_value(checks: &[Check]) -> Value {
    Value::Array(
        checks
            .iter()
            .map(|check| {
                let mut finding = Map::new();
                finding.insert("id".to_owned(), json!(check.name));
                finding.insert(
                    "severity".to_owned(),
                    json!(if check.ok { "info" } else { "error" }),
                );
                finding.insert("message".to_owned(), json!(check.detail));
                if !check.ok {
                    finding.insert(
                        "fix".to_owned(),
                        json!(
                            "reinstall verify or report a bug: the embedded schema wiring is broken"
                        ),
                    );
                }
                Value::Object(finding)
            })
            .collect(),
    )
}

fn side_effects_value() -> Value {
    let mut effects = Map::new();
    for name in SIDE_EFFECTS {
        effects.insert((*name).to_owned(), Value::Bool(false));
    }
    Value::Object(effects)
}

fn human_health(diagnostics: &Diagnostics) -> String {
    let mut lines = vec![
        format!("verify doctor health: {}", diagnostics.status()),
        format!("version: {}", env!("CARGO_PKG_VERSION")),
        format!("contract: {DOCTOR_CONTRACT}"),
        format!("report_schema: {}", diagnostics.report_schema_title),
        format!("constraint_schema: {}", diagnostics.constraint_schema_title),
        "side_effects: none".to_owned(),
        "fixers: none".to_owned(),
        "checks:".to_owned(),
    ];

    for check in &diagnostics.checks {
        let status = if check.ok { "ok" } else { "fail" };
        lines.push(format!("- {}: {status} ({})", check.name, check.detail));
    }

    lines.join("\n")
}

fn human_capabilities() -> String {
    [
        "verify doctor capabilities",
        "commands:",
        "- doctor health [--json]",
        "- doctor capabilities --json",
        "- doctor triage --json",
        "- doctor robot-docs",
        "- doctor --robot-triage (alias of doctor triage --json)",
        "read_only: true",
        "side_effects: none",
        "fixers: none",
    ]
    .join("\n")
}

fn human_top_level_capabilities() -> String {
    [
        "verify capabilities",
        "schema: operator.v1 (`verify capabilities --json` is identical to `verify --describe`)",
        "standard agent surfaces:",
        "- verify doctor triage --json (alias: verify --robot-triage)",
        "- verify capabilities --json",
        "- verify robot-docs guide",
        "- verify --describe",
        "exit codes:",
        "- 0: PASS, or a successful read (an empty witness query prints [] and exits 0)",
        "- 1: FAIL (doctor: unhealthy)",
        "- 2: REFUSAL / CLI error",
    ]
    .join("\n")
}

fn top_level_robot_docs() -> String {
    [
        "verify robot-docs guide",
        "",
        "Quick start:",
        "- `verify doctor triage --json` (alias `verify --robot-triage`) returns one read-only `cmdrvl.doctor.v1` packet.",
        "- `verify capabilities --json` returns the `operator.v1` command contract (same document as `verify --describe`).",
        "- `verify run COMPILED.verify.json --bind input=data.csv --json --no-witness` evaluates a compiled constraint set without appending a witness record.",
        "- `verify ground CLAIMS.jsonl --bind filing=document.xml --lock filing.lock.json --json --no-witness` checks locked document quotes and numeric scales using `verify.report.v1`.",
        "- `verify DATASET.csv --rules rules.yaml --json --no-witness` compiles and runs the arity-1 shortcut.",
        "- `verify compile AUTHORING.yaml --check --json` validates authoring without writing an output file.",
        "",
        "Outcome contract:",
        "- `0` means PASS, or a successful read command.",
        "- `1` means FAIL (a constraint failed), or `doctor` found the install unhealthy.",
        "- `2` means REFUSAL or a CLI error. It is never a verdict and never doctor-unhealthy.",
        "- JSON report output is `verify.report.v1`.",
        "",
        "Scripting (`set -e`):",
        "- `verify run ... ` exits `1` on FAIL by design; guard it: `verify run c.json --bind input=d.csv || [ $? -eq 1 ]`.",
        "- `verify witness query|count|last` never exits `1` for an empty result: `query` prints `[]`, `count` prints `{\"count\":0}`, `last` prints `null`, all with exit `0`.",
        "- Narrow a query with `--outcome PASS|FAIL|REFUSAL`, `--command run|compile|...` and `--limit N`.",
        "",
        "Redaction (`--explicit`):",
        "- Default `verify run` output replaces raw failing key and cell values with ordinal refs: `redacted:ref:k1`, `k2`.. for keys and `redacted:ref:v1`, `v2`.. for cells (separate namespaces), numbered by first appearance over the entries actually printed, byte-identical on rerun. No hash of any value appears. DuckDB error text on SQL_ERROR refusals is withheld.",
        "- A ref reveals the equality pattern inside that one output and the number of distinct values, not their content; refs are not comparable across different inputs or different `--sample-affected` runs.",
        "- Opt-in stable markers: set `VERIFY_REDACTION_KEY` (at least 16 bytes) and values become `redacted:hmac-sha256:<32 hex>` (HMAC-SHA256 of the canonical JSON value), stable across runs for the key holder. An empty or shorter key refuses with E_REDACTION_KEY (exit 2), never falling back to refs. The key is never printed, logged or written to the witness ledger.",
        "- Pass `--explicit` to include raw values, e.g. `verify run c.json --bind input=d.csv --json --explicit`.",
        "- NOT redacted either way: column, binding and rule names, rule SQL text, file paths, counts.",
        "- Use `--explicit` for local non-sensitive runs; omit it for logged, shared, or veil zero-retention workflows.",
        "",
        "Witness boundary:",
        "- Run/evaluate commands append local witness records by default.",
        "- Add `--no-witness` for read-side smoke tests and deterministic agent probes.",
        "- Doctor, triage, capabilities, robot-docs, schema, and validate surfaces do not append witness records.",
        "",
        "Composition:",
        "- Use `verify` after profile's bounded materialization has produced already-canonical named relations.",
        "- `verify` never loads profiles, resolves `--profile-id`, or rewrites headers.",
        "- CRV1 canonical input/profile provenance rides in `lock.v0` `profiles[]` and pack membership, not `verify.report.v1`.",
        "- Use embedded verify inside twinning/decoding for portable runtime constraints.",
        "- Use `assess` after verify when a proceed/escalate/block decision is needed.",
        "- Use `pack` to bundle reports and evidence.",
    ]
    .join("\n")
}

fn robot_docs() -> String {
    [
        "# verify doctor robot docs",
        "",
        "Read-only commands:",
        "- `verify doctor health [--json]` reports embedded schema and dispatch health.",
        "- `verify doctor capabilities --json` reports the `operator.v1` contract (same document as `verify capabilities --json`) inside a `cmdrvl.doctor.v1` report.",
        "- `verify doctor triage --json` emits the `cmdrvl.doctor.v1` triage report; `verify doctor --robot-triage` and `verify --robot-triage` are aliases.",
        "- `verify doctor robot-docs` prints this compact agent-facing reference.",
        "",
        "Safety contract:",
        "- The doctor surface does not read datasets, authoring files, compiled constraints, or stdin.",
        "- The doctor surface does not load DuckDB or execute constraint rules.",
        "- The doctor surface does not open, append, or create witness ledger files.",
        "- The implicit witness ledger fallback is `~/.cmdrvl/state/witness/witness.jsonl`; `EPISTEMIC_WITNESS` remains an explicit operator override.",
        "- No `doctor --fix` mode exists in this slice.",
        "",
        "Exit codes:",
        "- `0`: doctor checks healthy, or documentation completed.",
        "- `1`: doctor found an unhealthy diagnostic (an `error` finding). Changed from `2` in the family contract.",
        "- `2`: command-line refusal or usage error only.",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use spine_rules::doctor::{DoctorMode, DoctorReport, check_doctor_run};

    use super::*;

    fn diagnostics(failing: bool) -> Diagnostics {
        Diagnostics {
            report_schema_title: "verify.report.v1".to_owned(),
            constraint_schema_title: "verify.constraint.v1".to_owned(),
            witness_ledger_path: "/nonexistent/witness.jsonl".to_owned(),
            checks: vec![
                Check {
                    name: "report_schema_embedded_json",
                    ok: !failing,
                    detail: "planted".to_owned(),
                },
                Check {
                    name: "doctor_dispatch_read_only",
                    ok: true,
                    detail: "ok".to_owned(),
                },
            ],
        }
    }

    fn args(json: bool, robot_triage: bool, action: Option<DoctorAction>) -> DoctorArgs {
        DoctorArgs {
            robot_triage,
            json,
            action,
        }
    }

    fn json_flag() -> DoctorJsonArgs {
        DoctorJsonArgs { json: true }
    }

    /// Planted negative: an unhealthy doctor exits 1 in every JSON spelling,
    /// and the exit code agrees with the report under the family checker.
    #[test]
    fn unhealthy_doctor_exits_one_never_two() {
        let spellings: [(DoctorMode, DoctorArgs); 5] = [
            (DoctorMode::Health, args(true, false, None)),
            (
                DoctorMode::Health,
                args(false, false, Some(DoctorAction::Health(json_flag()))),
            ),
            (
                DoctorMode::Capabilities,
                args(false, false, Some(DoctorAction::Capabilities(json_flag()))),
            ),
            (
                DoctorMode::Triage,
                args(false, false, Some(DoctorAction::Triage(json_flag()))),
            ),
            // Deprecated alias still works and still exits 1.
            (DoctorMode::Triage, args(false, true, None)),
        ];
        for (mode, spelling) in spellings {
            let result = render(spelling.clone(), &diagnostics(true));
            assert_eq!(
                result.exit_code, 1,
                "{mode:?}: unhealthy must exit 1, not 2"
            );
            let stdout = result.stdout.expect("json report");
            let violations = check_doctor_run(mode, stdout.as_bytes(), Some(1));
            assert!(violations.is_empty(), "{mode:?}: {violations:?}");
            let report = DoctorReport::parse(&stdout).expect("report parses");
            assert!(!report.healthy);
            assert!(
                report
                    .findings
                    .iter()
                    .any(|f| f.id == "report_schema_embedded_json" && f.fix.is_some())
            );
            // The checker really does reject the old behavior.
            assert!(
                !check_doctor_run(mode, stdout.as_bytes(), Some(2)).is_empty(),
                "{mode:?}: exit 2 for an unhealthy report must be a contract violation"
            );
        }
    }

    #[test]
    fn healthy_doctor_exits_zero_and_human_mode_follows_health() {
        let healthy = diagnostics(false);
        assert_eq!(render(args(true, false, None), &healthy).exit_code, 0);
        let unhealthy = diagnostics(true);
        let human = render(args(false, false, None), &unhealthy);
        assert_eq!(human.exit_code, 1);
        assert!(human.stdout.is_some_and(|text| text.contains("unhealthy")));
        // robot-docs is documentation, never a health verdict.
        let docs = render(
            args(false, false, Some(DoctorAction::RobotDocs)),
            &unhealthy,
        );
        assert_eq!(docs.exit_code, 0);
    }

    #[test]
    fn capabilities_report_embeds_the_operator_contract_verbatim() {
        let result = render(
            args(false, false, Some(DoctorAction::Capabilities(json_flag()))),
            &diagnostics(false),
        );
        let report: Value = serde_json::from_str(&result.stdout.expect("json")).expect("parses");
        let operator: Value = serde_json::from_str(render::operator_contract()).expect("parses");
        assert_eq!(report["contract"], operator);
    }
}
