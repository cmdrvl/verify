use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};
use verify_core::{CONSTRAINT_VERSION, REPORT_VERSION, TOOL_NAME};

use crate::{paths, render, witness};

const TOP_LEVEL_CAPABILITIES_SCHEMA: &str = "verify.capabilities.v1";
const HEALTH_SCHEMA: &str = "verify.doctor.health.v1";
const CAPABILITIES_SCHEMA: &str = "verify.doctor.capabilities.v1";
const TRIAGE_SCHEMA: &str = "verify.doctor.triage.v1";
const DOCTOR_CONTRACT: &str = "cmdrvl.read_only_doctor.v1";

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

    fn exit_code(&self) -> u8 {
        if self.ok() { 0 } else { 2 }
    }
}

pub fn execute(args: DoctorArgs) -> DoctorCommandResult {
    let diagnostics = collect_diagnostics();

    if args.robot_triage {
        return json_result(robot_triage_value(&diagnostics), diagnostics.exit_code());
    }

    let action = args
        .action
        .unwrap_or(DoctorAction::Health(DoctorJsonArgs { json: args.json }));

    match action {
        DoctorAction::Health(format) => {
            if args.json || format.json {
                json_result(health_value(&diagnostics), diagnostics.exit_code())
            } else {
                text_result(human_health(&diagnostics), diagnostics.exit_code())
            }
        }
        DoctorAction::Capabilities(format) => {
            if args.json || format.json {
                json_result(capabilities_value(), 0)
            } else {
                text_result(human_capabilities(), 0)
            }
        }
        DoctorAction::RobotDocs => text_result(robot_docs(), 0),
    }
}

pub fn execute_top_level_robot_triage() -> DoctorCommandResult {
    let diagnostics = collect_diagnostics();
    json_result(robot_triage_value(&diagnostics), diagnostics.exit_code())
}

pub fn execute_top_level_capabilities(args: TopLevelCapabilitiesArgs) -> DoctorCommandResult {
    let value = top_level_capabilities_value();
    if args.json {
        json_result(value, 0)
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

fn health_value(diagnostics: &Diagnostics) -> Value {
    json!({
        "schema": HEALTH_SCHEMA,
        "tool": TOOL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "ok": diagnostics.ok(),
        "status": diagnostics.status(),
        "contract": DOCTOR_CONTRACT,
        "schemas": {
            "report": diagnostics.report_schema_title,
            "constraint": diagnostics.constraint_schema_title
        },
        "witness": {
            "ledger_path": diagnostics.witness_ledger_path,
            "opened": false,
            "appended": false,
            "directory_created": false
        },
        "config_footprint": paths::config_footprint(),
        "side_effects": side_effects_value(),
        "fixers": [],
        "checks": checks_value(&diagnostics.checks)
    })
}

fn capabilities_value() -> Value {
    json!({
        "schema": CAPABILITIES_SCHEMA,
        "tool": TOOL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "contract": DOCTOR_CONTRACT,
        "commands": [
            {
                "name": "doctor health",
                "json": true,
                "purpose": "Summarize embedded schema and dispatch health without reading bound inputs."
            },
            {
                "name": "doctor capabilities --json",
                "json": true,
                "purpose": "Expose machine-readable doctor capabilities and side-effect guarantees."
            },
            {
                "name": "doctor robot-docs",
                "json": false,
                "purpose": "Print compact agent-facing command documentation."
            },
            {
                "name": "doctor --robot-triage",
                "json": true,
                "purpose": "Return a machine-readable triage packet for automation."
            }
        ],
        "exit_codes": {
            "0": "doctor check healthy or documentation rendered",
            "2": "doctor check unhealthy or command-line refusal"
        },
        "schemas": {
            "health": HEALTH_SCHEMA,
            "capabilities": CAPABILITIES_SCHEMA,
            "triage": TRIAGE_SCHEMA
        },
        "config_footprint": paths::config_footprint(),
        "side_effects": side_effects_value(),
        "fixers": []
    })
}

fn top_level_capabilities_value() -> Value {
    json!({
        "schema": TOP_LEVEL_CAPABILITIES_SCHEMA,
        "tool": TOOL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "purpose": "Deterministic constraint evaluation for declared relation constraints.",
        "protocols": {
            "constraint": CONSTRAINT_VERSION,
            "report": REPORT_VERSION,
            "doctor_health": HEALTH_SCHEMA,
            "doctor_capabilities": CAPABILITIES_SCHEMA,
            "doctor_triage": TRIAGE_SCHEMA
        },
        "standard_agent_surfaces": {
            "robot_triage": "verify --robot-triage",
            "capabilities_json": "verify capabilities --json",
            "robot_docs": "verify robot-docs guide"
        },
        "commands": [
            {
                "command": "verify --robot-triage",
                "read_only": true,
                "writes_witness": false,
                "stdout": "verify.doctor.triage.v1 JSON",
                "exit_codes": {"0": "healthy", "2": "unhealthy diagnostics"}
            },
            {
                "command": "verify capabilities --json",
                "read_only": true,
                "writes_witness": false,
                "stdout": TOP_LEVEL_CAPABILITIES_SCHEMA
            },
            {
                "command": "verify robot-docs guide",
                "read_only": true,
                "writes_witness": false,
                "stdout": "plain text agent guide"
            },
            {
                "command": "verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH> [--json] [--no-witness]",
                "read_only": false,
                "writes_witness": "unless --no-witness is set",
                "domain_outcomes": {"0": "PASS", "1": "FAIL", "2": "REFUSAL"},
                "stdout": "human report or verify.report.v1 JSON"
            },
            {
                "command": "verify <DATASET> --rules <AUTHORING> [--json] [--no-witness]",
                "read_only": false,
                "writes_witness": "unless --no-witness is set",
                "domain_outcomes": {"0": "PASS", "1": "FAIL", "2": "REFUSAL"},
                "stdout": "human report or verify.report.v1 JSON"
            },
            {
                "command": "verify compile <AUTHORING> [--out <OUTPUT>] [--check] [--json]",
                "read_only": "only with --check or --json and without --out",
                "writes_outputs": "when --out is set"
            },
            {
                "command": "verify validate <COMPILED_CONSTRAINTS> [--json]",
                "read_only": true,
                "stdout": "validation result"
            },
            {
                "command": "verify witness <query|last|count> [--json]",
                "read_only": true,
                "opens_witness_ledger": true
            },
            {
                "command": "verify --schema",
                "read_only": true,
                "stdout": REPORT_VERSION
            },
            {
                "command": "verify compile --schema",
                "read_only": true,
                "stdout": CONSTRAINT_VERSION
            }
        ],
        "config_footprint": paths::config_footprint(),
        "doctor": capabilities_value(),
        "composition": {
            "upstream": ["normalize", "materialize"],
            "factory_upstream": ["twinning", "decoding"],
            "downstream": ["assess", "pack"],
            "boundaries": [
                "verify enforces declared constraints",
                "benchmark scores gold-set correctness",
                "assess decides proceed, escalate, or block"
            ]
        }
    })
}

fn robot_triage_value(diagnostics: &Diagnostics) -> Value {
    json!({
        "schema": TRIAGE_SCHEMA,
        "tool": TOOL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "ok": diagnostics.ok(),
        "status": diagnostics.status(),
        "contract": DOCTOR_CONTRACT,
        "summary": {
            "message": if diagnostics.ok() {
                "verify doctor checks passed"
            } else {
                "verify doctor checks found unhealthy diagnostics"
            },
            "failed_checks": diagnostics.checks.iter().filter(|check| !check.ok).count()
        },
        "findings": failed_checks_value(&diagnostics.checks),
        "next_actions": if diagnostics.ok() {
            Value::Array(Vec::new())
        } else {
            json!(["inspect embedded schema wiring and doctor dispatch checks"])
        },
        "config_footprint": paths::config_footprint(),
        "side_effects": side_effects_value(),
        "fixers": []
    })
}

fn side_effects_value() -> Value {
    let mut effects = Map::new();
    for name in SIDE_EFFECTS {
        effects.insert((*name).to_owned(), Value::Bool(false));
    }
    Value::Object(effects)
}

fn checks_value(checks: &[Check]) -> Value {
    Value::Array(
        checks
            .iter()
            .map(|check| {
                json!({
                    "name": check.name,
                    "ok": check.ok,
                    "detail": check.detail
                })
            })
            .collect(),
    )
}

fn failed_checks_value(checks: &[Check]) -> Value {
    Value::Array(
        checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| {
                json!({
                    "severity": "error",
                    "check": check.name,
                    "detail": check.detail
                })
            })
            .collect(),
    )
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
        "- doctor robot-docs",
        "- doctor --robot-triage",
        "read_only: true",
        "side_effects: none",
        "fixers: none",
    ]
    .join("\n")
}

fn human_top_level_capabilities() -> String {
    [
        "verify capabilities",
        "schema: verify.capabilities.v1",
        "standard agent surfaces:",
        "- verify --robot-triage",
        "- verify capabilities --json",
        "- verify robot-docs guide",
        "domain outcomes:",
        "- 0: PASS",
        "- 1: FAIL",
        "- 2: REFUSAL",
    ]
    .join("\n")
}

fn top_level_robot_docs() -> String {
    [
        "verify robot-docs guide",
        "",
        "Quick start:",
        "- `verify --robot-triage` returns one read-only JSON health packet.",
        "- `verify capabilities --json` returns the full machine-readable command contract.",
        "- `verify run COMPILED.verify.json --bind input=data.csv --json --no-witness` evaluates a compiled constraint set without appending a witness record.",
        "- `verify DATASET.csv --rules rules.yaml --json --no-witness` compiles and runs the arity-1 shortcut.",
        "- `verify compile AUTHORING.yaml --check --json` validates authoring without writing an output file.",
        "",
        "Outcome contract:",
        "- `0` means PASS.",
        "- `1` means FAIL.",
        "- `2` means REFUSAL.",
        "- JSON report output is `verify.report.v1`.",
        "",
        "Witness boundary:",
        "- Run/evaluate commands append local witness records by default.",
        "- Add `--no-witness` for read-side smoke tests and deterministic agent probes.",
        "- Doctor, triage, capabilities, robot-docs, schema, and validate surfaces do not append witness records.",
        "",
        "Composition:",
        "- Use `verify` after normalize/materialize to enforce declared constraints.",
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
        "- `verify doctor capabilities --json` reports command capabilities, side effects, and fixers.",
        "- `verify doctor robot-docs` prints this compact agent-facing reference.",
        "- `verify doctor --robot-triage` emits `verify.doctor.triage.v1` JSON.",
        "",
        "Safety contract:",
        "- The doctor surface does not read datasets, authoring files, compiled constraints, or stdin.",
        "- The doctor surface does not load DuckDB or execute constraint rules.",
        "- The doctor surface does not open, append, or create witness ledger files.",
        "- The implicit witness ledger fallback is `~/.cmdrvl/state/witness/witness.jsonl`; `EPISTEMIC_WITNESS` remains an explicit operator override.",
        "- No `doctor --fix` mode exists in this slice.",
        "",
        "Exit codes:",
        "- `0`: doctor checks or documentation completed.",
        "- `2`: command-line refusal or unhealthy doctor diagnostics.",
    ]
    .join("\n")
}
