use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::Value;

const OPERATOR_JSON: &str = include_str!("../operator.json");

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn verify_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
}

fn isolated_witness_path(name: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("verify-doctor-{name}-{}-{id}", std::process::id()))
        .join("witness.jsonl")
}

fn verify_command_with_witness(path: &Path) -> Command {
    let mut command = verify_command();
    command.env("EPISTEMIC_WITNESS", path);
    command
}

fn parse_stdout(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).expect("doctor stdout should be valid json")
}

fn assert_witness_absent(path: &Path) {
    assert!(
        !path.exists(),
        "doctor command should not create or append a witness ledger"
    );

    if let Some(parent) = path.parent() {
        assert!(
            !parent.exists(),
            "doctor command should not create a witness directory"
        );
    }
}

#[test]
fn doctor_health_json_is_read_only() {
    let witness = isolated_witness_path("health");
    let output = verify_command_with_witness(&witness)
        .args(["doctor", "health", "--json"])
        .output()
        .expect("doctor health should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let payload = parse_stdout(&output.stdout);
    assert_eq!(payload["schema"], "verify.doctor.health.v1");
    assert_eq!(payload["tool"], "verify");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["witness"]["opened"], false);
    assert_eq!(payload["witness"]["appended"], false);
    assert_eq!(payload["witness"]["directory_created"], false);
    assert_eq!(
        payload["config_footprint"]["managed_state_paths"][0],
        "~/.cmdrvl/state/witness/witness.jsonl"
    );
    assert_eq!(
        payload["config_footprint"]["legacy_migration_required"],
        true
    );
    assert_eq!(payload["side_effects"]["appends_witness_ledger"], false);
    assert_eq!(payload["side_effects"]["writes_migration_logs"], false);
    assert_eq!(payload["side_effects"]["writes_deprecation_notices"], false);
}

#[test]
fn doctor_capabilities_json_advertises_no_fixers_or_side_effects() {
    let witness = isolated_witness_path("capabilities");
    let output = verify_command_with_witness(&witness)
        .args(["doctor", "capabilities", "--json"])
        .output()
        .expect("doctor capabilities should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let payload = parse_stdout(&output.stdout);
    assert_eq!(payload["schema"], "verify.doctor.capabilities.v1");
    assert_eq!(payload["fixers"].as_array().map(Vec::len), Some(0));
    assert_eq!(payload["config_footprint"]["self_contained"], true);

    let side_effects = payload["side_effects"]
        .as_object()
        .expect("side effects should be an object");
    for (name, enabled) in side_effects {
        assert_eq!(enabled, false, "side effect `{name}` must remain disabled");
    }
}

#[test]
fn doctor_robot_triage_json_is_machine_readable() {
    let witness = isolated_witness_path("triage");
    let output = verify_command_with_witness(&witness)
        .args(["doctor", "--robot-triage"])
        .output()
        .expect("doctor robot triage should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let payload = parse_stdout(&output.stdout);
    assert_eq!(payload["schema"], "verify.doctor.triage.v1");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["summary"]["failed_checks"], 0);
    assert_eq!(payload["findings"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        payload["config_footprint"]["deprecation_notices"],
        "~/.cmdrvl/notices/deprecated-paths.jsonl"
    );
    assert_eq!(payload["side_effects"]["loads_duckdb"], false);
}

#[test]
fn top_level_robot_triage_json_is_machine_readable_and_read_only() {
    let witness = isolated_witness_path("top-level-triage");
    let output = verify_command_with_witness(&witness)
        .arg("--robot-triage")
        .output()
        .expect("top-level robot triage should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let payload = parse_stdout(&output.stdout);
    assert_eq!(payload["schema"], "verify.doctor.triage.v1");
    assert_eq!(payload["tool"], "verify");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["side_effects"]["loads_duckdb"], false);
}

#[test]
fn top_level_capabilities_json_describes_standard_agent_surfaces() {
    let witness = isolated_witness_path("top-level-capabilities");
    let output = verify_command_with_witness(&witness)
        .args(["capabilities", "--json"])
        .output()
        .expect("top-level capabilities should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let payload = parse_stdout(&output.stdout);
    assert_eq!(payload["schema"], "verify.capabilities.v1");
    assert_eq!(
        payload["standard_agent_surfaces"]["robot_triage"],
        "verify --robot-triage"
    );
    assert_eq!(
        payload["standard_agent_surfaces"]["capabilities_json"],
        "verify capabilities --json"
    );
    assert_eq!(
        payload["standard_agent_surfaces"]["operator_contract"],
        "verify --describe"
    );
    assert_eq!(payload["protocols"]["operator"], "operator.v0");
    assert!(
        payload["commands"]
            .as_array()
            .expect("commands should be an array")
            .iter()
            .any(|command| command["command"] == "verify --describe"
                && command["stdout"] == "operator.v0 JSON")
    );
    assert!(
        payload["commands"]
            .as_array()
            .expect("commands should be an array")
            .iter()
            .any(|command| command["command"]
                == "verify run <COMPILED_CONSTRAINTS> --bind <NAME=PATH> [--json] [--no-witness]"
                && command["domain_outcomes"]["1"] == "FAIL")
    );
    assert_eq!(payload["doctor"]["schema"], "verify.doctor.capabilities.v1");
    assert_eq!(
        payload["composition"]["input_boundary"]["relation_state"],
        "already_canonical"
    );
    assert_eq!(
        payload["composition"]["input_boundary"]["produced_by"],
        "profile's bounded materialization"
    );
    assert!(
        payload["composition"]["input_boundary"]["verify_never"]
            .as_array()
            .expect("verify_never should be an array")
            .iter()
            .any(|entry| entry == "resolves --profile-id")
    );
    assert_eq!(
        payload["composition"]["provenance_boundary"]["canonical_input_profile_provenance"],
        "lock.v0 profiles[] and pack membership"
    );
    assert_eq!(
        payload["composition"]["provenance_boundary"]["not_in"],
        "verify.report.v1"
    );
}

#[test]
fn top_level_robot_docs_guide_is_plain_text_and_read_only() {
    let witness = isolated_witness_path("top-level-docs");
    let output = verify_command_with_witness(&witness)
        .args(["robot-docs", "guide"])
        .output()
        .expect("top-level robot docs should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("verify robot-docs guide"));
    assert!(stdout.contains("verify --robot-triage"));
    assert!(stdout.contains("Outcome contract"));
    assert!(stdout.contains("already-canonical named relations"));
    assert!(stdout.contains("never loads profiles"));
    assert!(stdout.contains("lock.v0"));
    assert!(stdout.contains("not `verify.report.v1`"));
    assert!(stdout.contains("assess"));
    assert!(stdout.contains("pack"));
}

#[test]
fn operator_json_is_valid_and_mentions_crv1_boundary() {
    let manifest: Value =
        serde_json::from_str(OPERATOR_JSON).expect("operator.json should be valid JSON");

    assert_eq!(manifest["schema_version"], "operator.v0");
    assert_eq!(manifest["name"], "verify");
    assert_eq!(manifest["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest["capabilities"]["profile_aware"], false);
    assert_eq!(manifest["capabilities"]["header_rewriting"], false);
    assert_eq!(
        manifest["capabilities"]["crv1_boundary"]["input"],
        "already-canonical named relations produced upstream by profile's bounded materialization"
    );
    assert!(
        manifest["capabilities"]["crv1_boundary"]["verify_never"]
            .as_array()
            .expect("verify_never should be an array")
            .iter()
            .any(|entry| entry == "resolves --profile-id")
    );
    assert_eq!(
        manifest["capabilities"]["crv1_boundary"]["provenance"],
        "canonical input and profile provenance rides in lock.v0 profiles[] and pack membership, not verify.report.v1"
    );
}

#[test]
fn describe_emits_operator_manifest_and_is_read_only() {
    let witness = isolated_witness_path("describe");
    let output = verify_command_with_witness(&witness)
        .arg("--describe")
        .output()
        .expect("describe should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let manifest = parse_stdout(&output.stdout);
    assert_eq!(manifest["schema_version"], "operator.v0");
    assert_eq!(manifest["name"], "verify");
    assert_eq!(manifest["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest["capabilities"]["profile_aware"], false);
    assert_eq!(
        manifest["capabilities"]["agent_surfaces"]["operator_contract"],
        "verify --describe"
    );
}

#[test]
fn doctor_robot_docs_is_plain_text_and_read_only() {
    let witness = isolated_witness_path("docs");
    let output = verify_command_with_witness(&witness)
        .args(["doctor", "robot-docs"])
        .output()
        .expect("doctor robot docs should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_witness_absent(&witness);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("verify doctor robot docs"));
    assert!(stdout.contains("verify doctor health [--json]"));
    assert!(stdout.contains("~/.cmdrvl/state/witness/witness.jsonl"));
    assert!(stdout.contains("No `doctor --fix` mode exists"));
}

#[test]
fn doctor_help_is_not_routed_to_shortcut_parser() {
    let output = verify_command()
        .args(["doctor", "--help"])
        .output()
        .expect("doctor help should run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("health"));
    assert!(stdout.contains("capabilities"));
    assert!(!stdout.contains("__shortcut__"));
}

#[test]
fn doctor_fix_is_not_available() {
    let witness = isolated_witness_path("fix");
    let output = verify_command_with_witness(&witness)
        .args(["doctor", "--fix"])
        .output()
        .expect("doctor fix refusal should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_witness_absent(&witness);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--fix"));
}
