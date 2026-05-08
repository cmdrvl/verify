use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::Value;

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
    assert_eq!(payload["side_effects"]["appends_witness_ledger"], false);
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
    assert_eq!(payload["side_effects"]["loads_duckdb"], false);
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
