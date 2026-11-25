//! verify-specific checks for the family contract (bd-dbr), beyond what
//! `golden_rules_suite!` generates. Each one fails against the pre-change
//! behavior: doctor unhealthy exiting 2, an empty witness query exiting 1,
//! old doctor spellings emitting per-tool schema ids, and so on.

use std::{fs, path::PathBuf, process::Command};

use serde_json::{Value, json};

fn verify() -> Command {
    Command::new(env!("CARGO_BIN_EXE_verify"))
}

fn fixture(relative: &str) -> String {
    format!("{}/../../{relative}", env!("CARGO_MANIFEST_DIR"))
}

fn run_json(command: &mut Command) -> (Option<i32>, Value, String) {
    let output = command.output().expect("verify should run");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let value = serde_json::from_str(&stdout).unwrap_or(Value::Null);
    (output.status.code(), value, stdout)
}

fn ledger() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("witness.jsonl");
    (dir, path)
}

// ---- empty witness reads are success, never exit 1 -------------------------------

#[test]
fn empty_witness_query_exits_zero_and_prints_exactly_an_empty_array() {
    let (_dir, path) = ledger();
    let output = verify()
        .env("EPISTEMIC_WITNESS", &path)
        .args(["witness", "query", "--json"])
        .output()
        .expect("witness query should run");
    assert_eq!(output.status.code(), Some(0), "empty query must exit 0");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "[]");
    assert!(!path.exists(), "a read must not create the ledger");
}

#[test]
fn empty_witness_count_and_last_exit_zero_with_typed_empty_values() {
    let (_dir, path) = ledger();
    let (code, value, _) = run_json(
        verify()
            .env("EPISTEMIC_WITNESS", &path)
            .args(["witness", "count", "--json"]),
    );
    assert_eq!((code, value), (Some(0), json!({"count": 0})));
    let (code, value, _) = run_json(
        verify()
            .env("EPISTEMIC_WITNESS", &path)
            .args(["witness", "last", "--json"]),
    );
    assert_eq!((code, value), (Some(0), Value::Null));
    // Human mode: still exit 0.
    let output = verify()
        .env("EPISTEMIC_WITNESS", &path)
        .args(["witness", "query"])
        .output()
        .expect("human query");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn witness_filters_select_records_and_an_unmatched_filter_is_still_exit_zero() {
    let (_dir, path) = ledger();
    let run = verify()
        .env("EPISTEMIC_WITNESS", &path)
        .args([
            "run",
            &fixture("fixtures/constraints/arity1/not_null_loans.verify.json"),
            "--bind",
            &format!("input={}", fixture("fixtures/inputs/arity1/loans.csv")),
            "--json",
        ])
        .output()
        .expect("run should execute");
    assert_eq!(run.status.code(), Some(0));

    let query = |extra: &[&str]| {
        let mut args = vec!["witness", "query", "--json"];
        args.extend_from_slice(extra);
        run_json(verify().env("EPISTEMIC_WITNESS", &path).args(args))
    };
    let (code, all, _) = query(&[]);
    assert_eq!((code, all.as_array().map(Vec::len)), (Some(0), Some(1)));
    let (_, pass, _) = query(&["--outcome", "PASS"]);
    assert_eq!(pass.as_array().map(Vec::len), Some(1));
    let (code, fail, text) = query(&["--outcome", "FAIL"]);
    assert_eq!((code, fail, text.trim()), (Some(0), json!([]), "[]"));
    let (_, by_command, _) = query(&["--command", "run", "--limit", "5"]);
    assert_eq!(by_command.as_array().map(Vec::len), Some(1));
    let (code, count, _) = run_json(verify().env("EPISTEMIC_WITNESS", &path).args([
        "witness",
        "count",
        "--json",
        "--outcome",
        "REFUSAL",
    ]));
    assert_eq!((code, count), (Some(0), json!({"count": 0})));
}

#[test]
fn witness_usage_errors_still_exit_two() {
    let output = verify()
        .args(["witness", "bogus"])
        .output()
        .expect("witness should run");
    assert_eq!(output.status.code(), Some(2));
}

// ---- doctor: family schema, old spellings kept as aliases ------------------------

#[test]
fn every_doctor_spelling_emits_cmdrvl_doctor_v1_with_the_right_mode() {
    let spellings: [(&[&str], &str); 7] = [
        (&["doctor", "--json"], "health"),
        (&["doctor", "health", "--json"], "health"),
        (&["doctor", "capabilities", "--json"], "capabilities"),
        (&["doctor", "triage", "--json"], "triage"),
        // deprecated aliases, still working
        (&["doctor", "--robot-triage"], "triage"),
        (&["--robot-triage"], "triage"),
        (&["doctor", "health"], "health"), // human text, checked separately below
    ];
    for (args, mode) in &spellings[..6] {
        let (code, report, text) = run_json(verify().args(*args));
        assert_eq!(code, Some(0), "{args:?}: {text}");
        assert_eq!(report["schema_version"], "cmdrvl.doctor.v1", "{args:?}");
        assert_eq!(report["mode"], *mode, "{args:?}");
        assert_eq!(report["tool"], "verify");
        assert_eq!(report["healthy"], true);
        for old in [
            "verify.doctor.health.v1",
            "verify.doctor.triage.v1",
            "verify.doctor.capabilities.v1",
        ] {
            assert!(!text.contains(old), "{args:?} still mentions {old}");
        }
        assert!(
            report.get("schema").is_none(),
            "old `schema` key is gone: {args:?}"
        );
    }
    let output = verify()
        .args(spellings[6].0)
        .output()
        .expect("human doctor");
    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&output.stdout).contains("verify doctor health: healthy"));
}

#[test]
fn doctor_json_keeps_the_read_only_guarantees() {
    let (_, report, _) = run_json(verify().args(["doctor", "--json"]));
    assert_eq!(report["witness"]["opened"], false);
    assert_eq!(report["witness"]["appended"], false);
    assert_eq!(report["side_effects"]["loads_duckdb"], false);
    assert_eq!(report["fixers"], json!([]));
}

// ---- capabilities == describe, operator.v1 ----------------------------------------

#[test]
fn capabilities_json_is_the_operator_v1_document_and_equals_describe() {
    let (code, describe, _) = run_json(verify().arg("--describe"));
    assert_eq!(code, Some(0));
    let (code, caps, _) = run_json(verify().args(["capabilities", "--json"]));
    assert_eq!(code, Some(0));
    assert_eq!(describe, caps);
    assert_eq!(caps["schema_version"], "operator.v1");
    assert_eq!(caps["exit_codes"]["1"]["domain"], "negative");
    assert!(
        caps["exit_codes"]["1"]["meaning"]
            .as_str()
            .is_some_and(|m| m.contains("(doctor: unhealthy)"))
    );
    assert_eq!(
        caps["composition"]["input_boundary"]["relation_state"],
        "already_canonical"
    );
}

#[test]
fn cli_package_version_matches_the_workspace_version() {
    // verify-cli carries an explicit version (spine-rules rejects workspace inheritance);
    // keep it in lockstep with the root `[workspace.package] version`.
    let root = fs::read_to_string(format!("{}/../../Cargo.toml", env!("CARGO_MANIFEST_DIR")))
        .expect("root Cargo.toml");
    let workspace_version = spine_rules::contract::cargo_package_version(
        &root.replace("[workspace.package]", "[package]"),
    )
    .expect("workspace version");
    assert_eq!(workspace_version, env!("CARGO_PKG_VERSION"));
}

// ---- typo hints, NO_COLOR, set -e guidance ------------------------------------------

#[test]
fn mistyped_subcommand_gets_a_hint_instead_of_a_dataset_error() {
    for (typo, want) in [
        ("docter", "doctor"),
        ("witnes", "witness"),
        ("valdate", "validate"),
    ] {
        let output = verify().arg(typo).output().expect("verify should run");
        assert_eq!(output.status.code(), Some(2), "{typo}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("did you mean `{want}`")),
            "{typo}: {stderr}"
        );
    }
    // A dataset path that merely looks close is not hijacked: it reaches the shortcut parser.
    let output = verify()
        .arg("input.csv")
        .output()
        .expect("verify should run");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("did you mean"));
}

#[test]
fn no_color_and_dumb_terminal_output_has_no_ansi() {
    let escape = b"\x1b[";
    let has_ansi = |bytes: &[u8]| bytes.windows(2).any(|w| w == escape);
    for args in [
        &["--describe"][..],
        &["doctor", "health"],
        &["robot-docs", "guide"],
        &["docter"],
    ] {
        let output = verify()
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .args(args)
            .output()
            .expect("verify should run");
        assert!(
            !has_ansi(&output.stdout) && !has_ansi(&output.stderr),
            "{args:?}"
        );
    }
}

#[test]
fn robot_docs_document_the_exit_code_change_and_set_e_guidance() {
    let output = verify()
        .args(["robot-docs", "guide"])
        .output()
        .expect("robot docs should run");
    let guide = String::from_utf8_lossy(&output.stdout);
    for needle in [
        "set -e",
        "[ $? -eq 1 ]",
        "never exits `1` for an empty result",
        "doctor",
    ] {
        assert!(guide.contains(needle), "robot-docs guide lacks `{needle}`");
    }
    let output = verify()
        .args(["doctor", "robot-docs"])
        .output()
        .expect("doctor robot docs should run");
    let docs = String::from_utf8_lossy(&output.stdout);
    assert!(
        docs.contains("`1`: doctor found an unhealthy diagnostic"),
        "{docs}"
    );
    assert!(
        !docs.contains("`2`: command-line refusal or unhealthy"),
        "old exit-2 text remains"
    );
}
