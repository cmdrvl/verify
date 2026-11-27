//! The binary under test against what the released `verify` 0.7.6 printed.
//!
//! `fixtures/released-0.7.6/parity.json` holds synthetic inputs and the exit status, stdout and
//! stderr OBSERVED from the released macOS x86_64 binary (checked against its `SHA256SUMS`). It
//! was not computed by this repository's code. It covers lock checking in `run` and `ground`,
//! the hashes a report carries, ordinal and keyed redaction in JSON and human output, the
//! redaction-key refusals and witness ledger lines.
//!
//! Some recorded results are weaknesses that are pinned as they are, not endorsed: for example,
//! two locks listing one member path accept either hash (bd-6nk). A deliberate behaviour change
//! must change the affected cases in the same commit and say why. Do not record from the code
//! under test.
//!
//! In witness lines `id`, `binary_hash`, `ts` and the tool version vary, so they are masked.
//! The `id` is checked separately: it is FNV-1a 64 of the line with the `id` left empty.

use std::{fs, path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;

const RECORD: &str = include_str!("../fixtures/released-0.7.6/parity.json");
const CASES: usize = 105;
const WITNESS_CASES: usize = 8;

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().expect("parity record string field")
}

/// `verify` in `work`, with a private HOME and ledger and no inherited redaction key.
fn verify(work: &Path, ledger: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    command
        .current_dir(work)
        .env("HOME", work.join("home"))
        .env("EPISTEMIC_WITNESS", work.join(ledger))
        .env_remove("VERIFY_REDACTION_KEY");
    command
}

/// Replace what stands between `prefix` and the next `"` after its first occurrence.
fn mask(line: &str, prefix: &str, with: &str) -> String {
    let Some(found) = line.find(prefix) else {
        return line.to_owned();
    };
    let start = found + prefix.len();
    let Some(length) = line[start..].find('"') else {
        return line.to_owned();
    };
    format!("{}{with}{}", &line[..start], &line[start + length..])
}

fn fnv1a64(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    });
    format!("fnv1a64:{hash:016x}")
}

/// Every way this case's results differ from the released ones.
fn differences(case: &Value) -> Vec<String> {
    let work = TempDir::new().expect("temporary directory");
    let work = work.path();
    fs::create_dir(work.join("home")).expect("home directory");
    for (path, content) in case["files"].as_object().expect("files") {
        let path = work.join(path);
        fs::create_dir_all(path.parent().expect("parent")).expect("case directory");
        fs::write(path, content.as_str().expect("file text")).expect("case file");
    }
    let compile_source = match &case["compile"] {
        Value::Bool(true) => Some("rules.yaml"),
        Value::String(kind) if kind == "sql" => Some("rules.sql"),
        _ => None,
    };
    if let Some(source) = compile_source {
        let output = verify(work, "setup-witness.jsonl")
            .args(["compile", source, "--out", "c.json"])
            .output()
            .expect("compile runs");
        assert!(output.status.success(), "setup compile failed: {output:?}");
    }

    let mut command = verify(work, "witness.jsonl");
    for argument in case["argv"].as_array().expect("argv") {
        command.arg(argument.as_str().expect("argument"));
    }
    if let Some(key) = case["key"].as_str() {
        command.env("VERIFY_REDACTION_KEY", key);
    }
    let output = command.output().expect("verify runs");

    let expected = &case["expected"];
    let mut found = Vec::new();
    let mut compare = |what: &str, actual: &str, released: &str| {
        if actual != released {
            found.push(format!(
                "{what}\n  released: {released}\n  now:      {actual}"
            ));
        }
    };
    compare(
        "exit",
        &format!("{:?}", output.status.code()),
        &format!("{:?}", expected["exit"].as_i64()),
    );
    compare(
        "stdout",
        &String::from_utf8_lossy(&output.stdout),
        text(expected, "stdout"),
    );
    compare(
        "stderr",
        &String::from_utf8_lossy(&output.stderr),
        text(expected, "stderr"),
    );
    if let Some(artifact) = expected["artifact"].as_str() {
        let written = fs::read_to_string(work.join("c.json")).unwrap_or_default();
        compare("compiled artifact", &written, artifact);
    }
    if case["witness"].as_bool() == Some(true) {
        let ledger = fs::read_to_string(work.join("witness.jsonl")).unwrap_or_default();
        let line = ledger.trim_end_matches('\n');
        let id = serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|record| record["id"].as_str().map(str::to_owned))
            .unwrap_or_default();
        compare(
            "witness id over its own line",
            &id,
            &fnv1a64(line.replacen(&id, "", 1).as_bytes()),
        );
        let masked = mask(line, "\"id\":\"", "<id>");
        let masked = mask(&masked, "\"binary_hash\":\"", "<binary_hash>");
        let masked = mask(&masked, "\"ts\":\"", "<ts>");
        let masked = mask(&masked, "\"tool\":\"verify\",\"version\":\"", "<version>");
        compare("witness line", &masked, text(expected, "witness"));
    }
    found
}

#[test]
fn every_recorded_case_matches_the_released_binary() {
    let record: Value = serde_json::from_str(RECORD).expect("parity record is JSON");
    assert_eq!(text(&record, "recorded_from"), "verify 0.7.6");
    let cases = record["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), CASES);
    assert_eq!(
        cases
            .iter()
            .filter(|case| case["witness"].as_bool() == Some(true))
            .count(),
        WITNESS_CASES
    );

    let failures: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let found = differences(case);
            (!found.is_empty()).then(|| format!("{}\n{}", text(case, "name"), found.join("\n")))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {CASES} cases differ from released 0.7.6:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
