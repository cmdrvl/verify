//! Redaction markers (bd-dav): ordinal refs by default, opt-in keyed HMAC, key hygiene.
//! The bd-q4d canary tests in tests/cli.rs keep covering "zero raw values in every channel".

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

const KEY_ENV: &str = "VERIFY_REDACTION_KEY";
const CANARY_KEY: &str = "CANARY-KEY-7f3a91";
const CANARY_CELL: &str = "CANARY-CELL-c2d8e4";
/// A 16-byte key; markers below were cross-checked with
/// `printf '%s' '"<value>"' | openssl dgst -sha256 -hmac 0123456789abcdef`.
const TEST_KEY: &str = "0123456789abcdef";
const KEY_MARKER: &str = "redacted:hmac-sha256:32dceb15d63e18d75268bf4d7b813cdf";
const CELL_MARKER: &str = "redacted:hmac-sha256:0dd139a43c5c4a0d7ae4619dde84bcec";
/// Same canary key value under key `0123456789abcdeg` (openssl).
const KEY_MARKER_OTHER_KEY: &str = "redacted:hmac-sha256:95dc32c800ca174b9a137cb4824ec5ad";
const PLANTED_CANARY: &str = "PLANTED-KEYCANARY-9d41";

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "verify-redaction-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, contents).expect("write scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn constraints(scratch: &Scratch) -> PathBuf {
    scratch.write(
        "rule.verify.json",
        r#"{
          "version": "verify.constraint.v1",
          "constraint_set_id": "redaction.test",
          "bindings": [{"name": "property", "kind": "relation", "key_fields": ["property_id"]}],
          "rules": [{
            "id": "EVERY_ROW",
            "severity": "error",
            "portability": "batch_only",
            "check": {"op": "query_zero_rows", "bindings": ["property"],
                      "query": "SELECT property_id AS key__property_id, secret AS value FROM property"}
          }]
        }"#,
    )
}

fn run(rule: &Path, csv: &Path, key: Option<&str>, extra: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    command.env_remove(KEY_ENV);
    if let Some(key) = key {
        command.env(KEY_ENV, key);
    }
    command
        .args(["run"])
        .arg(rule)
        .args(["--bind", &format!("property={}", csv.display())])
        .args(extra)
        .arg("--no-witness")
        .output()
        .expect("verify run should execute")
}

fn text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("json report on stdout")
}

/// (key marker, cell marker) per affected entry, in output order.
fn markers(report: &Value) -> Vec<(String, String)> {
    report["results"][0]["affected"]
        .as_array()
        .expect("affected entries")
        .iter()
        .map(|entry| {
            (
                entry["key"]["property_id"]
                    .as_str()
                    .expect("key marker")
                    .to_owned(),
                entry["value"].as_str().expect("cell marker").to_owned(),
            )
        })
        .collect()
}

fn old_tokens(value: &str) -> Vec<String> {
    let canonical = serde_json::to_string(&Value::String(value.to_owned())).expect("json");
    let digest = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    vec![digest[..16].to_owned(), digest]
}

fn canary_csv(scratch: &Scratch) -> PathBuf {
    scratch.write(
        "canary.csv",
        &format!("property_id,secret\n{CANARY_KEY},{CANARY_CELL}\n"),
    )
}

// ---------------------------------------------------------------------------
// Default: ordinal refs
// ---------------------------------------------------------------------------

#[test]
fn default_refs_are_ordinal_shared_for_equal_values_and_namespaced() {
    let scratch = Scratch::new("ordinals");
    let rule = constraints(&scratch);
    // Entries sort by key. Cells: shared, shared, other. One cell equals a key's bytes.
    let csv = scratch.write(
        "rows.csv",
        "property_id,secret\nid-a,same\nid-b,same\nid-c,other\nsame,id-a\n",
    );
    let output = run(&rule, &csv, None, &["--json"]);
    assert_eq!(output.status.code(), Some(1));
    let pairs = markers(&json(&output));
    let keys: Vec<&str> = pairs.iter().map(|p| p.0.as_str()).collect();
    let cells: Vec<&str> = pairs.iter().map(|p| p.1.as_str()).collect();

    // Four distinct keys numbered by first appearance in output order.
    assert_eq!(
        keys,
        [
            "redacted:ref:k1",
            "redacted:ref:k2",
            "redacted:ref:k3",
            "redacted:ref:k4"
        ]
    );
    // Equal cells share a ref, distinct cells never do, numbered by first appearance.
    assert_eq!(
        cells,
        [
            "redacted:ref:v1",
            "redacted:ref:v1",
            "redacted:ref:v2",
            "redacted:ref:v3"
        ]
    );
    // The key "same" (entry 4) and the cell "same" (entries 1-2) hold equal bytes yet get
    // refs in different namespaces, so their equality is not revealed.
    assert_ne!(keys[3], cells[0]);
    assert!(keys[3].starts_with("redacted:ref:k") && cells[0].starts_with("redacted:ref:v"));

    // Byte-identical on rerun.
    assert_eq!(output.stdout, run(&rule, &csv, None, &["--json"]).stdout);
}

#[test]
fn sampling_numbers_only_the_emitted_entries() {
    let scratch = Scratch::new("sample");
    let rule = constraints(&scratch);
    let csv = scratch.write(
        "rows.csv",
        "property_id,secret\nid-a,c1\nid-b,c2\nid-c,c3\nid-d,c4\n",
    );
    let output = run(&rule, &csv, None, &["--sample-affected", "1"]);
    assert_eq!(output.status.code(), Some(1));
    let out = text(&output);
    assert!(
        out.contains("redacted:ref:k1") && out.contains("redacted:ref:v1"),
        "{out}"
    );
    for hidden in ["k2", "k3", "k4", "v2", "v3", "v4"] {
        assert!(
            !out.contains(&format!("redacted:ref:{hidden}")),
            "{hidden} leaked: {out}"
        );
    }
    assert!(!out.contains("id-a") && !out.contains("c1"), "{out}");
    assert_eq!(
        output.stdout,
        run(&rule, &csv, None, &["--sample-affected", "1"]).stdout
    );
}

#[test]
fn the_old_sha256_token_for_a_known_canary_matches_nothing() {
    let scratch = Scratch::new("old-token");
    let rule = constraints(&scratch);
    let csv = canary_csv(&scratch);
    for (label, key) in [("default", None), ("keyed", Some(TEST_KEY))] {
        for extra in [&["--json"][..], &[][..]] {
            let out = text(&run(&rule, &csv, key, extra));
            for canary in [CANARY_KEY, CANARY_CELL] {
                for token in old_tokens(canary) {
                    assert!(
                        !out.contains(&token),
                        "{label} {extra:?} contains old token {token}"
                    );
                }
                assert!(!out.contains(canary), "{label} {extra:?} leaked {canary}");
            }
            assert!(!out.contains("redacted:sha256:"), "{label}: {out}");
        }
    }
    // Default output computes no hash of the value at all: no marker looks like one.
    let out = text(&run(&rule, &csv, None, &["--json"]));
    assert!(!out.contains("hmac"), "{out}");
}

// ---------------------------------------------------------------------------
// Keyed markers
// ---------------------------------------------------------------------------

#[test]
fn keyed_markers_match_the_openssl_known_answers_and_are_stable() {
    let scratch = Scratch::new("keyed");
    let rule = constraints(&scratch);
    let csv = canary_csv(&scratch);
    let output = run(&rule, &csv, Some(TEST_KEY), &["--json"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        markers(&json(&output)),
        vec![(KEY_MARKER.to_owned(), CELL_MARKER.to_owned())]
    );

    // Same key, different input and ordering: the canary entry keeps its markers.
    let other = scratch.write(
        "other.csv",
        &format!("property_id,secret\nzzz,other\n{CANARY_KEY},{CANARY_CELL}\n"),
    );
    let again = markers(&json(&run(&rule, &other, Some(TEST_KEY), &["--json"])));
    assert!(
        again.contains(&(KEY_MARKER.to_owned(), CELL_MARKER.to_owned())),
        "{again:?}"
    );

    // A different key gives different markers (checked against openssl for key ...deg).
    let different = markers(&json(&run(
        &rule,
        &csv,
        Some("0123456789abcdeg"),
        &["--json"],
    )));
    assert_eq!(different[0].0, KEY_MARKER_OTHER_KEY);
    assert_ne!(different[0].0, KEY_MARKER);

    // Human output is keyed too; no ordinal refs appear in keyed mode.
    let human = text(&run(&rule, &csv, Some(TEST_KEY), &[]));
    assert!(
        human.contains(KEY_MARKER) && !human.contains("redacted:ref:"),
        "{human}"
    );
}

#[test]
fn empty_and_short_keys_are_refused_with_the_typed_code_and_16_bytes_work() {
    let scratch = Scratch::new("key-refusal");
    let rule = constraints(&scratch);
    let csv = canary_csv(&scratch);
    for (label, key) in [("empty", ""), ("15 bytes", "ABCDEFGHIJKLMNO")] {
        for extra in [&["--json"][..], &[][..]] {
            let output = run(&rule, &csv, Some(key), extra);
            assert_eq!(output.status.code(), Some(2), "{label} {extra:?}");
            let out = text(&output);
            assert!(
                out.contains("E_REDACTION_KEY") || out.contains("VERIFY_REDACTION_KEY"),
                "{label}: {out}"
            );
            assert!(
                !out.contains("ABCDEFGHIJKLMNO") || key.is_empty(),
                "{label} echoed the key: {out}"
            );
            assert!(
                !out.contains("redacted:ref:"),
                "{label} must not fall back to ordinals: {out}"
            );
            assert!(!out.contains(CANARY_KEY) && !out.contains(CANARY_CELL));
        }
        let refusal = json(&run(&rule, &csv, Some(key), &["--json"]));
        assert_eq!(refusal["refusal"]["code"], "E_REDACTION_KEY", "{label}");
        assert_eq!(refusal["refusal"]["detail"]["min_bytes"], 16);
    }
    // `--explicit` does not excuse a bad key: a misconfigured key is never silently ignored.
    let explicit = run(&rule, &csv, Some(""), &["--json", "--explicit"]);
    assert_eq!(explicit.status.code(), Some(2));

    let ok = run(&rule, &csv, Some("0123456789abcdef"), &["--json"]);
    assert_eq!(ok.status.code(), Some(1));
    assert!(text(&ok).contains("redacted:hmac-sha256:"));
}

#[test]
fn explicit_still_restores_raw_values_even_with_a_key_set() {
    let scratch = Scratch::new("explicit");
    let rule = constraints(&scratch);
    let csv = canary_csv(&scratch);
    let out = text(&run(&rule, &csv, Some(TEST_KEY), &["--json", "--explicit"]));
    assert!(
        out.contains(CANARY_KEY) && out.contains(CANARY_CELL),
        "{out}"
    );
    assert!(!out.contains("redacted:"), "{out}");
}

// ---------------------------------------------------------------------------
// Key hygiene
// ---------------------------------------------------------------------------

#[test]
fn the_key_never_appears_in_any_channel_including_the_witness_ledger_and_doctor() {
    let scratch = Scratch::new("hygiene");
    let rule = constraints(&scratch);
    let csv = canary_csv(&scratch);
    let ledger = scratch.0.join("witness.jsonl");

    let mut seen = Vec::new();
    // A real keyed run that DOES write a witness record.
    for extra in [&["--json"][..], &[][..]] {
        let output = Command::new(env!("CARGO_BIN_EXE_verify"))
            .env(KEY_ENV, PLANTED_CANARY)
            .env("EPISTEMIC_WITNESS", &ledger)
            .arg("run")
            .arg(&rule)
            .args(["--bind", &format!("property={}", csv.display())])
            .args(extra)
            .output()
            .expect("verify run");
        assert_eq!(output.status.code(), Some(1));
        seen.push(("run", text(&output)));
    }
    // A refusal run (key accepted, input missing) and a bad-key refusal, both with a ledger.
    for key in [PLANTED_CANARY, "short-key-PLANTED"] {
        let output = Command::new(env!("CARGO_BIN_EXE_verify"))
            .env(KEY_ENV, key)
            .env("EPISTEMIC_WITNESS", &ledger)
            .args([
                "run",
                "/nonexistent/rule.verify.json",
                "--bind",
                "property=/nonexistent.csv",
            ])
            .output()
            .expect("verify run");
        seen.push(("refusal", text(&output)));
    }
    // Doctor, capabilities and robot-docs with the key set.
    for args in [
        &["doctor", "health", "--json"][..],
        &["doctor", "triage", "--json"][..],
        &["capabilities", "--json"][..],
        &["robot-docs", "guide"][..],
        &["--describe"][..],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_verify"))
            .env(KEY_ENV, PLANTED_CANARY)
            .env("EPISTEMIC_WITNESS", &ledger)
            .args(args)
            .output()
            .expect("verify command");
        seen.push(("introspection", text(&output)));
    }
    for (label, out) in &seen {
        assert!(
            !out.contains(PLANTED_CANARY),
            "{label} leaked the key: {out}"
        );
        assert!(
            !out.contains("short-key-PLANTED"),
            "{label} leaked the key: {out}"
        );
    }
    let ledger_text = fs::read_to_string(&ledger).expect("witness ledger was written");
    assert!(!ledger_text.is_empty());
    assert!(!ledger_text.contains(PLANTED_CANARY) && !ledger_text.contains("short-key-PLANTED"));
    assert!(!ledger_text.contains(CANARY_KEY) && !ledger_text.contains(CANARY_CELL));
}
