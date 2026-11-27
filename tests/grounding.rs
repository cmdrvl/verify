//! Grounding CLI coverage using a real SEC ABS-EE XML fixture.
//!
//! The document excerpt is copied byte-for-byte from the real filing fixture at
//! `/private/tmp/claude-501/-Users-zac-Source-cmdrvl/e0a56435-ad98-4f8d-91e6-79be859078a2/scratchpad/xml-fixtures/abs-ee/exh_102.xml`.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const FIXTURE: &str = "fixtures/grounding/abs_ee_exh_102_excerpt.xml";
const HTML_FIXTURE: &str = "fixtures/grounding/sec_8k_ex99_1_excerpt.html";

fn fixture_path() -> PathBuf {
    Path::new(WORKSPACE_ROOT).join(FIXTURE)
}

fn html_fixture_path() -> PathBuf {
    Path::new(WORKSPACE_ROOT).join(HTML_FIXTURE)
}

fn run_ground(claims: &Path, document: &Path, lock: &Path, explicit: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    command
        .args(["ground"])
        .arg(claims)
        .args(["--bind", &format!("filing={}", document.display())])
        .args(["--lock"])
        .arg(lock)
        .args(["--json", "--no-witness"]);
    if explicit {
        command.arg("--explicit");
    }
    command.output().expect("ground command should execute")
}

fn span(bytes: &[u8], needle: &[u8]) -> (usize, usize) {
    let start = bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("fixture contains requested span");
    (start, start + needle.len())
}

fn claims_file(dir: &TempDir, name: &str, claims: &[Value]) -> PathBuf {
    let path = dir.path().join(name);
    let contents = claims
        .iter()
        .map(|claim| serde_json::to_string(claim).expect("claim serializes"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&path, contents).expect("claims file writes");
    path
}

fn lock_file(dir: &TempDir, document: &Path, hash: String) -> PathBuf {
    let path = dir.path().join("filing.lock.json");
    let lock = json!({
        "version": "lock.v0",
        "members": [{
            "path": document.file_name().expect("document filename").to_string_lossy(),
            "bytes_hash": hash
        }]
    });
    fs::write(&path, serde_json::to_vec(&lock).expect("lock serializes"))
        .expect("lock file writes");
    path
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("ground JSON report")
}

fn assert_unit_is_not_established(dir: &TempDir, document: &Path, lock: &Path, claim: &Value) {
    let claims = claims_file(dir, "unstated-unit.jsonl", std::slice::from_ref(claim));
    let output = run_ground(&claims, document, lock, false);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("E_UNIT_NOT_ESTABLISHED"), "{stdout}");
}

#[test]
fn lock_is_required_for_grounding() {
    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .args([
            "ground",
            "claims.jsonl",
            "--bind",
            "filing=document.xml",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("ground command should execute");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--lock <LOCKFILE>"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn real_fixture_passes_locked_quote_whitespace_and_numeric_scale_checks() {
    let dir = TempDir::new().expect("scratch directory");
    let source = fixture_path();
    let bytes = fs::read(&source).expect("real fixture reads");
    let document = dir.path().join("exh_102.xml");
    fs::write(&document, &bytes).expect("document writes");

    let (quote_start, quote_end) = span(&bytes, b"95 Morton Street");
    let (number_start, number_end) = span(&bytes, b"95000000.00000000");
    let claims = claims_file(
        &dir,
        "claims.jsonl",
        &[
            json!({
                "claim_id": "quote-whitespace",
                "source": "filing",
                "quote": "95   Morton Street",
                "byte_span": {"start": quote_start, "end": quote_end}
            }),
            json!({
                "claim_id": "loan-millions",
                "source": "filing",
                "quote": "95000000.00000000",
                "byte_span": {"start": number_start, "end": number_end},
                "value": 95,
                "scale": "millions"
            }),
        ],
    );
    let lock = lock_file(&dir, &document, sha256(&bytes));
    // The XML states no currency, so claiming one does not pass (bd-w2p).
    assert_unit_is_not_established(
        &dir,
        &document,
        &lock,
        &json!({
            "claim_id": "unstated-unit",
            "source": "filing",
            "quote": "95000000.00000000",
            "byte_span": {"start": number_start, "end": number_end},
            "value": 95,
            "scale": "millions",
            "unit": "USD"
        }),
    );

    let first = run_ground(&claims, &document, &lock, false);
    assert_eq!(first.status.code(), Some(0));
    let first_report = report(&first);
    assert_eq!(first_report["version"], "verify.report.v1");
    assert_eq!(first_report["constraint_set_id"], "grounding.v1");
    assert_eq!(first_report["outcome"], "PASS");
    assert_eq!(
        first_report["bindings"]["filing"]["input_verification"]["status"],
        "VERIFIED"
    );
    assert_eq!(first_report["summary"]["total_rules"], 3);
    assert_eq!(
        first.stdout,
        run_ground(&claims, &document, &lock, false).stdout
    );
}

#[test]
fn planted_quote_span_scale_and_redaction_negatives_are_real_failures() {
    let dir = TempDir::new().expect("scratch directory");
    let source = fixture_path();
    let bytes = fs::read(&source).expect("real fixture reads");
    let document = dir.path().join("exh_102.xml");
    fs::write(&document, &bytes).expect("document writes");
    let (quote_start, quote_end) = span(&bytes, b"95 Morton Street");
    let (number_start, number_end) = span(&bytes, b"95000000.00000000");
    let claims = claims_file(
        &dir,
        "negative-claims.jsonl",
        &[
            json!({
                "claim_id": "one-byte-off",
                "source": "filing",
                "quote": "95 Morton Street",
                "byte_span": {"start": quote_start + 1, "end": quote_end + 1}
            }),
            json!({
                "claim_id": "fabricated-quote",
                "source": "filing",
                "quote": "77 Fabricated Street",
                "byte_span": {"start": quote_start, "end": quote_end}
            }),
            json!({
                "claim_id": "wrong-thousands-scale",
                "source": "filing",
                "quote": "95000000.00000000",
                "byte_span": {"start": number_start, "end": number_end},
                "value": 95,
                "scale": "thousands",
                "unit": "USD"
            }),
        ],
    );
    let lock = lock_file(&dir, &document, sha256(&bytes));

    let redacted = run_ground(&claims, &document, &lock, false);
    assert_eq!(redacted.status.code(), Some(1));
    let output = String::from_utf8_lossy(&redacted.stdout);
    assert!(output.contains("E_SPAN_MISMATCH"), "{output}");
    assert!(output.contains("E_QUOTE_NOT_FOUND"), "{output}");
    assert!(output.contains("E_SCALE_MISMATCH"), "{output}");
    assert!(output.contains("redacted:ref:"), "{output}");
    assert!(
        !output.contains("95 Morton Street"),
        "raw quote leaked: {output}"
    );
    assert!(
        !output.contains("95000000.00000000"),
        "raw number leaked: {output}"
    );

    let explicit = run_ground(&claims, &document, &lock, true);
    assert_eq!(explicit.status.code(), Some(1));
    let explicit_output = String::from_utf8_lossy(&explicit.stdout);
    assert!(
        explicit_output.contains("95 Morton Street"),
        "{explicit_output}"
    );
    assert!(
        explicit_output.contains("95000000.00000000"),
        "{explicit_output}"
    );
}

#[test]
fn lock_drift_is_a_reported_refusal() {
    let dir = TempDir::new().expect("scratch directory");
    let source = fixture_path();
    let original = fs::read(&source).expect("real fixture reads");
    let document = dir.path().join("exh_102.xml");
    let mut changed = original.clone();
    changed.push(b'\n');
    fs::write(&document, &changed).expect("changed document writes");
    let (start, end) = span(&original, b"CREFI");
    let claims = claims_file(
        &dir,
        "claims.jsonl",
        &[json!({
            "claim_id": "locked-quote",
            "source": "filing",
            "quote": "CREFI",
            "byte_span": {"start": start, "end": end}
        })],
    );
    let lock = lock_file(&dir, &document, sha256(&original));

    let output = run_ground(&claims, &document, &lock, false);
    assert_eq!(output.status.code(), Some(2));
    let value = report(&output);
    assert_eq!(value["outcome"], "REFUSAL");
    assert_eq!(value["refusal"]["code"], "E_INPUT_DRIFT");
}

#[test]
fn real_sec_html_fixture_resolves_entities_tags_and_numeric_scale() {
    let dir = TempDir::new().expect("scratch directory");
    let bytes = fs::read(html_fixture_path()).expect("real HTML fixture reads");
    let document = dir.path().join("exh99_1-crefimlpa.htm");
    fs::write(&document, &bytes).expect("document writes");
    let (amount_start, amount_end) = span(&bytes, b"50,000,000.00");
    let claims = claims_file(
        &dir,
        "html-claims.jsonl",
        &[
            json!({
                "claim_id": "entity-ampersand",
                "source": "filing",
                "quote": "Orrick, Herrington & Sutcliffe LLP"
            }),
            json!({
                "claim_id": "entity-nbsp",
                "source": "filing",
                "quote": "Exhibit B Mortgage Loan Representations and Warranties"
            }),
            json!({
                "claim_id": "loan-millions",
                "source": "filing",
                "quote": "50,000,000.00",
                "byte_span": {"start": amount_start, "end": amount_end},
                "value": 50,
                "scale": "millions"
            }),
        ],
    );
    let lock = lock_file(&dir, &document, sha256(&bytes));
    // The table cell is a bare amount, so claiming a currency does not pass (bd-w2p).
    assert_unit_is_not_established(
        &dir,
        &document,
        &lock,
        &json!({
            "claim_id": "unstated-unit",
            "source": "filing",
            "quote": "50,000,000.00",
            "byte_span": {"start": amount_start, "end": amount_end},
            "value": 50,
            "unit": "USD",
            "scale": "millions"
        }),
    );

    let output = run_ground(&claims, &document, &lock, false);
    assert_eq!(output.status.code(), Some(0));
    let value = report(&output);
    assert_eq!(value["outcome"], "PASS");
    assert_eq!(value["summary"]["total_rules"], 4);
}

#[test]
fn omitted_span_ambiguity_is_a_typed_refusal_with_match_count() {
    let dir = TempDir::new().expect("scratch directory");
    let bytes = fs::read(html_fixture_path()).expect("real HTML fixture reads");
    let document = dir.path().join("exh99_1-crefimlpa.htm");
    fs::write(&document, &bytes).expect("document writes");
    let claims = claims_file(
        &dir,
        "ambiguous.jsonl",
        &[json!({
            "claim_id": "duplicate-header",
            "source": "filing",
            "quote": "CREFI Mortgage Loan Purchase Agreement"
        })],
    );
    let lock = lock_file(&dir, &document, sha256(&bytes));

    let output = run_ground(&claims, &document, &lock, false);
    assert_eq!(output.status.code(), Some(2));
    let value = report(&output);
    assert_eq!(value["outcome"], "REFUSAL");
    assert_eq!(value["refusal"]["code"], "E_BAD_AUTHORING");
    assert_eq!(value["refusal"]["detail"]["code"], "E_QUOTE_AMBIGUOUS");
    assert_eq!(value["refusal"]["detail"]["match_count"], 2);
}

#[test]
fn planted_html_negatives_cover_fabrication_span_scale_and_hidden_text() {
    let dir = TempDir::new().expect("scratch directory");
    let bytes = fs::read(html_fixture_path()).expect("real HTML fixture reads");
    let document = dir.path().join("exh99_1-crefimlpa.htm");
    fs::write(&document, &bytes).expect("document writes");
    let (crefi_start, crefi_end) = span(&bytes, b"CREFI");
    let (amount_start, amount_end) = span(&bytes, b"50,000,000.00");
    let claims = claims_file(
        &dir,
        "negative-html.jsonl",
        &[
            json!({
                "claim_id": "one-byte-off",
                "source": "filing",
                "quote": "CREFI",
                "byte_span": {"start": crefi_start + 1, "end": crefi_end + 1}
            }),
            json!({
                "claim_id": "fabricated-quote",
                "source": "filing",
                "quote": "Fabricated Street"
            }),
            json!({
                "claim_id": "wrong-thousands-scale",
                "source": "filing",
                "quote": "50,000,000.00",
                "byte_span": {"start": amount_start, "end": amount_end},
                "value": 50,
                "unit": "USD",
                "scale": "thousands"
            }),
            json!({
                "claim_id": "comment-text",
                "source": "filing",
                "quote": "Field: Rule-Page"
            }),
            json!({
                "claim_id": "script-text",
                "source": "filing",
                "quote": "M01OIG1RMA"
            }),
        ],
    );
    let lock = lock_file(&dir, &document, sha256(&bytes));

    let redacted = run_ground(&claims, &document, &lock, false);
    assert_eq!(redacted.status.code(), Some(1));
    let output = String::from_utf8_lossy(&redacted.stdout);
    assert!(output.contains("E_SPAN_MISMATCH"), "{output}");
    assert!(output.contains("E_QUOTE_NOT_FOUND"), "{output}");
    assert!(output.contains("E_SCALE_MISMATCH"), "{output}");
    assert!(!output.contains("CREFI"), "raw HTML text leaked: {output}");
    assert!(
        !output.contains("50,000,000.00"),
        "raw numeric text leaked: {output}"
    );
    assert!(
        !output.contains("M01OIG1RMA"),
        "script text leaked: {output}"
    );

    let explicit = run_ground(&claims, &document, &lock, true);
    assert_eq!(explicit.status.code(), Some(1));
    let explicit_output = String::from_utf8_lossy(&explicit.stdout);
    assert!(explicit_output.contains("CREFI"), "{explicit_output}");
    assert!(
        explicit_output.contains("50,000,000.00"),
        "{explicit_output}"
    );
}
