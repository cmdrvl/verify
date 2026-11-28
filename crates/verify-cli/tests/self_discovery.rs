//! bd-esf: an agent can learn how to author `ground` and `compile` inputs from the binary
//! alone, and what it is told matches what the binary accepts.

use std::{
    fs,
    process::{Command, Output},
};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use verify_engine::grounding::parse_claims_jsonl;

fn verify(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_verify"))
        .args(args)
        .env_remove("VERIFY_REDACTION_KEY")
        .output()
        .expect("verify runs")
}

fn stdout(args: &[&str]) -> String {
    let output = verify(args);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "{args:?} wrote to stderr");
    String::from_utf8(output.stdout).expect("UTF-8 output")
}

fn json(args: &[&str]) -> Value {
    serde_json::from_str(&stdout(args)).unwrap_or_else(|error| {
        let found = format!("{args:?} is not JSON: {error}");
        assert!(found.is_empty(), "{found}");
        Value::Null
    })
}

const CLAIM_FIELDS: [&str; 7] = [
    "claim_id",
    "source",
    "quote",
    "byte_span",
    "value",
    "scale",
    "unit",
];

#[test]
fn ground_discovery_needs_no_other_arguments() {
    for flag in ["--schema", "--describe", "--input-schema"] {
        let schema = json(&["ground", flag]);
        assert_eq!(schema["title"], "verify.claims.v1", "{flag}");
        for field in CLAIM_FIELDS {
            assert!(schema["properties"][field].is_object(), "{flag}: {field}");
        }
    }
}

#[test]
fn compile_discovery_emits_the_compile_schemas() {
    for flag in ["--schema", "--describe"] {
        assert_eq!(
            json(&["compile", flag])["title"],
            "verify.constraint.v1",
            "{flag}"
        );
    }
    assert_eq!(
        json(&["compile", "--input-schema"])["title"],
        "verify.authoring.v1"
    );
}

#[test]
fn the_root_flags_are_unchanged() {
    assert_eq!(json(&["--schema"])["title"], "verify.report.v1");
    assert_eq!(json(&["--describe"])["schema_version"], "operator.v1");
}

#[test]
fn ground_still_requires_its_inputs_to_run() {
    for args in [
        &["ground"][..],
        &["ground", "claims.jsonl"],
        &["ground", "claims.jsonl", "--bind", "a=b"],
    ] {
        let output = verify(args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--lock"), "{args:?}: {stderr}");
    }
    let output = verify(&["compile"]);
    assert_eq!(output.status.code(), Some(2));
}

/// The schema's required fields and scale words are the ones the parser enforces.
#[test]
fn the_claims_schema_agrees_with_the_parser() {
    let schema = json(&["ground", "--schema"]);
    let full: Value = serde_json::json!({
        "claim_id": "c1", "source": "filing", "quote": "x", "value": 1, "scale": "units"
    });
    let parses = |claim: &Value| parse_claims_jsonl(claim.to_string().as_bytes()).is_ok();
    assert!(parses(&full));
    let required: Vec<&str> = schema["required"]
        .as_array()
        .expect("required list")
        .iter()
        .map(|field| field.as_str().expect("field name"))
        .collect();
    assert_eq!(required, ["claim_id", "source", "quote"]);
    for field in CLAIM_FIELDS {
        let mut claim = full.clone();
        claim.as_object_mut().expect("object").remove(field);
        // `value` and `scale` are optional only together.
        let expected = !required.contains(&field) && field != "value" && field != "scale";
        assert_eq!(parses(&claim), expected, "without {field}");
    }
    let scales: Vec<String> = schema["properties"]["scale"]["enum"]
        .as_array()
        .expect("scale enum")
        .iter()
        .filter_map(|scale| scale.as_str().map(str::to_owned))
        .collect();
    assert!(!scales.is_empty());
    for scale in &scales {
        let mut claim = full.clone();
        claim["scale"] = Value::String(scale.clone());
        assert!(parses(&claim), "the schema lists `{scale}`");
    }
    let mut claim = full;
    claim["scale"] = Value::String("gazillions".to_owned());
    assert!(!parses(&claim));
}

/// Every place an agent is told to look names each claim field, the main scale words, the
/// document formats and a one-line example.
#[test]
fn help_guide_and_capabilities_document_claim_authoring() {
    let help = stdout(&["ground", "--help"]);
    let guide = stdout(&["robot-docs", "guide"]);
    let capabilities = json(&["capabilities", "--json"]);
    let ground = capabilities["commands"]
        .as_array()
        .expect("commands")
        .iter()
        .find(|command| command["name"] == "ground")
        .expect("ground command")
        .to_string();
    let arguments = capabilities["arguments"].to_string();
    assert!(arguments.contains("claims_jsonl"), "{arguments}");
    let bind = capabilities["options"]
        .as_array()
        .expect("options")
        .iter()
        .find(|option| option["name"] == "bind")
        .expect("bind option")
        .to_string();
    for needle in ["UTF-8", "Markdown", "HTML"] {
        assert!(bind.contains(needle), "--bind lacks {needle}");
    }
    for (place, text) in [
        ("help", &help),
        ("guide", &guide),
        ("capabilities", &ground),
    ] {
        for needle in CLAIM_FIELDS.iter().copied().chain([
            "units",
            "thousands",
            "millions",
            "percent",
            "bps",
            "UTF-8",
            "Markdown",
            "HTML",
        ]) {
            assert!(text.contains(needle), "{place} lacks `{needle}`");
        }
        assert!(
            text.contains(r#"{"claim_id""#) || text.contains(r#"{\"claim_id\""#),
            "{place} lacks a one-line example"
        );
    }
}

fn line_starting(text: &str, prefix: &str) -> String {
    let found = text.lines().find(|line| line.trim().starts_with(prefix));
    assert!(found.is_some(), "no line starts with {prefix}");
    found.expect("checked above").trim().to_owned()
}

/// The guide's two copy-paste examples work as printed.
#[test]
fn the_guide_examples_run() {
    let guide = stdout(&["robot-docs", "guide"]);
    let directory = TempDir::new().expect("scratch");
    let path = |name: &str| directory.path().join(name);

    let sentence = line_starting(&guide, "Cash totaled");
    let claim = line_starting(&guide, r#"{"claim_id""#);
    fs::write(path("filing.txt"), &sentence).expect("document");
    fs::write(path("claims.jsonl"), format!("{claim}\n")).expect("claims");
    let digest = format!("sha256:{:x}", Sha256::digest(sentence.as_bytes()));
    fs::write(
        path("filing.lock.json"),
        format!(
            r#"{{"version":"lock.v0","members":[{{"path":"filing.txt","bytes_hash":"{digest}"}}]}}"#
        ),
    )
    .expect("lock");
    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .current_dir(directory.path())
        .args([
            "ground",
            "claims.jsonl",
            "--bind",
            "filing=filing.txt",
            "--lock",
            "filing.lock.json",
            "--json",
            "--no-witness",
        ])
        .output()
        .expect("ground runs");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );

    let (_, after) = guide.split_once("```yaml\n").expect("a yaml example");
    let (rules, _) = after.split_once("```").expect("the yaml example ends");
    fs::write(path("rules.yaml"), rules).expect("rules");
    let output = Command::new(env!("CARGO_BIN_EXE_verify"))
        .current_dir(directory.path())
        .args(["compile", "rules.yaml", "--check"])
        .output()
        .expect("compile runs");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The authoring schema lists the operators and severities the compiler accepts.
#[test]
fn the_authoring_schema_names_what_the_compiler_accepts() {
    let schema = json(&["compile", "--input-schema"]);
    let rule = &schema["$defs"]["rule"];
    let names = |value: &Value| -> Vec<String> {
        value
            .as_array()
            .expect("enum")
            .iter()
            .filter_map(|name| name.as_str().map(str::to_owned))
            .collect()
    };
    assert_eq!(
        names(&rule["properties"]["op"]["enum"]),
        [
            "unique",
            "not_null",
            "predicate",
            "row_count",
            "aggregate_compare",
            "foreign_key"
        ]
    );
    let directory = TempDir::new().expect("scratch");
    for severity in names(&rule["properties"]["severity"]["enum"]) {
        let rules = format!(
            "constraint_set_id: a.b\nbindings:\n  input: {{}}\nrules:\n  - {{id: R, severity: {severity}, binding: input, op: not_null, columns: [a]}}\n"
        );
        let file = directory.path().join("rules.yaml");
        fs::write(&file, rules).expect("rules");
        let output = verify(&["compile", file.to_str().expect("path"), "--check"]);
        assert_eq!(output.status.code(), Some(0), "severity {severity}");
    }
}
