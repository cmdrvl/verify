//! Lock rule R1 for `verify run --lock` and `verify ground --lock` (bd-6nk).
//!
//! Per input, from names and recorded digests only: every leading `./` is dropped; the members
//! with that path are the candidates, else the members with that file name when they all share
//! one path; `\` is an ordinary character; and EVERY candidate must agree with the input.
//! Only the matching changed: refusal codes and report fields are as released.

use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const BODY: &str = "loan_id,balance\n1,2\n";
const OTHER_BODY: &str = "loan_id,balance\n1,3\n";
const ZEROS: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const BLAKE3_ZEROS: &str =
    "blake3:0000000000000000000000000000000000000000000000000000000000000000";
const MD5: &str = "md5:00000000000000000000000000000000";
const RULES: &str = "constraint_set_id: r1.lock\nbindings:\n  input:\n    key_fields: [loan_id]\nrules:\n  - id: REQUIRED_INPUT\n    severity: error\n    binding: input\n    op: not_null\n    columns: [loan_id]\n";
const CLAIMS: &str = "{\"claim_id\": \"c1\", \"source\": \"input\", \"quote\": \"loan_id\"}\n";

#[derive(Debug, Clone, Copy)]
enum Surface {
    Run,
    Ground,
}

const SURFACES: [Surface; 2] = [Surface::Run, Surface::Ground];

fn digest(text: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
}

fn write(work: &Path, name: &str, contents: &str) {
    let path = work.join(name);
    fs::create_dir_all(path.parent().expect("parent")).expect("test directory");
    fs::write(path, contents).expect("test file");
}

fn command(work: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verify"));
    command
        .current_dir(work)
        .env("HOME", work.join("home"))
        .env_remove("VERIFY_REDACTION_KEY");
    command
}

/// Check the input typed as `typed` (a file holding `BODY`) against one lock per entry of
/// `locks`, named `0.lock`, `1.lock` and so on, each listing `(path, digest)` members.
fn check(surface: Surface, typed: &str, locks: &[&[(&str, &str)]]) -> Value {
    let work = TempDir::new().expect("temporary directory");
    let work = work.path();
    write(work, "loans.csv", BODY);
    write(work, "rules.yaml", RULES);
    write(work, "claims.jsonl", CLAIMS);

    let mut arguments: Vec<String> = match surface {
        Surface::Run => {
            let compiled = command(work)
                .args(["compile", "rules.yaml", "--out", "c.json"])
                .output()
                .expect("compile runs");
            assert!(compiled.status.success(), "{compiled:?}");
            vec!["run".to_owned(), "c.json".to_owned()]
        }
        Surface::Ground => vec!["ground".to_owned(), "claims.jsonl".to_owned()],
    };
    arguments.extend(["--bind".to_owned(), format!("input={typed}")]);
    for (index, members) in locks.iter().enumerate() {
        let name = format!("{index}.lock");
        let members: Vec<Value> = members
            .iter()
            .map(|(path, hash)| json!({"path": path, "bytes_hash": hash}))
            .collect();
        write(
            work,
            &name,
            &json!({"version": "lock.v0", "members": members}).to_string(),
        );
        arguments.extend(["--lock".to_owned(), name]);
    }
    arguments.extend(["--json".to_owned(), "--no-witness".to_owned()]);

    let output = command(work).args(arguments).output().expect("verify runs");
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| panic!("{error}: {output:?}"))
}

fn assert_passes(surface: Surface, report: &Value, locks: &[&str]) {
    assert_eq!(report["outcome"], "PASS", "{surface:?}: {report}");
    assert_eq!(
        report["bindings"]["input"]["input_verification"],
        json!({"status": "VERIFIED", "locks": locks}),
        "{surface:?}"
    );
}

fn assert_code(surface: Surface, report: &Value, code: &str) {
    assert_eq!(report["outcome"], "REFUSAL", "{surface:?}: {report}");
    assert_eq!(report["refusal"]["code"], code, "{surface:?}: {report}");
}

/// `E_INPUT_DRIFT` naming `expected` and the digest of `BODY`, in each surface's released fields.
fn assert_drift(surface: Surface, report: &Value, expected: &str) {
    assert_code(surface, report, "E_INPUT_DRIFT");
    let detail = match surface {
        Surface::Run => json!({
            "binding": "input",
            "path": "loans.csv",
            "expected_hash": expected,
            "observed_hash": digest(BODY),
        }),
        Surface::Ground => json!({
            "path": "loans.csv",
            "expected_hash": expected,
            "observed_hashes": [digest(BODY)],
        }),
    };
    assert_eq!(report["refusal"]["detail"], detail, "{surface:?}");
}

#[test]
fn either_of_two_locks_does_not_verify() {
    let good = digest(BODY);
    let other = digest(OTHER_BODY);
    let matching: &[(&str, &str)] = &[("loans.csv", &good)];
    let differing: &[(&str, &str)] = &[("loans.csv", &other)];
    for surface in SURFACES {
        for locks in [[matching, differing], [differing, matching]] {
            assert_drift(surface, &check(surface, "loans.csv", &locks), &other);
        }
    }
}

#[test]
fn every_candidate_must_agree() {
    let good = digest(BODY);
    let cases: [&[&[(&str, &str)]]; 4] = [
        // One path twice in one lock, the bad entry first.
        &[&[("d/loans.csv", ZEROS), ("d/loans.csv", &good)]],
        // The same bytes recorded under two algorithms, one of them wrong.
        &[&[("loans.csv", &good), ("loans.csv", BLAKE3_ZEROS)]],
        // Leading `./` does not make a second path. If it did, only the good plain entry would
        // be a candidate and the input would pass.
        &[&[("./loans.csv", ZEROS), ("loans.csv", &good)]],
        // By file name, across two locks.
        &[&[("d/loans.csv", &good)], &[("d/loans.csv", ZEROS)]],
    ];
    let expected = [ZEROS, BLAKE3_ZEROS, ZEROS, ZEROS];
    for surface in SURFACES {
        for (locks, expected) in cases.iter().zip(expected) {
            let report = check(surface, "loans.csv", locks);
            assert_code(surface, &report, "E_INPUT_DRIFT");
            assert_eq!(
                report["refusal"]["detail"]["expected_hash"], expected,
                "{surface:?}: {report}"
            );
        }
    }
}

#[test]
fn a_passing_report_lists_every_vouching_lock_once() {
    let good = digest(BODY);
    let member: &[(&str, &str)] = &[("loans.csv", &good)];
    let twice: &[(&str, &str)] = &[("loans.csv", &good), ("loans.csv", &good)];
    for surface in SURFACES {
        let report = check(surface, "loans.csv", &[member, member]);
        assert_passes(surface, &report, &["0.lock", "1.lock"]);
        let report = check(surface, "loans.csv", &[twice]);
        assert_passes(surface, &report, &["0.lock"]);
    }
}

#[test]
fn a_backslash_is_part_of_a_member_name() {
    let good = digest(BODY);
    for surface in SURFACES {
        // `e\loans.csv` is its own file name, so only `d/loans.csv` is a candidate.
        let members: &[(&str, &str)] = &[("d/loans.csv", &good), ("e\\loans.csv", &good)];
        assert_passes(
            surface,
            &check(surface, "loans.csv", &[members]),
            &["0.lock"],
        );
    }
}

#[test]
fn member_paths_are_not_rewritten() {
    let good = digest(BODY);
    for surface in SURFACES {
        for path in ["d\\loans.csv", " loans.csv ", "e/loans.csv/", "loans.csv\\"] {
            let members: &[(&str, &str)] = &[(path, &good)];
            let report = check(surface, "loans.csv", &[members]);
            assert_code(surface, &report, "E_INPUT_NOT_LOCKED");
        }
    }
}

#[test]
fn two_different_paths_with_one_file_name_are_not_locked() {
    let good = digest(BODY);
    for surface in SURFACES {
        let members: &[(&str, &str)] = &[("d/loans.csv", &good), ("e/loans.csv", &good)];
        let report = check(surface, "loans.csv", &[members]);
        assert_code(surface, &report, "E_INPUT_NOT_LOCKED");
    }
}

#[test]
fn refusals_spell_the_typed_name_as_released() {
    let members: &[(&str, &str)] = &[("other.csv", ZEROS)];
    // `run` has always reported the name with one leading `./` removed, `ground` as typed.
    for (surface, spelled) in [
        (Surface::Run, "./loans.csv"),
        (Surface::Ground, "././loans.csv"),
    ] {
        let report = check(surface, "././loans.csv", &[members]);
        assert_code(surface, &report, "E_INPUT_NOT_LOCKED");
        assert_eq!(
            report["refusal"]["detail"]["path"], spelled,
            "{surface:?}: {report}"
        );
    }
}

#[test]
fn run_keeps_its_released_codes_for_unusable_digests() {
    let good = digest(BODY);
    let run = |locks: &[&[(&str, &str)]]| check(Surface::Run, "loans.csv", locks);

    // A known algorithm with a bad digest is drift, shown as the lock wrote it.
    assert_drift(
        Surface::Run,
        &run(&[&[("loans.csv", "sha256:00")]]),
        "sha256:00",
    );
    assert_drift(
        Surface::Run,
        &run(&[&[("loans.csv", &good)], &[("loans.csv", "sha256:00")]]),
        "sha256:00",
    );
    // An unknown algorithm on any candidate refuses the lock, in either order.
    for locks in [
        [("loans.csv", good.as_str()), ("loans.csv", MD5)],
        [("loans.csv", MD5), ("loans.csv", good.as_str())],
        [("loans.csv", "sha256:00"), ("loans.csv", MD5)],
    ] {
        assert_code(Surface::Run, &run(&[&locks]), "E_IO");
    }
    // The message names the member with one leading `./` removed.
    let report = run(&[&[("././loans.csv", MD5)]]);
    assert_code(Surface::Run, &report, "E_IO");
    assert_eq!(
        report["refusal"]["message"],
        format!(
            "invalid lock file 0.lock: unsupported hash algorithm in '{MD5}' for member ./loans.csv; expected sha256: or blake3:"
        )
    );
}

#[test]
fn one_lock_over_the_parent_folder_holds_two_periods() {
    let december = digest(OTHER_BODY);
    let november = digest(BODY);
    let lock = json!({"version": "lock.v0", "members": [
        {"path": "dec/loans.csv", "bytes_hash": december},
        {"path": "nov/loans.csv", "bytes_hash": november},
    ]})
    .to_string();
    let rules = "constraint_set_id: r1.periods\nbindings:\n  current:\n    key_fields: [loan_id]\n  prior:\n    key_fields: [loan_id]\nrules:\n  - id: REQUIRED_CURRENT\n    severity: error\n    binding: current\n    op: not_null\n    columns: [loan_id]\n";
    let claims = "{\"claim_id\": \"c1\", \"source\": \"current\", \"quote\": \"loan_id\"}\n{\"claim_id\": \"c2\", \"source\": \"prior\", \"quote\": \"loan_id\"}\n";
    for (dec_body, nov_body, outcome) in [(OTHER_BODY, BODY, "PASS"), (BODY, OTHER_BODY, "REFUSAL")]
    {
        let work = TempDir::new().expect("temporary directory");
        let work = work.path();
        write(work, "dec/loans.csv", dec_body);
        write(work, "nov/loans.csv", nov_body);
        write(work, "months.lock", &lock);
        write(work, "rules.yaml", rules);
        write(work, "claims.jsonl", claims);
        let compiled = command(work)
            .args(["compile", "rules.yaml", "--out", "c.json"])
            .output()
            .expect("compile runs");
        assert!(compiled.status.success(), "{compiled:?}");
        for subcommand in [["run", "c.json"], ["ground", "claims.jsonl"]] {
            let output = command(work)
                .args(subcommand)
                .args([
                    "--bind",
                    "current=dec/loans.csv",
                    "--bind",
                    "prior=nov/loans.csv",
                    "--lock",
                    "months.lock",
                    "--json",
                    "--no-witness",
                ])
                .output()
                .expect("verify runs");
            let report: Value = serde_json::from_slice(&output.stdout).expect("report is JSON");
            assert_eq!(report["outcome"], outcome, "{subcommand:?}: {report}");
            if outcome == "REFUSAL" {
                assert_eq!(report["refusal"]["code"], "E_INPUT_DRIFT", "{report}");
            }
        }
    }
}
