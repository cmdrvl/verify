use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use verify_core::{
    constraint::ConstraintSet, refusal::RefusalCode, report::InputVerificationStatus,
};
use verify_duckdb::{BatchBindingInput, BatchBindingLimits, prepare_batch_context, verify_locks};

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("fixtures")
        .join(relative)
}

fn load_constraints() -> Result<ConstraintSet, Box<dyn Error>> {
    let path = fixture_path("constraints/arity1/not_null_loans.verify.json");
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn fixture_context() -> Result<verify_duckdb::BatchContext, Box<dyn Error>> {
    let constraints = load_constraints()?;
    Ok(prepare_batch_context(
        &constraints,
        vec![BatchBindingInput::new(
            "input",
            fixture_path("inputs/arity1/loans.csv"),
        )],
        BatchBindingLimits::default(),
    )?)
}

fn unique_lock_path(stem: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("verify-{stem}-{nanos}.lock.json"))
}

fn write_lock_file(stem: &str, members: &[(&str, &str)]) -> Result<PathBuf, Box<dyn Error>> {
    let path = unique_lock_path(stem);
    let members = members
        .iter()
        .map(|(member_path, hash)| {
            json!({
                "path": member_path,
                "content_hash": hash,
            })
        })
        .collect::<Vec<_>>();

    let contents = serde_json::to_string_pretty(&json!({
        "version": "lock.input.v1",
        "members": members,
    }))?;
    fs::write(&path, contents)?;
    Ok(path)
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
}

fn bound_input_path(context: &verify_duckdb::BatchContext) -> String {
    context
        .bindings()
        .get("input")
        .expect("input binding exists")
        .source_path()
        .to_string_lossy()
        .into_owned()
}

#[test]
fn fixture_lock_verification_marks_binding_verified() -> Result<(), Box<dyn Error>> {
    let context = fixture_context()?;
    let binding = context
        .bindings()
        .get("input")
        .expect("input binding exists");
    let bound_input = bound_input_path(&context);
    let lock_path = write_lock_file(
        "verified",
        &[(
            bound_input.as_str(),
            binding.metadata().content_hash.as_str(),
        )],
    )?;

    let result = verify_locks(std::slice::from_ref(&lock_path), context.bindings())?;

    let verification = result
        .get("input")
        .expect("input verification should exist");
    assert_eq!(verification.status, InputVerificationStatus::Verified);
    assert_eq!(
        verification.locks,
        vec![
            lock_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        ]
    );

    cleanup(&lock_path);
    Ok(())
}

#[test]
fn fixture_lock_verification_reports_input_not_locked() -> Result<(), Box<dyn Error>> {
    let context = fixture_context()?;
    let lock_path = write_lock_file(
        "missing",
        &[("fixtures/inputs/arity1/other.csv", "sha256:deadbeef")],
    )?;

    let error = verify_locks(std::slice::from_ref(&lock_path), context.bindings())
        .expect_err("should refuse");
    let refusal = error.to_refusal();

    assert_eq!(refusal.code, RefusalCode::InputNotLocked);
    assert_eq!(refusal.detail["binding"], json!("input"));
    assert_eq!(refusal.detail["path"], json!(bound_input_path(&context)));
    assert_eq!(
        refusal.detail["locks_checked"],
        json!([lock_path.file_name().unwrap().to_string_lossy().to_string()])
    );

    cleanup(&lock_path);
    Ok(())
}

#[test]
fn fixture_lock_verification_reports_input_drift() -> Result<(), Box<dyn Error>> {
    let context = fixture_context()?;
    let bound_input = bound_input_path(&context);
    let lock_path = write_lock_file(
        "drift",
        &[(
            bound_input.as_str(),
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )],
    )?;

    let error = verify_locks(std::slice::from_ref(&lock_path), context.bindings())
        .expect_err("should refuse");
    let refusal = error.to_refusal();

    assert_eq!(refusal.code, RefusalCode::InputDrift);
    assert_eq!(refusal.detail["binding"], json!("input"));
    assert_eq!(refusal.detail["path"], json!(bound_input));
    assert_eq!(
        refusal.detail["expected_hash"],
        json!("sha256:1111111111111111111111111111111111111111111111111111111111111111")
    );
    assert_eq!(
        refusal.detail["observed_hash"],
        json!(
            context
                .bindings()
                .get("input")
                .unwrap()
                .metadata()
                .content_hash
                .clone()
        )
    );

    cleanup(&lock_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// Canonical lock.v0 (bd-pnr). The two lock files under fixtures/locks/lock_v0 are
// REAL output of the installed `vacuum data | hashbytes --algorithm <alg> | lock`
// pipeline over loans.csv in that directory, not hand-written JSON.
// ---------------------------------------------------------------------------

fn lock_v0_fixture(file: &str) -> PathBuf {
    fixture_path("locks/lock_v0").join(file)
}

fn context_for(path: &Path) -> Result<verify_duckdb::BatchContext, Box<dyn Error>> {
    Ok(prepare_batch_context(
        &load_constraints()?,
        vec![BatchBindingInput::new("input", path)],
        BatchBindingLimits::default(),
    )?)
}

/// A copy of the locked CSV with one byte changed, in its own scratch dir, still named loans.csv.
fn tampered_loans_csv(stem: &str) -> Result<PathBuf, Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!(
        "verify-{stem}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&dir)?;
    let path = dir.join("loans.csv");
    let original = fs::read_to_string(lock_v0_fixture("loans.csv"))?;
    fs::write(&path, original.replace("LN-2,20", "LN-2,21"))?;
    Ok(path)
}

#[test]
fn real_lock_v0_verifies_with_sha256_and_blake3_bytes_hash() -> Result<(), Box<dyn Error>> {
    for lock_file in ["sha256.lock.json", "blake3.lock.json"] {
        let lock = fs::read_to_string(lock_v0_fixture(lock_file))?;
        assert!(lock.contains("\"version\":\"lock.v0\""), "{lock_file}");
        let context = context_for(&lock_v0_fixture("loans.csv"))?;
        let result = verify_locks(&[lock_v0_fixture(lock_file)], context.bindings())
            .map_err(|error| format!("{lock_file} should verify: {error}"))?;
        let verification = result.get("input").expect("input verified");
        assert_eq!(verification.status, InputVerificationStatus::Verified);
        assert_eq!(verification.locks, vec![lock_file.to_owned()]);
    }
    Ok(())
}

#[test]
fn real_lock_v0_refuses_tampered_bytes_with_the_locks_own_algorithm() -> Result<(), Box<dyn Error>>
{
    let tampered = tampered_loans_csv("tampered")?;
    for (lock_file, prefix) in [
        ("sha256.lock.json", "sha256:"),
        ("blake3.lock.json", "blake3:"),
    ] {
        let context = context_for(&tampered)?;
        let refusal = verify_locks(&[lock_v0_fixture(lock_file)], context.bindings())
            .expect_err("tampered bytes must be refused")
            .to_refusal();
        assert_eq!(refusal.code, RefusalCode::InputDrift, "{lock_file}");
        let expected = refusal.detail["expected_hash"].as_str().expect("expected");
        let observed = refusal.detail["observed_hash"].as_str().expect("observed");
        assert!(expected.starts_with(prefix) && observed.starts_with(prefix));
        assert_ne!(expected, observed);
    }
    Ok(())
}

#[test]
fn lock_v0_with_an_unknown_hash_algorithm_is_refused_deterministically()
-> Result<(), Box<dyn Error>> {
    let real = fs::read_to_string(lock_v0_fixture("sha256.lock.json"))?;
    let lock_path = unique_lock_path("md5");
    let forged = real.replace("sha256:973ef89c", "md5:973ef89c");
    assert_ne!(real, forged, "the fixture's hash prefix changed upstream");
    fs::write(&lock_path, forged)?;

    let context = context_for(&lock_v0_fixture("loans.csv"))?;
    let refusal = verify_locks(std::slice::from_ref(&lock_path), context.bindings())
        .expect_err("unknown algorithm must be refused")
        .to_refusal();
    assert_eq!(refusal.code, RefusalCode::Io);
    assert!(
        refusal.detail["detail"]
            .as_str()
            .is_some_and(|text| text.contains("unsupported hash algorithm")),
        "{refusal:?}"
    );
    cleanup(&lock_path);
    Ok(())
}

#[test]
fn lock_v0_member_without_bytes_hash_and_unknown_versions_are_refused() -> Result<(), Box<dyn Error>>
{
    let context = context_for(&lock_v0_fixture("loans.csv"))?;
    for (stem, body) in [
        (
            "nohash",
            r#"{"version":"lock.v0","members":[{"path":"loans.csv"}]}"#,
        ),
        ("v9", r#"{"version":"lock.v9","members":[]}"#),
    ] {
        let lock_path = unique_lock_path(stem);
        fs::write(&lock_path, body)?;
        let refusal = verify_locks(std::slice::from_ref(&lock_path), context.bindings())
            .expect_err("must refuse")
            .to_refusal();
        assert_eq!(refusal.code, RefusalCode::Io, "{stem}");
        cleanup(&lock_path);
    }
    Ok(())
}

#[test]
fn two_members_with_the_same_basename_are_not_locked_rather_than_guessed()
-> Result<(), Box<dyn Error>> {
    let real: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(lock_v0_fixture("sha256.lock.json"))?)?;
    let member = real["members"][0].clone();
    let mut a = member.clone();
    a["path"] = json!("a/loans.csv");
    let mut b = member;
    b["path"] = json!("b/loans.csv");
    let lock_path = unique_lock_path("ambiguous");
    fs::write(
        &lock_path,
        json!({"version": "lock.v0", "members": [a, b]}).to_string(),
    )?;

    let context = context_for(&lock_v0_fixture("loans.csv"))?;
    let refusal = verify_locks(std::slice::from_ref(&lock_path), context.bindings())
        .expect_err("ambiguous basename must not verify")
        .to_refusal();
    assert_eq!(refusal.code, RefusalCode::InputNotLocked);
    cleanup(&lock_path);
    Ok(())
}
