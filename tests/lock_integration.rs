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
