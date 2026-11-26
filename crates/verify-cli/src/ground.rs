use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use clap::{ArgAction, Args};
use serde_json::{Map, Value, json};
use verify_core::{
    refusal::RefusalCode,
    report::{ExecutionMode, VerifyReport},
};
use verify_engine::grounding::{
    GroundingDocument, GroundingError, LockError, evaluate, parse_claims_jsonl, verify_locks,
};

use crate::{
    redact::RedactionKey,
    run::{RunCommandResult, redaction_key_refusal, report_to_result},
    witness::{self, WitnessInput},
};

#[derive(Debug, Clone, Args)]
pub struct GroundArgs {
    #[arg(value_name = "CLAIMS_JSONL")]
    pub claims: PathBuf,
    #[arg(long = "bind", action = ArgAction::Append, value_name = "NAME=DOCUMENT", required = true)]
    pub binds: Vec<String>,
    #[arg(
        long = "lock",
        action = ArgAction::Append,
        value_name = "LOCKFILE",
        required = true
    )]
    pub locks: Vec<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub no_witness: bool,
    /// Include raw quote and numeric evidence (default output is redacted).
    #[arg(long)]
    pub explicit: bool,
}

pub fn execute(args: GroundArgs) -> RunCommandResult {
    let key = match RedactionKey::from_env() {
        Ok(key) => key,
        Err(error) => {
            let mut result = RunCommandResult::refusal(redaction_key_refusal(error), args.json);
            append_witness(&args, &mut result);
            return result;
        }
    };

    let claims_hash = fs::read(&args.claims)
        .map(|bytes| crate::run::sha256_of_bytes(&bytes))
        .unwrap_or_else(|_| "sha256:unavailable".to_owned());
    let mut report = execute_inner(&args, &claims_hash).unwrap_or_else(|report| *report);
    redact_unless_explicit_ground(&mut report, args.explicit, key.as_ref());
    let mut result = report_to_result(report, args.json, None);
    append_witness(&args, &mut result);
    result
}

fn execute_inner(args: &GroundArgs, claims_hash: &str) -> Result<VerifyReport, Box<VerifyReport>> {
    let claims_bytes = fs::read(&args.claims).map_err(|error| {
        Box::new(refusal(
            claims_hash,
            RefusalCode::Io,
            format!(
                "failed to read grounding claims file {}: {error}",
                args.claims.display()
            ),
            json!({ "path": args.claims.to_string_lossy() }),
        ))
    })?;
    let claims = parse_claims_jsonl(&claims_bytes)
        .map_err(|error| Box::new(grounding_refusal(claims_hash, error)))?;
    let documents = load_documents(&args.binds)
        .map_err(|(code, message, detail)| Box::new(refusal(claims_hash, code, message, detail)))?;

    let lock_results = if args.locks.is_empty() {
        None
    } else {
        let lock_files = args
            .locks
            .iter()
            .map(|path| read_lock(path))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|(message, detail)| {
                Box::new(refusal(claims_hash, RefusalCode::Io, message, detail))
            })?;
        Some(
            verify_locks(&lock_files, &documents)
                .map_err(|error| Box::new(grounding_refusal(claims_hash, error)))?,
        )
    };

    evaluate(&claims, &documents, claims_hash, lock_results.as_ref())
        .map_err(|error| Box::new(grounding_refusal(claims_hash, error)))
}

fn load_documents(
    binds: &[String],
) -> Result<BTreeMap<String, GroundingDocument>, (RefusalCode, String, Value)> {
    let mut documents = BTreeMap::new();
    for bind in binds {
        let (name, path) = bind.split_once('=').ok_or_else(|| {
            (
                RefusalCode::BadAuthoring,
                "invalid --bind: expected NAME=DOCUMENT".to_owned(),
                json!({ "bind": bind }),
            )
        })?;
        if name.is_empty() || path.is_empty() {
            return Err((
                RefusalCode::BadAuthoring,
                "invalid --bind: NAME and DOCUMENT must not be empty".to_owned(),
                json!({ "bind": bind }),
            ));
        }
        if documents.contains_key(name) {
            return Err((
                RefusalCode::DuplicateBinding,
                format!("duplicate document binding: {name}"),
                json!({ "binding": name }),
            ));
        }
        let bytes = fs::read(path).map_err(|error| {
            (
                RefusalCode::Io,
                format!("failed to read document {path}: {error}"),
                json!({ "binding": name, "path": path }),
            )
        })?;
        documents.insert(
            name.to_owned(),
            GroundingDocument::new(path.to_owned(), bytes),
        );
    }
    Ok(documents)
}

fn read_lock(path: &Path) -> Result<(String, Vec<u8>), (String, Value)> {
    let bytes = fs::read(path).map_err(|error| {
        (
            format!("failed to read lock file {}: {error}", path.display()),
            json!({ "path": path.to_string_lossy() }),
        )
    })?;
    let name = path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string_lossy().to_string());
    Ok((name, bytes))
}

fn grounding_refusal(claims_hash: &str, error: GroundingError) -> VerifyReport {
    match error {
        GroundingError::Lock(error) => lock_refusal(claims_hash, error),
        GroundingError::InvalidClaim { line, detail } => refusal(
            claims_hash,
            RefusalCode::BadAuthoring,
            format!("invalid grounding claim at line {line}: {detail}"),
            json!({ "line": line, "detail": detail }),
        ),
        GroundingError::EmptyClaims => refusal(
            claims_hash,
            RefusalCode::BadAuthoring,
            "grounding claims input is empty".to_owned(),
            json!({}),
        ),
        GroundingError::DuplicateClaimId { claim_id } => refusal(
            claims_hash,
            RefusalCode::BadAuthoring,
            "grounding claims contain a duplicate claim_id".to_owned(),
            json!({ "claim_id": claim_id }),
        ),
        GroundingError::MissingSource { claim_id, source } => refusal(
            claims_hash,
            RefusalCode::MissingBinding,
            "grounding claim references an unbound source".to_owned(),
            json!({ "claim_id": claim_id, "source": source }),
        ),
        GroundingError::InvalidSpan { claim_id, detail } => refusal(
            claims_hash,
            RefusalCode::BadAuthoring,
            "grounding claim has an invalid byte span".to_owned(),
            json!({ "claim_id": claim_id, "detail": detail }),
        ),
        GroundingError::AmbiguousQuote {
            claim_id,
            match_count,
        } => refusal(
            claims_hash,
            RefusalCode::BadAuthoring,
            "grounding quote is ambiguous; supply byte_span".to_owned(),
            json!({
                "claim_id": claim_id,
                "code": "E_QUOTE_AMBIGUOUS",
                "match_count": match_count,
            }),
        ),
    }
}

fn lock_refusal(claims_hash: &str, error: LockError) -> VerifyReport {
    match error {
        LockError::InputNotLocked {
            source,
            locks_checked,
        } => refusal(
            claims_hash,
            RefusalCode::InputNotLocked,
            "grounding source is not present in any provided lock".to_owned(),
            json!({ "path": source, "locks_checked": locks_checked }),
        ),
        LockError::InputDrift {
            source,
            expected_hash,
            observed_hashes,
        } => refusal(
            claims_hash,
            RefusalCode::InputDrift,
            "grounding source bytes differ from the provided lock".to_owned(),
            json!({
                "path": source,
                "expected_hash": expected_hash,
                "observed_hashes": observed_hashes,
            }),
        ),
        LockError::Invalid { name, detail } => refusal(
            claims_hash,
            RefusalCode::Io,
            "provided lock is invalid".to_owned(),
            json!({ "path": name, "detail": detail }),
        ),
    }
}

fn refusal(claims_hash: &str, code: RefusalCode, message: String, detail: Value) -> VerifyReport {
    VerifyReport::refusal(
        ExecutionMode::Batch,
        verify_engine::grounding::GROUNDING_CONSTRAINT_SET,
        claims_hash,
        code,
        message,
        detail,
    )
}

fn redact_unless_explicit_ground(
    report: &mut VerifyReport,
    explicit: bool,
    key: Option<&RedactionKey>,
) {
    if !explicit {
        crate::redact::redact_report(report, key, None);
    }
}

fn append_witness(args: &GroundArgs, result: &mut RunCommandResult) {
    if args.no_witness {
        return;
    }
    let mut params = Map::new();
    params.insert("surface".to_owned(), Value::String("ground".to_owned()));
    params.insert("json".to_owned(), Value::Bool(args.json));
    params.insert(
        "binds".to_owned(),
        Value::Array(
            args.binds
                .iter()
                .filter_map(|bind| bind.split_once('=').map(|(name, _)| json!(name)))
                .collect(),
        ),
    );
    let mut inputs = vec![WitnessInput::from_path(&args.claims)];
    inputs.extend(args.binds.iter().map(|bind| {
        WitnessInput::from_path(bind.split_once('=').map_or(bind.as_str(), |(_, path)| path))
    }));
    inputs.extend(args.locks.iter().map(WitnessInput::from_path));

    let mut output = Vec::new();
    if let Some(stdout) = &result.stdout {
        output.extend_from_slice(stdout.as_bytes());
    }
    if let Some(stderr) = &result.stderr {
        output.extend_from_slice(stderr.as_bytes());
    }
    if let Err(error) = witness::append_run_record(
        "ground",
        result.exit.witness_outcome(),
        result.exit.exit_code(),
        inputs,
        params,
        &output,
    ) {
        result.append_warning(format!(
            "verify: warning: failed to append witness record: {error}"
        ));
    }
}
