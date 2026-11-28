//! `--lock` checking for `verify run`.
//!
//! Which members vouch for an input is lock rule R1, from `cmdrvl_core::lock_matching`
//! (bd-6nk): every candidate member must agree with the input. Reading locks, hashing, refusals
//! and report fields stay here. `verify-engine`'s `grounding` module uses the same rule for
//! `verify ground`; unlike this one, it rejects a lock holding any unusable hash.

use std::{borrow::Cow, collections::BTreeMap, fs, path::PathBuf};

use cmdrvl_core::{
    hash::{Algorithm, HEX_DIGITS},
    lock_matching::{Member, SelectError, Verdict, select},
};
use serde::Deserialize;
use serde_json::json;
use verify_core::{
    refusal::{Refusal, RefusalCode},
    report::{InputVerification, InputVerificationStatus},
};
use verify_engine::grounding::typed_name_for_matching;

use crate::BindingRegistry;

/// Canonical lockfile version emitted by the `lock` tool: `members[].bytes_hash`
/// is `<algorithm>:<hex>` of the raw file bytes, with `sha256` and `blake3` supported.
const LOCK_V0: &str = "lock.v0";
/// Deprecated verify-private format (`members[].content_hash`, sha256 only). Kept so
/// existing fixtures and any lock written for it keep working; real locks are `lock.v0`.
const LEGACY_LOCK_VERSION: &str = "lock.input.v1";

#[derive(Debug, Clone, Deserialize)]
struct LockFile {
    version: String,
    members: Vec<LockMember>,
}

#[derive(Debug, Clone, Deserialize)]
struct LockMember {
    path: String,
    #[serde(default)]
    bytes_hash: Option<String>,
    #[serde(default)]
    content_hash: Option<String>,
}

/// `<algorithm>:<lowercase hex>` of the file at `path`, streamed (cmdrvl-core hashes only
/// bytes already in memory).
fn digest_file(algorithm: Algorithm, path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;

    let mut file = fs::File::open(path)?;
    let mut buffer = [0_u8; 64 * 1024];
    match algorithm {
        Algorithm::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            Ok(format!("sha256:{:x}", hasher.finalize()))
        }
        Algorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            Ok(format!("blake3:{}", hasher.finalize().to_hex()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    Io {
        path: PathBuf,
        detail: String,
    },
    BadLock {
        path: PathBuf,
        detail: String,
    },
    InputNotLocked {
        binding: String,
        source_path: String,
        locks_checked: Vec<String>,
    },
    InputDrift {
        binding: String,
        source_path: String,
        expected_hash: String,
        observed_hash: String,
    },
}

impl LockError {
    pub fn refusal_code(&self) -> RefusalCode {
        match self {
            Self::Io { .. } => RefusalCode::Io,
            Self::BadLock { .. } => RefusalCode::Io,
            Self::InputNotLocked { .. } => RefusalCode::InputNotLocked,
            Self::InputDrift { .. } => RefusalCode::InputDrift,
        }
    }

    pub fn to_refusal(&self) -> Refusal {
        Refusal::new(self.refusal_code(), self.to_string(), self.detail())
    }

    pub fn detail(&self) -> serde_json::Value {
        match self {
            Self::Io { path, detail } => json!({
                "path": path.to_string_lossy(),
                "detail": detail,
            }),
            Self::BadLock { path, detail } => json!({
                "path": path.to_string_lossy(),
                "detail": detail,
            }),
            Self::InputNotLocked {
                binding,
                source_path,
                locks_checked,
            } => json!({
                "binding": binding,
                "path": source_path,
                "locks_checked": locks_checked,
            }),
            Self::InputDrift {
                binding,
                source_path,
                expected_hash,
                observed_hash,
            } => json!({
                "binding": binding,
                "path": source_path,
                "expected_hash": expected_hash,
                "observed_hash": observed_hash,
            }),
        }
    }
}

impl std::fmt::Display for LockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, detail } => {
                write!(
                    formatter,
                    "cannot read lock file {}: {detail}",
                    path.to_string_lossy()
                )
            }
            Self::BadLock { path, detail } => {
                write!(
                    formatter,
                    "invalid lock file {}: {detail}",
                    path.to_string_lossy()
                )
            }
            Self::InputNotLocked {
                binding,
                source_path,
                ..
            } => {
                write!(
                    formatter,
                    "binding {binding} ({source_path}) is not present in any provided lock"
                )
            }
            Self::InputDrift {
                binding,
                expected_hash,
                observed_hash,
                ..
            } => {
                write!(
                    formatter,
                    "binding {binding} content hash drifted: expected {expected_hash}, observed {observed_hash}"
                )
            }
        }
    }
}

impl std::error::Error for LockError {}

/// One locked member with the lock file it came from.
#[derive(Debug, Clone)]
struct LockedMember {
    /// Member path exactly as written in the lock. Matching reads this one.
    path: String,
    /// `<algorithm>:<hex>` the lock expects (`bytes_hash`, or legacy `content_hash`).
    expected_hash: String,
    lock_name: String,
    lock_path: PathBuf,
}

/// Loaded lock data across every `--lock` file.
#[derive(Debug, Clone)]
struct LoadedLock {
    members: Vec<LockedMember>,
    /// Names of all lock files that were loaded.
    lock_names: Vec<String>,
}

/// The digest R1 is given for a recorded one. `verify run` reports a known algorithm with a
/// bad digest as drift, not as a bad lock. R1 would refuse the digest outright, so it gets a
/// well-formed one that cannot match: hashers print lowercase. An unknown algorithm is passed
/// through and refused.
fn digest_for_matching(recorded: &str) -> Cow<'_, str> {
    let Some(algorithm) = Algorithm::of(recorded) else {
        return Cow::Borrowed(recorded);
    };
    let digest = &recorded[algorithm.prefix().len() + 1..];
    if digest.len() == HEX_DIGITS && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Cow::Borrowed(recorded)
    } else {
        Cow::Owned(format!("{}:{}", algorithm.prefix(), "F".repeat(HEX_DIGITS)))
    }
}

/// Verify all bindings in a registry against the provided lock files.
///
/// Returns a map of binding name -> InputVerification for each binding that was
/// verified. If no lock files are provided, returns an empty map (lock
/// verification is opt-in via `--lock`).
pub fn verify_locks(
    lock_paths: &[PathBuf],
    registry: &BindingRegistry,
) -> Result<BTreeMap<String, InputVerification>, LockError> {
    if lock_paths.is_empty() {
        return Ok(BTreeMap::new());
    }

    let loaded = load_locks(lock_paths)?;
    let digests: Vec<Cow<'_, str>> = loaded
        .members
        .iter()
        .map(|member| digest_for_matching(&member.expected_hash))
        .collect();
    let mut verifications = BTreeMap::new();

    for (name, binding) in registry.iter() {
        let typed = binding.source_path().to_string_lossy();
        // Refusals have always reported this spelling. It is not what gets matched.
        let source_path = reported_path(&typed);
        let content_hash = &binding.metadata().content_hash;

        // `Member::lock` carries the member's place in `loaded.members`, not a lock number.
        // R1 hands it back untouched, so reports and messages use this module's own record.
        let members =
            loaded
                .members
                .iter()
                .zip(&digests)
                .enumerate()
                .map(|(place, (member, digest))| Member {
                    lock: place,
                    path: &member.path,
                    digest,
                });
        let selection = match select(&typed_name_for_matching(&typed), members) {
            Ok(selection) => selection,
            Err(SelectError::NotListed | SelectError::SeveralPaths(_)) => {
                return Err(LockError::InputNotLocked {
                    binding: name.to_owned(),
                    source_path,
                    locks_checked: loaded.lock_names.clone(),
                });
            }
            Err(SelectError::UnusableDigest(unusable)) => {
                let member = &loaded.members[unusable.lock];
                return Err(LockError::BadLock {
                    path: member.lock_path.clone(),
                    detail: format!(
                        "unsupported hash algorithm in '{}' for member {}; expected sha256: or blake3:",
                        member.expected_hash,
                        reported_path(&member.path)
                    ),
                });
            }
        };

        // The binding already carries the sha256 of its bytes. blake3 is computed, only if a
        // candidate names it, from the file the binding was loaded from; the empty string
        // left otherwise is never compared.
        let blake3 = if selection.algorithms().contains(&Algorithm::Blake3) {
            digest_file(Algorithm::Blake3, binding.source_path()).map_err(|error| {
                LockError::Io {
                    path: binding.source_path().to_path_buf(),
                    detail: error.to_string(),
                }
            })?
        } else {
            String::new()
        };
        let verdict = selection
            .judge(&[
                (Algorithm::Sha256, content_hash.as_str()),
                (Algorithm::Blake3, blake3.as_str()),
            ])
            .expect("a digest is given for both algorithms");

        let vouching_locks = match verdict {
            Verdict::Verified { matched, .. } => {
                let mut locks: Vec<String> = Vec::new();
                for member in matched {
                    let lock_name = &loaded.members[member.lock].lock_name;
                    if !locks.contains(lock_name) {
                        locks.push(lock_name.clone());
                    }
                }
                locks
            }
            Verdict::Drift { differing, .. } => {
                let expected_hash = loaded.members[differing[0].lock].expected_hash.clone();
                let observed_hash = match Algorithm::of(&expected_hash) {
                    Some(Algorithm::Blake3) => blake3,
                    _ => content_hash.clone(),
                };
                return Err(LockError::InputDrift {
                    binding: name.to_owned(),
                    source_path,
                    expected_hash,
                    observed_hash,
                });
            }
        };
        verifications.insert(
            name.to_owned(),
            InputVerification {
                status: InputVerificationStatus::Verified,
                locks: vouching_locks,
            },
        );
    }

    Ok(verifications)
}

fn load_locks(paths: &[PathBuf]) -> Result<LoadedLock, LockError> {
    let mut members = Vec::new();
    let mut lock_names = Vec::new();

    for path in paths {
        let lock_name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());

        let contents = fs::read_to_string(path).map_err(|error| LockError::Io {
            path: path.clone(),
            detail: error.to_string(),
        })?;

        let lock_file: LockFile =
            serde_json::from_str(&contents).map_err(|error| LockError::BadLock {
                path: path.clone(),
                detail: error.to_string(),
            })?;

        let legacy = match lock_file.version.as_str() {
            LOCK_V0 => false,
            LEGACY_LOCK_VERSION => true,
            other => {
                return Err(LockError::BadLock {
                    path: path.clone(),
                    detail: format!(
                        "unsupported lock version: expected {LOCK_V0} (or deprecated {LEGACY_LOCK_VERSION}), got {other}"
                    ),
                });
            }
        };

        for member in lock_file.members {
            let expected = if legacy {
                member.content_hash
            } else {
                member.bytes_hash
            };
            let Some(expected_hash) = expected else {
                return Err(LockError::BadLock {
                    path: path.clone(),
                    detail: format!(
                        "member {} has no {}",
                        member.path,
                        if legacy { "content_hash" } else { "bytes_hash" }
                    ),
                });
            };
            members.push(LockedMember {
                path: member.path,
                expected_hash,
                lock_name: lock_name.clone(),
                lock_path: path.clone(),
            });
        }

        lock_names.push(lock_name);
    }

    Ok(LoadedLock {
        members,
        lock_names,
    })
}

/// The spelling `verify run` has always given a path in refusals: trimmed, one leading `./`
/// removed, trailing separators dropped. Only for messages; matching is R1's.
fn reported_path(path: &str) -> String {
    let trimmed = path.trim();
    let stripped = trimmed.strip_prefix("./").unwrap_or(trimmed);
    stripped
        .trim_end_matches('/')
        .trim_end_matches('\\')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use verify_core::report::InputVerificationStatus;

    use super::{LockError, reported_path, verify_locks};
    use crate::{
        BatchBindingInput, BatchBindingLimits, BindingRegistry, bindings::load_binding_registry,
    };
    use duckdb::Connection;
    use verify_core::constraint::{Binding, BindingKind, ConstraintSet};

    static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> io::Result<Self> {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_nanos();

            for _ in 0..16 {
                let unique = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
                let path =
                    std::env::temp_dir().join(format!("verify-lock-check-{timestamp}-{unique}"));

                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self { path }),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }

            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "failed to allocate unique lock check test directory",
            ))
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_file(dir: &TestDir, name: &str, contents: &str) -> io::Result<PathBuf> {
        let path = dir.path().join(name);
        fs::write(&path, contents)?;
        Ok(path)
    }

    fn constraint_with_bindings(names: &[&str]) -> ConstraintSet {
        let mut constraints = ConstraintSet::new("fixtures.lock_check");
        constraints.bindings = names
            .iter()
            .map(|name| Binding {
                name: (*name).to_owned(),
                kind: BindingKind::Relation,
                key_fields: Vec::new(),
            })
            .collect();
        constraints
    }

    fn make_registry(
        constraints: &ConstraintSet,
        inputs: Vec<BatchBindingInput>,
    ) -> (Connection, BindingRegistry) {
        let connection = Connection::open_in_memory().expect("connection opens");
        let registry = load_binding_registry(
            &connection,
            constraints,
            inputs,
            BatchBindingLimits::default(),
        )
        .expect("registry loads");
        (connection, registry)
    }

    fn make_lock_json(members: &[(&str, &str)]) -> String {
        let member_entries: Vec<String> = members
            .iter()
            .map(|(path, hash)| format!(r#"    {{ "path": "{path}", "content_hash": "{hash}" }}"#))
            .collect();
        format!(
            r#"{{
  "version": "lock.input.v1",
  "members": [
{}
  ]
}}"#,
            member_entries.join(",\n")
        )
    }

    /// Compute the sha256 hash of a file in the same format bindings.rs uses.
    fn file_hash(path: &Path) -> String {
        use sha2::{Digest, Sha256};
        let bytes = fs::read(path).expect("file reads");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        format!("sha256:{digest:x}")
    }

    #[test]
    fn no_locks_returns_empty_map() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");
        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let result = verify_locks(&[], &registry).expect("empty locks succeeds");
        assert!(result.is_empty());
    }

    #[test]
    fn successful_lock_verification() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");
        let hash = file_hash(&csv_path);
        let source_path = csv_path.to_string_lossy().to_string();

        let lock_json = make_lock_json(&[(&source_path, &hash)]);
        let lock_path = write_file(&dir, "test.lock.json", &lock_json).expect("lock writes");

        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let result = verify_locks(&[lock_path], &registry).expect("verification succeeds");

        assert_eq!(result.len(), 1);
        let verification = result.get("input").expect("input verified");
        assert_eq!(verification.status, InputVerificationStatus::Verified);
        assert_eq!(verification.locks, vec!["test.lock.json".to_owned()]);
    }

    #[test]
    fn input_not_locked_error() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");

        // Lock file that references a different path
        let lock_json = make_lock_json(&[("other/path.csv", "sha256:deadbeef")]);
        let lock_path = write_file(&dir, "test.lock.json", &lock_json).expect("lock writes");

        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let error = verify_locks(&[lock_path], &registry).expect_err("should fail");

        assert!(matches!(error, LockError::InputNotLocked { .. }));
        assert_eq!(
            error.refusal_code(),
            verify_core::refusal::RefusalCode::InputNotLocked
        );
    }

    #[test]
    fn input_drift_error() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");
        let source_path = csv_path.to_string_lossy().to_string();

        // Lock with wrong hash
        let lock_json = make_lock_json(&[(
            &source_path,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )]);
        let lock_path = write_file(&dir, "test.lock.json", &lock_json).expect("lock writes");

        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let error = verify_locks(&[lock_path], &registry).expect_err("should fail");

        assert!(matches!(error, LockError::InputDrift { .. }));
        assert_eq!(
            error.refusal_code(),
            verify_core::refusal::RefusalCode::InputDrift
        );
    }

    #[test]
    fn bad_lock_version_rejected() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");
        let lock_json = r#"{ "version": "lock.input.v99", "members": [] }"#;
        let lock_path = write_file(&dir, "test.lock.json", lock_json).expect("lock writes");

        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let error = verify_locks(&[lock_path], &registry).expect_err("should fail");
        assert!(matches!(error, LockError::BadLock { .. }));
    }

    #[test]
    fn unreadable_lock_returns_io_error() {
        let dir = TestDir::new().expect("test dir");
        let csv_path = write_file(&dir, "input.csv", "loan_id\nLN-001\n").expect("csv writes");
        let missing_lock = dir.path().join("nonexistent.lock.json");

        let constraints = constraint_with_bindings(&["input"]);
        let (_conn, registry) = make_registry(
            &constraints,
            vec![BatchBindingInput::new("input", &csv_path)],
        );

        let error = verify_locks(&[missing_lock], &registry).expect_err("should fail");
        assert!(matches!(error, LockError::Io { .. }));
    }

    #[test]
    fn reported_path_strips_dot_slash_prefix() {
        assert_eq!(reported_path("./fixtures/input.csv"), "fixtures/input.csv");
        assert_eq!(reported_path("fixtures/input.csv"), "fixtures/input.csv");
        assert_eq!(reported_path("/abs/path/input.csv"), "/abs/path/input.csv");
    }

    #[test]
    fn refusal_detail_matches_spec_shape() {
        let not_locked = LockError::InputNotLocked {
            binding: "input".to_owned(),
            source_path: "tape.csv".to_owned(),
            locks_checked: vec!["dec.lock.json".to_owned()],
        };
        let detail = not_locked.detail();
        assert_eq!(detail["binding"], "input");
        assert_eq!(detail["path"], "tape.csv");
        assert!(detail["locks_checked"].is_array());

        let drift = LockError::InputDrift {
            binding: "input".to_owned(),
            source_path: "tape.csv".to_owned(),
            expected_hash: "sha256:aaa".to_owned(),
            observed_hash: "sha256:bbb".to_owned(),
        };
        let detail = drift.detail();
        assert_eq!(detail["binding"], "input");
        assert_eq!(detail["path"], "tape.csv");
        assert_eq!(detail["expected_hash"], "sha256:aaa");
        assert_eq!(detail["observed_hash"], "sha256:bbb");
    }
}
