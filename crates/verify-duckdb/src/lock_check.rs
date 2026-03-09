use std::{collections::BTreeMap, fs, path::PathBuf};

use serde::Deserialize;
use serde_json::json;
use verify_core::{
    refusal::{Refusal, RefusalCode},
    report::{InputVerification, InputVerificationStatus},
};

use crate::BindingRegistry;

const LOCK_VERSION: &str = "lock.input.v1";

#[derive(Debug, Clone, Deserialize)]
struct LockFile {
    version: String,
    members: Vec<LockMember>,
}

#[derive(Debug, Clone, Deserialize)]
struct LockMember {
    path: String,
    content_hash: String,
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

/// Loaded lock data: maps normalized source paths to their expected content hashes
/// and tracks which lock files contributed each member.
#[derive(Debug, Clone)]
struct LoadedLock {
    /// Normalized path -> (expected content_hash, lock file source name)
    members: BTreeMap<String, (String, String)>,
    /// Names of all lock files that were loaded.
    lock_names: Vec<String>,
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
    let mut verifications = BTreeMap::new();

    for (name, binding) in registry.iter() {
        let source_path = normalize_path(&binding.source_path().to_string_lossy());
        let content_hash = &binding.metadata().content_hash;

        match loaded.members.get(&source_path) {
            Some((expected_hash, _lock_source)) => {
                if expected_hash != content_hash {
                    return Err(LockError::InputDrift {
                        binding: name.to_owned(),
                        source_path,
                        expected_hash: expected_hash.clone(),
                        observed_hash: content_hash.clone(),
                    });
                }

                let contributing_locks = locks_containing(&loaded, &source_path);
                verifications.insert(
                    name.to_owned(),
                    InputVerification {
                        status: InputVerificationStatus::Verified,
                        locks: contributing_locks,
                    },
                );
            }
            None => {
                return Err(LockError::InputNotLocked {
                    binding: name.to_owned(),
                    source_path,
                    locks_checked: loaded.lock_names.clone(),
                });
            }
        }
    }

    Ok(verifications)
}

fn load_locks(paths: &[PathBuf]) -> Result<LoadedLock, LockError> {
    let mut members = BTreeMap::new();
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

        if lock_file.version != LOCK_VERSION {
            return Err(LockError::BadLock {
                path: path.clone(),
                detail: format!(
                    "unsupported lock version: expected {LOCK_VERSION}, got {}",
                    lock_file.version
                ),
            });
        }

        for member in lock_file.members {
            let normalized = normalize_path(&member.path);
            members.insert(normalized, (member.content_hash, lock_name.clone()));
        }

        lock_names.push(lock_name);
    }

    Ok(LoadedLock {
        members,
        lock_names,
    })
}

/// Collect the names of all lock files that contain a given path.
fn locks_containing(loaded: &LoadedLock, normalized_path: &str) -> Vec<String> {
    if let Some((_, lock_name)) = loaded.members.get(normalized_path) {
        vec![lock_name.clone()]
    } else {
        Vec::new()
    }
}

/// Normalize a path string for lock membership matching.
///
/// Lock files use relative paths (e.g. `fixtures/inputs/arity1/loans.csv`).
/// Binding source paths may be absolute or relative. This normalization strips
/// leading `./` and trailing separators to increase matching likelihood.
fn normalize_path(path: &str) -> String {
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

    use super::{LockError, normalize_path, verify_locks};
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
    fn normalize_path_strips_dot_slash_prefix() {
        assert_eq!(normalize_path("./fixtures/input.csv"), "fixtures/input.csv");
        assert_eq!(normalize_path("fixtures/input.csv"), "fixtures/input.csv");
        assert_eq!(normalize_path("/abs/path/input.csv"), "/abs/path/input.csv");
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
