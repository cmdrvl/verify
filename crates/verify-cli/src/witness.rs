use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::Args;
use serde_json::{Map, Value, json};
use verify_core::{TOOL_NAME, order::canonical_json_bytes};

#[derive(Debug, Clone, Args)]
pub struct WitnessArgs {
    #[arg(value_name = "ACTION")]
    pub action: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessCommandResult {
    pub exit_code: u8,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WitnessInput {
    pub path: String,
    pub hash: Option<String>,
    pub bytes: Option<u64>,
}

impl WitnessInput {
    pub(crate) fn from_path(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();

        Self {
            path: path.display().to_string(),
            hash: None,
            bytes: fs::metadata(path).ok().map(|metadata| metadata.len()),
        }
    }
}

impl WitnessCommandResult {
    fn success(stdout: String) -> Self {
        Self {
            exit_code: 0,
            stdout: Some(stdout),
            stderr: None,
        }
    }

    fn no_match(stdout: Option<String>, message: &str) -> Self {
        Self {
            exit_code: 1,
            stdout,
            stderr: Some(message.to_owned()),
        }
    }

    fn error(message: String) -> Self {
        Self {
            exit_code: 2,
            stdout: None,
            stderr: Some(message),
        }
    }
}

pub fn execute(args: WitnessArgs) -> WitnessCommandResult {
    match args.action.as_deref() {
        Some("query") => execute_query(args.json),
        Some("last") => execute_last(args.json),
        Some("count") => execute_count(args.json),
        Some(action) => WitnessCommandResult::error(format!(
            "verify: witness action must be one of query, last, or count; got `{action}`"
        )),
        None => WitnessCommandResult::error(
            "verify: witness action is required (query, last, or count)".to_owned(),
        ),
    }
}

pub(crate) fn append_run_record(
    command: &str,
    outcome: &str,
    exit_code: u8,
    inputs: Vec<WitnessInput>,
    params: Map<String, Value>,
    output_bytes: &[u8],
) -> Result<(), String> {
    let path = witness_ledger_path();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create witness directory: {error}"))?;
    }

    let record = build_record(command, outcome, exit_code, inputs, params, output_bytes)?;
    let encoded = serde_json::to_string(&record)
        .map_err(|error| format!("cannot encode witness record: {error}"))?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| format!("cannot open witness ledger: {error}"))?;

    writeln!(file, "{encoded}").map_err(|error| format!("cannot write witness ledger: {error}"))?;
    Ok(())
}

pub(crate) fn witness_ledger_path() -> PathBuf {
    witness_ledger_path_from_env(|key| std::env::var(key).ok())
}

fn execute_query(json_output: bool) -> WitnessCommandResult {
    let records = match load_verify_records() {
        Ok(records) => records,
        Err(error) => return WitnessCommandResult::error(error),
    };

    if records.is_empty() {
        return if json_output {
            WitnessCommandResult::no_match(
                Some("[]".to_owned()),
                "verify: no witness records found",
            )
        } else {
            WitnessCommandResult::no_match(None, "verify: no witness records found")
        };
    }

    if json_output {
        WitnessCommandResult::success(
            serde_json::to_string(&records).expect("witness records should serialize"),
        )
    } else {
        WitnessCommandResult::success(
            records
                .iter()
                .map(format_record_human)
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}

fn execute_last(json_output: bool) -> WitnessCommandResult {
    let records = match load_verify_records() {
        Ok(records) => records,
        Err(error) => return WitnessCommandResult::error(error),
    };

    let Some(record) = records.last() else {
        return if json_output {
            WitnessCommandResult::no_match(
                Some("null".to_owned()),
                "verify: witness ledger is empty",
            )
        } else {
            WitnessCommandResult::no_match(None, "verify: witness ledger is empty")
        };
    };

    if json_output {
        WitnessCommandResult::success(
            serde_json::to_string(record).expect("witness record should serialize"),
        )
    } else {
        WitnessCommandResult::success(format_record_human(record))
    }
}

fn execute_count(json_output: bool) -> WitnessCommandResult {
    let records = match load_verify_records() {
        Ok(records) => records,
        Err(error) => return WitnessCommandResult::error(error),
    };

    if records.is_empty() {
        return if json_output {
            WitnessCommandResult::no_match(
                Some(json!({ "count": 0 }).to_string()),
                "verify: no witness records found",
            )
        } else {
            WitnessCommandResult::no_match(None, "verify: no witness records found")
        };
    }

    if json_output {
        WitnessCommandResult::success(json!({ "count": records.len() }).to_string())
    } else {
        WitnessCommandResult::success(records.len().to_string())
    }
}

fn load_verify_records() -> Result<Vec<Value>, String> {
    let path = witness_ledger_path();
    load_records_from_path(&path)
}

fn load_records_from_path(path: &Path) -> Result<Vec<Value>, String> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!("verify: witness: failed to read ledger: {error}"));
        }
    };

    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = match line {
            Ok(line) if !line.trim().is_empty() => line,
            Ok(_) => continue,
            Err(_) => continue,
        };

        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };

        if record.get("tool").and_then(Value::as_str) == Some(TOOL_NAME) {
            records.push(record);
        }
    }

    Ok(records)
}

fn build_record(
    command: &str,
    outcome: &str,
    exit_code: u8,
    inputs: Vec<WitnessInput>,
    params: Map<String, Value>,
    output_bytes: &[u8],
) -> Result<Value, String> {
    let mut record = Map::new();
    record.insert("id".to_owned(), Value::String(String::new()));
    record.insert("tool".to_owned(), Value::String(TOOL_NAME.to_owned()));
    record.insert(
        "version".to_owned(),
        Value::String(env!("CARGO_PKG_VERSION").to_owned()),
    );
    record.insert("command".to_owned(), Value::String(command.to_owned()));
    record.insert(
        "binary_hash".to_owned(),
        Value::String(binary_hash().unwrap_or_default()),
    );
    record.insert(
        "inputs".to_owned(),
        Value::Array(inputs.into_iter().map(input_to_value).collect()),
    );
    record.insert("params".to_owned(), Value::Object(params));
    record.insert("outcome".to_owned(), Value::String(outcome.to_owned()));
    record.insert(
        "exit_code".to_owned(),
        Value::Number(serde_json::Number::from(exit_code)),
    );
    record.insert(
        "output_hash".to_owned(),
        Value::String(local_hash(output_bytes)),
    );
    record.insert("ts".to_owned(), Value::String(timestamp_string()));

    let mut value = Value::Object(record);
    let id = local_hash(
        &canonical_json_bytes(&value)
            .map_err(|error| format!("cannot canonicalize witness record: {error}"))?,
    );
    value
        .as_object_mut()
        .expect("witness record should be an object")
        .insert("id".to_owned(), Value::String(id));

    Ok(value)
}

fn input_to_value(input: WitnessInput) -> Value {
    let mut value = Map::new();
    value.insert("path".to_owned(), Value::String(input.path));

    if let Some(hash) = input.hash {
        value.insert("hash".to_owned(), Value::String(hash));
    }

    if let Some(bytes) = input.bytes {
        value.insert(
            "bytes".to_owned(),
            Value::Number(serde_json::Number::from(bytes)),
        );
    }

    Value::Object(value)
}

fn format_record_human(record: &Value) -> String {
    let ts = record.get("ts").and_then(Value::as_str).unwrap_or("-");
    let command = record.get("command").and_then(Value::as_str).unwrap_or("-");
    let outcome = record.get("outcome").and_then(Value::as_str).unwrap_or("-");
    let exit_code = record.get("exit_code").and_then(Value::as_u64).unwrap_or(0);
    let input_count = record
        .get("inputs")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);

    format!("{ts} {command} {outcome} exit={exit_code} inputs={input_count}")
}

fn witness_ledger_path_from_env<F>(get_env: F) -> PathBuf
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(path) = get_env("EPISTEMIC_WITNESS")
        && !path.trim().is_empty()
    {
        return PathBuf::from(path);
    }

    let home = home_from_env(&get_env).unwrap_or_else(|| PathBuf::from("."));
    home.join(".epistemic").join("witness.jsonl")
}

fn home_from_env<F>(get_env: &F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    #[cfg(unix)]
    {
        get_env("HOME")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
    }

    #[cfg(windows)]
    {
        get_env("USERPROFILE")
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
    }

    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

fn binary_hash() -> Option<String> {
    let path = std::env::current_exe().ok()?;
    let bytes = fs::read(path).ok()?;
    Some(local_hash(&bytes))
}

fn timestamp_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}

fn local_hash(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;

    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!("fnv1a64:{hash:016x}")
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::{Map, json};

    use super::{
        WitnessInput, build_record, format_record_human, load_records_from_path, local_hash,
        witness_ledger_path_from_env,
    };

    #[test]
    fn explicit_epistemic_witness_path_wins() {
        let path = witness_ledger_path_from_env(|key| match key {
            "EPISTEMIC_WITNESS" => Some("/tmp/custom-ledger.jsonl".to_owned()),
            "HOME" => Some("/tmp/home".to_owned()),
            _ => None,
        });

        assert_eq!(path, PathBuf::from("/tmp/custom-ledger.jsonl"));
    }

    #[test]
    fn home_fallback_uses_standard_ledger_location() {
        let path = witness_ledger_path_from_env(|key| match key {
            "EPISTEMIC_WITNESS" => Some(String::new()),
            "HOME" => Some("/tmp/home".to_owned()),
            _ => None,
        });

        assert_eq!(path, PathBuf::from("/tmp/home/.epistemic/witness.jsonl"));
    }

    #[test]
    fn local_hash_is_stable() {
        assert_eq!(local_hash(b"verify"), local_hash(b"verify"));
        assert_ne!(local_hash(b"verify"), local_hash(b"witness"));
    }

    #[test]
    fn build_record_populates_expected_fields() {
        let record = build_record(
            "run",
            "REFUSAL",
            2,
            vec![WitnessInput {
                path: "compiled.verify.json".to_owned(),
                hash: None,
                bytes: Some(42),
            }],
            Map::from_iter([("json".to_owned(), json!(true))]),
            b"{\"outcome\":\"REFUSAL\"}",
        )
        .expect("record should build");

        assert_eq!(record["tool"], "verify");
        assert_eq!(record["command"], "run");
        assert_eq!(record["outcome"], "REFUSAL");
        assert_eq!(record["exit_code"], 2);
        assert_eq!(record["inputs"][0]["path"], "compiled.verify.json");
        assert!(
            record["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("fnv1a64:"))
        );
    }

    #[test]
    fn malformed_ledger_lines_are_ignored() {
        let path = std::env::temp_dir().join(format!(
            "verify-witness-test-{}-malformed.jsonl",
            std::process::id()
        ));
        fs::write(
            &path,
            format!(
                "{}\n{}\nnot-json\n",
                json!({"tool": "verify", "command": "run", "outcome": "REFUSAL"}),
                json!({"tool": "pack", "command": "verify", "outcome": "OK"})
            ),
        )
        .expect("ledger file should write");

        let records = load_records_from_path(&path).expect("ledger should load");
        fs::remove_file(&path).expect("temporary ledger should be removed");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["tool"], "verify");
    }

    #[test]
    fn human_format_is_compact() {
        let formatted = format_record_human(&json!({
            "ts": "12345",
            "command": "run",
            "outcome": "REFUSAL",
            "exit_code": 2,
            "inputs": [{ "path": "compiled.verify.json" }],
        }));

        assert_eq!(formatted, "12345 run REFUSAL exit=2 inputs=1");
    }
}
