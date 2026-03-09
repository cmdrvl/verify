/// Performance smoke tests for verify.
///
/// These tests exercise the full compile → bind → evaluate → render pipeline
/// at non-trivial row counts and capture per-phase timing. They are designed
/// to detect runtime regressions, not to serve as benchmarks.
///
/// # Baseline procedure
///
/// 1. Run: `cargo test -p verify-cli --test perf_smoke -- --nocapture`
/// 2. Each test prints phase timings to stdout.
/// 3. Compare against prior baselines after implementation changes.
/// 4. A guardrail assertion fails if total time exceeds the budget.
mod gen_fixtures;

use std::{
    path::PathBuf,
    process::Command,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn verify_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_verify"))
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "verify-perf-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

struct PhaseTimer {
    scenario: String,
    phases: Vec<(String, std::time::Duration)>,
    start: Instant,
}

impl PhaseTimer {
    fn new(scenario: &str) -> Self {
        Self {
            scenario: scenario.to_owned(),
            phases: Vec::new(),
            start: Instant::now(),
        }
    }

    fn phase(&mut self, name: &str) -> PhaseGuard<'_> {
        PhaseGuard {
            timer: self,
            name: name.to_owned(),
            start: Instant::now(),
        }
    }

    fn total(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    fn report(&self) {
        println!("\n--- perf_smoke: {} ---", self.scenario);
        for (name, duration) in &self.phases {
            println!("  {name}: {duration:.1?}");
        }
        println!("  TOTAL: {:.1?}", self.total());
        println!("---");
    }
}

struct PhaseGuard<'a> {
    timer: &'a mut PhaseTimer,
    name: String,
    start: Instant,
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        self.timer
            .phases
            .push((self.name.clone(), self.start.elapsed()));
    }
}

// ---------------------------------------------------------------------------
// Arity-1 PASS: 10k rows, 3 portable rules
// ---------------------------------------------------------------------------

#[test]
fn arity1_pass_10k_rows_completes_under_budget() {
    let root = temp_root("arity1-pass-10k");
    let fixture = gen_fixtures::generate_arity1_pass(&root, 10_000);
    let mut timer = PhaseTimer::new("arity1_pass_10k");

    // Phase: full CLI run (compile is skipped — using pre-compiled constraints)
    let bind_arg = format!("input={}", fixture.bindings[0].1.display());

    let output = {
        let _guard = timer.phase("cli_run_json");
        Command::new(verify_binary())
            .args([
                "run",
                fixture.constraint_path.to_str().unwrap(),
                "--bind",
                &bind_arg,
                "--json",
                "--no-witness",
            ])
            .output()
            .expect("verify should run")
    };

    timer.report();

    assert_eq!(
        output.status.code(),
        Some(0),
        "10k-row PASS should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["summary"]["total_rules"], 3);

    // Guardrail: 10k rows with 3 portable rules should complete in under 30s.
    // This is generous — typical runs are <5s. The budget catches catastrophic regressions.
    assert!(
        timer.total().as_secs() < 30,
        "arity1 10k PASS exceeded 30s budget: {:.1?}",
        timer.total()
    );
}

// ---------------------------------------------------------------------------
// Arity-1 FAIL: 10k rows, some violations
// ---------------------------------------------------------------------------

#[test]
fn arity1_fail_10k_rows_completes_under_budget() {
    let root = temp_root("arity1-fail-10k");
    let fixture = gen_fixtures::generate_arity1_fail(&root, 10_000, 100);
    let mut timer = PhaseTimer::new("arity1_fail_10k_100violations");

    let bind_arg = format!("input={}", fixture.bindings[0].1.display());

    let output = {
        let _guard = timer.phase("cli_run_json");
        Command::new(verify_binary())
            .args([
                "run",
                fixture.constraint_path.to_str().unwrap(),
                "--bind",
                &bind_arg,
                "--json",
                "--no-witness",
            ])
            .output()
            .expect("verify should run")
    };

    timer.report();

    assert_eq!(
        output.status.code(),
        Some(1),
        "10k-row FAIL should exit 1; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "FAIL");
    assert_eq!(report["summary"]["failed_rules"], 1);
    assert!(report["results"][0]["violation_count"].as_u64().unwrap() >= 100);

    assert!(
        timer.total().as_secs() < 30,
        "arity1 10k FAIL exceeded 30s budget: {:.1?}",
        timer.total()
    );
}

// ---------------------------------------------------------------------------
// Arity-N PASS: 10k properties, 500 tenants, foreign key + unique checks
// ---------------------------------------------------------------------------

#[test]
fn arity_n_pass_10k_rows_completes_under_budget() {
    let root = temp_root("arity-n-pass-10k");
    let fixture = gen_fixtures::generate_arity_n_pass(&root, 10_000, 500);
    let mut timer = PhaseTimer::new("arity_n_pass_10k_properties_500_tenants");

    let bind_args: Vec<String> = fixture
        .bindings
        .iter()
        .map(|(name, path)| format!("{name}={}", path.display()))
        .collect();

    let mut cmd = Command::new(verify_binary());
    cmd.args(["run", fixture.constraint_path.to_str().unwrap()]);
    for bind in &bind_args {
        cmd.args(["--bind", bind]);
    }
    cmd.args(["--json", "--no-witness"]);

    let output = {
        let _guard = timer.phase("cli_run_json");
        cmd.output().expect("verify should run")
    };

    timer.report();

    assert_eq!(
        output.status.code(),
        Some(0),
        "10k-property PASS should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "PASS");
    assert_eq!(report["summary"]["total_rules"], 3);

    assert!(
        timer.total().as_secs() < 30,
        "arity_n 10k PASS exceeded 30s budget: {:.1?}",
        timer.total()
    );
}

// ---------------------------------------------------------------------------
// Shortcut path: compile + evaluate end to end
// ---------------------------------------------------------------------------

#[test]
fn shortcut_10k_rows_compile_and_evaluate_under_budget() {
    let root = temp_root("shortcut-10k");
    // Generate the CSV only (we'll use the existing authoring fixture)
    let fixture = gen_fixtures::generate_arity1_pass(&root, 10_000);
    let authoring = format!("{WORKSPACE_ROOT}/fixtures/authoring/arity1/not_null_loans.yaml");
    let mut timer = PhaseTimer::new("shortcut_10k_compile_evaluate");

    let output = {
        let _guard = timer.phase("shortcut_full");
        Command::new(verify_binary())
            .args([
                fixture.bindings[0].1.to_str().unwrap(),
                "--rules",
                &authoring,
                "--key",
                "loan_id",
                "--json",
                "--no-witness",
            ])
            .output()
            .expect("verify shortcut should run")
    };

    timer.report();

    assert_eq!(
        output.status.code(),
        Some(0),
        "shortcut 10k PASS should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid json");
    assert_eq!(report["outcome"], "PASS");

    assert!(
        timer.total().as_secs() < 30,
        "shortcut 10k exceeded 30s budget: {:.1?}",
        timer.total()
    );
}

// ---------------------------------------------------------------------------
// Human output path timing
// ---------------------------------------------------------------------------

#[test]
fn human_render_10k_rows_under_budget() {
    let root = temp_root("human-10k");
    let fixture = gen_fixtures::generate_arity1_fail(&root, 10_000, 50);
    let mut timer = PhaseTimer::new("human_render_10k_50violations");

    let bind_arg = format!("input={}", fixture.bindings[0].1.display());

    let output = {
        let _guard = timer.phase("cli_run_human");
        Command::new(verify_binary())
            .args([
                "run",
                fixture.constraint_path.to_str().unwrap(),
                "--bind",
                &bind_arg,
                "--no-witness",
            ])
            .output()
            .expect("verify should run")
    };

    timer.report();

    assert_eq!(
        output.status.code(),
        Some(1),
        "10k-row human FAIL should exit 1; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("VERIFY FAIL"));

    assert!(
        timer.total().as_secs() < 30,
        "human render 10k exceeded 30s budget: {:.1?}",
        timer.total()
    );
}
