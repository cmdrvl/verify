//! Spine family contract (spine-rules `golden_rules_suite!`): operator.v1
//! manifest, cmdrvl.doctor.v1 reports, exit-code dictionary, flag dictionary,
//! ANSI-free output, bare invocation, and the empty-query rule.
//!
//! Paths are relative to the verify-cli package root, which is the working
//! directory `cargo test` gives the spawned binary.

use spine_rules::golden_rules_suite;

golden_rules_suite! {
    binary: env!("CARGO_BIN_EXE_verify"),
    operator_json: include_str!("../operator.json"),
    source_files: &["../verify-core/src/refusal.rs"],
    fixture_success_args: &[
        "run",
        "../../fixtures/constraints/arity1/not_null_loans.verify.json",
        "--bind",
        "input=../../fixtures/inputs/arity1/loans.csv",
        "--json",
        "--no-witness",
    ],
    fixture_refusal_args: &[
        "run",
        "nonexistent.verify.json",
        "--bind",
        "input=../../fixtures/inputs/arity1/loans.csv",
        "--json",
        "--no-witness",
    ],
    // A valid query that matches nothing, whatever the machine's real ledger holds.
    fixture_empty_query_args: &["witness", "query", "--json", "--command", "no-such-command"],
}
