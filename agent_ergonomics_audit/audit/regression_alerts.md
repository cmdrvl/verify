# Regression Alerts

No regressions were detected in pass 1.

Verification completed in this pass:

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- Intent corpus replay: 0 silent failures, 0 useless errors
- Five audit regression scripts under `audit/regression_tests/`
