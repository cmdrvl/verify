#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub const FIXTURE_ROOT: &str = "fixtures";

pub const FIXTURE_FAMILIES: &[&str] = &[
    "authoring/arity1",
    "authoring/arity_n",
    "authoring/predicate_grammar",
    "authoring/query_rules",
    "authoring/refusals",
    "constraints/arity1",
    "constraints/arity_n",
    "constraints/query_rules",
    "inputs/arity1",
    "inputs/arity_n",
    "inputs/batch_missingness",
    "locks/arity1",
    "reports/fail",
    "reports/pass",
    "reports/query_localization",
    "reports/refusal",
];

pub const AUTHORING_SUFFIXES: &[&str] = &[".yaml", ".sql"];
pub const CONSTRAINT_SUFFIX: &str = ".verify.json";
pub const REPORT_SUFFIXES: &[&str] = &[".pass.json", ".fail.json", ".refusal.json"];

pub fn fixture_path(relative: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(FIXTURE_ROOT).join(relative)
}

pub fn fixture_family_path(family: &str, name: &str) -> PathBuf {
    fixture_path(Path::new(family).join(name))
}
