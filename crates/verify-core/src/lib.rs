#![forbid(unsafe_code)]

pub mod constraint;
pub mod order;
pub mod refusal;
pub mod report;

pub const TOOL_NAME: &str = "verify";
pub const CONSTRAINT_VERSION: &str = "verify.constraint.v1";
pub const REPORT_VERSION: &str = "verify.report.v1";

pub use constraint::ConstraintSet;
pub use report::VerifyReport;
