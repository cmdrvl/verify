/// Deterministic fixture generators for perf smoke tests.
///
/// Generates CSV data and compiled constraint artifacts at configurable
/// row counts for arity-1 (loans) and arity-N (property + tenants).
///
/// All generators are seeded deterministically — the same row count
/// always produces the same output bytes.
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

/// Generated fixture family for a single perf smoke scenario.
#[allow(dead_code)]
pub struct GeneratedFixture {
    pub root: PathBuf,
    pub constraint_path: PathBuf,
    pub bindings: Vec<(String, PathBuf)>,
    pub row_count: u64,
}

impl Drop for GeneratedFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Generate an arity-1 loan dataset with `row_count` rows.
///
/// - All rows have valid `loan_id` values (PASS scenario).
/// - Columns: loan_id, balance, occupancy_status
/// - Constraint: not_null on loan_id
pub fn generate_arity1_pass(root: &Path, row_count: u64) -> GeneratedFixture {
    let fixture_dir = root.join("arity1_pass");
    fs::create_dir_all(&fixture_dir).expect("create fixture dir");

    // Generate CSV
    let mut csv = String::from("loan_id,balance,occupancy_status\n");
    let statuses = ["owner", "investor", "vacant", "second_home"];
    for i in 0..row_count {
        let balance = 100_000.0 + (i as f64 * 17.31);
        let status = statuses[(i as usize) % statuses.len()];
        writeln!(csv, "LN-{i:06},{balance:.2},{status}").expect("write csv row");
    }
    let csv_path = fixture_dir.join("loans.csv");
    fs::write(&csv_path, &csv).expect("write csv");

    // Generate compiled constraints
    let constraints = r#"{
  "version": "verify.constraint.v1",
  "constraint_set_id": "perf.arity1.not_null_loans",
  "bindings": [
    {
      "name": "input",
      "kind": "relation",
      "key_fields": ["loan_id"]
    }
  ],
  "rules": [
    {
      "id": "LOAN_ID_PRESENT",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "not_null",
        "binding": "input",
        "columns": ["loan_id"]
      }
    },
    {
      "id": "LOAN_ID_UNIQUE",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "unique",
        "binding": "input",
        "columns": ["loan_id"]
      }
    },
    {
      "id": "BALANCE_POSITIVE",
      "severity": "warn",
      "portability": "portable",
      "check": {
        "op": "predicate",
        "binding": "input",
        "expr": {
          "gt": [{"column": "balance"}, 0]
        }
      }
    }
  ]
}"#;
    let constraint_path = fixture_dir.join("constraints.verify.json");
    fs::write(&constraint_path, constraints).expect("write constraints");

    GeneratedFixture {
        root: fixture_dir,
        constraint_path,
        bindings: vec![("input".to_owned(), csv_path)],
        row_count,
    }
}

/// Generate an arity-1 loan dataset with `fail_count` rows having empty loan_id.
pub fn generate_arity1_fail(root: &Path, row_count: u64, fail_count: u64) -> GeneratedFixture {
    let fixture_dir = root.join("arity1_fail");
    fs::create_dir_all(&fixture_dir).expect("create fixture dir");

    let mut csv = String::from("loan_id,balance,occupancy_status\n");
    let statuses = ["owner", "investor", "vacant", "second_home"];
    for i in 0..row_count {
        let balance = 100_000.0 + (i as f64 * 17.31);
        let status = statuses[(i as usize) % statuses.len()];
        if i < fail_count {
            writeln!(csv, ",{balance:.2},{status}").expect("write csv row");
        } else {
            writeln!(csv, "LN-{i:06},{balance:.2},{status}").expect("write csv row");
        }
    }
    let csv_path = fixture_dir.join("loans.csv");
    fs::write(&csv_path, &csv).expect("write csv");

    let constraints = r#"{
  "version": "verify.constraint.v1",
  "constraint_set_id": "perf.arity1.not_null_loans_fail",
  "bindings": [
    {
      "name": "input",
      "kind": "relation",
      "key_fields": ["loan_id"]
    }
  ],
  "rules": [
    {
      "id": "LOAN_ID_PRESENT",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "not_null",
        "binding": "input",
        "columns": ["loan_id"]
      }
    }
  ]
}"#;
    let constraint_path = fixture_dir.join("constraints.verify.json");
    fs::write(&constraint_path, constraints).expect("write constraints");

    GeneratedFixture {
        root: fixture_dir,
        constraint_path,
        bindings: vec![("input".to_owned(), csv_path)],
        row_count,
    }
}

/// Generate an arity-N scenario with property + tenants tables.
///
/// - `property_rows` property records, each referencing one of `tenant_rows` tenants.
/// - All foreign keys are valid (PASS scenario).
/// - Constraint: foreign_key property.tenant_id → tenants.tenant_id
pub fn generate_arity_n_pass(
    root: &Path,
    property_rows: u64,
    tenant_rows: u64,
) -> GeneratedFixture {
    let fixture_dir = root.join("arity_n_pass");
    fs::create_dir_all(&fixture_dir).expect("create fixture dir");

    // Generate tenants CSV
    let mut tenants_csv = String::from("tenant_id,tenant_name,industry\n");
    let industries = ["finance", "tech", "healthcare", "retail", "manufacturing"];
    for i in 0..tenant_rows {
        let industry = industries[(i as usize) % industries.len()];
        writeln!(tenants_csv, "T-{i:06},Tenant {i},{industry}").expect("write tenant row");
    }
    let tenants_path = fixture_dir.join("tenants.csv");
    fs::write(&tenants_path, &tenants_csv).expect("write tenants csv");

    // Generate property CSV (all reference valid tenants)
    let mut property_csv = String::from("property_id,tenant_id,address,sqft\n");
    for i in 0..property_rows {
        let tenant_idx = i % tenant_rows;
        let sqft = 1000 + (i % 50_000);
        writeln!(
            property_csv,
            "P-{i:06},T-{tenant_idx:06},{i} Main Street,{sqft}"
        )
        .expect("write property row");
    }
    let property_path = fixture_dir.join("property.csv");
    fs::write(&property_path, &property_csv).expect("write property csv");

    let constraints = r#"{
  "version": "verify.constraint.v1",
  "constraint_set_id": "perf.arity_n.foreign_key_property_tenants",
  "bindings": [
    {
      "name": "property",
      "kind": "relation",
      "key_fields": ["property_id"]
    },
    {
      "name": "tenants",
      "kind": "relation",
      "key_fields": ["tenant_id"]
    }
  ],
  "rules": [
    {
      "id": "PROPERTY_TENANT_EXISTS",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "foreign_key",
        "binding": "property",
        "columns": ["tenant_id"],
        "ref_binding": "tenants",
        "ref_columns": ["tenant_id"]
      }
    },
    {
      "id": "PROPERTY_ID_UNIQUE",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "unique",
        "binding": "property",
        "columns": ["property_id"]
      }
    },
    {
      "id": "TENANT_ID_UNIQUE",
      "severity": "error",
      "portability": "portable",
      "check": {
        "op": "unique",
        "binding": "tenants",
        "columns": ["tenant_id"]
      }
    }
  ]
}"#;
    let constraint_path = fixture_dir.join("constraints.verify.json");
    fs::write(&constraint_path, constraints).expect("write constraints");

    GeneratedFixture {
        root: fixture_dir,
        constraint_path,
        bindings: vec![
            ("property".to_owned(), property_path),
            ("tenants".to_owned(), tenants_path),
        ],
        row_count: property_rows,
    }
}
