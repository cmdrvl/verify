use duckdb::types::Value as DuckValue;
use serde_json::{Number, Value};
use verify_engine::scalar::ScalarCategory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckValueErrorKind {
    NonScalar,
    Unrepresentable,
}

impl DuckValueErrorKind {
    pub const fn key_reason(self) -> &'static str {
        match self {
            Self::NonScalar => "non_scalar_component",
            Self::Unrepresentable => "unrepresentable_component",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckValueError {
    pub kind: DuckValueErrorKind,
    pub value_type: String,
}

impl std::fmt::Display for DuckValueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "DuckDB {} value cannot be represented as a protocol scalar",
            self.value_type
        )
    }
}

impl std::error::Error for DuckValueError {}

/// Convert one dynamic DuckDB value to the exact JSON scalar consumed by the
/// portable engine. Nested and engine-specific values fail closed.
pub fn duckdb_value_to_protocol(value: DuckValue) -> Result<Value, DuckValueError> {
    match value {
        DuckValue::Null => Ok(Value::Null),
        DuckValue::Boolean(value) => Ok(Value::Bool(value)),
        DuckValue::TinyInt(value) => Ok(Value::Number(value.into())),
        DuckValue::SmallInt(value) => Ok(Value::Number(value.into())),
        DuckValue::Int(value) => Ok(Value::Number(value.into())),
        DuckValue::BigInt(value) => Ok(Value::Number(value.into())),
        DuckValue::HugeInt(value) => huge_int_to_protocol(value),
        DuckValue::UTinyInt(value) => Ok(Value::Number(value.into())),
        DuckValue::USmallInt(value) => Ok(Value::Number(value.into())),
        DuckValue::UInt(value) => Ok(Value::Number(value.into())),
        DuckValue::UBigInt(value) => Ok(Value::Number(value.into())),
        DuckValue::Float(value) => finite_number(f64::from(value), "float"),
        DuckValue::Double(value) => finite_number(value, "double"),
        DuckValue::Decimal(value) => value
            .to_string()
            .parse::<Number>()
            .map(Value::Number)
            .map_err(|_| unrepresentable("decimal")),
        DuckValue::Text(value) | DuckValue::Enum(value) => Ok(Value::String(value)),
        DuckValue::List(_) => Err(non_scalar("list")),
        DuckValue::Struct(_) => Err(non_scalar("struct")),
        DuckValue::Array(_) => Err(non_scalar("array")),
        DuckValue::Map(_) => Err(non_scalar("map")),
        DuckValue::Union(_) => Err(non_scalar("union")),
        DuckValue::Timestamp(..) => Err(unrepresentable("timestamp")),
        DuckValue::Blob(_) => Err(unrepresentable("blob")),
        DuckValue::Date32(_) => Err(unrepresentable("date")),
        DuckValue::Time64(..) => Err(unrepresentable("time")),
        DuckValue::Interval { .. } => Err(unrepresentable("interval")),
    }
}

/// Classify a DESCRIBE type without asking DuckDB to compare it. This is used
/// to prove key and predicate operand compatibility before semantic execution.
pub fn duckdb_type_category(data_type: &str) -> Result<ScalarCategory, DuckValueError> {
    let normalized = data_type.trim().to_ascii_uppercase();
    let base = normalized
        .split_once('(')
        .map_or(normalized.as_str(), |(base, _)| base.trim());

    if matches!(
        base,
        "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "INT"
            | "BIGINT"
            | "HUGEINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
            | "UINT"
            | "UBIGINT"
            | "FLOAT"
            | "REAL"
            | "DOUBLE"
            | "DECIMAL"
    ) {
        return Ok(ScalarCategory::Number);
    }
    if base == "BOOLEAN" || base == "BOOL" {
        return Ok(ScalarCategory::Boolean);
    }
    if matches!(base, "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "ENUM") {
        return Ok(ScalarCategory::String);
    }
    if base == "NULL" {
        return Ok(ScalarCategory::Null);
    }
    if normalized.contains("STRUCT(")
        || normalized.contains("MAP(")
        || normalized.contains("UNION(")
        || normalized.contains("LIST")
        || normalized.contains("[]")
        || normalized.starts_with("ARRAY")
    {
        return Err(non_scalar(&normalized.to_ascii_lowercase()));
    }

    Err(unrepresentable(&normalized.to_ascii_lowercase()))
}

pub(crate) fn duckdb_value_sort_key(value: &DuckValue) -> String {
    match value {
        DuckValue::Null => "00:null".to_owned(),
        DuckValue::Boolean(value) => format!("01:boolean:{value}"),
        DuckValue::TinyInt(value) => format!("02:number:{value}"),
        DuckValue::SmallInt(value) => format!("02:number:{value}"),
        DuckValue::Int(value) => format!("02:number:{value}"),
        DuckValue::BigInt(value) => format!("02:number:{value}"),
        DuckValue::HugeInt(value) => format!("02:number:{value}"),
        DuckValue::UTinyInt(value) => format!("02:number:{value}"),
        DuckValue::USmallInt(value) => format!("02:number:{value}"),
        DuckValue::UInt(value) => format!("02:number:{value}"),
        DuckValue::UBigInt(value) => format!("02:number:{value}"),
        DuckValue::Float(value) => format!("02:number:{value:?}"),
        DuckValue::Double(value) => format!("02:number:{value:?}"),
        DuckValue::Decimal(value) => format!("02:number:{value}"),
        DuckValue::Text(value) => format!("03:string:{value}"),
        DuckValue::Enum(value) => format!("03:string:{value}"),
        other => format!("99:{other:?}"),
    }
}

fn huge_int_to_protocol(value: i128) -> Result<Value, DuckValueError> {
    if let Ok(value) = i64::try_from(value) {
        return Ok(Value::Number(value.into()));
    }
    if let Ok(value) = u64::try_from(value) {
        return Ok(Value::Number(value.into()));
    }
    Err(unrepresentable("hugeint"))
}

fn finite_number(value: f64, value_type: &str) -> Result<Value, DuckValueError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| unrepresentable(value_type))
}

fn non_scalar(value_type: &str) -> DuckValueError {
    DuckValueError {
        kind: DuckValueErrorKind::NonScalar,
        value_type: value_type.to_owned(),
    }
}

fn unrepresentable(value_type: &str) -> DuckValueError {
    DuckValueError {
        kind: DuckValueErrorKind::Unrepresentable,
        value_type: value_type.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use duckdb::types::{TimeUnit, Value as DuckValue};
    use serde_json::{Value, json};
    use verify_engine::scalar::ScalarCategory;

    use super::{DuckValueErrorKind, duckdb_type_category, duckdb_value_to_protocol};

    #[test]
    fn converts_every_protocol_scalar_family_without_coercion() {
        for (input, expected) in [
            (DuckValue::Null, Value::Null),
            (DuckValue::Boolean(true), json!(true)),
            (DuckValue::TinyInt(-1), json!(-1)),
            (DuckValue::BigInt(42), json!(42)),
            (DuckValue::UBigInt(u64::MAX), json!(u64::MAX)),
            (DuckValue::Float(1.5), json!(1.5)),
            (DuckValue::Double(2.5), json!(2.5)),
            (DuckValue::Text("01".to_owned()), json!("01")),
            (DuckValue::Enum("open".to_owned()), json!("open")),
        ] {
            assert_eq!(
                duckdb_value_to_protocol(input).expect("value should be representable"),
                expected
            );
        }
    }

    #[test]
    fn rejects_non_finite_engine_specific_and_nested_values() {
        let cases = [
            (
                DuckValue::Double(f64::NAN),
                DuckValueErrorKind::Unrepresentable,
            ),
            (
                DuckValue::Timestamp(TimeUnit::Second, 0),
                DuckValueErrorKind::Unrepresentable,
            ),
            (
                DuckValue::List(vec![DuckValue::Int(1)]),
                DuckValueErrorKind::NonScalar,
            ),
        ];

        for (value, expected_kind) in cases {
            let error = duckdb_value_to_protocol(value).expect_err("value should fail closed");
            assert_eq!(error.kind, expected_kind);
        }
    }

    #[test]
    fn describe_type_classification_is_protocol_shaped() {
        for (data_type, expected) in [
            ("BIGINT", ScalarCategory::Number),
            ("DECIMAL(18,2)", ScalarCategory::Number),
            ("BOOLEAN", ScalarCategory::Boolean),
            ("VARCHAR", ScalarCategory::String),
            ("NULL", ScalarCategory::Null),
        ] {
            assert_eq!(
                duckdb_type_category(data_type).expect("type should classify"),
                expected
            );
        }

        assert_eq!(
            duckdb_type_category("INTEGER[]")
                .expect_err("list type should fail")
                .kind,
            DuckValueErrorKind::NonScalar
        );
        assert_eq!(
            duckdb_type_category("DATE")
                .expect_err("date should fail")
                .kind,
            DuckValueErrorKind::Unrepresentable
        );
    }
}
