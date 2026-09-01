use std::cmp::Ordering;

use serde_json::Value;

/// Scalar categories admitted by the verify predicate protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarCategory {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl ScalarCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        }
    }

    pub const fn is_protocol_scalar(self) -> bool {
        matches!(
            self,
            Self::Null | Self::Boolean | Self::Number | Self::String
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarComparisonError {
    pub left_type: ScalarCategory,
    pub right_type: ScalarCategory,
}

pub const fn value_category(value: &Value) -> ScalarCategory {
    match value {
        Value::Null => ScalarCategory::Null,
        Value::Bool(_) => ScalarCategory::Boolean,
        Value::Number(_) => ScalarCategory::Number,
        Value::String(_) => ScalarCategory::String,
        Value::Array(_) => ScalarCategory::Array,
        Value::Object(_) => ScalarCategory::Object,
    }
}

pub const fn value_type(value: &Value) -> &'static str {
    value_category(value).as_str()
}

/// Protocol equality. Null equals null, null differs from every other scalar,
/// and non-null values compare only within their scalar category.
pub fn values_equal(left: &Value, right: &Value) -> Result<bool, ScalarComparisonError> {
    match (left, right) {
        (Value::Number(left_number), Value::Number(right_number)) => left_number
            .as_f64()
            .zip(right_number.as_f64())
            .map(|(left, right)| left == right)
            .ok_or_else(|| comparison_error(left, right)),
        (Value::String(left), Value::String(right)) => Ok(left == right),
        (Value::Bool(left), Value::Bool(right)) => Ok(left == right),
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Err(comparison_error(left, right)),
    }
}

/// Protocol ordering. Only non-null values in the same scalar category have an
/// ordering; numbers use the same total f64 ordering as the original portable
/// evaluator.
pub fn compare_values(left: &Value, right: &Value) -> Result<Ordering, ScalarComparisonError> {
    match (left, right) {
        (Value::Number(left_number), Value::Number(right_number)) => left_number
            .as_f64()
            .zip(right_number.as_f64())
            .map(|(left, right)| left.total_cmp(&right))
            .ok_or_else(|| comparison_error(left, right)),
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        _ => Err(comparison_error(left, right)),
    }
}

/// V0 missingness: null, empty string, and whitespace-only string are blank.
pub fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.is_empty() || value.chars().all(char::is_whitespace),
        _ => false,
    }
}

fn comparison_error(left: &Value, right: &Value) -> ScalarComparisonError {
    ScalarComparisonError {
        left_type: value_category(left),
        right_type: value_category(right),
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use serde_json::{Value, json};

    use super::{
        ScalarCategory, compare_values, is_blank, value_category, value_type, values_equal,
    };

    #[test]
    fn classifies_every_json_value_category() {
        for (value, expected) in [
            (Value::Null, ScalarCategory::Null),
            (json!(true), ScalarCategory::Boolean),
            (json!(1.5), ScalarCategory::Number),
            (json!("value"), ScalarCategory::String),
            (json!([]), ScalarCategory::Array),
            (json!({}), ScalarCategory::Object),
        ] {
            assert_eq!(value_category(&value), expected);
            assert_eq!(value_type(&value), expected.as_str());
        }
    }

    #[test]
    fn equality_truth_table_covers_every_category_pair() {
        let representatives = [
            Value::Null,
            json!(true),
            json!(1),
            json!("1"),
            json!([]),
            json!({}),
        ];

        for left in &representatives {
            for right in &representatives {
                let left_category = value_category(left);
                let right_category = value_category(right);
                let result = values_equal(left, right);
                let defined = (left_category == right_category
                    && left_category.is_protocol_scalar())
                    || left_category == ScalarCategory::Null
                    || right_category == ScalarCategory::Null;
                assert_eq!(
                    result.is_ok(),
                    defined,
                    "unexpected equality result for {left_category:?}/{right_category:?}"
                );
            }
        }

        assert!(values_equal(&Value::Null, &Value::Null).expect("null equality is defined"));
        assert!(!values_equal(&Value::Null, &json!(0)).expect("mixed null equality is defined"));
        assert!(values_equal(&json!(1), &json!(1.0)).expect("numeric equality is defined"));
    }

    #[test]
    fn ordering_truth_table_rejects_null_and_mixed_categories() {
        let representatives = [Value::Null, json!(false), json!(1), json!("1")];

        for left in &representatives {
            for right in &representatives {
                let left_category = value_category(left);
                let right_category = value_category(right);
                let defined = left_category == right_category
                    && matches!(
                        left_category,
                        ScalarCategory::Boolean | ScalarCategory::Number | ScalarCategory::String
                    );
                assert_eq!(
                    compare_values(left, right).is_ok(),
                    defined,
                    "unexpected ordering result for {left_category:?}/{right_category:?}"
                );
            }
        }

        assert_eq!(
            compare_values(&json!(1), &json!(2)).expect("numbers are ordered"),
            Ordering::Less
        );
    }

    #[test]
    fn blank_semantics_match_the_portable_contract() {
        assert!(is_blank(&Value::Null));
        assert!(is_blank(&json!("")));
        assert!(is_blank(&json!(" \t\n")));
        assert!(!is_blank(&json!(false)));
        assert!(!is_blank(&json!(0)));
        assert!(!is_blank(&json!("present")));
    }
}
