//! Reads query results without letting duckdb-rs build a DECIMAL it cannot hold.
//!
//! duckdb-rs 1.4.4 turns every DECIMAL with a scale into `rust_decimal`, which
//! panics beyond 96 bits or scale 28; the release binary then aborts with no
//! report. Here cells come from the Arrow arrays DuckDB returns. A cell holding no
//! such value is converted by duckdb-rs, as `Row::get` would convert it. Only the
//! others are handled here, from the stored integer. The statement itself is
//! prepared and executed unchanged.

use std::sync::Arc;

use duckdb::{
    Params, Statement,
    arrow::{
        array::{
            Array, ArrayRef, Decimal128Array, FixedSizeListArray, LargeListArray, ListArray,
            MapArray, StructArray, UnionArray,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
        record_batch::RecordBatch,
    },
    types::{ListType, Value as DuckValue, ValueRef},
};
use serde_json::Number;

const MAX_SCALE: u32 = 28;
const MAX_MAGNITUDE: u128 = (1 << 96) - 1;

#[derive(Debug, Clone)]
pub enum Cell {
    /// Converted by duckdb-rs.
    Value(DuckValue),
    /// A DECIMAL beyond the limits.
    WideDecimal(WideDecimal),
    /// A non-NULL LIST, STRUCT, ARRAY, MAP or UNION holding such a DECIMAL.
    WideNested(WideNested),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WideDecimal {
    unscaled: i128,
    /// At least 1: DuckDB's scale-0 decimals arrive as integers.
    scale: u32,
}

impl WideDecimal {
    /// Every stored digit, with exactly `scale` of them after the point.
    pub fn text(&self) -> String {
        let scale = self.scale as usize;
        let digits = format!(
            "{:0>width$}",
            self.unscaled.unsigned_abs(),
            width = scale + 1
        );
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        let sign = if self.unscaled < 0 { "-" } else { "" };
        format!("{sign}{whole}.{fraction}")
    }

    /// The nearest `f64`, only if no digit is lost: the shortest text a report
    /// prints for it is this decimal. An interim limit until numbers are
    /// compared exactly.
    pub fn lossless_number(&self) -> Option<Number> {
        let text = self.text();
        let number = Number::from_f64(text.parse().ok()?)?;
        // Judged on the text the report will print. One double can have two
        // equally short texts, and formatters differ in which one they choose.
        (decimal_parts(&number.to_string())? == decimal_parts(&text)?).then_some(number)
    }
}

/// `(negative, significant digits, power of ten of the last digit)` of a decimal
/// text, with or without an exponent. Zero has no sign and no digits.
pub(crate) fn decimal_parts(text: &str) -> Option<(bool, String, i64)> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let kept = digits.trim_end_matches('0');
    let power = exponent.parse::<i64>().ok()? - i64::try_from(fraction.len()).ok()?
        + i64::try_from(digits.len() - kept.len()).ok()?;
    let significant = kept.trim_start_matches('0');
    if significant.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((negative, significant.to_owned(), power))
}

#[derive(Debug, Clone)]
pub struct WideNested {
    column: ArrayRef,
    row: usize,
}

impl WideNested {
    /// The kind name used for the same column type inside the limits.
    pub fn kind(&self) -> &'static str {
        match self.column.data_type() {
            DataType::Struct(_) => "struct",
            DataType::FixedSizeList(..) => "array",
            DataType::Map(..) => "map",
            DataType::Union(..) => "union",
            _ => "list",
        }
    }

    /// The text a query result shows for a nested value, with every digit of
    /// each DECIMAL.
    pub fn debug_text(&self) -> String {
        debug_text(&self.column, self.row)
    }
}

/// A result being read. `names` are the result's column names.
pub struct CellRows<'statement> {
    pub names: Vec<String>,
    pub rows: Box<dyn Iterator<Item = Vec<Cell>> + 'statement>,
}

/// Execute `statement` and read its result cell by cell.
pub fn query_cells<'statement, P: Params>(
    statement: &'statement mut Statement<'_>,
    params: P,
) -> duckdb::Result<CellRows<'statement>> {
    let batches = statement.query_arrow(params)?;
    let names = batches
        .get_schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok(CellRows {
        names,
        rows: Box::new(batches.flat_map(batch_rows)),
    })
}

fn batch_rows(batch: RecordBatch) -> impl Iterator<Item = Vec<Cell>> {
    let mut columns = batch
        .columns()
        .iter()
        .map(|column| column_cells(column).into_iter())
        .collect::<Vec<_>>();
    // Every column of a batch has `num_rows` cells.
    (0..batch.num_rows()).map(move |_| columns.iter_mut().filter_map(Iterator::next).collect())
}

fn column_cells(column: &ArrayRef) -> Vec<Cell> {
    if !type_can_be_wide(column.data_type()) {
        return library_values(column, 0, column.len())
            .into_iter()
            .map(Cell::Value)
            .collect();
    }
    (0..column.len())
        .map(|row| {
            if !holds_wide(column, row) {
                Cell::Value(library_value(column, row))
            } else if let Some(decimal) = wide_scalar(column, row) {
                Cell::WideDecimal(decimal)
            } else {
                Cell::WideNested(WideNested {
                    column: Arc::clone(column),
                    row,
                })
            }
        })
        .collect()
}

/// Rows `start..end` of `column`, converted by duckdb-rs. Its per-cell conversion
/// is private; converting a list applies it to every element.
fn library_values(column: &ArrayRef, start: usize, end: usize) -> Vec<DuckValue> {
    let offset = |row: usize| i32::try_from(row).expect("a DuckDB batch fits i32 offsets");
    let list = ListArray::new(
        Arc::new(Field::new("item", column.data_type().clone(), true)),
        OffsetBuffer::new(vec![offset(start), offset(end)].into()),
        Arc::clone(column),
        None,
    );
    match DuckValue::from(ValueRef::List(ListType::Regular(&list), 0)) {
        DuckValue::List(values) => values,
        other => unreachable!("a list converts to a list, not {other:?}"),
    }
}

fn library_value(column: &ArrayRef, row: usize) -> DuckValue {
    library_values(column, row, row + 1).swap_remove(0)
}

fn type_can_be_wide(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(precision, scale) => {
            // Up to 28 digits fit 96 bits.
            *scale > 0 && (u32::from(*precision) > MAX_SCALE || i32::from(*scale) > 28)
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => type_can_be_wide(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| type_can_be_wide(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| type_can_be_wide(field.data_type())),
        _ => false,
    }
}

fn wide_scalar(column: &ArrayRef, row: usize) -> Option<WideDecimal> {
    let DataType::Decimal128(_, scale) = column.data_type() else {
        return None;
    };
    let scale = u32::try_from(*scale).ok().filter(|scale| *scale > 0)?;
    let unscaled = column
        .as_any()
        .downcast_ref::<Decimal128Array>()?
        .value(row);
    (scale > MAX_SCALE || unscaled.unsigned_abs() > MAX_MAGNITUDE)
        .then_some(WideDecimal { unscaled, scale })
}

/// The child cells duckdb-rs would convert for one nested cell: the column they
/// are in and their row.
fn children(column: &ArrayRef, row: usize) -> Vec<(ArrayRef, usize)> {
    let any = column.as_any();
    let span = |values: &ArrayRef, start: usize, end: usize| {
        (start..end)
            .map(|child| (Arc::clone(values), child))
            .collect::<Vec<_>>()
    };
    let index = |offset: i64| usize::try_from(offset).expect("a non-negative offset");
    if let Some(list) = any.downcast_ref::<ListArray>() {
        let offsets = list.value_offsets();
        span(
            list.values(),
            index(offsets[row].into()),
            index(offsets[row + 1].into()),
        )
    } else if let Some(list) = any.downcast_ref::<LargeListArray>() {
        let offsets = list.value_offsets();
        span(list.values(), index(offsets[row]), index(offsets[row + 1]))
    } else if let Some(array) = any.downcast_ref::<FixedSizeListArray>() {
        let length = index(array.value_length().into());
        span(array.values(), row * length, (row + 1) * length)
    } else if let Some(fields) = any.downcast_ref::<StructArray>() {
        fields
            .columns()
            .iter()
            .map(|field| (Arc::clone(field), row))
            .collect()
    } else if let Some(map) = any.downcast_ref::<MapArray>() {
        let offsets = map.value_offsets();
        (index(offsets[row].into())..index(offsets[row + 1].into()))
            .flat_map(|entry| {
                [
                    (Arc::clone(map.keys()), entry),
                    (Arc::clone(map.values()), entry),
                ]
            })
            .collect()
    } else if let Some(union) = any.downcast_ref::<UnionArray>() {
        vec![(Arc::clone(union.child(union.type_id(row))), row)]
    } else {
        Vec::new()
    }
}

fn holds_wide(column: &ArrayRef, row: usize) -> bool {
    if column.is_null(row) || !type_can_be_wide(column.data_type()) {
        return false;
    }
    wide_scalar(column, row).is_some()
        || children(column, row)
            .iter()
            .any(|(child, child_row)| holds_wide(child, *child_row))
}

/// The layout of `format!("{value:?}")` for a duckdb-rs value, which is what a
/// query result shows for nested values.
fn debug_text(column: &ArrayRef, row: usize) -> String {
    if !holds_wide(column, row) {
        return format!("{:?}", library_value(column, row));
    }
    if let Some(decimal) = wide_scalar(column, row) {
        return format!("Decimal({})", decimal.text());
    }
    let parts = children(column, row)
        .iter()
        .map(|(child, child_row)| debug_text(child, *child_row))
        .collect::<Vec<_>>();
    match column.data_type() {
        DataType::Struct(fields) => {
            let entries = fields
                .iter()
                .zip(&parts)
                .map(|(field, part)| format!("({:?}, {part})", field.name()))
                .collect::<Vec<_>>();
            format!("Struct(OrderedMap([{}]))", entries.join(", "))
        }
        DataType::Map(..) => {
            let entries = parts
                .chunks(2)
                .map(|pair| format!("({})", pair.join(", ")))
                .collect::<Vec<_>>();
            format!("Map(OrderedMap([{}]))", entries.join(", "))
        }
        DataType::FixedSizeList(..) => format!("Array([{}])", parts.join(", ")),
        DataType::Union(..) => format!("Union({})", parts.join(", ")),
        _ => format!("List([{}])", parts.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use duckdb::{Connection, types::Value as DuckValue};

    use super::{Cell, query_cells};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// A column whose type could hold a wide DECIMAL, with values inside the
    /// limits and every kind duckdb-rs nests, reads exactly as `Row::get` reads it.
    #[test]
    fn cells_inside_the_limits_are_what_row_get_returns() -> TestResult {
        let connection = Connection::open_in_memory().expect("DuckDB");
        let d = "CAST('-0.5' AS DECIMAL(38,28))";
        let sql = format!(
            "SELECT * FROM (VALUES \
             (1, 'a', 1.5::DOUBLE, {d}, [{d}, NULL], {{'a': {d}, 'b': 'x'}}, \
              CAST([{d}] AS DECIMAL(38,28)[1]), map {{{d}: 'k'}}, DATE '2024-01-02', \
              CAST(union_value(n := {d}) AS UNION(n DECIMAL(38,28), s VARCHAR)), \
              CAST('100.25' AS DECIMAL(38,10)), CAST('.5' AS DECIMAL(1,1))), \
             (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
             (3, '', 0.0, CAST(0 AS DECIMAL(38,28)), [], {{'a': NULL, 'b': NULL}}, \
              CAST([NULL] AS DECIMAL(38,28)[1]), map {{}}, NULL, \
              CAST(union_value(s := 'y') AS UNION(n DECIMAL(38,28), s VARCHAR)), \
              CAST('-0.5' AS DECIMAL(38,10)), NULL) \
             ) AS t"
        );
        let mut by_row = connection.prepare(&sql).expect("prepare");
        let mut rows = by_row.query([]).expect("query");
        let mut expected = Vec::new();
        while let Some(row) = rows.next().expect("row") {
            expected.push(
                (0..12)
                    .map(|index| format!("{:?}", row.get::<_, DuckValue>(index).expect("value")))
                    .collect::<Vec<_>>(),
            );
        }
        let mut by_cell = connection.prepare(&sql).expect("prepare");
        let actual = query_cells(&mut by_cell, [])
            .expect("query")
            .rows
            .map(|row| {
                row.into_iter()
                    .map(|cell| match cell {
                        Cell::Value(value) => Ok(format!("{value:?}")),
                        other => Err(format!("{other:?} is inside the limits")),
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 3);
        Ok(())
    }

    /// Beyond the limits the text is put together here. It must be the library's
    /// layout for the same shape inside them, differing only in the digits.
    #[test]
    fn nested_text_beyond_the_limits_has_the_library_layout() -> TestResult {
        let connection = Connection::open_in_memory().expect("DuckDB");
        let shapes = [
            "[$v, NULL, $v]",
            "{'a \"q\"': $v, 'b': 'x', 'c': NULL}",
            "CAST([$v, $v] AS $t[2])",
            "map {'k': $v, 'j': NULL}",
            "map {$v: 'v'}",
            "CAST(union_value(n := $v) AS UNION(n $t, s VARCHAR))",
            "{'a': [map {'k': [$v]}], 'when': DATE '2024-01-02'}",
        ];
        let read = |shape: &str, scale: u32| {
            let kind = format!("DECIMAL(38,{scale})");
            let value = format!("CAST('-0.5' AS {kind})");
            let sql = format!(
                "SELECT {}",
                shape.replace("$v", &value).replace("$t", &kind)
            );
            let mut statement = connection.prepare(&sql).expect(&sql);
            let mut rows = query_cells(&mut statement, []).expect(&sql).rows;
            rows.next().expect("a row").swap_remove(0)
        };
        let digits = |scale: usize| format!("-0.5{}", "0".repeat(scale - 1));
        for shape in shapes {
            let Cell::Value(inside) = read(shape, 28) else {
                return Err(format!("{shape}: scale 28 is inside the limits").into());
            };
            let Cell::WideNested(beyond) = read(shape, 30) else {
                return Err(format!("{shape}: scale 30 is beyond the limits").into());
            };
            let expected = format!("{inside:?}");
            assert!(expected.contains(&digits(28)), "{shape}: {expected}");
            assert_eq!(
                beyond.debug_text().replace(&digits(30), &digits(28)),
                expected,
                "{shape}"
            );
        }
        Ok(())
    }

    /// Many batches, with wide and narrow cells mixed, stay in row order.
    #[test]
    fn rows_stay_aligned_across_batches() -> TestResult {
        let connection = Connection::open_in_memory().expect("DuckDB");
        let mut statement = connection
            .prepare(
                "SELECT i, CASE WHEN i % 7 = 0 THEN CAST('9999999999999999999999999999.5' AS DECIMAL(38,1)) \
                 ELSE CAST(i AS DECIMAL(38,1)) END FROM range(10000) t(i) ORDER BY i",
            )
            .expect("prepare");
        let mut count = 0_i64;
        for row in query_cells(&mut statement, []).expect("query").rows {
            let [Cell::Value(DuckValue::BigInt(i)), second] = row.as_slice() else {
                return Err(format!("unexpected row {row:?}").into());
            };
            assert_eq!(*i, count);
            match second {
                Cell::WideDecimal(wide) if i % 7 == 0 => {
                    assert_eq!(wide.text(), "9999999999999999999999999999.5");
                    assert!(wide.lossless_number().is_none());
                }
                Cell::Value(DuckValue::Decimal(value)) if i % 7 != 0 => {
                    assert_eq!(value.to_string(), format!("{i}.0"));
                }
                other => return Err(format!("row {i}: {other:?}").into()),
            }
            count += 1;
        }
        assert_eq!(count, 10000);
        Ok(())
    }
}
