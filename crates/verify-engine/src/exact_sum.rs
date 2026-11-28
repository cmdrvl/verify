//! Exact decimal sums for `aggregate_compare` `sum` and `avg`, so that no row order can change
//! a verdict.
//!
//! A received integer is itself. A received binary float means the shortest decimal a report
//! prints for it, so `0.1` is one tenth. Addition, and comparison against `threshold x rows`,
//! are exact; only a reported value that no `f64` holds is rounded.

use std::cmp::Ordering;

use serde_json::{Number, Value};

/// Floats of this magnitude or more may be integers that loading already rounded, so an exact
/// sum could pass something false. Interim, until numbers are loaded exactly (bd-1t5).
const FLOAT_LIMIT: f64 = 9_007_199_254_740_992.0;

/// Significant digits carried into a rounded quotient before the sticky digit.
const QUOTIENT_DIGITS: usize = 40;

const BASE: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unreadable {
    NotANumber,
    WideFloat,
}

/// One received number: `(-1)^negative x coefficient x 10^exponent`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Term {
    negative: bool,
    coefficient: u64,
    exponent: i32,
}

impl Term {
    pub(crate) fn read(number: &Number) -> Result<Self, Unreadable> {
        if let Some(value) = number.as_u64() {
            return Ok(Self::integer(false, value));
        }
        if let Some(value) = number.as_i64() {
            return Ok(Self::integer(value < 0, value.unsigned_abs()));
        }
        let float = number.as_f64().ok_or(Unreadable::NotANumber)?;
        Self::float(float)
    }

    pub(crate) fn float(float: f64) -> Result<Self, Unreadable> {
        if !float.is_finite() {
            return Err(Unreadable::NotANumber);
        }
        if float.abs() >= FLOAT_LIMIT {
            return Err(Unreadable::WideFloat);
        }
        // The text a report prints for this float: its shortest round-trip decimal.
        let text = Number::from_f64(float)
            .ok_or(Unreadable::NotANumber)?
            .to_string();
        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.as_str()),
        };
        let (mantissa, power) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, power)) => (
                mantissa,
                power.parse::<i32>().map_err(|_| Unreadable::NotANumber)?,
            ),
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let fraction = fraction.trim_end_matches('0');
        let coefficient = format!("{whole}{fraction}")
            .parse::<u64>()
            .map_err(|_| Unreadable::NotANumber)?;
        let places = i32::try_from(fraction.len()).map_err(|_| Unreadable::NotANumber)?;
        Ok(Self {
            negative,
            coefficient,
            exponent: power - places,
        })
    }

    fn integer(negative: bool, coefficient: u64) -> Self {
        Self {
            negative,
            coefficient,
            exponent: 0,
        }
    }
}

/// A non-negative integer of any size, in base 10^9, least significant limb first, with no
/// leading zero limb.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Magnitude(Vec<u32>);

impl Magnitude {
    /// `coefficient x 10^shift`.
    fn scaled(coefficient: u64, shift: u32) -> Self {
        let mut limbs = Self::default();
        let mut rest = coefficient;
        while rest > 0 {
            limbs.0.push((rest % BASE) as u32);
            rest /= BASE;
        }
        if !limbs.0.is_empty() {
            limbs.multiply(10_u64.pow(shift % 9));
            limbs
                .0
                .splice(0..0, std::iter::repeat_n(0, (shift / 9) as usize));
        }
        limbs
    }

    fn multiply(&mut self, factor: u64) {
        let mut carry: u128 = 0;
        for limb in &mut self.0 {
            let product = u128::from(*limb) * u128::from(factor) + carry;
            *limb = (product % u128::from(BASE)) as u32;
            carry = product / u128::from(BASE);
        }
        while carry > 0 {
            self.0.push((carry % u128::from(BASE)) as u32);
            carry /= u128::from(BASE);
        }
        self.trim();
    }

    fn add(&mut self, other: &Self) {
        let mut carry = 0_u64;
        for index in 0..self.0.len().max(other.0.len()) {
            if index == self.0.len() {
                self.0.push(0);
            }
            let total = u64::from(self.0[index])
                + u64::from(other.0.get(index).copied().unwrap_or(0))
                + carry;
            self.0[index] = (total % BASE) as u32;
            carry = total / BASE;
        }
        if carry > 0 {
            self.0.push(carry as u32);
        }
    }

    /// `self - other`, for `self >= other`.
    fn subtract(&mut self, other: &Self) {
        let mut borrow = 0_i64;
        for index in 0..self.0.len() {
            let mut total = i64::from(self.0[index])
                - i64::from(other.0.get(index).copied().unwrap_or(0))
                - borrow;
            borrow = i64::from(total < 0);
            if total < 0 {
                total += BASE as i64;
            }
            self.0[index] = total as u32;
        }
        self.trim();
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn compare(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }

    fn digits(&self) -> String {
        let Some((top, rest)) = self.0.split_last() else {
            return "0".to_owned();
        };
        let mut text = top.to_string();
        for limb in rest.iter().rev() {
            text.push_str(&format!("{limb:09}"));
        }
        text
    }
}

/// An exact decimal: `(-1)^negative x magnitude x 10^exponent`. Zero is never negative.
#[derive(Debug, Clone)]
pub(crate) struct Exact {
    negative: bool,
    magnitude: Magnitude,
    exponent: i32,
}

impl Exact {
    /// The sum of `terms`. Positive and negative terms are totalled apart and subtracted once,
    /// so the order of the terms cannot matter.
    pub(crate) fn sum(terms: &[Term]) -> Self {
        let exponent = terms.iter().map(|term| term.exponent).min().unwrap_or(0);
        let mut positive = Magnitude::default();
        let mut negative = Magnitude::default();
        for term in terms {
            let shift = term.exponent.abs_diff(exponent);
            let value = Magnitude::scaled(term.coefficient, shift);
            if term.negative {
                negative.add(&value);
            } else {
                positive.add(&value);
            }
        }
        Self::difference(positive, negative, exponent)
    }

    fn difference(mut left: Magnitude, mut right: Magnitude, exponent: i32) -> Self {
        let negative = left.compare(&right) == Ordering::Less;
        let magnitude = if negative {
            right.subtract(&left);
            right
        } else {
            left.subtract(&right);
            left
        };
        Self {
            negative,
            magnitude,
            exponent,
        }
    }

    pub(crate) fn times(mut self, factor: u64) -> Self {
        self.magnitude.multiply(factor);
        self.negative &= !self.magnitude.0.is_empty();
        self
    }

    /// The magnitude written at the smaller exponent `exponent`.
    fn at(&self, exponent: i32) -> Magnitude {
        let mut value = self.magnitude.clone();
        if !value.0.is_empty() {
            let shift = self.exponent.abs_diff(exponent);
            value.multiply(10_u64.pow(shift % 9));
            value
                .0
                .splice(0..0, std::iter::repeat_n(0, (shift / 9) as usize));
        }
        value
    }

    pub(crate) fn compare(&self, other: &Self) -> Ordering {
        let exponent = self.exponent.min(other.exponent);
        match (self.negative, other.negative) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.at(exponent).compare(&other.at(exponent)),
            (true, true) => other.at(exponent).compare(&self.at(exponent)),
        }
    }

    /// `|self - other| <= bound`, for a bound of zero or more.
    pub(crate) fn within(&self, other: &Self, bound: &Self) -> bool {
        let exponent = self.exponent.min(other.exponent);
        let (mut left, right) = (self.at(exponent), other.at(exponent));
        let distance = if self.negative == other.negative {
            Self::difference(left, right, exponent)
        } else {
            left.add(&right);
            Self {
                negative: false,
                magnitude: left,
                exponent,
            }
        };
        Self {
            negative: false,
            ..distance
        }
        .compare(bound)
            != Ordering::Greater
    }

    /// `[-]digits e exponent`, which `f64` parsing rounds correctly.
    fn scientific(&self, digits: &str, exponent: i64) -> String {
        let sign = if self.negative { "-" } else { "" };
        format!("{sign}{digits}e{exponent}")
    }

    /// The value a report shows for this sum: the `f64` when it prints as exactly this
    /// decimal, else the integer when it is one that fits, else the nearest `f64`.
    pub(crate) fn reported(&self) -> Option<Value> {
        let digits = self.magnitude.digits();
        let nearest: f64 = self
            .scientific(&digits, i64::from(self.exponent))
            .parse()
            .ok()?;
        let held = Term::float(nearest)
            .is_ok_and(|term| Self::sum(&[term]).compare(self) == Ordering::Equal);
        if !held && let Some(integer) = self.integer() {
            return Some(integer);
        }
        Number::from_f64(nearest).map(Value::Number)
    }

    fn integer(&self) -> Option<Value> {
        let mut digits = self.magnitude.digits();
        if self.exponent >= 0 {
            digits.extend(std::iter::repeat_n('0', self.exponent as usize));
        } else {
            let places = self.exponent.unsigned_abs() as usize;
            let keep = digits.len().checked_sub(places)?;
            if digits[keep..].bytes().any(|digit| digit != b'0') {
                return None;
            }
            digits.truncate(keep);
        }
        let sign = if self.negative { "-" } else { "" };
        let text = format!("{sign}{digits}");
        text.parse::<u64>()
            .map(Value::from)
            .or_else(|_| text.parse::<i64>().map(Value::from))
            .ok()
    }

    /// The `f64` nearest `self / divisor`, for a report. Verdicts never use it.
    pub(crate) fn reported_quotient(&self, divisor: u64) -> Option<Value> {
        let divisor = u128::from(divisor.max(1));
        let dividend = self.magnitude.digits();
        let mut incoming = dividend.bytes().map(|digit| u128::from(digit - b'0'));
        let mut quotient = String::new();
        let mut remainder = 0_u128;
        let mut exponent = i64::from(self.exponent);
        for digit in &mut incoming {
            remainder = remainder * 10 + digit;
            if !quotient.is_empty() || remainder >= divisor {
                quotient.push(char::from(b'0' + (remainder / divisor) as u8));
            }
            remainder %= divisor;
        }
        while remainder > 0 && quotient.len() < QUOTIENT_DIGITS {
            remainder *= 10;
            if !quotient.is_empty() || remainder >= divisor {
                quotient.push(char::from(b'0' + (remainder / divisor) as u8));
            }
            remainder %= divisor;
            exponent -= 1;
        }
        // A sticky digit keeps rounding right when digits were left over.
        if remainder > 0 {
            quotient.push('1');
            exponent -= 1;
        }
        if quotient.is_empty() {
            quotient.push('0');
        }
        let nearest: f64 = self.scientific(&quotient, exponent).parse().ok()?;
        Number::from_f64(nearest).map(Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use serde_json::{Number, json};

    use super::{Exact, Term};

    fn term(value: serde_json::Value) -> Term {
        let number = value.as_number().expect("tests pass numbers only");
        Term::read(number).expect("a readable number")
    }

    /// Checked against `i128`, which holds these sums exactly.
    #[test]
    fn integer_sums_match_i128() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        for _ in 0..500 {
            let mut terms = Vec::new();
            let mut expected = 0_i128;
            for _ in 0..8 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let value = state as i64 >> (state % 50);
                expected += i128::from(value);
                terms.push(Term::read(&Number::from(value)).expect("integer"));
            }
            let sum = Exact::sum(&terms);
            let text = format!(
                "{}{}",
                if sum.negative { "-" } else { "" },
                sum.magnitude.digits()
            );
            assert_eq!(text, expected.to_string());
        }
    }

    #[test]
    fn far_apart_exponents_stay_exact() {
        let tiny = term(json!(5e-324));
        let one = term(json!(1.0));
        let above = Exact::sum(&[one, tiny]);
        assert_eq!(above.compare(&Exact::sum(&[one])), Ordering::Greater);
        let back = Exact::sum(&[one, tiny, term(json!(-5e-324))]);
        assert_eq!(back.compare(&Exact::sum(&[one])), Ordering::Equal);
    }
}
