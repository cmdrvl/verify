//! Pure-Rust grounding checks for claims cited against pinned document bytes.
//!
//! This module deliberately accepts documents in memory. Filesystem loading and
//! CLI rendering stay outside the engine so the same checks remain wasm-clean.

use std::collections::{BTreeMap, BTreeSet};

use cmdrvl_core::hash::{self, Algorithm};
use serde::Deserialize;
use serde_json::{Number, Value, json};
use verify_core::{
    constraint::{BindingKind, Severity},
    order::sort_report,
    report::{
        AffectedEntry, BindingReport, ExecutionMode, InputVerification, Outcome, ResultStatus,
        RuleResult, VerifyReport,
    },
};

pub const GROUNDING_CONSTRAINT_SET: &str = "grounding.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteSpan {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericClaim {
    pub value: Number,
    pub scale: Scale,
    pub unit: Option<String>,
    target: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroundingClaim {
    pub claim_id: String,
    pub source: String,
    pub quote: String,
    pub byte_span: Option<ByteSpan>,
    pub numeric: Option<NumericClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroundingDocument {
    pub path: String,
    pub bytes: Vec<u8>,
}

impl GroundingDocument {
    pub fn new(path: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            path: path.into(),
            bytes,
        }
    }

    pub fn content_hash(&self) -> String {
        sha256_hash(&self.bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroundingError {
    InvalidClaim {
        line: usize,
        detail: String,
    },
    EmptyClaims,
    DuplicateClaimId {
        claim_id: String,
    },
    MissingSource {
        claim_id: String,
        source: String,
    },
    InvalidSpan {
        claim_id: String,
        detail: String,
    },
    AmbiguousQuote {
        claim_id: String,
        match_count: usize,
    },
    Lock(LockError),
}

impl std::fmt::Display for GroundingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClaim { line, detail } => {
                write!(
                    formatter,
                    "invalid grounding claim at line {line}: {detail}"
                )
            }
            Self::EmptyClaims => formatter.write_str("grounding claims input is empty"),
            Self::DuplicateClaimId { claim_id } => {
                write!(formatter, "duplicate grounding claim_id: {claim_id}")
            }
            Self::MissingSource { claim_id, source } => {
                write!(
                    formatter,
                    "claim {claim_id} references unknown source {source}"
                )
            }
            Self::InvalidSpan { claim_id, detail } => {
                write!(
                    formatter,
                    "claim {claim_id} has invalid byte_span: {detail}"
                )
            }
            Self::AmbiguousQuote {
                claim_id,
                match_count,
            } => write!(
                formatter,
                "claim {claim_id} has {match_count} matching normalized quote locations"
            ),
            Self::Lock(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for GroundingError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    Invalid {
        name: String,
        detail: String,
    },
    InputNotLocked {
        source: String,
        locks_checked: Vec<String>,
    },
    InputDrift {
        source: String,
        expected_hash: String,
        observed_hashes: Vec<String>,
    },
}

impl std::fmt::Display for LockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid { name, detail } => write!(formatter, "invalid lock {name}: {detail}"),
            Self::InputNotLocked {
                source,
                locks_checked,
            } => write!(
                formatter,
                "source {source} is not present in provided locks ({})",
                locks_checked.join(", ")
            ),
            Self::InputDrift {
                source,
                expected_hash,
                observed_hashes,
            } => write!(
                formatter,
                "source {source} drifted from {expected_hash}; observed {}",
                observed_hashes.join(", ")
            ),
        }
    }
}

impl std::error::Error for LockError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
    Units,
    Thousands,
    Millions,
    Percent,
    Bps,
}

impl Scale {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "units" | "unit" => Some(Self::Units),
            "thousands" | "thousand" | "k" => Some(Self::Thousands),
            "millions" | "million" | "m" => Some(Self::Millions),
            "percent" | "%" => Some(Self::Percent),
            "bps" | "bp" | "basis_points" => Some(Self::Bps),
            _ => None,
        }
    }

    fn apply(self, value: Decimal) -> Option<Decimal> {
        match self {
            Self::Units => Some(value),
            Self::Thousands => value.multiply(1_000),
            Self::Millions => value.multiply(1_000_000),
            Self::Percent => value.divide_power_of_ten(2),
            Self::Bps => value.divide_power_of_ten(4),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Units => "units",
            Self::Thousands => "thousands",
            Self::Millions => "millions",
            Self::Percent => "percent",
            Self::Bps => "bps",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Decimal {
    coefficient: i128,
    scale: u32,
}

impl Decimal {
    fn new(coefficient: i128, scale: u32) -> Self {
        let mut value = Self { coefficient, scale };
        while value.scale > 0 && value.coefficient % 10 == 0 {
            value.coefficient /= 10;
            value.scale -= 1;
        }
        value
    }

    fn parse(raw: &str) -> Option<Self> {
        let mut text = raw.trim();
        text = text
            .strip_prefix('$')
            .or_else(|| text.strip_prefix('€'))
            .or_else(|| text.strip_prefix('£'))
            .unwrap_or(text);
        text = text.strip_suffix('%').unwrap_or(text);

        let (mantissa, exponent) = match text.find(['e', 'E']) {
            Some(index) => {
                let exponent = text[index + 1..].parse::<i32>().ok()?;
                (&text[..index], exponent)
            }
            None => (text, 0),
        };

        let negative = mantissa.starts_with('-');
        let unsigned = mantissa
            .strip_prefix(['+', '-'])
            .unwrap_or(mantissa)
            .replace(',', "");
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((&unsigned, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        let digits = format!("{whole}{fraction}");
        let mut coefficient = digits.parse::<i128>().ok()?;
        if negative {
            coefficient = coefficient.checked_neg()?;
        }
        let mut scale = u32::try_from(fraction.len()).ok()?;
        if exponent >= 0 {
            let shift = u32::try_from(exponent).ok()?;
            if shift >= scale {
                coefficient = coefficient.checked_mul(pow10(shift - scale)?)?;
                scale = 0;
            } else {
                scale -= shift;
            }
        } else {
            scale = scale.checked_add(exponent.unsigned_abs())?;
        }
        Some(Self::new(coefficient, scale))
    }

    fn multiply(self, factor: i128) -> Option<Self> {
        Some(Self::new(self.coefficient.checked_mul(factor)?, self.scale))
    }

    fn divide_power_of_ten(self, places: u32) -> Option<Self> {
        Some(Self::new(self.coefficient, self.scale.checked_add(places)?))
    }
}

fn pow10(power: u32) -> Option<i128> {
    let mut value = 1_i128;
    for _ in 0..power {
        value = value.checked_mul(10)?;
    }
    Some(value)
}

#[derive(Debug, Deserialize)]
struct RawClaim {
    claim_id: String,
    source: String,
    quote: String,
    #[serde(default)]
    byte_span: Option<RawByteSpan>,
    #[serde(default)]
    value: Option<Number>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    scale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawByteSpan {
    start: usize,
    end: usize,
}

/// Parse the grounding JSONL without echoing a source line in any error.
pub fn parse_claims_jsonl(input: &[u8]) -> Result<Vec<GroundingClaim>, GroundingError> {
    let mut claims = Vec::new();
    let mut seen = BTreeSet::new();

    for (line_index, raw_line) in input.split(|byte| *byte == b'\n').enumerate() {
        let line = line_index + 1;
        if raw_line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let text = std::str::from_utf8(raw_line).map_err(|_| GroundingError::InvalidClaim {
            line,
            detail: "claim line is not valid UTF-8".to_owned(),
        })?;
        let raw: RawClaim =
            serde_json::from_str(text).map_err(|error| GroundingError::InvalidClaim {
                line,
                detail: format!("claim JSON does not match the grounding input shape: {error}"),
            })?;
        if raw.claim_id.trim().is_empty() {
            return Err(GroundingError::InvalidClaim {
                line,
                detail: "claim_id must not be empty".to_owned(),
            });
        }
        if !seen.insert(raw.claim_id.clone()) {
            return Err(GroundingError::DuplicateClaimId {
                claim_id: raw.claim_id,
            });
        }
        if raw.source.trim().is_empty() {
            return Err(GroundingError::InvalidClaim {
                line,
                detail: "source must not be empty".to_owned(),
            });
        }
        if raw.quote.is_empty() {
            return Err(GroundingError::InvalidClaim {
                line,
                detail: "quote must not be empty".to_owned(),
            });
        }

        let numeric = match (raw.value, raw.scale) {
            (None, None) if raw.unit.is_none() => None,
            (Some(value), Some(scale)) => {
                let scale = Scale::parse(&scale).ok_or_else(|| GroundingError::InvalidClaim {
                    line,
                    detail: "scale must be units, thousands, millions, percent, or bps".to_owned(),
                })?;
                let target = Decimal::parse(&value.to_string()).ok_or_else(|| {
                    GroundingError::InvalidClaim {
                        line,
                        detail: "value must be a finite decimal JSON number".to_owned(),
                    }
                })?;
                let target = scale
                    .apply(target)
                    .ok_or_else(|| GroundingError::InvalidClaim {
                        line,
                        detail: "value is outside the supported exact decimal range".to_owned(),
                    })?;
                Some(NumericClaim {
                    value,
                    scale,
                    unit: raw.unit,
                    target,
                })
            }
            _ => {
                return Err(GroundingError::InvalidClaim {
                    line,
                    detail: "value, scale, and optional unit must be supplied together".to_owned(),
                });
            }
        };

        claims.push(GroundingClaim {
            claim_id: raw.claim_id,
            source: raw.source,
            quote: raw.quote,
            byte_span: raw.byte_span.map(|span| ByteSpan {
                start: span.start,
                end: span.end,
            }),
            numeric,
        });
    }

    if claims.is_empty() {
        return Err(GroundingError::EmptyClaims);
    }
    Ok(claims)
}

#[derive(Debug, Deserialize)]
struct LockFile {
    version: String,
    members: Vec<LockMember>,
}

#[derive(Debug, Deserialize)]
struct LockMember {
    path: String,
    bytes_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct LockedMember {
    path: String,
    expected_hash: String,
    lock_name: String,
}

/// Verify each in-memory document against the same lock.v0 prefix semantics as
/// the DuckDB batch path, without importing DuckDB into this crate.
///
/// Member matching stays local on purpose (cmd-zjr): `cmdrvl_core::lockfile` refuses inputs
/// this accepts, and its verdict does not say which lock vouched. Only hashing is shared.
pub fn verify_locks(
    lock_files: &[(String, Vec<u8>)],
    documents: &BTreeMap<String, GroundingDocument>,
) -> Result<BTreeMap<String, InputVerification>, GroundingError> {
    let mut members = Vec::new();
    let mut lock_names = Vec::new();
    for (lock_name, bytes) in lock_files {
        let lock: LockFile = serde_json::from_slice(bytes).map_err(|error| {
            GroundingError::Lock(LockError::Invalid {
                name: lock_name.clone(),
                detail: format!("invalid JSON: {error}"),
            })
        })?;
        if lock.version != "lock.v0" {
            return Err(GroundingError::Lock(LockError::Invalid {
                name: lock_name.clone(),
                detail: format!("unsupported version {}; expected lock.v0", lock.version),
            }));
        }
        for member in lock.members {
            let Some(bytes_hash) = member.bytes_hash else {
                return Err(GroundingError::Lock(LockError::Invalid {
                    name: lock_name.clone(),
                    detail: format!("member {} has no bytes_hash", member.path),
                }));
            };
            if !is_supported_hash(&bytes_hash) {
                return Err(GroundingError::Lock(LockError::Invalid {
                    name: lock_name.clone(),
                    detail: format!("member {} has unsupported bytes_hash prefix", member.path),
                }));
            }
            members.push(LockedMember {
                path: normalize_path(&member.path),
                expected_hash: bytes_hash,
                lock_name: lock_name.clone(),
            });
        }
        lock_names.push(lock_name.clone());
    }

    let mut results = BTreeMap::new();
    for (name, document) in documents {
        let normalized = normalize_path(&document.path);
        let candidates = lock_candidates(&members, &normalized);
        if candidates.is_empty() {
            return Err(GroundingError::Lock(LockError::InputNotLocked {
                source: document.path.clone(),
                locks_checked: lock_names.clone(),
            }));
        }

        let mut vouching_locks = Vec::new();
        let mut observed_hashes = Vec::new();
        let mut first_expected = None;
        for member in candidates {
            let observed = hash_bytes(&member.expected_hash, &document.bytes).ok_or_else(|| {
                GroundingError::Lock(LockError::Invalid {
                    name: member.lock_name.clone(),
                    detail: format!("member {} has malformed digest", member.path),
                })
            })?;
            first_expected.get_or_insert_with(|| member.expected_hash.clone());
            if !observed_hashes.contains(&observed) {
                observed_hashes.push(observed.clone());
            }
            if observed == member.expected_hash && !vouching_locks.contains(&member.lock_name) {
                vouching_locks.push(member.lock_name.clone());
            }
        }

        if vouching_locks.is_empty() {
            return Err(GroundingError::Lock(LockError::InputDrift {
                source: document.path.clone(),
                expected_hash: first_expected.expect("candidate set is non-empty"),
                observed_hashes,
            }));
        }
        results.insert(
            name.clone(),
            InputVerification {
                status: verify_core::report::InputVerificationStatus::Verified,
                locks: vouching_locks,
            },
        );
    }
    Ok(results)
}

fn lock_candidates<'a>(members: &'a [LockedMember], normalized: &str) -> Vec<&'a LockedMember> {
    let exact: Vec<_> = members.iter().filter(|m| m.path == normalized).collect();
    if !exact.is_empty() {
        return exact;
    }
    let wanted = path_basename(normalized);
    let candidates: Vec<_> = members
        .iter()
        .filter(|member| path_basename(&member.path) == wanted)
        .collect();
    let distinct_paths: BTreeSet<&str> = candidates
        .iter()
        .map(|member| member.path.as_str())
        .collect();
    if distinct_paths.len() == 1 {
        candidates
    } else {
        Vec::new()
    }
}

fn path_basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or("")
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    let stripped = trimmed.strip_prefix("./").unwrap_or(trimmed);
    stripped
        .trim_end_matches('/')
        .trim_end_matches('\\')
        .to_owned()
}

fn is_supported_hash(hash: &str) -> bool {
    let Some((algorithm, digest)) = hash.split_once(':') else {
        return false;
    };
    let expected_len = match algorithm {
        "sha256" | "blake3" => 64,
        _ => return false,
    };
    digest.len() == expected_len && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `bytes` hashed with the algorithm `expected` names. `None` if unsupported.
fn hash_bytes(expected: &str, bytes: &[u8]) -> Option<String> {
    Algorithm::of(expected).map(|algorithm| hash::hash_bytes(algorithm, bytes))
}

fn sha256_hash(bytes: &[u8]) -> String {
    hash::hash_bytes(Algorithm::Sha256, bytes)
}

/// Evaluate all claims into the existing verify.report.v1 structures.
pub fn evaluate(
    claims: &[GroundingClaim],
    documents: &BTreeMap<String, GroundingDocument>,
    claims_hash: &str,
    lock_results: Option<&BTreeMap<String, InputVerification>>,
) -> Result<VerifyReport, GroundingError> {
    let mut report = VerifyReport::new(ExecutionMode::Batch, GROUNDING_CONSTRAINT_SET, claims_hash);
    for (name, document) in documents {
        report.bindings.insert(
            name.clone(),
            BindingReport {
                kind: BindingKind::Relation,
                source: document.path.clone(),
                content_hash: document.content_hash(),
                input_verification: lock_results.and_then(|results| results.get(name).cloned()),
            },
        );
    }

    for claim in claims {
        let document =
            documents
                .get(&claim.source)
                .ok_or_else(|| GroundingError::MissingSource {
                    claim_id: claim.claim_id.clone(),
                    source: claim.source.clone(),
                })?;
        let resolution = resolve_quote(claim, document)?;
        report
            .results
            .push(quote_result(claim, &resolution, document));
        if let Some(numeric) = &claim.numeric {
            report
                .results
                .push(numeric_result(claim, numeric, &resolution.normalized_text));
        }
    }

    crate::SummaryEngine::apply(&mut report);
    report.outcome = if report.summary.failed_rules > 0 {
        Outcome::Fail
    } else {
        Outcome::Pass
    };
    sort_report(&mut report);
    Ok(report)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawSpan {
    start: usize,
    end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedText {
    text: String,
    origins: Vec<RawSpan>,
}

impl NormalizedText {
    fn raw_span_for_match(&self, start: usize, end: usize) -> Option<ByteSpan> {
        if start >= end || end > self.text.len() {
            return None;
        }
        let start_char = self.text[..start].chars().count();
        let end_char = self.text[..end].chars().count();
        let first = self.origins.get(start_char)?;
        let last = self.origins.get(end_char.checked_sub(1)?)?;
        Some(ByteSpan {
            start: first.start,
            end: last.end,
        })
    }

    fn overlaps_raw_span(&self, span: &ByteSpan) -> bool {
        self.origins
            .iter()
            .any(|origin| origin.start < span.end && span.start < origin.end)
    }
}

#[derive(Debug, Default)]
struct NormalizedBuilder {
    text: String,
    origins: Vec<RawSpan>,
    pending_whitespace: Option<RawSpan>,
}

impl NormalizedBuilder {
    fn push(&mut self, character: char, origin: RawSpan) {
        if character.is_whitespace() {
            self.pending_whitespace = Some(match self.pending_whitespace {
                Some(previous) => RawSpan {
                    start: previous.start,
                    end: origin.end,
                },
                None => origin,
            });
            return;
        }

        if let Some(origin) = self.pending_whitespace.take()
            && !self.text.is_empty()
        {
            self.text.push(' ');
            self.origins.push(origin);
        }
        self.text.push(character);
        self.origins.push(origin);
    }

    fn finish(self) -> NormalizedText {
        NormalizedText {
            text: self.text,
            origins: self.origins,
        }
    }
}

#[derive(Debug, Clone)]
struct QuoteResolution {
    raw_span: Option<ByteSpan>,
    normalized_text: String,
    failure: Option<&'static str>,
}

fn resolve_quote(
    claim: &GroundingClaim,
    document: &GroundingDocument,
) -> Result<QuoteResolution, GroundingError> {
    let target = normalize_whitespace(&claim.quote);
    let normalized_document = normalize_html(&document.bytes);

    if let Some(span) = &claim.byte_span {
        if span.start > span.end || span.end > document.bytes.len() {
            return Err(GroundingError::InvalidSpan {
                claim_id: claim.claim_id.clone(),
                detail: format!(
                    "range {}..{} is outside document length {}",
                    span.start,
                    span.end,
                    document.bytes.len()
                ),
            });
        }
        let raw = &document.bytes[span.start..span.end];
        let normalized_span = normalize_html(raw);
        let html_match = normalized_span.as_ref().is_some_and(|normalized| {
            normalized.text == target
                && normalized_document
                    .as_ref()
                    .is_some_and(|document| document.overlaps_raw_span(span))
        });
        let exact_match = raw == claim.quote.as_bytes()
            && normalized_document
                .as_ref()
                .is_some_and(|document| document.overlaps_raw_span(span));
        if html_match || exact_match {
            return Ok(QuoteResolution {
                raw_span: Some(span.clone()),
                normalized_text: normalized_span
                    .map(|normalized| normalized.text)
                    .unwrap_or_else(|| String::from_utf8_lossy(raw).into_owned()),
                failure: None,
            });
        }

        let failure = normalized_document
            .as_ref()
            .is_some_and(|document| !normalized_matches(document, &target).is_empty());
        return Ok(QuoteResolution {
            raw_span: Some(span.clone()),
            normalized_text: normalized_span
                .map(|normalized| normalized.text)
                .unwrap_or_else(|| String::from_utf8_lossy(raw).into_owned()),
            failure: Some(if failure {
                "E_SPAN_MISMATCH"
            } else {
                "E_QUOTE_NOT_FOUND"
            }),
        });
    }

    let Some(normalized_document) = normalized_document else {
        return Ok(QuoteResolution {
            raw_span: None,
            normalized_text: String::new(),
            failure: Some("E_QUOTE_NOT_FOUND"),
        });
    };
    let matches = normalized_matches(&normalized_document, &target);
    match matches.as_slice() {
        [] => Ok(QuoteResolution {
            raw_span: None,
            normalized_text: String::new(),
            failure: Some("E_QUOTE_NOT_FOUND"),
        }),
        [(start, end)] => Ok(QuoteResolution {
            raw_span: normalized_document.raw_span_for_match(*start, *end),
            normalized_text: normalized_document.text[*start..*end].to_owned(),
            failure: None,
        }),
        _ => Err(GroundingError::AmbiguousQuote {
            claim_id: claim.claim_id.clone(),
            match_count: matches.len(),
        }),
    }
}

fn quote_result(
    claim: &GroundingClaim,
    resolution: &QuoteResolution,
    document: &GroundingDocument,
) -> RuleResult {
    let code = resolution.failure;
    let (rule_id, affected) = match code {
        None => (format!("grounding.{}.quote", claim.claim_id), Vec::new()),
        Some(code) => (
            format!("grounding.{}.{}", claim.claim_id, code),
            vec![AffectedEntry {
                binding: claim.source.clone(),
                key: Some(BTreeMap::from([(
                    "claim_id".to_owned(),
                    Value::String(claim.claim_id.clone()),
                )])),
                field: Some("quote".to_owned()),
                value: Some(json!({
                    "code": code,
                    "quote": claim.quote,
                    "byte_span": claim.byte_span.as_ref().map(|span| json!({
                        "start": span.start,
                        "end": span.end,
                    })),
                    "observed": resolution.raw_span.as_ref().map(|span| {
                        String::from_utf8_lossy(&document.bytes[span.start..span.end]).into_owned()
                    }),
                })),
            }],
        ),
    };
    RuleResult {
        rule_id,
        severity: Severity::Error,
        status: if affected.is_empty() {
            ResultStatus::Pass
        } else {
            ResultStatus::Fail
        },
        violation_count: usize::from(!affected.is_empty()),
        affected,
    }
}

fn normalized_matches(document: &NormalizedText, target: &str) -> Vec<(usize, usize)> {
    document
        .text
        .match_indices(target)
        .map(|(start, value)| (start, start + value.len()))
        .collect()
}

fn normalize_whitespace(value: &str) -> String {
    let mut normalized = String::new();
    let mut pending_space = false;
    for character in value.chars() {
        if character.is_whitespace() {
            pending_space = true;
        } else {
            if pending_space && !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.push(character);
            pending_space = false;
        }
    }
    normalized
}

fn normalize_html(bytes: &[u8]) -> Option<NormalizedText> {
    let source = std::str::from_utf8(bytes).ok()?;
    let mut builder = NormalizedBuilder::default();
    let mut index = 0;
    while index < source.len() {
        let remaining = &source[index..];
        if remaining.starts_with("<!--") {
            index = source[index + 4..]
                .find("-->")
                .map(|end| index + 4 + end + 3)
                .unwrap_or(source.len());
            continue;
        }
        if starts_element(source, index, "script") || starts_element(source, index, "style") {
            let name = if starts_element(source, index, "script") {
                "script"
            } else {
                "style"
            };
            index = skip_element(source, index, name);
            continue;
        }
        if remaining.as_bytes().first() == Some(&b'<')
            && let Some(end) = find_tag_end(source, index)
        {
            index = end;
            continue;
        }
        if remaining.as_bytes().first() == Some(&b'&')
            && let Some((character, end)) = decode_entity(source, index)
        {
            builder.push(character, RawSpan { start: index, end });
            index = end;
            continue;
        }
        let character = remaining.chars().next()?;
        let end = index + character.len_utf8();
        builder.push(character, RawSpan { start: index, end });
        index = end;
    }
    Some(builder.finish())
}

fn starts_element(source: &str, index: usize, name: &str) -> bool {
    let prefix = format!("<{name}");
    if !source.as_bytes()[index..].starts_with(prefix.as_bytes()) {
        return false;
    }
    source[index + prefix.len()..]
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_whitespace() || matches!(character, '>' | '/'))
}

fn skip_element(source: &str, index: usize, name: &str) -> usize {
    let opening_end = find_tag_end(source, index).unwrap_or(source.len());
    let closing_prefix = format!("</{name}");
    let mut search = opening_end;
    while let Some(relative) = find_ascii_case_insensitive(&source[search..], &closing_prefix) {
        let close_start = search + relative;
        let boundary = source[close_start + closing_prefix.len()..]
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_whitespace() || character == '>');
        if boundary {
            return find_tag_end(source, close_start).unwrap_or(source.len());
        }
        search = close_start + closing_prefix.len();
    }
    source.len()
}

fn find_tag_end(source: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in source.as_bytes()[start..].iter().copied().enumerate() {
        match quote {
            Some(delimiter) if byte == delimiter => quote = None,
            Some(_) => {}
            None if matches!(byte, b'\'' | b'"') => quote = Some(byte),
            None if byte == b'>' => return Some(start + offset + 1),
            None => {}
        }
    }
    None
}

fn find_ascii_case_insensitive(source: &str, needle: &str) -> Option<usize> {
    source
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| source.as_bytes()[*index..].starts_with(needle.as_bytes()))
}

fn decode_entity(source: &str, start: usize) -> Option<(char, usize)> {
    let remainder = &source[start + 1..];
    let end = remainder.find(';')?;
    if end > 32 {
        return None;
    }
    let entity = &remainder[..end];
    let character = match entity {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => '\u{00a0}',
        _ if entity.starts_with("#x") || entity.starts_with("#X") => {
            char::from_u32(u32::from_str_radix(&entity[2..], 16).ok()?)?
        }
        _ if entity.starts_with('#') => char::from_u32(entity[1..].parse::<u32>().ok()?)?,
        _ => return None,
    };
    Some((character, start + 1 + end + 1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NumericSurface {
    value: Decimal,
    scale: Scale,
    raw: String,
}

fn numeric_result(claim: &GroundingClaim, numeric: &NumericClaim, text: &str) -> RuleResult {
    let surfaces = numeric_surfaces(text);
    let matching = surfaces
        .iter()
        .any(|surface| surface.scale.apply(surface.value) == Some(numeric.target));
    let code = if matching {
        None
    } else if surfaces.is_empty() {
        Some("E_NUMBER_NOT_IN_SPAN")
    } else {
        Some("E_SCALE_MISMATCH")
    };
    let (rule_id, affected) = match code {
        None => (format!("grounding.{}.numeric", claim.claim_id), Vec::new()),
        Some(code) => (
            format!("grounding.{}.{}", claim.claim_id, code),
            vec![AffectedEntry {
                binding: claim.source.clone(),
                key: Some(BTreeMap::from([(
                    "claim_id".to_owned(),
                    Value::String(claim.claim_id.clone()),
                )])),
                field: Some("numeric".to_owned()),
                value: Some(json!({
                    "code": code,
                    "value": numeric.value,
                    "scale": numeric.scale.name(),
                    "unit": numeric.unit,
                    "surfaces": surfaces.iter().map(|surface| &surface.raw).collect::<Vec<_>>(),
                })),
            }],
        ),
    };
    RuleResult {
        rule_id,
        severity: Severity::Error,
        status: if affected.is_empty() {
            ResultStatus::Pass
        } else {
            ResultStatus::Fail
        },
        violation_count: usize::from(!affected.is_empty()),
        affected,
    }
}

fn numeric_surfaces(text: &str) -> Vec<NumericSurface> {
    let bytes = text.as_bytes();
    let mut surfaces = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let starts_number = bytes[index].is_ascii_digit()
            || (matches!(bytes[index], b'+' | b'-')
                && bytes.get(index + 1).is_some_and(u8::is_ascii_digit))
            || (bytes[index] == b'.' && bytes.get(index + 1).is_some_and(u8::is_ascii_digit))
            || (matches!(bytes[index], b'$' | 0xC2)
                && bytes.get(index + 1).is_some_and(u8::is_ascii_digit));
        if !starts_number {
            index += 1;
            continue;
        }

        let start = index;
        if matches!(bytes[index], b'$' | 0xC2) {
            index += 1;
        }
        if bytes
            .get(index)
            .is_some_and(|byte| matches!(byte, b'+' | b'-'))
        {
            index += 1;
        }
        let number_start = index;
        let mut dots = 0;
        while index < bytes.len()
            && (bytes[index].is_ascii_digit() || bytes[index] == b',' || bytes[index] == b'.')
        {
            if bytes[index] == b'.' {
                dots += 1;
            }
            index += 1;
        }
        if number_start == index || dots > 1 {
            index = start + 1;
            continue;
        }
        let number_end = index;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }

        let (scale, end) = if bytes.get(index) == Some(&b'%') {
            (Scale::Percent, index + 1)
        } else {
            let suffix_start = index;
            while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                index += 1;
            }
            let suffix = &text[suffix_start..index];
            let scale = Scale::parse(suffix).or_else(|| {
                if suffix.eq_ignore_ascii_case("basis") {
                    let mut after_basis = index;
                    while after_basis < bytes.len() && bytes[after_basis].is_ascii_whitespace() {
                        after_basis += 1;
                    }
                    if text[after_basis..]
                        .to_ascii_lowercase()
                        .starts_with("points")
                    {
                        return Some(Scale::Bps);
                    }
                }
                None
            });
            (scale.unwrap_or(Scale::Units), index)
        };
        let raw_end = end.max(number_end);
        let raw = text.get(start..raw_end).unwrap_or_default().to_owned();
        let number = text.get(start..number_end).unwrap_or_default();
        if let Some(value) = Decimal::parse(number) {
            surfaces.push(NumericSurface { value, scale, raw });
        }
        index = raw_end.max(start + 1);
    }
    surfaces
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> BTreeMap<String, GroundingDocument> {
        BTreeMap::from([(
            "filing".to_owned(),
            GroundingDocument::new(
                "8-k.html",
                b"Revenue was $42.5 million.\nThe margin was 125 bps.".to_vec(),
            ),
        )])
    }

    fn claim_json(
        quote: &str,
        start: usize,
        end: usize,
        value: Option<&str>,
        scale: Option<&str>,
    ) -> String {
        let numeric = match (value, scale) {
            (Some(value), Some(scale)) => format!(",\"value\":{value},\"scale\":\"{scale}\""),
            _ => String::new(),
        };
        format!(
            "{{\"claim_id\":\"C1\",\"source\":\"filing\",\"quote\":{quote:?},\"byte_span\":{{\"start\":{start},\"end\":{end}}}{numeric}}}"
        )
    }

    fn claim_json_without_span(quote: &str) -> String {
        format!("{{\"claim_id\":\"C1\",\"source\":\"filing\",\"quote\":{quote:?}}}")
    }

    #[test]
    fn exact_and_whitespace_quotes_and_numeric_scales_pass() {
        let bytes = b"Revenue was $42.5 million.";
        let exact = parse_claims_jsonl(
            claim_json(
                "Revenue was $42.5 million.",
                0,
                bytes.len(),
                Some("42.5"),
                Some("millions"),
            )
            .as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&exact, &document(), "sha256:claims", None).expect("evaluates");
        assert_eq!(report.outcome, Outcome::Pass);
        assert!(
            report
                .results
                .iter()
                .all(|result| result.affected.is_empty())
        );

        let whitespace = parse_claims_jsonl(
            claim_json("Revenue was $42.5   million.", 0, bytes.len(), None, None).as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&whitespace, &document(), "sha256:claims", None).expect("evaluates");
        assert_eq!(report.outcome, Outcome::Pass);
    }

    #[test]
    fn fabricated_quote_and_wrong_span_are_distinct_failures() {
        let fabricated = parse_claims_jsonl(
            claim_json("Revenue was $99 million.", 0, 24, None, None).as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&fabricated, &document(), "sha256:claims", None).expect("evaluates");
        assert!(report.results[0].rule_id.ends_with("E_QUOTE_NOT_FOUND"));

        let wrong_span = parse_claims_jsonl(
            claim_json("Revenue was $42.5 million.", 1, 25, None, None).as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&wrong_span, &document(), "sha256:claims", None).expect("evaluates");
        assert!(report.results[0].rule_id.ends_with("E_SPAN_MISMATCH"));
    }

    #[test]
    fn wrong_scale_fails_and_missing_number_is_not_in_span() {
        let wrong_scale = parse_claims_jsonl(
            claim_json(
                "Revenue was $42.5 million.",
                0,
                26,
                Some("42.5"),
                Some("thousands"),
            )
            .as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&wrong_scale, &document(), "sha256:claims", None).expect("evaluates");
        assert!(
            report
                .results
                .iter()
                .any(|result| result.rule_id.ends_with("E_SCALE_MISMATCH"))
        );

        let missing = parse_claims_jsonl(
            claim_json("The margin was 125 bps.", 0, 10, Some("125"), Some("bps")).as_bytes(),
        )
        .expect("claim parses");
        let report = evaluate(&missing, &document(), "sha256:claims", None).expect("evaluates");
        assert!(
            report
                .results
                .iter()
                .any(|result| result.rule_id.ends_with("E_NUMBER_NOT_IN_SPAN"))
        );
    }

    #[test]
    fn every_supported_scale_uses_exact_decimal_arithmetic() {
        let bytes =
            b"units 7; thousands 8 thousand; millions 9 million; percent 4.25%; bps 125 bps";
        let specifications = [
            ("units", "7", "7", "units"),
            ("thousands", "8 thousand", "8", "thousands"),
            ("millions", "9 million", "9", "millions"),
            ("percent", "4.25%", "4.25", "percent"),
            ("bps", "125 bps", "125", "bps"),
        ];
        let lines = specifications
            .iter()
            .map(|(claim_id, quote, value, scale)| {
                let start = bytes
                    .windows(quote.len())
                    .position(|window| window == quote.as_bytes())
                    .expect("scale fixture contains quote");
                json!({
                    "claim_id": claim_id,
                    "source": "filing",
                    "quote": quote,
                    "byte_span": {"start": start, "end": start + quote.len()},
                    "value": value.parse::<f64>().expect("test value"),
                    "scale": scale
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        let claims = parse_claims_jsonl(lines.as_bytes()).expect("claims parse");
        let docs = BTreeMap::from([(
            "filing".to_owned(),
            GroundingDocument::new("scales.txt", bytes.to_vec()),
        )]);
        let report = evaluate(&claims, &docs, "sha256:claims", None).expect("evaluates");
        assert_eq!(report.outcome, Outcome::Pass);
        assert_eq!(report.summary.total_rules, 10);
        assert!(
            report
                .results
                .iter()
                .all(|result| result.affected.is_empty())
        );
    }

    #[test]
    fn lock_v0_sha256_and_blake3_prefixes_pin_documents() {
        let docs = document();
        let bytes = &docs["filing"].bytes;
        let sha = sha256_hash(bytes);
        let blake = format!("blake3:{}", blake3::hash(bytes).to_hex());
        for (name, hash) in [("sha.lock.json", sha), ("blake.lock.json", blake)] {
            let lock = format!(
                r#"{{"version":"lock.v0","members":[{{"path":"8-k.html","bytes_hash":"{hash}"}}]}}"#
            );
            let result = verify_locks(&[(name.to_owned(), lock.into_bytes())], &docs)
                .expect("lock should verify");
            assert_eq!(result["filing"].locks, vec![name]);
        }
    }

    #[test]
    fn html_tags_entities_and_cross_tag_whitespace_are_normalized() {
        let bytes = b"<p>Real&nbsp;<b>&amp;</b>\ntext</p>";
        let start = 0;
        let end = bytes.len();
        let claims =
            parse_claims_jsonl(claim_json("Real & text", start, end, None, None).as_bytes())
                .expect("claim parses");
        let report = evaluate(
            &claims,
            &BTreeMap::from([(
                "filing".to_owned(),
                GroundingDocument::new("fixture.html", bytes.to_vec()),
            )]),
            "sha256:claims",
            None,
        )
        .expect("evaluates");
        assert_eq!(report.outcome, Outcome::Pass);
    }

    #[test]
    fn omitted_span_refuses_ambiguous_visible_matches_with_count() {
        let claims = parse_claims_jsonl(claim_json_without_span("Repeated").as_bytes())
            .expect("claim parses");
        let error = evaluate(
            &claims,
            &BTreeMap::from([(
                "filing".to_owned(),
                GroundingDocument::new(
                    "fixture.html",
                    b"<p>Repeated</p><div>Repeated</div>".to_vec(),
                ),
            )]),
            "sha256:claims",
            None,
        )
        .expect_err("ambiguous quote must refuse");
        assert_eq!(
            error,
            GroundingError::AmbiguousQuote {
                claim_id: "C1".to_owned(),
                match_count: 2,
            }
        );
    }

    #[test]
    fn script_and_comment_text_are_not_visible_to_quote_search() {
        let claims =
            parse_claims_jsonl(claim_json_without_span("hidden").as_bytes()).expect("claim parses");
        let report = evaluate(
            &claims,
            &BTreeMap::from([(
                "filing".to_owned(),
                GroundingDocument::new(
                    "fixture.html",
                    b"<!-- hidden --><script>hidden</script><p>visible</p>".to_vec(),
                ),
            )]),
            "sha256:claims",
            None,
        )
        .expect("non-ambiguous hidden quote is a domain failure");
        assert_eq!(report.outcome, Outcome::Fail);
        assert!(report.results[0].rule_id.ends_with("E_QUOTE_NOT_FOUND"));
    }
}
