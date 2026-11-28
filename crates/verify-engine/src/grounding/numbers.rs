//! Numbers in a normalized document.
//!
//! Every number is read from the whole document, never from the quote alone, so a quote cannot
//! drop a sign, parenthesis, scale word, percent sign or currency that changes what the number
//! says. The rule throughout is fail-closed: when the value, sign or scale cannot be established
//! from the text next to the number, the number is unbound and no numeric claim passes on it.

use super::{Decimal, Scale};

/// A currency stated next to a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Currency {
    /// A named currency, by ISO 4217 code.
    Code(&'static str),
    /// A bare `$` (or the word "dollars"): some dollar, not established which.
    Dollar,
    /// A bare `¥`: yen or yuan.
    Yen,
    /// A `$` with a prefix written apart from it (`CA $`), naming the code the prefix points
    /// to. The words in front of a `$` may be prose, so alone it establishes no unit, not even
    /// a bare dollar; it may only accompany an explicit marker of that same code.
    Prefixed(&'static str),
    /// A currency marker this module cannot name.
    Other,
}

/// A run of digits in the document.
#[derive(Debug, Clone)]
pub(super) struct NumberToken {
    /// Byte range of the digits themselves.
    pub start: usize,
    pub end: usize,
    /// A quote that holds any of the digits must cover this whole range: the digits plus any
    /// sign, parenthesis, scale word or percent sign that changes what they mean.
    pub required_start: usize,
    pub required_end: usize,
    /// The text reported under `surfaces`.
    raw_start: usize,
    raw_end: usize,
    /// Digits that are part of a name (`Q3`, `10-K`, `3.5x`): never bound, never counted.
    pub identifier: bool,
    /// `None` when the value, sign or scale cannot be established.
    pub value: Option<(Decimal, Scale)>,
    /// The number carries its own scale word, percent sign or basis points.
    scaled: bool,
    /// Index one past the suffix, in characters.
    suffix_end_index: usize,
    core_start_index: usize,
    core_end_index: usize,
    pub currencies: Vec<Currency>,
    /// Byte range of the currency written after the number.
    trailing_marker: Option<(usize, usize)>,
}

impl NumberToken {
    /// The number as the quote at `start..end` shows it: never text from outside the quote.
    pub(super) fn surface<'a>(&self, text: &'a str, start: usize, end: usize) -> &'a str {
        &text[self.raw_start.max(start)..self.raw_end.min(end)]
    }
}

const CODES: [&str; 19] = [
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "HKD", "NZD", "CHF", "CNY", "INR", "SEK", "NOK",
    "DKK", "SGD", "MXN", "BRL", "ZAR", "KRW",
];

/// Currency names that may follow a number. Longer phrases come first.
const PHRASES: [(&str, Currency); 17] = [
    ("u.s. dollars", Currency::Code("USD")),
    ("u.s. dollar", Currency::Code("USD")),
    ("us dollars", Currency::Code("USD")),
    ("us dollar", Currency::Code("USD")),
    ("united states dollars", Currency::Code("USD")),
    ("canadian dollars", Currency::Code("CAD")),
    ("canadian dollar", Currency::Code("CAD")),
    ("australian dollars", Currency::Code("AUD")),
    ("hong kong dollars", Currency::Code("HKD")),
    ("pounds sterling", Currency::Code("GBP")),
    ("japanese yen", Currency::Code("JPY")),
    ("swiss francs", Currency::Code("CHF")),
    ("swiss franc", Currency::Code("CHF")),
    ("euros", Currency::Code("EUR")),
    ("euro", Currency::Code("EUR")),
    ("dollars", Currency::Dollar),
    ("dollar", Currency::Dollar),
];

/// Words after a number that change its magnitude in a way no claim scale can express.
const UNSUPPORTED_SCALES: [&str; 48] = [
    // Magnitude words in other languages.
    "billones",
    "billon",
    "bill\u{f3}n",
    "billionen",
    "millones",
    "millon",
    "millionen",
    "milliard",
    "milliards",
    "milliarden",
    "miljoen",
    "milhoes",
    "milh\u{f5}es",
    "tausend",
    "mille",
    "quadrillion",
    "mm",
    "mn",
    "mil",
    "mils",
    "mio",
    "mln",
    "mill",
    "bil",
    "bln",
    "bio",
    "mrd",
    "md",
    "thou",
    "thsd",
    "ths",
    "hundred",
    "hundreds",
    "dozen",
    "dozens",
    "grand",
    "crore",
    "crores",
    "cr",
    "lakh",
    "lakhs",
    "lac",
    "pc",
    "pp",
    "pps",
    "ppt",
    "ppts",
    "thousandths",
];

/// Every English magnitude word. One kept from the digits, or following another, still
/// leaves a number unbound, whether or not a claim scale can express it.
const MAGNITUDES: [&str; 12] = [
    "hundred",
    "thousand",
    "thousands",
    "million",
    "millions",
    "billion",
    "billions",
    "bn",
    "trillion",
    "trillions",
    "tn",
    "trn",
];

/// Short forms of a scale declaration; see also `declares_scale`.
const SCALE_DECLARATIONS: [&str; 33] = [
    "(\u{a3}bn)",
    "(\u{20ac}bn)",
    "($tn)",
    "(\u{a3}tn)",
    "(\u{20ac}tn)",
    "in $bn",
    "in \u{a3}bn",
    "in \u{20ac}bn",
    "in $tn",
    "$ bn)",
    "'000",
    "\u{2019}000",
    "\u{a3}000",
    "\u{20ac}000",
    "in $m",
    "in \u{a3}m",
    "in \u{20ac}m",
    "$ m)",
    "000s omitted",
    "000's omitted",
    "000\u{2019}s omitted",
    "(000s",
    "(000's",
    "(000\u{2019}s",
    "(000)",
    "($000",
    "$000s",
    "$000's",
    "($m)",
    "(\u{a3}m)",
    "(\u{20ac}m)",
    "($bn)",
    "($k)",
];

/// Where a tag, not the document's own text, separates two pieces of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TagBreak {
    /// Byte offset of the separating space in the normalized text.
    pub offset: usize,
    /// A `sup` or `sub` tag: what follows may be a footnote marker or an exponent.
    pub raised: bool,
}

/// Whether the document states a scale for its figures ("in millions", "(USD thousands)",
/// "Millions of dollars"): then a number without its own scale word might be scaled.
fn declares_scale(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    if SCALE_DECLARATIONS
        .iter()
        .any(|declaration| lower.contains(declaration))
    {
        return true;
    }
    ["thousand", "million", "billion", "trillion"]
        .iter()
        .any(|magnitude| {
            lower.match_indices(magnitude).any(|(start, _)| {
                let mut end = start + magnitude.len();
                let plural = lower[end..].starts_with('s');
                end += usize::from(plural);
                let before = text[..start].trim_end();
                if lower[..start].chars().next_back().is_some_and(is_word)
                    || lower[end..].starts_with(is_word)
                    || before.ends_with(|last: char| last.is_ascii_digit() || last == '-')
                {
                    return false;
                }
                let previous_word = before
                    .rsplit(|character: char| !character.is_alphabetic())
                    .next()
                    .unwrap_or("");
                before.ends_with('(')
                    || before.ends_with(is_currency_symbol)
                    || code_currency(previous_word).is_some()
                    || (plural && previous_word.eq_ignore_ascii_case("in"))
                    || [
                        " of dollars",
                        " of u.s. dollars",
                        " of us dollars",
                        " of euros",
                    ]
                    .iter()
                    .any(|tail| lower[end..].starts_with(tail))
            })
        })
}

pub(super) fn is_combining(character: char) -> bool {
    matches!(
        u32::from(character),
        0x0300..=0x036F
            | 0x0483..=0x0489
            | 0x0591..=0x05C7
            | 0x0610..=0x061A
            | 0x064B..=0x065F
            | 0x0900..=0x0903
            | 0x093A..=0x094F
            | 0x0E31
            | 0x0E34..=0x0E3A
            | 0x0E47..=0x0E4E
            | 0x1AB0..=0x1AFF
            | 0x1DC0..=0x1DFF
            | 0x20D0..=0x20FF
            | 0x3099..=0x309A
            | 0xFE20..=0xFE2F
    )
}

fn is_word(character: char) -> bool {
    character.is_alphanumeric() || character == '_' || is_combining(character)
}

/// Hyphens that join the letters on both sides into one word.
fn is_joining_hyphen(character: char) -> bool {
    matches!(character, '-' | '\u{2010}' | '\u{2011}')
}

/// Hyphen-minus and the characters used as a minus sign.
fn is_minus_like(character: char) -> bool {
    matches!(
        character,
        '-' | '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2212}' | '\u{fe63}' | '\u{ff0d}'
    )
}

fn is_dash(character: char) -> bool {
    is_minus_like(character) || matches!(character, '\u{2013}' | '\u{2014}' | '\u{2015}')
}

fn is_currency_symbol(character: char) -> bool {
    matches!(
        character,
        '$' | '\u{20ac}' | '\u{a3}' | '\u{a5}' | '\u{20b9}' | '\u{a2}' | '\u{20a0}'..='\u{20c0}'
    )
}

fn symbol_currency(prefix: &str, symbol: char) -> Currency {
    match (prefix, symbol) {
        ("", '$') => Currency::Dollar,
        ("US", '$') => Currency::Code("USD"),
        ("C" | "CA", '$') => Currency::Code("CAD"),
        ("A" | "AU", '$') => Currency::Code("AUD"),
        ("HK", '$') => Currency::Code("HKD"),
        ("NZ", '$') => Currency::Code("NZD"),
        ("", '\u{20ac}') => Currency::Code("EUR"),
        ("", '\u{a3}') => Currency::Code("GBP"),
        ("", '\u{a5}') => Currency::Yen,
        ("", '\u{20b9}') => Currency::Code("INR"),
        _ => Currency::Other,
    }
}

fn code_currency(word: &str) -> Option<Currency> {
    CODES
        .iter()
        .find(|code| **code == word)
        .map(|code| Currency::Code(code))
}

struct Document<'a> {
    text: &'a str,
    chars: Vec<char>,
    offsets: Vec<usize>,
}

impl<'a> Document<'a> {
    fn new(text: &'a str) -> Self {
        let (offsets, chars) = text.char_indices().unzip();
        Self {
            text,
            chars,
            offsets,
        }
    }

    fn at(&self, index: usize) -> Option<char> {
        self.chars.get(index).copied()
    }

    fn offset(&self, index: usize) -> usize {
        self.offsets.get(index).copied().unwrap_or(self.text.len())
    }

    fn slice(&self, start: usize, end: usize) -> &str {
        &self.text[self.offset(start)..self.offset(end)]
    }

    /// The letters starting at `index`, and the index after them.
    fn word_at(&self, index: usize) -> (String, usize) {
        let mut end = index;
        while self.at(end).is_some_and(char::is_alphabetic) {
            end += 1;
        }
        (self.slice(index, end).to_owned(), end)
    }

    /// The letters ending just before `index`, and where they start.
    fn word_before(&self, index: usize) -> (String, usize) {
        let mut start = index;
        while start > 0 && self.chars[start - 1].is_alphabetic() {
            start -= 1;
        }
        (self.slice(start, index).to_owned(), start)
    }

    fn skip_space(&self, index: usize) -> usize {
        if self.at(index) == Some(' ') {
            index + 1
        } else {
            index
        }
    }
}

/// What precedes the digits.
#[derive(Default)]
struct Prefix {
    /// Where the currency, sign and parenthesis in front of the digits start.
    start: usize,
    currencies: Vec<Currency>,
    negative_at: Option<usize>,
    paren_at: Option<usize>,
    unclear_sign: bool,
}

/// Where the currency marker ending in the symbol at `symbol` starts: the short letter runs
/// attached to it, with or without dots (`C$`, `US$`, `CN¥`, `Cdn.$`, `U.S.$`). A longer word
/// run into the symbol ("totaled$5") is not part of the marker.
fn marker_start(document: &Document<'_>, symbol: usize) -> usize {
    let mut start = symbol;
    loop {
        let dotted = start > 0 && document.chars[start - 1] == '.';
        let letters_end = start - usize::from(dotted);
        let mut letters_start = letters_end;
        while letters_start > 0 && document.chars[letters_start - 1].is_ascii_alphabetic() {
            letters_start -= 1;
        }
        let length = letters_end - letters_start;
        // Letters run into a longer or non-ASCII word are the end of that word.
        let inside_word = letters_start > 0 && document.chars[letters_start - 1].is_alphabetic();
        if length == 0 || length > 3 || inside_word {
            return start;
        }
        start = letters_start;
    }
}

/// Prefixes that belong to a `$` even when written apart from it, with the code each points
/// to. Case matters ("told us $5" is not one). Single letters (`C $`) are left out: "Class A
/// $5 million" would be caught.
const SPACED_PREFIXES: [(&str, &str); 9] = [
    ("US", "USD"),
    ("U.S.", "USD"),
    ("CA", "CAD"),
    ("Cdn", "CAD"),
    ("Cdn.", "CAD"),
    ("CDN", "CAD"),
    ("HK", "HKD"),
    ("NZ", "NZD"),
    ("AU", "AUD"),
];

/// A prefix one space before the `$` at `symbol`, as its own word: where it starts, and its
/// code. Line breaks, entities and tags have all become that one space by now.
fn spaced_prefix(document: &Document<'_>, symbol: usize) -> Option<(usize, &'static str)> {
    if document.at(symbol) != Some('$') || symbol == 0 || document.chars[symbol - 1] != ' ' {
        return None;
    }
    let end = symbol - 1;
    let mut start = end;
    while start > 0
        && (document.chars[start - 1].is_ascii_alphabetic() || document.chars[start - 1] == '.')
    {
        start -= 1;
    }
    // An ellipsis in front of the prefix is not part of it.
    while start < end && document.chars[start] == '.' {
        start += 1;
    }
    if start > 0 && document.chars[start - 1].is_alphanumeric() {
        return None;
    }
    let word = document.slice(start, end);
    SPACED_PREFIXES
        .iter()
        .find(|(prefix, _)| *prefix == word)
        .map(|(_, code)| (start, *code))
}

/// The currency marker ending in the symbol at `symbol`: where it starts and what it names.
fn marker_at(document: &Document<'_>, symbol: usize) -> (usize, Currency) {
    if let Some((start, code)) = spaced_prefix(document, symbol) {
        return (start, Currency::Prefixed(code));
    }
    let start = marker_start(document, symbol);
    let letters = document.slice(start, symbol).to_ascii_uppercase();
    // A dotted prefix (`Cdn.$`) is a marker this module cannot name.
    let currency = if letters.contains('.') {
        Currency::Other
    } else {
        symbol_currency(&letters, document.chars[symbol])
    };
    (start, currency)
}

/// Byte ranges of the currency markers of more than one character, in document order: a symbol
/// with its prefix, and currency names written after a number. A quote may not start or end
/// inside one.
pub(super) fn currency_markers(text: &str, tokens: &[NumberToken]) -> Vec<(usize, usize)> {
    let document = Document::new(text);
    let mut markers: Vec<(usize, usize)> = (0..document.chars.len())
        .filter(|index| is_currency_symbol(document.chars[*index]))
        .filter_map(|symbol| {
            let (start, _) = marker_at(&document, symbol);
            (start < symbol).then(|| (document.offset(start), document.offset(symbol + 1)))
        })
        .chain(tokens.iter().filter_map(|number| number.trailing_marker))
        .collect();
    markers.sort_unstable();
    markers
}

fn prefix(document: &Document<'_>, core: usize) -> Prefix {
    let mut found = Prefix {
        start: core,
        ..Prefix::default()
    };
    let mut cursor = core;
    loop {
        let spaced = cursor > 0 && document.chars[cursor - 1] == ' ';
        let end = if spaced { cursor - 1 } else { cursor };
        if end == 0 {
            break;
        }
        let character = document.chars[end - 1];
        if is_currency_symbol(character) {
            let (start, currency) = marker_at(document, end - 1);
            found.currencies.push(currency);
            cursor = start;
        } else if character.is_alphabetic() {
            let (word, start) = document.word_before(end);
            let Some(currency) = code_currency(&word) else {
                break;
            };
            found.currencies.push(currency);
            cursor = start;
        } else if character == '(' {
            found.paren_at = Some(end - 1);
            cursor = end - 1;
        } else if character == '+' {
            cursor = end - 1;
        } else if is_dash(character) {
            let sign = end - 1;
            let before = sign.checked_sub(1).and_then(|index| document.at(index));
            // A hyphen straight after a number links the two; see `link`.
            if !spaced && before.is_some_and(char::is_numeric) {
                break;
            }
            if is_minus_like(character) {
                if !spaced && !before.is_some_and(is_word) && matches!(character, '-' | '\u{2212}')
                {
                    found.negative_at = Some(sign);
                } else {
                    found.unclear_sign = true;
                }
            } else {
                // An en or em dash is punctuation unless it sits where a minus sign would.
                let opens = before.is_some_and(|previous| matches!(previous, ' ' | '('))
                    || (before.is_none() && character == '\u{2013}');
                if spaced || !opens {
                    break;
                }
                found.unclear_sign = true;
            }
            // "$-5" and "(-5)": the currency or parenthesis may come before the sign.
            cursor = sign;
        } else {
            if character == ';' && undecoded_entity_ends_at(document, end - 1) {
                found.unclear_sign = true;
            }
            break;
        }
        found.start = cursor;
    }
    let (word, _) = document.word_before(if cursor > 0 && document.chars[cursor - 1] == ' ' {
        cursor - 1
    } else {
        cursor
    });
    if word.eq_ignore_ascii_case("negative") || word.eq_ignore_ascii_case("minus") {
        found.unclear_sign = true;
    }
    found
}

/// `&name;` that was not decoded: it might be any sign or digit.
fn undecoded_entity_ends_at(document: &Document<'_>, semicolon: usize) -> bool {
    let mut index = semicolon;
    while index > 0 {
        let character = document.chars[index - 1];
        if character == '&' {
            return index < semicolon;
        }
        if !(character.is_ascii_alphanumeric() || character == '#') {
            return false;
        }
        index -= 1;
    }
    false
}

enum Suffix {
    None,
    Scale(Scale),
    Unsupported,
    /// Letters run into the digits: part of a name.
    Identifier,
}

/// The scale after the digits, and the index after it.
fn suffix(document: &Document<'_>, after: usize, has_currency: bool) -> (Suffix, usize) {
    let attached = document.at(after).is_some_and(char::is_alphabetic);
    let hyphenated = document.at(after).is_some_and(is_joining_hyphen)
        && document.at(after + 1).is_some_and(char::is_alphabetic);
    let start = if hyphenated {
        after + 1
    } else {
        document.skip_space(after)
    };
    match document.at(start) {
        Some('%') if !hyphenated => return (Suffix::Scale(Scale::Percent), start + 1),
        Some('\u{2030}' | '\u{2031}' | '\u{ff05}') => return (Suffix::Unsupported, start + 1),
        _ => {}
    }
    let (word, mut end) = document.word_at(start);
    if attached && code_currency(&word).is_some() {
        return (Suffix::None, after);
    }
    let word = word.to_ascii_lowercase();
    let next = document.skip_space(end);
    let (following, following_end) = document.word_at(next);
    let following = following.to_ascii_lowercase();
    let found = match word.as_str() {
        "percent" | "pct" => Suffix::Scale(Scale::Percent),
        "per" if following == "cent" => {
            end = following_end;
            Suffix::Scale(Scale::Percent)
        }
        "percentage" => {
            if following.starts_with("point") {
                end = following_end;
            }
            Suffix::Unsupported
        }
        "thousand" | "thousands" => Suffix::Scale(Scale::Thousands),
        "million" | "millions" => Suffix::Scale(Scale::Millions),
        "billion" | "billions" | "bn" => Suffix::Scale(Scale::Billions),
        "trillion" | "trillions" | "trn" => Suffix::Scale(Scale::Trillions),
        // `tn` is also tonnes: it means trillions only next to a currency.
        "tn" if has_currency => Suffix::Scale(Scale::Trillions),
        "tn" => Suffix::Unsupported,
        "bps" | "bp" => Suffix::Scale(Scale::Bps),
        "basis" if following == "point" || following == "points" => {
            end = following_end;
            Suffix::Scale(Scale::Bps)
        }
        "basis" => Suffix::Unsupported,
        // `k` means thousands only next to a currency; otherwise one letter may be a unit
        // (metres) or part of a name (`10-K`). Next to a currency `M` is millions to some
        // writers and thousands to others (who write `MM` for millions), so it is neither.
        "k" if has_currency && !hyphenated => Suffix::Scale(Scale::Thousands),
        "m" | "b" | "t" if has_currency && !hyphenated => Suffix::Unsupported,
        "k" | "m" | "b" if !attached && !hyphenated => Suffix::Unsupported,
        other if UNSUPPORTED_SCALES.contains(&other) => Suffix::Unsupported,
        _ if attached || hyphenated => return (Suffix::Identifier, after),
        _ => return (Suffix::None, after),
    };
    // "5 thousand million": two magnitudes cannot be expressed by one claim scale.
    let mut cursor = end;
    if document.at(cursor).is_some_and(is_joining_hyphen) {
        cursor += 1;
    }
    let (second, second_end) = document.word_at(document.skip_space(cursor));
    if MAGNITUDES.contains(&second.to_ascii_lowercase().as_str()) {
        return (Suffix::Unsupported, second_end);
    }
    (found, end)
}

fn is_magnitude(word: &str) -> bool {
    let word = word.to_lowercase();
    MAGNITUDES.contains(&word.as_str()) || UNSUPPORTED_SCALES.contains(&word.as_str())
}

/// A scale word kept from the digits by a footnote mark, bracket, quotation mark or dash
/// ("41.2* million", "41.2 [1] million", "5\u{2013}million"). Returns where it ends.
fn detached_scale(document: &Document<'_>, after: usize) -> Option<usize> {
    let mut cursor = after;
    let mut marked = false;
    let mut bracketed = false;
    while let Some(character) = document.at(cursor) {
        let mark = matches!(
            character,
            '*' | '\u{2020}'
                | '\u{2021}'
                | '\u{a7}'
                | '"'
                | '\u{201c}'
                | '\u{201d}'
                | '\''
                | '\u{2019}'
                | '['
                | ']'
                | ')'
        ) || (is_dash(character) && !is_joining_hyphen(character));
        if !(mark || character == ' ' || (bracketed && character.is_ascii_alphanumeric())) {
            break;
        }
        bracketed = (bracketed || character == '[') && character != ']';
        marked |= mark;
        cursor += 1;
    }
    let (word, end) = document.word_at(cursor);
    (marked && is_magnitude(&word)).then_some(end)
}

/// A currency written after the number: a code, a symbol or a name.
fn trailing_currency(document: &Document<'_>, after: usize) -> Option<(Currency, usize)> {
    let start = document.skip_space(after);
    let first = document.at(start)?;
    if is_currency_symbol(first) {
        // A symbol followed by digits belongs to the next number.
        let next = document.at(document.skip_space(start + 1));
        return (!next.is_some_and(char::is_numeric))
            .then(|| (symbol_currency("", first), start + 1));
    }
    let rest = &document.text[document.offset(start)..];
    for (phrase, currency) in PHRASES {
        let matches = rest
            .get(..phrase.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(phrase));
        if matches && !rest[phrase.len()..].starts_with(is_word) {
            return Some((currency, start + phrase.chars().count()));
        }
    }
    let (word, end) = document.word_at(start);
    let currency = code_currency(&word)?;
    // A code in front of digits belongs to the next number.
    let next = document.at(document.skip_space(end));
    (!next.is_some_and(char::is_numeric)).then_some((currency, end))
}

/// The exact value of `core`, or `None` for anything but a plainly written decimal.
fn parse_core(core: &str) -> Option<Decimal> {
    if !core
        .chars()
        .all(|character| character.is_ascii_digit() || matches!(character, '.' | ','))
    {
        return None;
    }
    let (whole, fraction) = core.split_once('.').unwrap_or((core, ""));
    if fraction.contains(['.', ',']) {
        return None;
    }
    if whole.contains(',') {
        let mut groups = whole.split(',');
        let head = groups.next()?;
        if head.is_empty() || head.len() > 3 || groups.any(|group| group.len() != 3) {
            return None;
        }
    }
    Decimal::parse(core)
}

fn starts_number(document: &Document<'_>, index: usize) -> bool {
    let character = document.chars[index];
    character.is_numeric()
        || (character == '.'
            && document.at(index + 1).is_some_and(|next| next.is_numeric())
            && index
                .checked_sub(1)
                .is_none_or(|previous| !document.chars[previous].is_alphanumeric()))
}

fn read_number(document: &Document<'_>, core: usize) -> NumberToken {
    let mut core_end = core + 1;
    while let Some(character) = document.at(core_end) {
        let continues = character.is_numeric()
            || (matches!(character, '.' | ',')
                && document.at(core_end + 1).is_some_and(char::is_numeric));
        if !continues {
            break;
        }
        core_end += 1;
    }

    let before = prefix(document, core);
    let mut currencies = before.currencies.clone();
    let preceding = core.checked_sub(1).map(|index| document.chars[index]);
    // `Q3`, `v2`, `COVID-19`: letters run into the digits. A currency code may (`USD41`).
    let glued_to_name = preceding.is_some_and(|previous| {
        (previous.is_alphabetic() || previous == '_' || is_combining(previous))
            && before.start == core
    }) || (preceding.is_some_and(is_joining_hyphen)
        && core
            .checked_sub(2)
            .is_some_and(|index| document.chars[index].is_alphabetic()));

    // "(5.0) million": the parenthesis closes before the scale word.
    let mut paren_end = None;
    let mut scan_from = core_end;
    if before.paren_at.is_some() {
        let close = document.skip_space(core_end);
        if document.at(close) == Some(')') {
            paren_end = Some(close + 1);
            scan_from = close + 1;
        }
    }
    let (found, mut suffix_end) = suffix(document, scan_from, !currencies.is_empty());
    let mut identifier = glued_to_name;
    let mut unbound = before.unclear_sign;
    let (scale, scaled) = match found {
        Suffix::None => (Scale::Units, false),
        Suffix::Scale(scale) => (scale, true),
        Suffix::Unsupported => {
            unbound = true;
            (Scale::Units, true)
        }
        Suffix::Identifier => {
            identifier = true;
            (Scale::Units, false)
        }
    };
    if suffix_end == scan_from {
        suffix_end = paren_end.unwrap_or(core_end);
    }

    let mut after_currency = suffix_end;
    let mut scale_after_currency = None;
    let mut trailing_marker = None;
    if let Some((currency, end)) = trailing_currency(document, suffix_end) {
        currencies.push(currency);
        trailing_marker = Some((
            document.offset(document.skip_space(suffix_end)),
            document.offset(end),
        ));
        after_currency = end;
        // "5 USD million": the scale sits beyond the currency.
        let (word, word_end) = document.word_at(document.skip_space(end));
        if is_magnitude(&word) {
            unbound = true;
            scale_after_currency = Some(word_end);
        }
    }
    // "5&frac12;": an entity that was not decoded may be more of the number.
    if document.at(core_end) == Some('&')
        && (core_end + 2..=core_end + 12).any(|index| document.at(index) == Some(';'))
    {
        unbound = true;
    }

    // Accounting negative or an aside in brackets: the sign cannot be established.
    if before.paren_at.is_some() && paren_end.is_none() {
        let close = document.skip_space(after_currency);
        if document.at(close) == Some(')') {
            paren_end = Some(close + 1);
        }
    }
    if !scaled && let Some(end) = detached_scale(document, scan_from) {
        unbound = true;
        scale_after_currency = Some(end);
    }
    let mut required_end = suffix_end.max(core_end);
    if let Some(end) = scale_after_currency {
        required_end = required_end.max(end);
    }
    if let Some(end) = paren_end {
        unbound = true;
        required_end = required_end.max(end);
    }
    // "500-": a trailing minus.
    if document.at(required_end).is_some_and(is_minus_like)
        && !document.at(required_end + 1).is_some_and(is_word)
    {
        unbound = true;
        required_end += 1;
    }

    let mut required_start = core;
    if let Some(sign) = before.negative_at {
        required_start = sign;
    }
    if let Some(open) = before.paren_at.filter(|_| paren_end.is_some()) {
        required_start = required_start.min(open);
    }

    let value = (!unbound && !identifier)
        .then(|| parse_core(document.slice(core, core_end)))
        .flatten()
        .and_then(|mut decimal| {
            if before.negative_at.is_some() {
                decimal.coefficient = decimal.coefficient.checked_neg()?;
            }
            Some((decimal, scale))
        });

    NumberToken {
        start: document.offset(core),
        end: document.offset(core_end),
        required_start: document.offset(required_start),
        required_end: document.offset(required_end),
        raw_start: document.offset(before.start.min(required_start)),
        raw_end: document.offset(required_end),
        identifier,
        value,
        scaled,
        suffix_end_index: required_end,
        core_start_index: core,
        core_end_index: core_end,
        currencies,
        trailing_marker,
    }
}

/// Numbers that only mean something together: ranges, dates, ratios and spaced digit groups.
fn link(document: &Document<'_>, tokens: &mut [NumberToken]) {
    // Right to left, so a borrowed scale carries along a list: "1.2, 1.3 and 1.5 million".
    let mut borrowed = vec![false; tokens.len()];
    for index in (1..tokens.len()).rev() {
        let (earlier, later) = tokens.split_at_mut(index);
        let (first, second) = (&mut earlier[index - 1], &mut later[0]);
        if first.suffix_end_index > second.core_start_index {
            continue;
        }
        let between: String = document.chars[first.suffix_end_index..second.core_start_index]
            .iter()
            .filter(|character| !is_currency_symbol(**character))
            .collect();
        // "9/30", "10-20", "3:1", "41'200", "5×10^6"
        let tight = !between.is_empty()
            && between.chars().all(|character| {
                is_dash(character)
                    || matches!(
                        character,
                        '/' | ':' | '\'' | '\u{2019}' | '_' | '^' | '*' | '\u{d7}'
                    )
            });
        // "5 1/2" and "5 ½"
        let mixed = between == " "
            && (document.at(second.core_end_index) == Some('/')
                || !document.chars[second.core_start_index].is_ascii());
        // "41 200"
        let first_digits = document.slice(first.core_start_index, first.core_end_index);
        let second_digits = document.slice(second.core_start_index, second.core_end_index);
        let spaced_group = between == " "
            && !first.scaled
            && first_digits.len() <= 3
            && second_digits.split([',', '.']).next().map(str::len) == Some(3);
        // "10 to 20 million": the first number borrows the second one's scale.
        let word = between.trim().to_ascii_lowercase();
        let joins = matches!(
            word.as_str(),
            "to" | "and"
                | "or"
                | "through"
                | "versus"
                | "vs"
                | "vs."
                | "and/or"
                | ","
                | ", and"
                | ", or"
        ) || (word.chars().count() == 1 && word.chars().all(is_dash));
        borrowed[index - 1] = !first.scaled && (second.scaled || borrowed[index]) && joins;
        if tight || spaced_group || mixed {
            first.value = None;
            second.value = None;
        } else if borrowed[index - 1] {
            first.value = None;
        }
    }
}

/// Every number in `text`, in order.
pub(super) fn tokens(text: &str, breaks: &[TagBreak]) -> Vec<NumberToken> {
    let document = Document::new(text);
    let mut found = Vec::new();
    let mut index = 0;
    while index < document.chars.len() {
        if starts_number(&document, index) {
            let number = read_number(&document, index);
            index = number.core_end_index;
            found.push(number);
        } else {
            index += 1;
        }
    }
    link(&document, &mut found);

    for token in &mut found {
        let across_tags = breaks.iter().any(|tag| {
            // The scale word or percent sign sits in another element.
            (token.end <= tag.offset && tag.offset < token.required_end)
                // A footnote marker or exponent follows the digits, or the digits are one.
                || (tag.raised && (tag.offset == token.end || tag.offset + 1 == token.start))
        });
        if across_tags {
            token.value = None;
        }
    }

    if declares_scale(text) {
        for token in &mut found {
            if !token.scaled {
                token.value = None;
            }
        }
    }
    if uses_long_scale(text) {
        for token in &mut found {
            if matches!(token.value, Some((_, Scale::Billions | Scale::Trillions))) {
                token.value = None;
            }
        }
    }
    found
}

/// Whether the document also uses words of a long-scale language, where "billion" is a
/// million millions: then English `billion` and `trillion` cannot be established.
fn uses_long_scale(text: &str) -> bool {
    let lower = text.to_lowercase();
    ["milliard", "billionen", "billones", "bill\u{f3}n"]
        .iter()
        .any(|word| lower.contains(word))
}

/// Whether `text[start..end]` stands on its own: it cuts neither a word, a number nor a
/// currency marker.
pub(super) fn stands_alone(
    text: &str,
    tokens: &[NumberToken],
    markers: &[(usize, usize)],
    start: usize,
    end: usize,
) -> bool {
    // Markers are in document order and do not overlap.
    let inside_marker = |position: usize| {
        let before = markers.partition_point(|(from, _)| *from < position);
        before > 0 && position < markers[before - 1].1
    };
    if inside_marker(start) || inside_marker(end) {
        return false;
    }
    let matched = &text[start..end];
    let (Some(first), Some(last)) = (matched.chars().next(), matched.chars().next_back()) else {
        return false;
    };
    if is_combining(first) {
        return false;
    }
    let mut before = text[..start].chars().rev();
    let (previous, earlier) = (before.next(), before.next());
    if is_word(first)
        && previous.is_some_and(|previous| {
            is_word(previous)
                || ((is_joining_hyphen(previous) || matches!(previous, '\'' | '\u{2019}'))
                    && earlier.is_some_and(is_word))
        })
    {
        return false;
    }
    let mut after = text[end..].chars();
    let (next, later, last_one) = (after.next(), after.next(), after.next());
    if next.is_some_and(is_combining) {
        return false;
    }
    if is_word(last)
        && let Some(next) = next
    {
        if is_word(next) {
            return false;
        }
        if is_joining_hyphen(next) && later.is_some_and(is_word) {
            return false;
        }
        if matches!(next, '\'' | '\u{2019}') && later.is_some_and(is_word) {
            // A possessive keeps the word's meaning; "can't" and "won't" do not.
            let possessive = matches!(later, Some('s' | 'S')) && !last_one.is_some_and(is_word);
            if !possessive {
                return false;
            }
        }
    }
    // Tokens are in document order and their digits do not overlap.
    let first_overlap = tokens.partition_point(|token| token.end <= start);
    tokens[first_overlap..]
        .iter()
        .take_while(|token| token.start < end)
        .all(|token| start <= token.required_start && token.required_end <= end)
}
