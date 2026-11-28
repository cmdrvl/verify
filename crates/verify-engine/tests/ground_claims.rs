//! bd-w2p: `ground` must never PASS a claim its document does not support, and must keep
//! passing claims it does. Every document here is synthetic.

use std::collections::BTreeMap;

use verify_core::report::Outcome;
use verify_engine::grounding::{GroundingDocument, evaluate, parse_claims_jsonl};

const BEAD: &str = "As of September 30, cash totaled $41.2 million. The notes are nonconvertible.";
const CASH: &str = "Cash totaled $41.2 million.";

/// (value, scale, unit); `value` is JSON number text.
type Numeric = Option<(&'static str, &'static str, Option<&'static str>)>;

fn n(value: &'static str, scale: &'static str) -> Numeric {
    Some((value, scale, None))
}

fn u(value: &'static str, scale: &'static str, unit: &'static str) -> Numeric {
    Some((value, scale, Some(unit)))
}

struct Case {
    name: &'static str,
    document: &'static str,
    quote: &'static str,
    numeric: Numeric,
    /// Send the byte range of this substring of the document as `byte_span`.
    span: Option<&'static str>,
}

fn case(name: &'static str, document: &'static str, quote: &'static str, numeric: Numeric) -> Case {
    Case {
        name,
        document,
        quote,
        numeric,
        span: None,
    }
}

fn spanned(
    name: &'static str,
    document: &'static str,
    quote: &'static str,
    numeric: Numeric,
    span: &'static str,
) -> Case {
    Case {
        name,
        document,
        quote,
        numeric,
        span: Some(span),
    }
}

/// `PASS`, or the sorted failure codes, or the refusal.
fn verdict(case: &Case) -> String {
    let mut line = format!(
        r#"{{"claim_id":"c","source":"filing","quote":{}"#,
        serde_json::to_string(case.quote).expect("quote serializes")
    );
    if let Some(span) = case.span {
        let found = case.document.find(span);
        assert!(
            found.is_some(),
            "{}: span is not in the document",
            case.name
        );
        let start = found.expect("checked above");
        line.push_str(&format!(
            r#","byte_span":{{"start":{start},"end":{}}}"#,
            start + span.len()
        ));
    }
    if let Some((value, scale, unit)) = case.numeric {
        line.push_str(&format!(r#","value":{value},"scale":"{scale}""#));
        if let Some(unit) = unit {
            line.push_str(&format!(r#","unit":"{unit}""#));
        }
    }
    line.push('}');
    let claims = parse_claims_jsonl(line.as_bytes()).expect("claim parses");
    let documents = BTreeMap::from([(
        "filing".to_owned(),
        GroundingDocument::new("doc.html", case.document.as_bytes().to_vec()),
    )]);
    match evaluate(&claims, &documents, "sha256:claims", None) {
        Err(error) => format!("REFUSAL {error}"),
        Ok(report) if report.outcome == Outcome::Pass => "PASS".to_owned(),
        Ok(report) => {
            let mut codes: Vec<String> = report
                .results
                .iter()
                .flat_map(|result| &result.affected)
                .filter_map(|entry| entry.value.as_ref())
                .filter_map(|value| value["code"].as_str().map(str::to_owned))
                .collect();
            codes.sort();
            codes.join("+")
        }
    }
}

fn assert_none_pass(cases: &[Case]) {
    let wrong: Vec<&str> = cases
        .iter()
        .filter(|case| verdict(case) == "PASS")
        .map(|case| case.name)
        .collect();
    assert!(
        wrong.is_empty(),
        "{} of {} claims must not pass but did: {wrong:#?}",
        wrong.len(),
        cases.len()
    );
}

fn assert_all_pass(cases: &[Case]) {
    let wrong: Vec<String> = cases
        .iter()
        .map(|case| (case.name, verdict(case)))
        .filter(|(_, verdict)| verdict != "PASS")
        .map(|(name, verdict)| format!("{name}: {verdict}"))
        .collect();
    assert!(
        wrong.is_empty(),
        "{} of {} supported claims did not pass: {wrong:#?}",
        wrong.len(),
        cases.len()
    );
}

#[test]
fn the_four_reported_claims_fail_with_these_codes() {
    for (case, expected) in [
        (
            case(
                "quote cuts a number",
                BEAD,
                "1.2 million",
                n("1.2", "millions"),
            ),
            "E_NUMBER_NOT_IN_SPAN+E_QUOTE_NOT_FOUND",
        ),
        (
            case("quote inside a word", BEAD, "convertible", None),
            "E_QUOTE_NOT_FOUND",
        ),
        (
            case(
                "wrong currency",
                BEAD,
                "cash totaled $41.2 million",
                u("41.2", "millions", "EUR"),
            ),
            "E_UNIT_NOT_ESTABLISHED",
        ),
        (
            case(
                "binds the date number",
                BEAD,
                "As of September 30, cash totaled $41.2 million.",
                n("30", "units"),
            ),
            "E_NUMBER_UNBOUND",
        ),
    ] {
        assert_eq!(verdict(&case), expected, "{}", case.name);
    }
}

#[test]
fn byte_spans_get_the_same_checks() {
    assert_none_pass(&[
        spanned(
            "cuts a number",
            BEAD,
            "1.2 million",
            n("1.2", "millions"),
            "1.2 million",
        ),
        spanned("inside a word", BEAD, "convertible", None, "convertible"),
        spanned(
            "wrong currency",
            BEAD,
            "$41.2 million",
            u("41.2", "millions", "EUR"),
            "$41.2 million",
        ),
        spanned(
            "binds the date number",
            BEAD,
            "As of September 30, cash totaled",
            n("30", "millions"),
            "As of September 30, cash totaled",
        ),
        spanned(
            "hidden script text glued to visible",
            "<p>a</p><script>var x='debt is zero</script><p>.</p>",
            "debt is zero.",
            None,
            "debt is zero</script><p>.",
        ),
        spanned(
            "starts inside a comment",
            "<p>Revenue</p><!-- fell --><p>rose</p>",
            "fell -->rose",
            None,
            "fell --><p>rose",
        ),
        spanned(
            "raw quote carries a comment",
            "<p>x</p><!-- debt is zero --><p>cash</p>",
            "<!-- debt is zero --><p>cash</p>",
            None,
            "<!-- debt is zero --><p>cash</p>",
        ),
        spanned(
            "raw quote carries an attribute",
            "<p title=\"debt is zero\">cash</p>",
            "<p title=\"debt is zero\">cash</p>",
            None,
            "<p title=\"debt is zero\">cash</p>",
        ),
        spanned("blank quote", "Cash   fell.", "  ", None, "   "),
        spanned(
            "half an entity",
            "<p>A &amp; B</p>",
            "amp; B",
            None,
            "amp; B",
        ),
    ]);
}

#[test]
fn a_quote_may_not_cut_a_number() {
    assert_none_pass(&[
        case("leading digits", CASH, "1.2 million", n("1.2", "millions")),
        case(
            "before the decimals",
            CASH,
            "Cash totaled $41",
            n("41", "units"),
        ),
        case(
            "after the decimal point",
            CASH,
            "2 million",
            n("2", "millions"),
        ),
        case(
            "trailing digits",
            "Cash was 41.25 million.",
            "Cash was 41.2",
            n("41.2", "units"),
        ),
        case(
            "thousands group head",
            "We sold 1,100 units.",
            "We sold 1",
            n("1", "units"),
        ),
        case(
            "thousands group tail",
            "We sold 1,100 units.",
            "100 units",
            n("100", "units"),
        ),
        case(
            "quote only, inside a number",
            "Cash was 141 dollars.",
            "41 dollars",
            None,
        ),
        case(
            "scale word cut",
            CASH,
            "Cash totaled $41.2",
            n("41.2", "units"),
        ),
        case(
            "scale word cut, quote only",
            CASH,
            "Cash totaled $41.2",
            None,
        ),
        case(
            "percent cut",
            "The rate was 41.2% then.",
            "The rate was 41.2",
            n("41.2", "units"),
        ),
        case(
            "sign cut",
            "The change was -5 million.",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "minus before currency",
            "Net result: -$5 million.",
            "$5 million",
            n("5", "millions"),
        ),
        case(
            "unicode minus",
            "The change was \u{2212}5 million.",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "minus entity",
            "<p>The change was &minus;5 million.</p>",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "numeric minus entity",
            "<p>The change was &#8722;5 million.</p>",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "parenthesised",
            "Net income (loss) was (5.0) million.",
            "5.0",
            n("5", "millions"),
        ),
        case(
            "parenthesised currency inside",
            "Net result was ($5.0 million) overall.",
            "$5.0 million",
            n("5", "millions"),
        ),
    ]);
}

#[test]
fn a_number_whose_value_cannot_be_established_does_not_bind() {
    assert_none_pass(&[
        case(
            "malformed grouping",
            "Cash was 1,00 dollars.",
            "Cash was 1,00 dollars",
            n("100", "units"),
        ),
        case(
            "european decimal comma",
            "Cash was 41,2 million.",
            "Cash was 41,2 million",
            n("412", "millions"),
        ),
        case(
            "dotted grouping",
            "Cash was 1.234.567 today.",
            "Cash was 1.234.567 today",
            n("567", "units"),
        ),
        case(
            "spaced grouping",
            "Cash was 41 200 today.",
            "Cash was 41",
            n("41", "units"),
        ),
        case(
            "apostrophe grouping",
            "Cash was 41'200 today.",
            "Cash was 41'200",
            n("41", "units"),
        ),
        case(
            "scientific notation",
            "Load was 4e6 watts.",
            "Load was 4e6 watts",
            n("4", "units"),
        ),
        case(
            "multiple suffix",
            "Leverage was 3.5x at year end.",
            "Leverage was 3.5x",
            n("3.5", "millions"),
        ),
        case(
            "digits inside an identifier",
            "Results for Q3 improved.",
            "Results for Q3 improved",
            n("3", "units"),
        ),
        case(
            "digits inside a form name",
            "See the Form 10-K filed.",
            "See the Form 10-K filed",
            n("10", "units"),
        ),
        case(
            "digits after a hyphen",
            "COVID-19 costs rose.",
            "COVID-19 costs rose",
            n("19", "units"),
        ),
        case(
            "fraction",
            "Owns 1/2 of the venture.",
            "Owns 1/2 of the venture",
            n("2", "units"),
        ),
        case(
            "ratio",
            "The ratio was 3:1 then.",
            "The ratio was 3:1",
            n("1", "units"),
        ),
        case(
            "superscript glued",
            "Area was 41\u{b2} then.",
            "Area was 41\u{b2}",
            n("41", "units"),
        ),
        case(
            "spaced minus",
            "The change was - 5 million.",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "en dash as minus",
            "The change was \u{2013}5 million.",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "word negative",
            "The change was negative 5 million.",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "parenthesised with currency",
            "Net result was $(5.0) million.",
            "$(5.0) million",
            n("5", "millions"),
        ),
        case(
            "parenthesised, negative claim",
            "Net result was (5.0) million.",
            "(5.0) million",
            n("-5", "millions"),
        ),
        case(
            "trailing minus",
            "Balance 500- carried.",
            "Balance 500- carried",
            n("500", "units"),
        ),
        case(
            "positive claim of a negative",
            "The change was -5 million.",
            "-5 million",
            n("5", "millions"),
        ),
        case(
            "billion",
            "Debt was $41.2 billion.",
            "$41.2 billion",
            n("41.2", "units"),
        ),
        case("bn", "Debt was $41.2bn.", "$41.2bn", n("41.2", "units")),
        case(
            "trillion",
            "Assets were 2 trillion.",
            "2 trillion",
            n("2", "units"),
        ),
        case("MM", "Debt was $41.2MM.", "$41.2MM", n("41.2", "units")),
        case(
            "mil",
            "Debt was $41.2 mil.",
            "$41.2 mil",
            n("41.2", "units"),
        ),
        case(
            "hundred",
            "About 5 hundred homes.",
            "5 hundred homes",
            n("5", "units"),
        ),
        case(
            "two scale words",
            "It was 5 thousand million.",
            "5 thousand million",
            n("5", "thousands"),
        ),
        case(
            "hyphenated scale",
            "A 5-million-dollar loan closed.",
            "A 5-million-dollar loan",
            n("5", "units"),
        ),
        case(
            "hyphenated percent",
            "A 5-percent fee applied.",
            "A 5-percent fee",
            n("5", "units"),
        ),
        case(
            "per cent read as units",
            "A fee of 5 per cent applied.",
            "5 per cent",
            n("5", "units"),
        ),
        case(
            "percentage points",
            "Margin rose 5 percentage points.",
            "5 percentage points",
            n("5", "units"),
        ),
        // In some finance writing M is thousands and MM millions, so M establishes neither.
        case(
            "M with a currency, claimed as millions",
            "Fees were $8M.",
            "$8M",
            n("8", "millions"),
        ),
        case(
            "M with a currency, claimed as thousands",
            "Fees were $8M.",
            "$8M",
            n("8", "thousands"),
        ),
        case(
            "metres read as millions",
            "The mast is 5 m tall.",
            "The mast is 5 m tall",
            n("5", "millions"),
        ),
        case(
            "low end of a worded range",
            "Guidance is 10 to 20 million.",
            "10",
            n("10", "units"),
        ),
        case(
            "low end of a dashed range",
            "Guidance is $10-$20 million.",
            "$10",
            n("10", "units"),
        ),
        case(
            "between-and range",
            "Between 10 and 20 million shares.",
            "Between 10",
            n("10", "units"),
        ),
        case(
            "table declares thousands",
            "Amounts in thousands. Cash 41,200",
            "Cash 41,200",
            n("41200", "units"),
        ),
        case(
            "header declares millions",
            "<p>($ in millions)</p><p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "percent claim of a bare number",
            "There are 0.05 shares each.",
            "0.05 shares",
            n("5", "percent"),
        ),
        case(
            "units claim of a percentage",
            "The rate was 5% then.",
            "The rate was 5%",
            n("0.05", "units"),
        ),
        case(
            "numeric date",
            "Signed 9/30/2024 here.",
            "Signed 9/30/2024",
            n("30", "units"),
        ),
        case(
            "iso date",
            "Signed 2024-09-30 here.",
            "Signed 2024-09-30",
            n("2024", "units"),
        ),
        case(
            "section number",
            "See Section 4.2.1 now.",
            "See Section 4.2.1 now",
            n("4.2", "units"),
        ),
        case(
            "year range",
            "Over 2023-2024 it grew.",
            "Over 2023-2024",
            n("2024", "units"),
        ),
    ]);
}

#[test]
fn scales_and_parts_written_away_from_the_digits_still_count() {
    assert_none_pass(&[
        case(
            "currency before the scale word",
            "Debt was 5 USD million.",
            "5",
            n("5", "units"),
        ),
        case(
            "currency before the scale word, quoted",
            "Debt was 5 USD million.",
            "5 USD",
            n("5", "units"),
        ),
        case(
            "header: currency and singular scale",
            "<p>Revenue (EUR million)</p>\n<p>Net 412</p>",
            "Net 412",
            n("412", "units"),
        ),
        case(
            "header: millions of dollars",
            "Millions of dollars\nCash 412",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "header: bare symbol and m",
            "<p>Revenue ($m)</p>\n<p>Net 412</p>",
            "Net 412",
            n("412", "units"),
        ),
        case(
            "footnote marker takes the scale",
            "<p>Cash $41.2<sup>1</sup> million</p>",
            "1 million",
            n("1", "millions"),
        ),
        case(
            "number under a footnote marker",
            "<p>Cash $41.2<sup>1</sup> million</p>",
            "$41.2",
            n("41.2", "units"),
        ),
        case(
            "power written as sup",
            "<p>About 10<sup>6</sup> homes</p>",
            "About 10",
            n("10", "units"),
        ),
        case(
            "scale in the next block",
            "<tr><td>5</td><td>million dollar homes</td></tr>",
            "5 million",
            n("5", "millions"),
        ),
        case(
            "mixed number",
            "Debt was 5 1/2 million.",
            "Debt was 5",
            n("5", "units"),
        ),
        case(
            "mixed number with a glyph",
            "Debt was 5 \u{bd} million.",
            "Debt was 5",
            n("5", "units"),
        ),
        case(
            "undecoded fraction entity",
            "<p>Debt was 5&frac12; million.</p>",
            "Debt was 5",
            n("5", "units"),
        ),
        case(
            "times ten to the power",
            "Load was 5\u{d7}10^6 watts.",
            "Load was 5",
            n("5", "units"),
        ),
        case(
            "underscore grouping",
            "Cash was 41_200 today.",
            "was 41_200",
            n("41", "units"),
        ),
        case(
            "pct read as units",
            "A fee of 5 pct applied.",
            "5 pct",
            n("5", "units"),
        ),
        case(
            "german billion",
            "Debt was 5 Mrd today.",
            "5 Mrd",
            n("5", "units"),
        ),
        case("thsd", "Cash was 3 thsd today.", "3 thsd", n("3", "units")),
    ]);
}

#[test]
fn markers_dashes_lists_and_headers_do_not_hide_a_scale() {
    assert_none_pass(&[
        case(
            "asterisk before the scale",
            "Cash was $41.2* million.",
            "$41.2",
            n("41.2", "units"),
        ),
        case(
            "bracketed footnote before the scale",
            "Cash was $41.2 [1] million.",
            "$41.2",
            n("41.2", "units"),
        ),
        case(
            "quoted number before the scale",
            "Cash was \"41.2\" million.",
            "41.2",
            n("41.2", "units"),
        ),
        case(
            "en dash before the scale",
            "A 5\u{2013}million loan closed.",
            "A 5",
            n("5", "units"),
        ),
        case(
            "em dash before the scale",
            "A 5\u{2014}million loan closed.",
            "A 5",
            n("5", "units"),
        ),
        case(
            "list sharing a scale, first",
            "Sales were 1.2, 1.3 and 1.5 million.",
            "Sales were 1.2",
            n("1.2", "units"),
        ),
        case(
            "versus sharing a scale",
            "It was 4 versus 5 million.",
            "It was 4",
            n("4", "units"),
        ),
        case(
            "header with apostrophe thousands",
            "<p>US$'000</p>\n<p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "header with pound thousands",
            "<p>\u{a3}000</p>\n<p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "header in sterling m",
            "<p>Figures in \u{a3}m</p>\n<p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "spanish millions",
            "Fueron 5 millones.",
            "5 millones",
            n("5", "units"),
        ),
        case(
            "german millions",
            "Es waren 5 Millionen.",
            "5 Millionen",
            n("5", "units"),
        ),
        case(
            "uppercase script text is hidden",
            "<p>a</p><SCRIPT>debt is zero</SCRIPT>",
            "debt is zero",
            None,
        ),
        case(
            "mixed-case style text is hidden",
            "<p>a</p><Style>debt is zero</Style>",
            "debt is zero",
            None,
        ),
    ]);
}

#[test]
fn more_than_one_number_in_a_quote_does_not_bind() {
    assert_none_pass(&[
        case(
            "two amounts, first claimed",
            "Cash was $41 million and debt $9 million.",
            "Cash was $41 million and debt $9 million",
            n("41", "millions"),
        ),
        case(
            "two amounts, second claimed",
            "Cash was $41 million and debt $9 million.",
            "Cash was $41 million and debt $9 million",
            n("9", "millions"),
        ),
        case(
            "year plus amount, year claimed",
            "In 2024 revenue was $5 million.",
            "In 2024 revenue was $5 million",
            n("2024", "units"),
        ),
        case(
            "year plus amount, amount claimed",
            "In 2024 revenue was $5 million.",
            "In 2024 revenue was $5 million",
            n("5", "millions"),
        ),
        case(
            "date plus amount, amount claimed",
            BEAD,
            "As of September 30, cash totaled $41.2 million.",
            n("41.2", "millions"),
        ),
    ]);
}

#[test]
fn a_unit_passes_only_when_the_document_states_it() {
    assert_none_pass(&[
        case(
            "euro claimed as USD",
            "Cash totaled \u{20ac}41.2 million.",
            "\u{20ac}41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "pound claimed as USD",
            "Cash totaled \u{a3}41.2 million.",
            "41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "C$ claimed as USD",
            "Cash totaled C$41.2 million.",
            "$41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "C$ claimed as a bare dollar",
            "Cash totaled C$41.2 million.",
            "C$41.2 million",
            u("41.2", "millions", "$"),
        ),
        case(
            "CAD claimed as USD",
            "Cash totaled CAD 41.2 million.",
            "41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "code after",
            "Cash totaled 41.2 million EUR.",
            "41.2 million EUR",
            u("41.2", "millions", "USD"),
        ),
        case(
            "currency word after",
            "Cash totaled 41.2 million euros.",
            "41.2 million euros",
            u("41.2", "millions", "USD"),
        ),
        case(
            "no currency anywhere",
            "We issued 41.2 million shares.",
            "41.2 million shares",
            u("41.2", "millions", "USD"),
        ),
        case(
            "unknown unit",
            CASH,
            "$41.2 million",
            u("41.2", "millions", "bananas"),
        ),
        case(
            "another number's currency",
            "We paid $9 and issued 5 million shares.",
            "5 million shares",
            u("5", "millions", "USD"),
        ),
        case(
            "conflicting markers",
            "Cash totaled \u{20ac}41.2 million USD.",
            "41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "bare dollar sign is not USD",
            CASH,
            "$41.2 million",
            u("41.2", "millions", "USD"),
        ),
    ]);
}

#[test]
fn a_quote_may_not_cut_a_word() {
    assert_none_pass(&[
        case(
            "hyphenated tail",
            "The notes are non-convertible.",
            "convertible",
            None,
        ),
        case(
            "unicode hyphen",
            "The notes are non\u{2010}convertible.",
            "convertible",
            None,
        ),
        case(
            "non-breaking hyphen",
            "The notes are non\u{2011}convertible.",
            "convertible",
            None,
        ),
        case(
            "hyphenated head",
            "It is a profit-sharing plan.",
            "It is a profit",
            None,
        ),
        case(
            "soft hyphen",
            "The notes are non\u{ad}convertible.",
            "convertible",
            None,
        ),
        case(
            "zero width space",
            "<p>The notes are non&#8203;convertible.</p>",
            "convertible",
            None,
        ),
        case(
            "accented continuation",
            "The caf\u{e9}s closed.",
            "The caf",
            None,
        ),
        case(
            "combining accent",
            "The cafe\u{301} closed.",
            "The cafe",
            None,
        ),
        case(
            "non-latin",
            "\u{394}\u{3b5}\u{3bb}\u{3c4}\u{3b1} rose.",
            "\u{3b5}\u{3bb}\u{3c4}\u{3b1} rose",
            None,
        ),
        case("underscore", "see net_income here", "income here", None),
        case("contraction", "We can't repay.", "We can", None),
        case(
            "curly contraction",
            "We won\u{2019}t repay.",
            "We won",
            None,
        ),
        case("word tail", "The firm is insolvent.", "solvent", None),
        case("word head", "No debts are owed.", "No debt", None),
    ]);
}

#[test]
fn html_normalisation_does_not_invent_text() {
    assert_none_pass(&[
        case(
            "table cells glue into a number",
            "<table><tr><td>41</td><td>2</td></tr></table>",
            "412",
            n("412", "units"),
        ),
        case(
            "table cells glue into words",
            "<table><tr><td>cash</td><td>flow</td></tr></table>",
            "cashflow",
            None,
        ),
        case(
            "paragraphs glue",
            "<p>Total debt 4</p><p>1.2 million</p>",
            "41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "line break glues",
            "owed 4<br>1 dollars",
            "41 dollars",
            n("41", "units"),
        ),
        case(
            "footnote glued to a number",
            "<p>Cash $41.2<sup>1</sup> million</p>",
            "$41.21 million",
            n("41.21", "millions"),
        ),
        case(
            "a less-than sign swallows text",
            "Cash was < 5 million; debt > 3 million.",
            "Cash was 3 million",
            n("3", "millions"),
        ),
    ]);
}

#[test]
fn supported_quotes_still_pass() {
    assert_all_pass(&[
        case("whole word", BEAD, "nonconvertible", None),
        case(
            "sentence with period",
            BEAD,
            "The notes are nonconvertible.",
            None,
        ),
        case(
            "sentence without period",
            BEAD,
            "The notes are nonconvertible",
            None,
        ),
        case(
            "whole hyphenated word",
            "The notes are non-convertible.",
            "non-convertible",
            None,
        ),
        case(
            "inside quotation marks",
            "He said \"cash is tight\" today.",
            "cash is tight",
            None,
        ),
        case(
            "inside single quotes",
            "He said 'cash is tight' today.",
            "cash is tight",
            None,
        ),
        case(
            "inside parentheses",
            "Liquidity (cash on hand) fell.",
            "cash on hand",
            None,
        ),
        case(
            "before a possessive",
            "The Company's cash fell.",
            "The Company",
            None,
        ),
        case(
            "before a curly possessive",
            "The Company\u{2019}s cash fell.",
            "The Company",
            None,
        ),
        case(
            "before a plural possessive",
            "The lenders' consent is needed.",
            "The lenders",
            None,
        ),
        case(
            "before a comma",
            "Cash, net of debt, fell.",
            "net of debt",
            None,
        ),
        case("before a colon", "Summary: cash fell.", "Summary", None),
        case(
            "after an em dash",
            "Results\u{2014}cash fell.",
            "cash fell",
            None,
        ),
        case(
            "before an em dash",
            "Cash fell\u{2014}sharply.",
            "Cash fell",
            None,
        ),
        case(
            "after a spaced hyphen",
            "Item 1 - cash fell.",
            "cash fell",
            None,
        ),
        case(
            "after a slash",
            "Assets and/or cash fell.",
            "cash fell",
            None,
        ),
        case(
            "before an ellipsis",
            "Cash fell... then rose.",
            "Cash fell",
            None,
        ),
        case(
            "after a semicolon",
            "Cash fell; debt rose.",
            "debt rose",
            None,
        ),
        case(
            "before a question mark",
            "Did cash fall? Yes.",
            "Did cash fall",
            None,
        ),
        case(
            "collapsed whitespace",
            "Cash\n  totaled\t$41.2   million.",
            "Cash totaled $41.2 million.",
            None,
        ),
        case("ends at an amount", CASH, "$41.2 million", None),
        case("ends at punctuation", CASH, "$41.2 million.", None),
        case(
            "accented word",
            "The caf\u{e9} closed.",
            "The caf\u{e9}",
            None,
        ),
        case("year", "Fiscal 2024 was strong.", "Fiscal 2024", None),
        case(
            "number before a period",
            "We opened store 12.",
            "We opened store 12",
            None,
        ),
        case(
            "number before a comma",
            "We had 12, then more.",
            "We had 12",
            None,
        ),
        case("identifier", "See the Form 10-K filed.", "Form 10-K", None),
        case("amount with its scale", CASH, "totaled $41.2 million", None),
        case("omits the currency symbol", CASH, "41.2 million", None),
        case("overlapping second occurrence", "xab ab", "ab", None),
        case(
            "less-than text is kept",
            "Cash was < 5 million today.",
            "Cash was < 5 million",
            None,
        ),
        case(
            "inline tags split a word",
            "<p><span>Rev</span><span>enue rose</span></p>",
            "Revenue rose",
            None,
        ),
        case(
            "table cells quoted with a space",
            "<table><tr><td>Cash</td><td>$41.2 million</td></tr></table>",
            "Cash $41.2 million",
            None,
        ),
        spanned(
            "span covers a whole element",
            "<p>Real&nbsp;<b>&amp;</b>\ntext</p>",
            "Real & text",
            None,
            "<p>Real&nbsp;<b>&amp;</b>\ntext</p>",
        ),
        spanned(
            "span of plain words",
            BEAD,
            "nonconvertible",
            None,
            "nonconvertible",
        ),
    ]);
}

#[test]
fn supported_numbers_still_pass() {
    assert_all_pass(&[
        case(
            "plain",
            CASH,
            "Cash totaled $41.2 million.",
            n("41.2", "millions"),
        ),
        case(
            "same value in units",
            CASH,
            "$41.2 million",
            n("41200000", "units"),
        ),
        case(
            "same value in thousands",
            CASH,
            "$41.2 million",
            n("41200", "thousands"),
        ),
        case(
            "narrow quote avoids the date",
            BEAD,
            "cash totaled $41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "grouped integer",
            "We sold 1,100 units.",
            "We sold 1,100 units",
            n("1100", "units"),
        ),
        case(
            "grouped to millions",
            "Cash was $41,200,000 then.",
            "$41,200,000",
            n("41.2", "millions"),
        ),
        case(
            "percent sign",
            "The rate was 4.25% then.",
            "4.25%",
            n("4.25", "percent"),
        ),
        case(
            "percent word",
            "The rate was 4.25 percent then.",
            "4.25 percent",
            n("4.25", "percent"),
        ),
        case(
            "per cent",
            "A fee of 5 per cent applied.",
            "5 per cent",
            n("5", "percent"),
        ),
        case(
            "percent at sentence end",
            "Occupancy was 95%.",
            "Occupancy was 95%",
            n("95", "percent"),
        ),
        case("bps", "The margin was 125 bps.", "125 bps", n("125", "bps")),
        case(
            "basis points",
            "The margin was 125 basis points.",
            "125 basis points",
            n("125", "bps"),
        ),
        case(
            "bps as percent",
            "The margin was 125 bps.",
            "125 bps",
            n("1.25", "percent"),
        ),
        case(
            "thousand",
            "Fees were 8 thousand.",
            "8 thousand",
            n("8", "thousands"),
        ),
        case(
            "K with a currency",
            "Fees were $8K.",
            "$8K",
            n("8", "thousands"),
        ),
        case(
            "leading decimal point",
            "The factor was .5 then.",
            "The factor was .5",
            n("0.5", "units"),
        ),
        case(
            "trailing period",
            "We opened store 12.",
            "store 12.",
            n("12", "units"),
        ),
        case(
            "noun follows",
            "We issued 41.2 million shares.",
            "41.2 million shares",
            n("41.2", "millions"),
        ),
        case(
            "hyphenated scale",
            "A 5-million-dollar loan closed.",
            "A 5-million-dollar loan",
            n("5", "millions"),
        ),
        case(
            "plus sign",
            "The change was +5 million.",
            "+5 million",
            n("5", "millions"),
        ),
        case(
            "negative",
            "The change was -5 million.",
            "-5 million",
            n("-5", "millions"),
        ),
        case(
            "negative with currency",
            "Net result: -$5 million.",
            "-$5 million",
            n("-5", "millions"),
        ),
        case(
            "inside quotation marks",
            "The cap is \"$5 million\" now.",
            "$5 million",
            n("5", "millions"),
        ),
        case(
            "before a parenthetical",
            "Cash was $5 million (unaudited).",
            "Cash was $5 million",
            n("5", "millions"),
        ),
        case(
            "after a spaced em dash",
            "Total \u{2014} $5 million.",
            "$5 million",
            n("5", "millions"),
        ),
        case(
            "beside a quarter name",
            "Q3 revenue was $5 million.",
            "Q3 revenue was $5 million",
            n("5", "millions"),
        ),
        case(
            "beside a form name",
            "The 10-K shows $5 million.",
            "The 10-K shows $5 million",
            n("5", "millions"),
        ),
        case(
            "beside a tenor",
            "It sold $5 million of 10-year notes.",
            "It sold $5 million of 10-year notes",
            n("5", "millions"),
        ),
        case(
            "inline tags split a number",
            "<p><font>$41</font><font>.2</font> <b>million</b></p>",
            "$41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "inline XBRL",
            "<p>$<ix:nonFraction name=\"x\">41.2</ix:nonFraction> million</p>",
            "$41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "non-breaking space before the scale",
            "<p>$41.2&nbsp;million</p>",
            "$41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "xml element value",
            "<a><amount>95000000.00</amount>\n<term>120</term></a>",
            "95000000.00",
            n("95", "millions"),
        ),
        case(
            "currency in its own cell",
            "<tr><td>$</td><td>41.2 million</td></tr>",
            "41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "magnitude words in prose",
            "Thousands of people came. We opened 12 stores.",
            "We opened 12 stores",
            n("12", "units"),
        ),
        case(
            "pct as percent",
            "A fee of 5 pct applied.",
            "5 pct",
            n("5", "percent"),
        ),
        spanned(
            "raw markup span",
            "<a><amt>95000000.00</amt></a>",
            "<amt>95000000.00</amt>",
            n("95", "millions"),
            "<amt>95000000.00</amt>",
        ),
    ]);
}

#[test]
fn stated_units_pass() {
    assert_all_pass(&[
        case(
            "US$",
            "Cash totaled US$41.2 million.",
            "US$41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "code before",
            "Cash totaled USD 41.2 million.",
            "USD 41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "code glued before",
            "Cash totaled USD41.2 million.",
            "USD41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "code after, lower-case claim",
            "Cash totaled 41.2 million USD.",
            "41.2 million USD",
            u("41.2", "millions", "usd"),
        ),
        case(
            "U.S. dollars",
            "Cash totaled 41.2 million U.S. dollars.",
            "41.2 million U.S. dollars",
            u("41.2", "millions", "USD"),
        ),
        case(
            "quote omits the code",
            "Cash totaled USD 41.2 million.",
            "41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "code and a dollar sign",
            "Cash totaled $41.2 million USD.",
            "$41.2 million USD",
            u("41.2", "millions", "USD"),
        ),
        case(
            "dollar sign claimed as such",
            CASH,
            "$41.2 million",
            u("41.2", "millions", "$"),
        ),
        case(
            "euro sign",
            "Cash totaled \u{20ac}41.2 million.",
            "\u{20ac}41.2 million",
            u("41.2", "millions", "EUR"),
        ),
        case(
            "euro word",
            "Cash totaled 41.2 million euros.",
            "41.2 million euros",
            u("41.2", "millions", "EUR"),
        ),
        case(
            "pound sign",
            "Cash totaled \u{a3}41.2 million.",
            "\u{a3}41.2 million",
            u("41.2", "millions", "GBP"),
        ),
        case(
            "pound entity",
            "<p>Cash totaled &pound;41.2 million.</p>",
            "41.2 million",
            u("41.2", "millions", "GBP"),
        ),
        case(
            "C$",
            "Cash totaled C$41.2 million.",
            "C$41.2 million",
            u("41.2", "millions", "CAD"),
        ),
        case(
            "percent unit",
            "The rate was 4.25% then.",
            "4.25%",
            u("4.25", "percent", "%"),
        ),
        case(
            "basis point unit",
            "The margin was 125 bps.",
            "125 bps",
            u("125", "bps", "bps"),
        ),
    ]);
}

/// Documents that put a multi-character marker in front of the amount, with the quote that
/// starts inside it and the quote that holds all of it.
const PREFIXED: [(&str, &str, &str); 16] = [
    (
        "Cash totaled C$41.2 million.",
        "$41.2 million",
        "C$41.2 million",
    ),
    (
        "Cash totaled CA$41.2 million.",
        "$41.2 million",
        "CA$41.2 million",
    ),
    (
        "Cash totaled US$41.2 million.",
        "$41.2 million",
        "US$41.2 million",
    ),
    (
        "Cash totaled HK$41.2 million.",
        "$41.2 million",
        "HK$41.2 million",
    ),
    (
        "Cash totaled A$41.2 million.",
        "$41.2 million",
        "A$41.2 million",
    ),
    (
        "Cash totaled AU$41.2 million.",
        "$41.2 million",
        "AU$41.2 million",
    ),
    (
        "Cash totaled NZ$41.2 million.",
        "$41.2 million",
        "NZ$41.2 million",
    ),
    (
        "Cash totaled S$41.2 million.",
        "$41.2 million",
        "S$41.2 million",
    ),
    (
        "Cash totaled R$41.2 million.",
        "$41.2 million",
        "R$41.2 million",
    ),
    (
        "Cash totaled NT$41.2 million.",
        "$41.2 million",
        "NT$41.2 million",
    ),
    (
        "Cash totaled U.S.$41.2 million.",
        "$41.2 million",
        "U.S.$41.2 million",
    ),
    (
        "Cash totaled Cdn.$41.2 million.",
        "$41.2 million",
        "Cdn.$41.2 million",
    ),
    (
        "Cash totaled C&#36;41.2 million.",
        "$41.2 million",
        "C$41.2 million",
    ),
    (
        "Cash totaled <b>C</b>$41.2 million.",
        "$41.2 million",
        "C$41.2 million",
    ),
    (
        "Cash totaled CN\u{a5}41.2 million.",
        "\u{a5}41.2 million",
        "CN\u{a5}41.2 million",
    ),
    (
        "Cash totaled E\u{a3}41.2 million.",
        "\u{a3}41.2 million",
        "E\u{a3}41.2 million",
    ),
];

#[test]
fn a_quote_may_not_start_or_end_inside_a_currency_marker() {
    let mut cases = Vec::new();
    for (document, inside, _) in PREFIXED {
        cases.push(case(document, document, inside, None));
        cases.push(case(document, document, inside, n("41.2", "millions")));
    }
    cases.extend([
        spanned(
            "span starts inside C$",
            "Cash totaled C$41.2 million.",
            "$41.2 million",
            None,
            "$41.2 million",
        ),
        spanned(
            "span starts inside US$, numeric",
            "Cash totaled US$41.2 million.",
            "$41.2 million",
            n("41.2", "millions"),
            "$41.2 million",
        ),
        spanned(
            "span ends inside C$",
            "Cash totaled C$41.2 million.",
            "Cash totaled C",
            None,
            "Cash totaled C",
        ),
        case(
            "starts inside U.S.$, after the first dot",
            "Cash totaled U.S.$41.2 million.",
            "S.$41.2 million",
            None,
        ),
        case(
            "ends inside C$",
            "Cash totaled C$41.2 million.",
            "Cash totaled C",
            None,
        ),
        case(
            "ends inside U.S.$",
            "Cash totaled U.S.$41.2 million.",
            "Cash totaled U",
            None,
        ),
        case(
            "ends inside a marker after the number",
            "We paid 41.2 million C$ today.",
            "41.2 million C",
            None,
        ),
        case(
            "ends inside Canadian dollars",
            "We paid 41.2 million Canadian dollars.",
            "41.2 million Canadian",
            None,
        ),
        case(
            "ends inside Canadian dollars, numeric",
            "We paid 41.2 million Canadian dollars.",
            "41.2 million Canadian",
            n("41.2", "millions"),
        ),
        case(
            "ends inside Hong Kong dollars",
            "We paid 41.2 million Hong Kong dollars.",
            "41.2 million Hong Kong",
            None,
        ),
        case(
            "ends inside U.S. dollars",
            "We paid 41.2 million U.S. dollars.",
            "41.2 million U",
            None,
        ),
        case(
            "ends after the first words of U.S. dollars",
            "We paid 41.2 million U.S. dollars.",
            "41.2 million U.S.",
            None,
        ),
        case(
            "ends inside pounds sterling",
            "We paid 41.2 million pounds sterling.",
            "41.2 million pounds",
            None,
        ),
        case(
            "starts inside Canadian dollars",
            "We paid 41.2 million Canadian dollars to them.",
            "dollars to them",
            None,
        ),
        spanned(
            "span ends inside Canadian dollars",
            "We paid 41.2 million Canadian dollars.",
            "41.2 million Canadian",
            None,
            "41.2 million Canadian",
        ),
    ]);
    assert_none_pass(&cases);
}

#[test]
fn letters_attached_to_a_currency_sign_change_which_currency_it_is() {
    assert_none_pass(&[
        case(
            "Egyptian pound claimed as GBP",
            "Cash totaled E\u{a3}41.2 million.",
            "E\u{a3}41.2 million",
            u("41.2", "millions", "GBP"),
        ),
        case(
            "Egyptian pound claimed as pound sign",
            "Cash totaled E\u{a3}41.2 million.",
            "E\u{a3}41.2 million",
            u("41.2", "millions", "\u{a3}"),
        ),
        case(
            "yuan sign claimed as bare yen",
            "Cash totaled CN\u{a5}41.2 million.",
            "CN\u{a5}41.2 million",
            u("41.2", "millions", "\u{a5}"),
        ),
        case(
            "Cdn. dollar claimed as a bare dollar",
            "Cash totaled Cdn.$41.2 million.",
            "Cdn.$41.2 million",
            u("41.2", "millions", "$"),
        ),
    ]);
}

#[test]
fn quotes_around_whole_currency_markers_still_pass() {
    let mut cases = Vec::new();
    for (document, _, whole) in PREFIXED {
        // The raw-markup rows read as `C$` once normalized.
        cases.push(case(document, document, whole, None));
        cases.push(case(document, document, whole, n("41.2", "millions")));
    }
    cases.extend([
        case(
            "starts before the marker",
            "Cash totaled US$41.2 million.",
            "totaled US$41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "starts after the whole marker",
            "Cash totaled C$41.2 million.",
            "41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "a bare dollar sign of the document",
            CASH,
            "$41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "a bare dollar sign, claimed as such",
            CASH,
            "$41.2 million",
            u("41.2", "millions", "$"),
        ),
        case(
            "a bare dollar sign after a colon",
            "Total:$5 million.",
            "$5 million",
            n("5", "millions"),
        ),
        case(
            "a bare dollar sign after a longer word",
            "They totaled$5 million.",
            "$5 million",
            None,
        ),
        spanned(
            "span of a bare dollar amount",
            CASH,
            "$41.2 million",
            n("41.2", "millions"),
            "$41.2 million",
        ),
        spanned(
            "span of a whole marker",
            "Cash totaled C$41.2 million.",
            "C$41.2 million",
            n("41.2", "millions"),
            "C$41.2 million",
        ),
        case(
            "ends just before the marker",
            "Cash totaled C$41.2 million.",
            "Cash totaled",
            None,
        ),
        case(
            "ends before a currency phrase",
            "We paid 41.2 million Canadian dollars.",
            "41.2 million",
            n("41.2", "millions"),
        ),
        case(
            "holds a whole currency phrase",
            "We paid 41.2 million Canadian dollars.",
            "41.2 million Canadian dollars",
            u("41.2", "millions", "CAD"),
        ),
        case(
            "starts after a currency phrase",
            "We paid 41.2 million Canadian dollars to them.",
            "to them",
            None,
        ),
        case(
            "the same words away from a number",
            "Canadian dollars fell.",
            "Canadian",
            None,
        ),
    ]);
    assert_all_pass(&cases);
}

// ---- bd-22j: a prefix written apart from its `$` ------------------------------------------

/// The prefixes in scope, each with the currency code it points to.
const SPACED: [(&str, &str); 9] = [
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

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

/// The ways a document can put space between `prefix` and `$41.2 million`: the layout's name,
/// the document, and the bytes of it that hold only the amount.
fn layouts(prefix: &str) -> Vec<(&'static str, &'static str, &'static str)> {
    let plain = "$41.2 million";
    [
        (
            "space",
            format!("Cash totaled {prefix} $41.2 million."),
            plain,
        ),
        (
            "line break",
            format!("Cash totaled {prefix}\n$41.2 million."),
            plain,
        ),
        (
            "nbsp entity",
            format!("<p>Cash totaled {prefix}&nbsp;$41.2 million.</p>"),
            plain,
        ),
        (
            "numeric entities",
            format!("<p>Cash totaled {prefix}&#160;&#36;41.2 million.</p>"),
            "&#36;41.2 million",
        ),
        (
            "inline tag",
            format!("Cash totaled <b>{prefix}</b> $41.2 million."),
            plain,
        ),
        (
            "br",
            format!("Cash totaled {prefix}<br>$41.2 million."),
            plain,
        ),
        (
            "table cells",
            format!("<tr><td>Cash totaled {prefix}</td><td>$41.2 million</td></tr>"),
            plain,
        ),
    ]
    .into_iter()
    .map(|(layout, document, amount)| {
        let name = leak(format!("{prefix}, {layout}"));
        (name, leak(document), amount)
    })
    .collect()
}

#[test]
fn a_quote_may_not_cut_a_spaced_currency_prefix() {
    let mut cases = Vec::new();
    for (prefix, _) in SPACED {
        for (name, document, amount) in layouts(prefix) {
            let cut = "$41.2 million";
            cases.push(case(name, document, cut, None));
            cases.push(case(name, document, cut, n("41.2", "millions")));
            cases.push(case(name, document, cut, u("41.2", "millions", "$")));
            cases.push(case(name, document, cut, u("41.2", "millions", "USD")));
            cases.push(spanned(name, document, cut, None, amount));
        }
        let document = leak(format!("Cash totaled {prefix} $41.2 million."));
        let inside = leak(format!("Cash totaled {prefix}"));
        cases.push(case(inside, document, inside, None));
    }
    cases.push(case(
        "ends inside U.S. before its last dot",
        "Cash totaled U.S. $41.2 million.",
        "Cash totaled U.S",
        None,
    ));
    // A sentence that ends "U.S." cannot be told apart from the prefix, so it is refused too.
    cases.push(case(
        "a sentence that ends U.S. before an amount",
        "It is sold in the U.S. $5 million was spent.",
        "$5 million was spent",
        None,
    ));
    assert_none_pass(&cases);
}

#[test]
fn a_spaced_prefix_establishes_no_unit_by_itself() {
    let mut cases = Vec::new();
    for (prefix, code) in SPACED {
        for (name, document, _) in layouts(prefix) {
            let whole = leak(format!("{prefix} $41.2 million"));
            // Not a bare dollar, because the prefix says which; not established as that
            // currency either, because the words in front of a `$` may be prose.
            cases.push(case(name, document, whole, u("41.2", "millions", "$")));
            cases.push(case(name, document, whole, u("41.2", "millions", "USD")));
            cases.push(case(name, document, whole, u("41.2", "millions", code)));
        }
    }
    cases.extend([
        case(
            "CA prefix, USD code after",
            "Cash totaled CA $41.2 million USD.",
            "CA $41.2 million USD",
            u("41.2", "millions", "USD"),
        ),
        case(
            "US prefix, CAD code after",
            "Cash totaled US $41.2 million CAD.",
            "US $41.2 million CAD",
            u("41.2", "millions", "CAD"),
        ),
        case(
            "HK prefix, code before says USD",
            "Cash totaled USD HK $41.2 million.",
            "USD HK $41.2 million",
            u("41.2", "millions", "USD"),
        ),
        case(
            "prefix left out of the quote, unit $",
            "Cash totaled US $41.2 million.",
            "41.2 million",
            u("41.2", "millions", "$"),
        ),
    ]);
    assert_none_pass(&cases);
}

#[test]
fn quotes_that_hold_a_spaced_prefix_still_pass() {
    let mut cases = Vec::new();
    for (prefix, code) in SPACED {
        for (name, document, _) in layouts(prefix) {
            let whole = leak(format!("{prefix} $41.2 million"));
            cases.push(case(name, document, whole, None));
            cases.push(case(name, document, whole, n("41.2", "millions")));
            // Starting after the whole marker keeps the rule it had.
            cases.push(case(name, document, "41.2 million", n("41.2", "millions")));
        }
        let document = leak(format!("Cash totaled {prefix} $41.2 million."));
        let before = leak(format!("totaled {prefix} $41.2 million"));
        cases.push(case(before, document, before, n("41.2", "millions")));
        cases.push(spanned(
            before,
            document,
            before,
            n("41.2", "millions"),
            before,
        ));
        // A code that agrees with the prefix still establishes the unit.
        let coded = leak(format!("Cash totaled {prefix} $41.2 million {code}."));
        let quote = leak(format!("{prefix} $41.2 million {code}"));
        cases.push(case(quote, coded, quote, u("41.2", "millions", code)));
    }
    cases.push(case(
        "attached US$ keeps establishing USD",
        "Cash totaled US$41.2 million.",
        "US$41.2 million",
        u("41.2", "millions", "USD"),
    ));
    assert_all_pass(&cases);
}

/// Ordinary text in front of a `$`: none of it may become part of a marker.
#[test]
fn ordinary_words_before_a_dollar_sign_are_not_markers() {
    let rows = [
        // The pronoun: only the capitals `US` are a prefix.
        ("They told us $5 million was lost.", "$5 million was lost"),
        ("They told Us $5 million was lost.", "$5 million was lost"),
        // A sentence ending "US." has a dot the prefix `US` never has.
        (
            "Sales rose in the US. $5 million came later.",
            "$5 million came later",
        ),
        // "ca." is circa; "Ca." and "CA." end a sentence or abbreviate.
        ("It cost ca. $5 million then.", "$5 million then"),
        ("It cost Ca. $5 million then.", "$5 million then"),
        (
            "We moved to CA. $5 million followed.",
            "$5 million followed",
        ),
        // Lower-case look-alikes are other words (a content network, French "au").
        ("The cdn $5 million budget grew.", "$5 million budget"),
        ("Paid au $5 million rate.", "$5 million rate"),
        // The prefix must be a whole word.
        ("A PLUS $5 million bonus.", "$5 million bonus"),
        ("The BUS $5 million fleet.", "$5 million fleet"),
        ("The EU $5 million grant.", "$5 million grant"),
        // Look-alikes of `Cdn.` and `CDN`: another case, a sentence end, a longer word.
        ("The cdn. $5 million budget grew.", "$5 million budget"),
        (
            "We left the CDN. $5 million came later.",
            "$5 million came later",
        ),
        ("Our CDNs $5 million budget grew.", "$5 million budget"),
        ("The ACDN $5 million deal closed.", "$5 million deal"),
        // Single letters are an open operator choice and are left alone.
        ("Class A $5 million notes.", "$5 million notes"),
        // Punctuation between the word and the sign breaks the marker.
        ("They told US, $5 million was lost.", "$5 million was lost"),
        ("Offices in HK: $5 million each.", "$5 million each"),
    ];
    let mut cases = Vec::new();
    for (document, quote) in rows {
        cases.push(case(document, document, quote, None));
        cases.push(case(document, document, quote, n("5", "millions")));
        cases.push(case(document, document, quote, u("5", "millions", "$")));
    }
    assert_all_pass(&cases);
}

// ---- bd-xas: billions and trillions ---------------------------------------------------------

/// A case whose document is the whole quoted amount in a sentence.
fn amount(quote: &'static str, numeric: Numeric) -> Case {
    let document = leak(format!("The fund reported {quote} today."));
    let name = leak(format!("{quote} claimed {numeric:?}"));
    case(name, document, quote, numeric)
}

#[test]
fn billions_and_trillions_pass_in_any_equivalent_scale() {
    let mut cases = Vec::new();
    for quote in [
        "$1.3 billion",
        "$1.3 billions",
        "$1.3billion",
        "$1.3bn",
        "$1.3 bn",
        "1.3 billion",
        "1.3bn",
        "1.3 BN",
        "$1.3 Billion",
    ] {
        cases.push(amount(quote, n("1.3", "billions")));
        cases.push(amount(quote, n("1300", "millions")));
        cases.push(amount(quote, n("1300000", "thousands")));
        cases.push(amount(quote, n("1300000000", "units")));
        cases.push(amount(quote, n("0.0013", "trillions")));
    }
    for quote in [
        "$2.4 trillion",
        "$2.4 trillions",
        "2.4 trillion",
        "$2.4tn",
        "$2.4 tn",
        "$2.4trn",
        "2.4 trn",
    ] {
        cases.push(amount(quote, n("2.4", "trillions")));
        cases.push(amount(quote, n("2400", "billions")));
        cases.push(amount(quote, n("2400000", "millions")));
        cases.push(amount(quote, n("2400000000000", "units")));
    }
    // Every spelling of the claim's own scale.
    for scale in ["billions", "billion", "bn", "BN", "Billions"] {
        cases.push(amount("$1.3 billion", n("1.3", scale)));
    }
    for scale in ["trillions", "trillion", "tn", "trn", "TRN"] {
        cases.push(amount("$2.4 trillion", n("2.4", scale)));
    }
    cases.extend([
        case(
            "negative",
            "The change was -$1.3 billion.",
            "-$1.3 billion",
            n("-1.3", "billions"),
        ),
        case(
            "hyphenated",
            "A 5-billion-dollar fund closed.",
            "A 5-billion-dollar fund",
            n("5", "billions"),
        ),
        case(
            "bare dollar unit",
            "AUM was $1.3 billion.",
            "$1.3 billion",
            u("1.3", "billions", "$"),
        ),
        case(
            "code unit",
            "AUM was USD 1.3bn.",
            "USD 1.3bn",
            u("1300", "millions", "USD"),
        ),
        case(
            "attached US$",
            "AUM was US$1.3 billion.",
            "US$1.3 billion",
            u("1.3", "billions", "USD"),
        ),
        case(
            "currency name after",
            "Cap was 2.4 trillion U.S. dollars.",
            "2.4 trillion U.S. dollars",
            u("2.4", "trillions", "USD"),
        ),
        case(
            "euro",
            "Cap was \u{20ac}2.4tn.",
            "\u{20ac}2.4tn",
            u("2.4", "trillions", "EUR"),
        ),
        case(
            "tags join a styled amount",
            "<p><b>$1.3</b> <span>billion</span></p>",
            "$1.3 billion",
            n("1.3", "billions"),
        ),
        case(
            "exact decimals",
            "It was $1.000000001 billion.",
            "$1.000000001 billion",
            n("1000000001", "units"),
        ),
        case(
            "count with billion",
            "There are 8 billion people.",
            "8 billion people",
            n("8", "billions"),
        ),
        case(
            "a billionaire is not a scale",
            "We met 5 billionaires.",
            "5 billionaires",
            n("5", "units"),
        ),
    ]);
    assert_all_pass(&cases);
}

#[test]
fn billion_and_trillion_rules_stay_fail_closed() {
    let mut cases = Vec::new();
    for quote in ["$1.3 billion", "$1.3bn", "$2.4 trillion", "$2.4tn"] {
        let digits = if quote.contains("1.3") { "1.3" } else { "2.4" };
        for scale in ["units", "thousands", "millions"] {
            cases.push(amount(quote, n(digits, scale)));
        }
    }
    cases.extend([
        amount("$1.3 billion", n("1.3", "trillions")),
        amount("$2.4 trillion", n("2.4", "billions")),
        amount("$1.3 billion", n("1.300000001", "billions")),
        // A quote may not drop the scale word.
        case(
            "cut the scale",
            "AUM was $1.3 billion.",
            "$1.3",
            n("1.3", "units"),
        ),
        case(
            "cut the scale, quote only",
            "AUM was $1.3 billion.",
            "AUM was $1.3",
            None,
        ),
        case(
            "cut inside the scale",
            "AUM was $1.3 billion.",
            "$1.3 b",
            None,
        ),
        case(
            "cut the attached scale",
            "AUM was $2.4tn.",
            "$2.4",
            n("2.4", "units"),
        ),
        // A lone B or T names nothing for certain.
        amount("$1.3B", n("1.3", "billions")),
        amount("$1.3 B", n("1.3", "billions")),
        amount("1.3b", n("1.3", "billions")),
        amount("$2.4T", n("2.4", "trillions")),
        amount("$2.4 T", n("2.4", "trillions")),
        // Without a currency, tn may be tonnes.
        amount("5 tn", n("5", "trillions")),
        amount("5tn", n("5", "trillions")),
        amount("5 tn", n("5", "units")),
        // Two magnitudes, or a magnitude past a currency.
        case(
            "thousand billion",
            "It was 5 thousand billion.",
            "5 thousand billion",
            n("5", "thousands"),
        ),
        case(
            "billion million",
            "It was 5 billion million.",
            "5 billion million",
            n("5", "billions"),
        ),
        case(
            "billion trillion",
            "It was 5 billion trillion.",
            "5 billion trillion",
            n("5", "billions"),
        ),
        case(
            "hundred billion",
            "It was 5 hundred billion.",
            "5 hundred billion",
            n("5", "billions"),
        ),
        case(
            "million bn",
            "It was 5 million bn.",
            "5 million bn",
            n("5", "millions"),
        ),
        case(
            "thousand trn",
            "It was $5 thousand trn.",
            "$5 thousand trn",
            n("5", "thousands"),
        ),
        case(
            "currency before the scale",
            "Debt was 5 USD billion.",
            "5 USD billion",
            n("5", "units"),
        ),
        // A scale kept from the digits by a mark, a footnote or another element.
        case(
            "asterisk",
            "Cash was $41.2* billion.",
            "$41.2",
            n("41.2", "units"),
        ),
        case(
            "bracketed footnote",
            "Cash was $41.2 [1] trillion.",
            "$41.2",
            n("41.2", "units"),
        ),
        case(
            "dash",
            "A 5\u{2013}billion fund closed.",
            "A 5",
            n("5", "units"),
        ),
        case(
            "sup footnote takes the scale",
            "<p>$1.3<sup>1</sup> billion</p>",
            "1 billion",
            n("1", "billions"),
        ),
        case(
            "next cell",
            "<tr><td>5</td><td>billion served</td></tr>",
            "5 billion",
            n("5", "billions"),
        ),
        // Ranges and lists that share a scale.
        case(
            "range low end",
            "Guidance is 10 to 20 billion.",
            "10",
            n("10", "units"),
        ),
        case(
            "list first",
            "Sales were 1.2, 1.3 and 1.5 trillion.",
            "Sales were 1.2",
            n("1.2", "units"),
        ),
        case(
            "dashed range",
            "Guidance is $10-$20 billion.",
            "$10",
            n("10", "units"),
        ),
        // Signs that cannot be established.
        case(
            "parentheses",
            "Net was (1.3) billion.",
            "(1.3) billion",
            n("1.3", "billions"),
        ),
        case(
            "minus cut",
            "The change was -1.3 billion.",
            "1.3 billion",
            n("1.3", "billions"),
        ),
        // A document-wide scale leaves bare numbers unbound.
        case(
            "in billions",
            "Amounts in billions. Loans 12 branches.",
            "12 branches",
            n("12", "units"),
        ),
        case(
            "in trillions",
            "<p>(in trillions)</p><p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "millions of the header word trillions",
            "Trillions of dollars\nCash 412",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "sterling bn header",
            "<p>Figures in \u{a3}bn</p><p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        case(
            "dollar tn header",
            "<p>Assets ($tn)</p><p>Cash 412</p>",
            "Cash 412",
            n("412", "units"),
        ),
        // Other languages and long-scale texts.
        case(
            "milliards",
            "Es waren 5 milliards.",
            "5 milliards",
            n("5", "billions"),
        ),
        case(
            "german plural",
            "Es waren 5 Billionen.",
            "5 Billionen",
            n("5", "billions"),
        ),
        case(
            "german plural as units",
            "Es waren 5 Billionen.",
            "5 Billionen",
            n("5", "units"),
        ),
        case(
            "spanish plural",
            "Fueron 5 billones.",
            "5 billones",
            n("5", "units"),
        ),
        case(
            "spanish singular",
            "Fue 1 bill\u{f3}n.",
            "1 bill\u{f3}n",
            n("1", "units"),
        ),
        case(
            "long scale beside Milliarden",
            "Es waren 2 Milliarden und dann 1 Billion.",
            "dann 1 Billion",
            n("1", "billions"),
        ),
        // Overflow of the exact arithmetic.
        case(
            "document beyond the range",
            "It was 99999999999999999999999999999999 trillion.",
            "99999999999999999999999999999999 trillion",
            n("1", "trillions"),
        ),
    ]);
    assert_none_pass(&cases);
}

#[test]
fn ambiguous_claim_scales_and_overflow_are_refused() {
    for scale in ["b", "t", "B", "T", "bil", "mrd"] {
        let line = format!(
            r#"{{"claim_id":"c","source":"filing","quote":"x","value":1,"scale":"{scale}"}}"#
        );
        assert!(parse_claims_jsonl(line.as_bytes()).is_err(), "{scale}");
    }
    let line = r#"{"claim_id":"c","source":"filing","quote":"x","value":99999999999999999999999999999,"scale":"trillions"}"#;
    assert!(parse_claims_jsonl(line.as_bytes()).is_err());
}
