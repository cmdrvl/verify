//! bd-w2p: `ground` must never PASS a claim its document does not support, and must keep
//! passing claims it does. Every document here is synthetic.

use std::collections::BTreeMap;

use verify_core::report::Outcome;
use verify_engine::grounding::{GroundingDocument, evaluate, parse_claims_jsonl};

const BEAD: &str =
    "As of September 30, cash totaled $41.2 million. The notes are nonconvertible.";
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

fn case(
    name: &'static str,
    document: &'static str,
    quote: &'static str,
    numeric: Numeric,
) -> Case {
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
        let start = case
            .document
            .find(span)
            .unwrap_or_else(|| panic!("{}: span is not in the document", case.name));
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
            case("quote cuts a number", BEAD, "1.2 million", n("1.2", "millions")),
            "E_NUMBER_NOT_IN_SPAN+E_QUOTE_NOT_FOUND",
        ),
        (case("quote inside a word", BEAD, "convertible", None), "E_QUOTE_NOT_FOUND"),
        (
            case("wrong currency", BEAD, "cash totaled $41.2 million", u("41.2", "millions", "EUR")),
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
        spanned("cuts a number", BEAD, "1.2 million", n("1.2", "millions"), "1.2 million"),
        spanned("inside a word", BEAD, "convertible", None, "convertible"),
        spanned("wrong currency", BEAD, "$41.2 million", u("41.2", "millions", "EUR"), "$41.2 million"),
        spanned("binds the date number", BEAD, "As of September 30, cash totaled", n("30", "millions"), "As of September 30, cash totaled"),
        spanned("hidden script text glued to visible", "<p>a</p><script>var x='debt is zero</script><p>.</p>", "debt is zero.", None, "debt is zero</script><p>."),
        spanned("starts inside a comment", "<p>Revenue</p><!-- fell --><p>rose</p>", "fell -->rose", None, "fell --><p>rose"),
        spanned("raw quote carries a comment", "<p>x</p><!-- debt is zero --><p>cash</p>", "<!-- debt is zero --><p>cash</p>", None, "<!-- debt is zero --><p>cash</p>"),
        spanned("raw quote carries an attribute", "<p title=\"debt is zero\">cash</p>", "<p title=\"debt is zero\">cash</p>", None, "<p title=\"debt is zero\">cash</p>"),
        spanned("blank quote", "Cash   fell.", "  ", None, "   "),
        spanned("half an entity", "<p>A &amp; B</p>", "amp; B", None, "amp; B"),
    ]);
}

#[test]
fn a_quote_may_not_cut_a_number() {
    assert_none_pass(&[
        case("leading digits", CASH, "1.2 million", n("1.2", "millions")),
        case("before the decimals", CASH, "Cash totaled $41", n("41", "units")),
        case("after the decimal point", CASH, "2 million", n("2", "millions")),
        case("trailing digits", "Cash was 41.25 million.", "Cash was 41.2", n("41.2", "units")),
        case("thousands group head", "We sold 1,100 units.", "We sold 1", n("1", "units")),
        case("thousands group tail", "We sold 1,100 units.", "100 units", n("100", "units")),
        case("quote only, inside a number", "Cash was 141 dollars.", "41 dollars", None),
        case("scale word cut", CASH, "Cash totaled $41.2", n("41.2", "units")),
        case("scale word cut, quote only", CASH, "Cash totaled $41.2", None),
        case("percent cut", "The rate was 41.2% then.", "The rate was 41.2", n("41.2", "units")),
        case("sign cut", "The change was -5 million.", "5 million", n("5", "millions")),
        case("minus before currency", "Net result: -$5 million.", "$5 million", n("5", "millions")),
        case("unicode minus", "The change was \u{2212}5 million.", "5 million", n("5", "millions")),
        case("minus entity", "<p>The change was &minus;5 million.</p>", "5 million", n("5", "millions")),
        case("numeric minus entity", "<p>The change was &#8722;5 million.</p>", "5 million", n("5", "millions")),
        case("parenthesised", "Net income (loss) was (5.0) million.", "5.0", n("5", "millions")),
        case("parenthesised currency inside", "Net result was ($5.0 million) overall.", "$5.0 million", n("5", "millions")),
    ]);
}

#[test]
fn a_number_whose_value_cannot_be_established_does_not_bind() {
    assert_none_pass(&[
        case("malformed grouping", "Cash was 1,00 dollars.", "Cash was 1,00 dollars", n("100", "units")),
        case("european decimal comma", "Cash was 41,2 million.", "Cash was 41,2 million", n("412", "millions")),
        case("dotted grouping", "Cash was 1.234.567 today.", "Cash was 1.234.567 today", n("567", "units")),
        case("spaced grouping", "Cash was 41 200 today.", "Cash was 41", n("41", "units")),
        case("apostrophe grouping", "Cash was 41'200 today.", "Cash was 41'200", n("41", "units")),
        case("scientific notation", "Load was 4e6 watts.", "Load was 4e6 watts", n("4", "units")),
        case("multiple suffix", "Leverage was 3.5x at year end.", "Leverage was 3.5x", n("3.5", "millions")),
        case("digits inside an identifier", "Results for Q3 improved.", "Results for Q3 improved", n("3", "units")),
        case("digits inside a form name", "See the Form 10-K filed.", "See the Form 10-K filed", n("10", "units")),
        case("digits after a hyphen", "COVID-19 costs rose.", "COVID-19 costs rose", n("19", "units")),
        case("fraction", "Owns 1/2 of the venture.", "Owns 1/2 of the venture", n("2", "units")),
        case("ratio", "The ratio was 3:1 then.", "The ratio was 3:1", n("1", "units")),
        case("superscript glued", "Area was 41\u{b2} then.", "Area was 41\u{b2}", n("41", "units")),
        case("spaced minus", "The change was - 5 million.", "5 million", n("5", "millions")),
        case("en dash as minus", "The change was \u{2013}5 million.", "5 million", n("5", "millions")),
        case("word negative", "The change was negative 5 million.", "5 million", n("5", "millions")),
        case("parenthesised with currency", "Net result was $(5.0) million.", "$(5.0) million", n("5", "millions")),
        case("parenthesised, negative claim", "Net result was (5.0) million.", "(5.0) million", n("-5", "millions")),
        case("trailing minus", "Balance 500- carried.", "Balance 500- carried", n("500", "units")),
        case("positive claim of a negative", "The change was -5 million.", "-5 million", n("5", "millions")),
        case("billion", "Debt was $41.2 billion.", "$41.2 billion", n("41.2", "units")),
        case("bn", "Debt was $41.2bn.", "$41.2bn", n("41.2", "units")),
        case("trillion", "Assets were 2 trillion.", "2 trillion", n("2", "units")),
        case("MM", "Debt was $41.2MM.", "$41.2MM", n("41.2", "units")),
        case("mil", "Debt was $41.2 mil.", "$41.2 mil", n("41.2", "units")),
        case("hundred", "About 5 hundred homes.", "5 hundred homes", n("5", "units")),
        case("two scale words", "It was 5 thousand million.", "5 thousand million", n("5", "thousands")),
        case("hyphenated scale", "A 5-million-dollar loan closed.", "A 5-million-dollar loan", n("5", "units")),
        case("hyphenated percent", "A 5-percent fee applied.", "A 5-percent fee", n("5", "units")),
        case("per cent read as units", "A fee of 5 per cent applied.", "5 per cent", n("5", "units")),
        case("percentage points", "Margin rose 5 percentage points.", "5 percentage points", n("5", "units")),
        case("metres read as millions", "The mast is 5 m tall.", "The mast is 5 m tall", n("5", "millions")),
        case("low end of a worded range", "Guidance is 10 to 20 million.", "10", n("10", "units")),
        case("low end of a dashed range", "Guidance is $10-$20 million.", "$10", n("10", "units")),
        case("between-and range", "Between 10 and 20 million shares.", "Between 10", n("10", "units")),
        case("table declares thousands", "Amounts in thousands. Cash 41,200", "Cash 41,200", n("41200", "units")),
        case("header declares millions", "<p>($ in millions)</p><p>Cash 412</p>", "Cash 412", n("412", "units")),
        case("percent claim of a bare number", "There are 0.05 shares each.", "0.05 shares", n("5", "percent")),
        case("units claim of a percentage", "The rate was 5% then.", "The rate was 5%", n("0.05", "units")),
        case("numeric date", "Signed 9/30/2024 here.", "Signed 9/30/2024", n("30", "units")),
        case("iso date", "Signed 2024-09-30 here.", "Signed 2024-09-30", n("2024", "units")),
        case("section number", "See Section 4.2.1 now.", "See Section 4.2.1 now", n("4.2", "units")),
        case("year range", "Over 2023-2024 it grew.", "Over 2023-2024", n("2024", "units")),
    ]);
}

#[test]
fn more_than_one_number_in_a_quote_does_not_bind() {
    assert_none_pass(&[
        case("two amounts, first claimed", "Cash was $41 million and debt $9 million.", "Cash was $41 million and debt $9 million", n("41", "millions")),
        case("two amounts, second claimed", "Cash was $41 million and debt $9 million.", "Cash was $41 million and debt $9 million", n("9", "millions")),
        case("year plus amount, year claimed", "In 2024 revenue was $5 million.", "In 2024 revenue was $5 million", n("2024", "units")),
        case("year plus amount, amount claimed", "In 2024 revenue was $5 million.", "In 2024 revenue was $5 million", n("5", "millions")),
        case("date plus amount, amount claimed", BEAD, "As of September 30, cash totaled $41.2 million.", n("41.2", "millions")),
    ]);
}

#[test]
fn a_unit_passes_only_when_the_document_states_it() {
    assert_none_pass(&[
        case("euro claimed as USD", "Cash totaled \u{20ac}41.2 million.", "\u{20ac}41.2 million", u("41.2", "millions", "USD")),
        case("pound claimed as USD", "Cash totaled \u{a3}41.2 million.", "41.2 million", u("41.2", "millions", "USD")),
        case("C$ claimed as USD", "Cash totaled C$41.2 million.", "$41.2 million", u("41.2", "millions", "USD")),
        case("C$ claimed as a bare dollar", "Cash totaled C$41.2 million.", "C$41.2 million", u("41.2", "millions", "$")),
        case("CAD claimed as USD", "Cash totaled CAD 41.2 million.", "41.2 million", u("41.2", "millions", "USD")),
        case("code after", "Cash totaled 41.2 million EUR.", "41.2 million EUR", u("41.2", "millions", "USD")),
        case("currency word after", "Cash totaled 41.2 million euros.", "41.2 million euros", u("41.2", "millions", "USD")),
        case("no currency anywhere", "We issued 41.2 million shares.", "41.2 million shares", u("41.2", "millions", "USD")),
        case("unknown unit", CASH, "$41.2 million", u("41.2", "millions", "bananas")),
        case("another number's currency", "We paid $9 and issued 5 million shares.", "5 million shares", u("5", "millions", "USD")),
        case("conflicting markers", "Cash totaled \u{20ac}41.2 million USD.", "41.2 million", u("41.2", "millions", "USD")),
        case("bare dollar sign is not USD", CASH, "$41.2 million", u("41.2", "millions", "USD")),
    ]);
}

#[test]
fn a_quote_may_not_cut_a_word() {
    assert_none_pass(&[
        case("hyphenated tail", "The notes are non-convertible.", "convertible", None),
        case("unicode hyphen", "The notes are non\u{2010}convertible.", "convertible", None),
        case("non-breaking hyphen", "The notes are non\u{2011}convertible.", "convertible", None),
        case("hyphenated head", "It is a profit-sharing plan.", "It is a profit", None),
        case("soft hyphen", "The notes are non\u{ad}convertible.", "convertible", None),
        case("zero width space", "<p>The notes are non&#8203;convertible.</p>", "convertible", None),
        case("accented continuation", "The caf\u{e9}s closed.", "The caf", None),
        case("combining accent", "The cafe\u{301} closed.", "The cafe", None),
        case("non-latin", "\u{394}\u{3b5}\u{3bb}\u{3c4}\u{3b1} rose.", "\u{3b5}\u{3bb}\u{3c4}\u{3b1} rose", None),
        case("underscore", "see net_income here", "income here", None),
        case("contraction", "We can't repay.", "We can", None),
        case("curly contraction", "We won\u{2019}t repay.", "We won", None),
        case("word tail", "The firm is insolvent.", "solvent", None),
        case("word head", "No debts are owed.", "No debt", None),
    ]);
}

#[test]
fn html_normalisation_does_not_invent_text() {
    assert_none_pass(&[
        case("table cells glue into a number", "<table><tr><td>41</td><td>2</td></tr></table>", "412", n("412", "units")),
        case("table cells glue into words", "<table><tr><td>cash</td><td>flow</td></tr></table>", "cashflow", None),
        case("paragraphs glue", "<p>Total debt 4</p><p>1.2 million</p>", "41.2 million", n("41.2", "millions")),
        case("line break glues", "owed 4<br>1 dollars", "41 dollars", n("41", "units")),
        case("footnote glued to a number", "<p>Cash $41.2<sup>1</sup> million</p>", "$41.21 million", n("41.21", "millions")),
        case("a less-than sign swallows text", "Cash was < 5 million; debt > 3 million.", "Cash was 3 million", n("3", "millions")),
    ]);
}

#[test]
fn supported_quotes_still_pass() {
    assert_all_pass(&[
        case("whole word", BEAD, "nonconvertible", None),
        case("sentence with period", BEAD, "The notes are nonconvertible.", None),
        case("sentence without period", BEAD, "The notes are nonconvertible", None),
        case("whole hyphenated word", "The notes are non-convertible.", "non-convertible", None),
        case("inside quotation marks", "He said \"cash is tight\" today.", "cash is tight", None),
        case("inside single quotes", "He said 'cash is tight' today.", "cash is tight", None),
        case("inside parentheses", "Liquidity (cash on hand) fell.", "cash on hand", None),
        case("before a possessive", "The Company's cash fell.", "The Company", None),
        case("before a curly possessive", "The Company\u{2019}s cash fell.", "The Company", None),
        case("before a plural possessive", "The lenders' consent is needed.", "The lenders", None),
        case("before a comma", "Cash, net of debt, fell.", "net of debt", None),
        case("before a colon", "Summary: cash fell.", "Summary", None),
        case("after an em dash", "Results\u{2014}cash fell.", "cash fell", None),
        case("before an em dash", "Cash fell\u{2014}sharply.", "Cash fell", None),
        case("after a spaced hyphen", "Item 1 - cash fell.", "cash fell", None),
        case("after a slash", "Assets and/or cash fell.", "cash fell", None),
        case("before an ellipsis", "Cash fell... then rose.", "Cash fell", None),
        case("after a semicolon", "Cash fell; debt rose.", "debt rose", None),
        case("before a question mark", "Did cash fall? Yes.", "Did cash fall", None),
        case("collapsed whitespace", "Cash\n  totaled\t$41.2   million.", "Cash totaled $41.2 million.", None),
        case("ends at an amount", CASH, "$41.2 million", None),
        case("ends at punctuation", CASH, "$41.2 million.", None),
        case("accented word", "The caf\u{e9} closed.", "The caf\u{e9}", None),
        case("year", "Fiscal 2024 was strong.", "Fiscal 2024", None),
        case("number before a period", "We opened store 12.", "We opened store 12", None),
        case("number before a comma", "We had 12, then more.", "We had 12", None),
        case("identifier", "See the Form 10-K filed.", "Form 10-K", None),
        case("amount with its scale", CASH, "totaled $41.2 million", None),
        case("omits the currency symbol", CASH, "41.2 million", None),
        case("overlapping second occurrence", "xab ab", "ab", None),
        case("less-than text is kept", "Cash was < 5 million today.", "Cash was < 5 million", None),
        case("inline tags split a word", "<p><span>Rev</span><span>enue rose</span></p>", "Revenue rose", None),
        case("table cells quoted with a space", "<table><tr><td>Cash</td><td>$41.2 million</td></tr></table>", "Cash $41.2 million", None),
        spanned("span covers a whole element", "<p>Real&nbsp;<b>&amp;</b>\ntext</p>", "Real & text", None, "<p>Real&nbsp;<b>&amp;</b>\ntext</p>"),
        spanned("span of plain words", BEAD, "nonconvertible", None, "nonconvertible"),
    ]);
}

#[test]
fn supported_numbers_still_pass() {
    assert_all_pass(&[
        case("plain", CASH, "Cash totaled $41.2 million.", n("41.2", "millions")),
        case("same value in units", CASH, "$41.2 million", n("41200000", "units")),
        case("same value in thousands", CASH, "$41.2 million", n("41200", "thousands")),
        case("narrow quote avoids the date", BEAD, "cash totaled $41.2 million", n("41.2", "millions")),
        case("grouped integer", "We sold 1,100 units.", "We sold 1,100 units", n("1100", "units")),
        case("grouped to millions", "Cash was $41,200,000 then.", "$41,200,000", n("41.2", "millions")),
        case("percent sign", "The rate was 4.25% then.", "4.25%", n("4.25", "percent")),
        case("percent word", "The rate was 4.25 percent then.", "4.25 percent", n("4.25", "percent")),
        case("per cent", "A fee of 5 per cent applied.", "5 per cent", n("5", "percent")),
        case("percent at sentence end", "Occupancy was 95%.", "Occupancy was 95%", n("95", "percent")),
        case("bps", "The margin was 125 bps.", "125 bps", n("125", "bps")),
        case("basis points", "The margin was 125 basis points.", "125 basis points", n("125", "bps")),
        case("bps as percent", "The margin was 125 bps.", "125 bps", n("1.25", "percent")),
        case("thousand", "Fees were 8 thousand.", "8 thousand", n("8", "thousands")),
        case("K with a currency", "Fees were $8K.", "$8K", n("8", "thousands")),
        case("M with a currency", "Fees were $8M.", "$8M", n("8", "millions")),
        case("leading decimal point", "The factor was .5 then.", "The factor was .5", n("0.5", "units")),
        case("trailing period", "We opened store 12.", "store 12.", n("12", "units")),
        case("noun follows", "We issued 41.2 million shares.", "41.2 million shares", n("41.2", "millions")),
        case("hyphenated scale", "A 5-million-dollar loan closed.", "A 5-million-dollar loan", n("5", "millions")),
        case("plus sign", "The change was +5 million.", "+5 million", n("5", "millions")),
        case("negative", "The change was -5 million.", "-5 million", n("-5", "millions")),
        case("negative with currency", "Net result: -$5 million.", "-$5 million", n("-5", "millions")),
        case("inside quotation marks", "The cap is \"$5 million\" now.", "$5 million", n("5", "millions")),
        case("before a parenthetical", "Cash was $5 million (unaudited).", "Cash was $5 million", n("5", "millions")),
        case("after a spaced em dash", "Total \u{2014} $5 million.", "$5 million", n("5", "millions")),
        case("beside a quarter name", "Q3 revenue was $5 million.", "Q3 revenue was $5 million", n("5", "millions")),
        case("beside a form name", "The 10-K shows $5 million.", "The 10-K shows $5 million", n("5", "millions")),
        case("beside a tenor", "It sold $5 million of 10-year notes.", "It sold $5 million of 10-year notes", n("5", "millions")),
        case("inline tags split a number", "<p><font>$41</font><font>.2</font> <b>million</b></p>", "$41.2 million", n("41.2", "millions")),
        case("inline XBRL", "<p>$<ix:nonFraction name=\"x\">41.2</ix:nonFraction> million</p>", "$41.2 million", n("41.2", "millions")),
        case("non-breaking space before the scale", "<p>$41.2&nbsp;million</p>", "$41.2 million", n("41.2", "millions")),
        case("xml element value", "<a><amount>95000000.00</amount>\n<term>120</term></a>", "95000000.00", n("95", "millions")),
        spanned("raw markup span", "<a><amt>95000000.00</amt></a>", "<amt>95000000.00</amt>", n("95", "millions"), "<amt>95000000.00</amt>"),
    ]);
}

#[test]
fn stated_units_pass() {
    assert_all_pass(&[
        case("US$", "Cash totaled US$41.2 million.", "US$41.2 million", u("41.2", "millions", "USD")),
        case("code before", "Cash totaled USD 41.2 million.", "USD 41.2 million", u("41.2", "millions", "USD")),
        case("code glued before", "Cash totaled USD41.2 million.", "USD41.2 million", u("41.2", "millions", "USD")),
        case("code after, lower-case claim", "Cash totaled 41.2 million USD.", "41.2 million USD", u("41.2", "millions", "usd")),
        case("U.S. dollars", "Cash totaled 41.2 million U.S. dollars.", "41.2 million U.S. dollars", u("41.2", "millions", "USD")),
        case("quote omits the code", "Cash totaled USD 41.2 million.", "41.2 million", u("41.2", "millions", "USD")),
        case("code and a dollar sign", "Cash totaled $41.2 million USD.", "$41.2 million USD", u("41.2", "millions", "USD")),
        case("dollar sign claimed as such", CASH, "$41.2 million", u("41.2", "millions", "$")),
        case("euro sign", "Cash totaled \u{20ac}41.2 million.", "\u{20ac}41.2 million", u("41.2", "millions", "EUR")),
        case("euro word", "Cash totaled 41.2 million euros.", "41.2 million euros", u("41.2", "millions", "EUR")),
        case("pound sign", "Cash totaled \u{a3}41.2 million.", "\u{a3}41.2 million", u("41.2", "millions", "GBP")),
        case("pound entity", "<p>Cash totaled &pound;41.2 million.</p>", "41.2 million", u("41.2", "millions", "GBP")),
        case("C$", "Cash totaled C$41.2 million.", "C$41.2 million", u("41.2", "millions", "CAD")),
        case("percent unit", "The rate was 4.25% then.", "4.25%", u("4.25", "percent", "%")),
        case("basis point unit", "The margin was 125 bps.", "125 bps", u("125", "bps", "bps")),
    ]);
}
