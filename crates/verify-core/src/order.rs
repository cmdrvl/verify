use std::cmp::Ordering;

pub fn cmp_option_str(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub fn sort_strings(values: &mut [String]) {
    values.sort();
}
