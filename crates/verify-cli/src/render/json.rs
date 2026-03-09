pub fn scaffold_message(surface: &str) -> String {
    serde_json::json!({
        "tool": "verify",
        "status": "scaffold_only",
        "surface": surface,
    })
    .to_string()
}
