pub mod human;
pub mod json;

pub fn scaffold_message(surface: &str, json_output: bool) -> String {
    if json_output {
        json::scaffold_message(surface)
    } else {
        human::scaffold_message(surface)
    }
}
