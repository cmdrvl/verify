//! Excel workbook bindings (`.xlsx`, `.xlsm`, `.xls`) through the shared `cmdrvl-tabular` reader.
//!
//! The selected worksheet is read by `cmdrvl-tabular` (canonical cell text, ambiguous and
//! unknown sheets refused, a decompressed-size guard against zip bombs) and written to a
//! temporary CSV that DuckDB then loads exactly like a CSV binding. Going through CSV keeps
//! type inference, null handling and every downstream rule identical to a CSV of the same
//! data, which is what the golden parity tests check.
//!
//! The temporary file lives in the OS temp directory only for the duration of the load and is
//! removed when the returned handle is dropped. It is written before the connection is locked
//! down, so it never needs external access afterwards.

use std::{io, path::Path};

use cmdrvl_tabular::{OpenOptions, SheetSel, open};
use tempfile::NamedTempFile;

use crate::bindings::BatchBindingError;

/// Extensions treated as Excel workbooks.
pub const WORKBOOK_EXTENSIONS: &[&str] = &["xlsx", "xlsm", "xls"];

pub fn is_workbook(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            WORKBOOK_EXTENSIONS
                .iter()
                .any(|known| known.eq_ignore_ascii_case(extension))
        })
}

/// Split `report.xlsx#Sheet1` into the workbook path and its sheet selector.
///
/// Only a `#` after a workbook extension counts, and only when the whole text is not itself an
/// existing file, so a real file that happens to contain `#` keeps working.
pub fn split_sheet_selector(spec: &str) -> (std::path::PathBuf, Option<String>) {
    if let Some((workbook, sheet)) = spec.rsplit_once('#')
        && is_workbook(Path::new(workbook))
        && !Path::new(spec).exists()
    {
        return (std::path::PathBuf::from(workbook), Some(sheet.to_owned()));
    }
    (std::path::PathBuf::from(spec), None)
}

fn refusal_error(
    binding: &str,
    path: &Path,
    refusal: &cmdrvl_tabular::Refusal,
) -> BatchBindingError {
    use cmdrvl_tabular::RefusalCode;

    if refusal.code == RefusalCode::EIo {
        return BatchBindingError::Io {
            binding: binding.to_owned(),
            path: path.to_path_buf(),
            source: io::Error::other("cannot read workbook"),
        };
    }
    // cmdrvl-tabular details and contexts carry codes, sheet names, headers and coordinates
    // only, never cell values or library error text.
    let context = if refusal.context.is_empty() {
        String::new()
    } else {
        format!(
            " {}",
            serde_json::to_string(&refusal.context).unwrap_or_default()
        )
    };
    BatchBindingError::FormatDetect {
        binding: binding.to_owned(),
        path: path.to_path_buf(),
        detail: format!("{}: {}{context}", refusal.code, refusal.detail),
    }
}

/// Read one worksheet of `path` and write it as a CSV in a temporary file.
///
/// `sheet` selects by name or 0-based index (digits become an index unless a sheet has that
/// exact name). A workbook with several sheets and no selector is refused, deterministically.
pub(crate) fn to_temp_csv(
    binding: &str,
    path: &Path,
    sheet: Option<&str>,
) -> Result<NamedTempFile, BatchBindingError> {
    let options = OpenOptions {
        sheet: sheet.map(SheetSel::parse),
        ..OpenOptions::default()
    };
    let mut table =
        open(path, &options).map_err(|refusal| refusal_error(binding, path, &refusal))?;

    let temp_io = |source: io::Error| BatchBindingError::Io {
        binding: binding.to_owned(),
        path: path.to_path_buf(),
        source,
    };
    let mut file = tempfile::Builder::new()
        .prefix("verify-xlsx-")
        .suffix(".csv")
        .tempfile()
        .map_err(temp_io)?;
    {
        let mut writer = csv::Writer::from_writer(&mut file);
        writer
            .write_record(table.headers())
            .map_err(|error| temp_io(io::Error::other(error.to_string())))?;
        for row in table.rows() {
            let row = row.map_err(|refusal| refusal_error(binding, path, &refusal))?;
            writer
                .write_record(row.cells.iter().map(|cell| cell.as_bytes()))
                .map_err(|error| temp_io(io::Error::other(error.to_string())))?;
        }
        writer.flush().map_err(temp_io)?;
    }
    Ok(file)
}
