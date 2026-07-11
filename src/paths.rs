use crate::error::AppError;
use std::path::{Path, PathBuf};

pub fn resolve_project_dir(workspace_root: &Path, project: &str) -> Result<PathBuf, AppError> {
    validate_project_name(project)?;
    Ok(workspace_root.join(project))
}

fn validate_project_name(project: &str) -> Result<(), AppError> {
    if project.is_empty()
        || project == "."
        || project == ".."
        || project.len() > 64
        || !validate_portable_segment(project)
    {
        return Err(AppError::InvalidProject);
    }

    if project
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        Ok(())
    } else {
        Err(AppError::InvalidProject)
    }
}

pub(crate) fn validate_portable_segment(segment: &str) -> bool {
    if segment.is_empty()
        || segment.len() > 255
        || segment.ends_with(['.', ' '])
        || segment.chars().any(|ch| ch <= '\u{1f}')
    {
        return false;
    }
    let stem = segment.split('.').next().unwrap_or(segment);
    let upper = stem.to_ascii_uppercase();
    !matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !(upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && matches!(upper.as_bytes()[3], b'1'..=b'9'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_safe_project_names() {
        assert!(validate_project_name("demo").is_ok());
        assert!(validate_project_name("demo-1_2.xpr").is_ok());
    }

    #[test]
    fn rejects_path_escape_project_names() {
        for name in [
            "", ".", "..", "../x", "a/b", r"a\b", "a:b", "中文", "demo.", "CON", "com1.xpr",
        ] {
            assert!(validate_project_name(name).is_err(), "{name}");
        }
    }
}
