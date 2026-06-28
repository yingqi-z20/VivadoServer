use crate::error::AppError;
use std::path::{Path, PathBuf};

pub fn resolve_project_dir(workspace_root: &Path, project: &str) -> Result<PathBuf, AppError> {
    validate_project_name(project)?;
    Ok(workspace_root.join(project))
}

fn validate_project_name(project: &str) -> Result<(), AppError> {
    if project.is_empty() || project == "." || project == ".." {
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
        for name in ["", ".", "..", "../x", "a/b", r"a\b", "a:b", "中文"] {
            assert!(validate_project_name(name).is_err(), "{name}");
        }
    }
}
