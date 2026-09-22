//! Shared workspace boundary and Linux path validation.
//!
//! Links are rejected, including links in workspace ancestors. Callers must
//! re-check immediately before mutations: ordinary path APIs cannot make a
//! complete check-and-use sequence atomic against another local writer. The
//! workspace therefore belongs to a dedicated, trusted service account.

use crate::error::AppError;
use std::{
    fs::Metadata,
    path::{Component, Path, PathBuf},
};

pub(crate) const INTERNAL_DIR: &str = ".vivado-server";

pub(crate) fn resolve_project_dir(
    workspace_root: &Path,
    project: &str,
) -> Result<PathBuf, AppError> {
    validate_project_name(project)?;
    Ok(workspace_root.join(project))
}

pub(crate) async fn ensure_project_dir(
    workspace_root: &Path,
    project: &str,
) -> Result<PathBuf, AppError> {
    let root = workspace_root.to_path_buf();
    let project = project.to_string();
    tokio::task::spawn_blocking(move || {
        ensure_real_directory_chain(&root)?;
        let project_dir = resolve_project_dir(&root, &project)?;
        match std::fs::symlink_metadata(&project_dir) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&project_dir).map_err(|err| {
                    AppError::Internal(format!("failed to create project directory: {err}"))
                })?;
            }
            Err(err) => {
                return Err(AppError::Internal(format!(
                    "failed to inspect project directory: {err}"
                )));
            }
        }
        ensure_safe_project_root_blocking(&root, &project_dir)?;
        Ok(project_dir)
    })
    .await
    .map_err(|err| AppError::Internal(format!("project path task failed: {err}")))?
}

pub(crate) async fn ensure_safe_project_root(
    workspace_root: PathBuf,
    project_dir: PathBuf,
) -> Result<(), AppError> {
    tokio::task::spawn_blocking(move || {
        ensure_safe_project_root_blocking(&workspace_root, &project_dir)
    })
    .await
    .map_err(|err| AppError::Internal(format!("project path check failed: {err}")))?
}

pub(crate) fn ensure_safe_project_root_blocking(
    workspace_root: &Path,
    project_dir: &Path,
) -> Result<(), AppError> {
    ensure_real_directory_chain(workspace_root)?;
    let canonical_root = workspace_root.canonicalize().map_err(|err| {
        AppError::Internal(format!("failed to canonicalize workspace root: {err}"))
    })?;
    let metadata = std::fs::symlink_metadata(project_dir)
        .map_err(|err| AppError::Internal(format!("failed to inspect project directory: {err}")))?;
    if !metadata.is_dir() || metadata_is_link(&metadata) {
        return Err(AppError::BadRequest(
            "project path must be a real directory, not a symbolic link".to_string(),
        ));
    }
    let canonical_project = project_dir.canonicalize().map_err(|err| {
        AppError::Internal(format!("failed to canonicalize project directory: {err}"))
    })?;
    if canonical_project.parent() != Some(canonical_root.as_path()) {
        return Err(AppError::BadRequest(
            "project directory escapes workspace_root".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn ensure_path_has_no_links(
    root: PathBuf,
    normalized_path: String,
) -> Result<(), AppError> {
    if normalized_path
        .split('/')
        .any(|part| !validate_portable_segment(part))
    {
        return Err(AppError::BadRequest("invalid workspace path".to_string()));
    }
    tokio::task::spawn_blocking(move || {
        ensure_real_directory_chain(&root)?;
        let mut current = root;
        for part in normalized_path.split('/') {
            current.push(part);
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata_is_link(&metadata) => {
                    return Err(AppError::BadRequest(format!(
                        "symbolic links are not supported: {normalized_path}"
                    )));
                }
                Ok(metadata) if !metadata.is_dir() => break,
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(err) => {
                    return Err(AppError::Internal(format!(
                        "failed to inspect workspace path: {err}"
                    )));
                }
            }
        }
        Ok(())
    })
    .await
    .map_err(|err| AppError::Internal(format!("workspace path check failed: {err}")))?
}

/// Validate every existing ancestor, before canonicalization could conceal a link.
pub(crate) fn ensure_real_directory_chain(path: &Path) -> Result<(), AppError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| AppError::Internal(format!("failed to get current directory: {err}")))?
            .join(path)
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::Normal(_) => current.push(component.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(AppError::BadRequest(
                    "directory paths must not contain parent traversal".to_string(),
                ));
            }
        }
        let metadata = std::fs::symlink_metadata(&current)
            .map_err(|err| AppError::Internal(format!("failed to inspect directory: {err}")))?;
        if !metadata.is_dir() || metadata_is_link(&metadata) {
            return Err(AppError::BadRequest(
                "directory paths must contain only real directories".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_project_name(project: &str) -> Result<(), AppError> {
    if project.is_empty()
        || project == "."
        || project == ".."
        || project.len() > 64
        || !validate_portable_segment(project)
        || !project
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(AppError::InvalidProject);
    }
    Ok(())
}

pub(crate) fn validate_portable_segment(segment: &str) -> bool {
    if segment.is_empty()
        || matches!(segment, "." | ".." | INTERNAL_DIR)
        || segment.len() > 255
        || segment.chars().any(|ch| {
            ch <= '\u{1f}'
                || ch == '\u{7f}'
                || matches!(ch, '<' | '>' | '"' | '|' | '?' | '*' | ':' | '/' | '\\')
        })
    {
        return false;
    }

    true
}

pub(crate) fn metadata_is_link(metadata: &Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_reject_traversal_reserved_metadata_and_ambiguous_wire_paths() {
        for segment in [
            "",
            ".",
            "..",
            "a:b",
            "a?b",
            "a*b",
            "a|b",
            ".vivado-server",
            "line\nfeed",
        ] {
            assert!(!validate_portable_segment(segment), "{segment:?}");
        }
        assert!(validate_portable_segment("top-module_1.tcl"));
        assert!(validate_portable_segment("CON"));
        assert!(validate_portable_segment("file."));
    }

    #[test]
    fn project_names_cannot_reach_metadata_or_escape_workspace() {
        for name in [".vivado-server", "../project", ".", "..", "", "a/b"] {
            assert!(validate_project_name(name).is_err(), "{name}");
        }
        assert!(validate_project_name("project-1.2").is_ok());
    }

    #[tokio::test]
    async fn project_creation_rejects_symlinked_workspace_ancestor() {
        let temp = tempfile::tempdir().unwrap();
        let actual = temp.path().join("actual");
        std::fs::create_dir(&actual).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        assert!(ensure_project_dir(&alias, "demo").await.is_err());
        assert!(!actual.join("demo").exists());
    }

    #[tokio::test]
    async fn path_check_rejects_traversal_and_links() {
        let temp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(temp.path(), temp.path().join("link")).unwrap();
        for path in ["link/file", "../file", ".vivado-server/state", "a//b"] {
            assert!(
                ensure_path_has_no_links(temp.path().to_path_buf(), path.to_string())
                    .await
                    .is_err(),
                "{path}"
            );
        }
    }
}
