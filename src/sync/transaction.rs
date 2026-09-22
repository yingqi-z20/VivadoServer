//! In-process commit transaction and best-effort rollback.
//!
//! This is deliberately not a durable journal. The manager owns the task for
//! cancellation safety, while this type owns every filesystem mutation made by
//! that task so an ordinary error can unwind all changes in strict reverse
//! order. Backup names and their original paths are tracked only in memory;
//! after a process restart, a dirty project must be rebuilt instead of replaying
//! these backups.

use crate::{error::AppError, workspace::metadata_is_link};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
enum CreatedPathKind {
    Dir,
}

#[derive(Debug)]
enum AppliedChange {
    Created {
        target: PathBuf,
        kind: CreatedPathKind,
    },
    Replaced {
        target: PathBuf,
        backup: PathBuf,
    },
    Installed {
        target: PathBuf,
        staged: PathBuf,
        backup: Option<PathBuf>,
    },
}

pub(super) struct CommitTransaction {
    backup_root: PathBuf,
    changes: Vec<AppliedChange>,
}

impl CommitTransaction {
    pub(super) async fn new(backup_root: PathBuf) -> Result<Self, AppError> {
        tokio::fs::create_dir_all(&backup_root)
            .await
            .map_err(|err| AppError::Internal(format!("failed to create rollback dir: {err}")))?;
        Ok(Self {
            backup_root,
            changes: Vec::new(),
        })
    }

    pub(super) async fn create_dir(&mut self, target: PathBuf, path: &str) -> Result<(), AppError> {
        match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) if metadata.is_dir() && !metadata_is_link(&metadata) => return Ok(()),
            Ok(_) => {
                return Err(AppError::Conflict(format!(
                    "target is not a directory: {path}"
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(AppError::Internal(format!(
                    "failed to inspect directory target: {err}"
                )));
            }
        }
        tokio::fs::create_dir(&target)
            .await
            .map_err(|err| AppError::Internal(format!("failed to create dir {path}: {err}")))?;
        self.changes.push(AppliedChange::Created {
            target,
            kind: CreatedPathKind::Dir,
        });
        Ok(())
    }

    pub(super) async fn install_file(
        &mut self,
        staged: PathBuf,
        target: PathBuf,
        path: &str,
    ) -> Result<(), AppError> {
        let backup = match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) if metadata.is_file() && !metadata_is_link(&metadata) => {
                Some(self.backup(&target).await?)
            }
            Ok(_) => {
                return Err(AppError::Conflict(format!(
                    "target is not a regular file: {path}"
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(AppError::Internal(format!(
                    "failed to inspect file target: {err}"
                )));
            }
        };

        // Record the moved target before installing the staged file. If the
        // install fails, rollback still knows where the original lives.
        if let Some(backup) = &backup {
            self.changes.push(AppliedChange::Replaced {
                target: target.clone(),
                backup: backup.clone(),
            });
        }
        if let Err(err) = tokio::fs::rename(&staged, &target).await {
            return Err(AppError::Internal(format!(
                "failed to commit staged file {path}: {err}"
            )));
        }
        if backup.is_some() {
            let replaced = self.changes.pop();
            debug_assert!(matches!(replaced, Some(AppliedChange::Replaced { .. })));
        }
        self.changes.push(AppliedChange::Installed {
            target,
            staged,
            backup,
        });
        Ok(())
    }

    pub(super) async fn replace_project(
        &mut self,
        staged: PathBuf,
        target: PathBuf,
    ) -> Result<(), AppError> {
        let backup = match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) if metadata.is_dir() && !metadata_is_link(&metadata) => {
                Some(self.backup(&target).await?)
            }
            Ok(_) => {
                return Err(AppError::Conflict(
                    "project target is not a directory".to_string(),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(AppError::Internal(format!(
                    "failed to inspect project target: {err}"
                )));
            }
        };

        // Keep the old tree recoverable even if installing the complete staged
        // tree fails. The caller owns this task until commit or rollback ends.
        if let Some(backup) = &backup {
            self.changes.push(AppliedChange::Replaced {
                target: target.clone(),
                backup: backup.clone(),
            });
        }
        tokio::fs::rename(&staged, &target).await.map_err(|err| {
            AppError::Internal(format!("failed to install rebuilt project: {err}"))
        })?;
        if backup.is_some() {
            let replaced = self.changes.pop();
            debug_assert!(matches!(replaced, Some(AppliedChange::Replaced { .. })));
        }
        self.changes.push(AppliedChange::Installed {
            target,
            staged,
            backup,
        });
        Ok(())
    }

    pub(super) async fn delete_file(
        &mut self,
        target: PathBuf,
        path: &str,
    ) -> Result<bool, AppError> {
        match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) if metadata.is_file() && !metadata_is_link(&metadata) => {
                let backup = self.backup(&target).await?;
                self.changes
                    .push(AppliedChange::Replaced { target, backup });
                Ok(true)
            }
            Ok(_) => Err(AppError::Conflict(format!(
                "file target changed type during commit: {path}"
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(AppError::Internal(format!(
                "failed to inspect file for deletion: {err}"
            ))),
        }
    }

    pub(super) async fn delete_empty_dir(
        &mut self,
        target: PathBuf,
        path: &str,
    ) -> Result<bool, AppError> {
        let metadata = match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(AppError::Internal(format!(
                    "failed to inspect directory for deletion: {err}"
                )));
            }
        };
        if !metadata.is_dir() || metadata_is_link(&metadata) {
            return Err(AppError::Conflict(format!(
                "directory target changed type during commit: {path}"
            )));
        }
        let mut entries = tokio::fs::read_dir(&target)
            .await
            .map_err(|err| AppError::Internal(format!("failed to read dir {path}: {err}")))?;
        if entries
            .next_entry()
            .await
            .map_err(|err| AppError::Internal(format!("failed to read dir {path}: {err}")))?
            .is_some()
        {
            return Err(AppError::Conflict(format!(
                "directory contains entries outside the sync plan: {path}"
            )));
        }
        let backup = self.backup(&target).await?;
        self.changes
            .push(AppliedChange::Replaced { target, backup });
        Ok(true)
    }

    async fn backup(&self, target: &Path) -> Result<PathBuf, AppError> {
        let backup = self.backup_root.join(Uuid::new_v4().to_string());
        tokio::fs::rename(target, &backup)
            .await
            .map_err(|err| AppError::Internal(format!("failed to stage rollback backup: {err}")))?;
        Ok(backup)
    }

    pub(super) async fn rollback(&mut self) -> Result<(), AppError> {
        let mut errors = Vec::new();
        while let Some(change) = self.changes.pop() {
            let result = match change {
                AppliedChange::Created { target, kind } => remove_created_path(&target, kind).await,
                AppliedChange::Replaced { target, backup } => {
                    match remove_replacement_target(&target).await {
                        Ok(()) => tokio::fs::rename(&backup, &target).await.map_err(|err| {
                            AppError::Internal(format!("failed to restore rollback backup: {err}"))
                        }),
                        Err(error) => Err(error),
                    }
                }
                AppliedChange::Installed {
                    target,
                    staged,
                    backup,
                } => {
                    let restore_staged = tokio::fs::rename(&target, &staged).await.map_err(|err| {
                        AppError::Internal(format!("failed to restore staged upload: {err}"))
                    });
                    if let Err(error) = restore_staged {
                        Err(error)
                    } else if let Some(backup) = backup {
                        tokio::fs::rename(&backup, &target).await.map_err(|err| {
                            AppError::Internal(format!("failed to restore replaced path: {err}"))
                        })
                    } else {
                        Ok(())
                    }
                }
            };
            if let Err(error) = result {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(AppError::Internal(format!(
                "rollback encountered {} error(s): {}",
                errors.len(),
                errors.join("; ")
            )))
        }
    }
}

async fn remove_created_path(path: &Path, kind: CreatedPathKind) -> Result<(), AppError> {
    let result = match kind {
        CreatedPathKind::Dir => tokio::fs::remove_dir(path).await,
    };
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(AppError::Internal(format!(
            "failed to remove created path during rollback: {err}"
        ))),
    }
}

async fn remove_replacement_target(path: &Path) -> Result<(), AppError> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(AppError::Internal(format!(
                "failed to inspect rollback target: {err}"
            )));
        }
    };
    let result = if metadata.is_dir() && !metadata_is_link(&metadata) {
        tokio::fs::remove_dir(path).await
    } else {
        tokio::fs::remove_file(path).await
    };
    result.map_err(|err| AppError::Internal(format!("failed to clear rollback target: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replace_project_removes_old_extras_and_can_restore_both_trees() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("project");
        let staged = temp.path().join("staged");
        tokio::fs::create_dir_all(target.join("cache"))
            .await
            .unwrap();
        tokio::fs::write(target.join("cache/old.bit"), b"old generated file")
            .await
            .unwrap();
        tokio::fs::write(target.join("design.v"), b"old design")
            .await
            .unwrap();
        tokio::fs::create_dir(&staged).await.unwrap();
        tokio::fs::write(staged.join("design.v"), b"new design")
            .await
            .unwrap();
        let mut transaction = CommitTransaction::new(temp.path().join("backup"))
            .await
            .unwrap();

        transaction
            .replace_project(staged.clone(), target.clone())
            .await
            .unwrap();

        assert!(!target.join("cache").exists());
        assert!(!staged.exists());
        assert_eq!(
            tokio::fs::read(target.join("design.v")).await.unwrap(),
            b"new design"
        );

        transaction.rollback().await.unwrap();

        assert_eq!(
            tokio::fs::read(target.join("design.v")).await.unwrap(),
            b"old design"
        );
        assert_eq!(
            tokio::fs::read(target.join("cache/old.bit")).await.unwrap(),
            b"old generated file"
        );
        assert_eq!(
            tokio::fs::read(staged.join("design.v")).await.unwrap(),
            b"new design"
        );
    }

    #[tokio::test]
    async fn failed_project_install_keeps_backup_available_for_rollback() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("project");
        tokio::fs::create_dir(&target).await.unwrap();
        tokio::fs::write(target.join("original"), b"original")
            .await
            .unwrap();
        let mut transaction = CommitTransaction::new(temp.path().join("backup"))
            .await
            .unwrap();

        assert!(
            transaction
                .replace_project(temp.path().join("missing"), target.clone())
                .await
                .is_err()
        );
        assert!(!target.exists(), "the original tree has moved into backup");

        transaction.rollback().await.unwrap();

        assert_eq!(
            tokio::fs::read(target.join("original")).await.unwrap(),
            b"original"
        );
    }

    #[tokio::test]
    async fn replacing_missing_project_rolls_back_to_missing_project() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("project");
        let staged = temp.path().join("staged");
        tokio::fs::create_dir(&staged).await.unwrap();
        tokio::fs::write(staged.join("design.v"), b"design")
            .await
            .unwrap();
        let mut transaction = CommitTransaction::new(temp.path().join("backup"))
            .await
            .unwrap();

        transaction
            .replace_project(staged.clone(), target.clone())
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(target.join("design.v")).await.unwrap(),
            b"design"
        );

        transaction.rollback().await.unwrap();

        assert!(!target.exists());
        assert_eq!(
            tokio::fs::read(staged.join("design.v")).await.unwrap(),
            b"design"
        );
    }

    #[tokio::test]
    async fn rollback_reverses_dependent_file_operations_in_order() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("created");
        let target = parent.join("design.v");
        let staged_first = temp.path().join("first-upload");
        let staged_second = temp.path().join("second-upload");
        tokio::fs::write(&staged_first, b"first").await.unwrap();
        tokio::fs::write(&staged_second, b"second").await.unwrap();
        let mut transaction = CommitTransaction::new(temp.path().join("backup"))
            .await
            .unwrap();

        transaction
            .create_dir(parent.clone(), "created")
            .await
            .unwrap();
        transaction
            .install_file(staged_first.clone(), target.clone(), "created/design.v")
            .await
            .unwrap();
        transaction
            .install_file(staged_second.clone(), target.clone(), "created/design.v")
            .await
            .unwrap();
        transaction
            .delete_file(target.clone(), "created/design.v")
            .await
            .unwrap();
        transaction
            .delete_empty_dir(parent.clone(), "created")
            .await
            .unwrap();
        assert!(!parent.exists());

        transaction.rollback().await.unwrap();

        assert!(!parent.exists());
        assert_eq!(tokio::fs::read(&staged_first).await.unwrap(), b"first");
        assert_eq!(tokio::fs::read(&staged_second).await.unwrap(), b"second");
        assert_eq!(transaction.changes.len(), 0);
        transaction.rollback().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replace_project_rejects_a_symlink_target_without_moving_it() {
        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("original");
        let target = temp.path().join("project");
        let staged = temp.path().join("staged");
        tokio::fs::create_dir(&original).await.unwrap();
        tokio::fs::create_dir(&staged).await.unwrap();
        std::os::unix::fs::symlink(&original, &target).unwrap();
        let mut transaction = CommitTransaction::new(temp.path().join("backup"))
            .await
            .unwrap();

        assert!(matches!(
            transaction
                .replace_project(staged.clone(), target.clone())
                .await,
            Err(AppError::Conflict(_))
        ));
        assert_eq!(tokio::fs::read_link(&target).await.unwrap(), original);
        assert!(staged.is_dir());
        assert!(transaction.changes.is_empty());
        transaction.rollback().await.unwrap();
    }
}
