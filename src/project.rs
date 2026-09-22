//! Durable project validity and exclusive ownership of a workspace.
//!
//! A marker is a commit boundary, not a recovery journal. Only an exact `clean`
//! marker together with a real project directory permits reuse after restart.
//! Nothing here attempts to replay or restore interrupted synchronization work.

use crate::{
    error::AppError,
    workspace::{
        INTERNAL_DIR, ensure_real_directory_chain, ensure_safe_project_root_blocking,
        resolve_project_dir, validate_project_name,
    },
};
use anyhow::Context;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const CLEAN: &[u8] = b"clean\n";
const DIRTY: &[u8] = b"dirty\n";

#[derive(Clone)]
pub(crate) struct ProjectStore {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    metadata_root: PathBuf,
    // flock belongs to this open file description. Every background operation
    // holds an Arc, so dropping its HTTP waiter cannot release workspace ownership.
    _instance_lock: File,
    marker_lock: Mutex<()>,
}

impl ProjectStore {
    pub(crate) async fn initialize(root: PathBuf) -> anyhow::Result<Self> {
        tokio::task::spawn_blocking(move || {
            ensure_real_directory_chain(&root)?;
            let root = root.canonicalize().context("failed to resolve workspace")?;
            let internal = root.join(INTERNAL_DIR);
            create_real_directory(&internal)?;

            let lock_path = internal.join("instance.lock");
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
                .open(&lock_path)
                .context("failed to open workspace instance lock")?;
            anyhow::ensure!(
                lock.metadata()?.is_file(),
                "workspace instance lock must be a regular file"
            );
            // O_NONBLOCK above prevents opening an unexpected FIFO from hanging;
            // LOCK_NB makes a second service fail immediately instead of waiting.
            retry_interrupted(|| unsafe {
                nix::libc::flock(lock.as_raw_fd(), nix::libc::LOCK_EX | nix::libc::LOCK_NB)
            })
            .context("workspace is already in use or cannot be locked")?;
            lock.sync_all()?;
            sync_directory(&internal)?;

            let metadata_root = internal.join("projects");
            create_real_directory(&metadata_root)?;
            Ok(Self {
                inner: Arc::new(Inner {
                    root,
                    metadata_root,
                    _instance_lock: lock,
                    marker_lock: Mutex::new(()),
                }),
            })
        })
        .await
        .context("workspace initialization task failed")?
    }

    /// Resolve a validated project location without creating it. New or dirty
    /// projects are built in staging, then installed by the synchronization layer.
    pub(crate) async fn project_path(&self, project: &str) -> Result<PathBuf, AppError> {
        validate_project_name(project)?;
        let project = project.to_string();
        self.run(move |inner| checked_project_path(inner, &project))
            .await
    }

    pub(crate) async fn is_clean(&self, project: &str) -> Result<bool, AppError> {
        validate_project_name(project)?;
        let project = project.to_string();
        self.run(move |inner| {
            // An unreadable, unknown, truncated, or linked marker never restores
            // trust in a project. A malformed project name remains a client error.
            Ok(read_clean(inner, &project).unwrap_or(false))
        })
        .await
    }

    /// Must finish before the first project mutation or before starting Vivado.
    pub(crate) async fn mark_dirty(&self, project: &str) -> Result<(), AppError> {
        self.write_marker(project, false).await
    }

    /// Call only after all writers and descendants have stopped. syncfs makes
    /// project data durable before publishing a marker that permits future reuse.
    pub(crate) async fn mark_clean(&self, project: &str) -> Result<(), AppError> {
        self.write_marker(project, true).await
    }

    async fn write_marker(&self, project: &str, clean: bool) -> Result<(), AppError> {
        validate_project_name(project)?;
        let project = project.to_string();
        self.run(move |inner| {
            let _guard = inner
                .marker_lock
                .lock()
                .map_err(|_| AppError::Internal("project marker lock poisoned".to_string()))?;
            let project_dir = checked_project_path(inner, &project)?;
            if clean {
                ensure_safe_project_root_blocking(&inner.root, &project_dir)?;
                let project_fd = open_directory(&project_dir).map_err(io_error)?;
                retry_interrupted(|| unsafe { nix::libc::syncfs(project_fd.as_raw_fd()) })
                    .map_err(io_error)?;
            }
            let directory = inner.metadata_root.join(&project);
            create_real_directory(&directory)?;
            atomic_marker(&directory, if clean { CLEAN } else { DIRTY })
        })
        .await
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T, AppError> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || operation(&inner))
            .await
            .map_err(|err| AppError::Internal(format!("project metadata task failed: {err}")))?
    }
}

fn checked_project_path(inner: &Inner, project: &str) -> Result<PathBuf, AppError> {
    ensure_real_directory_chain(&inner.root)?;
    let path = resolve_project_dir(&inner.root, project)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => ensure_safe_project_root_blocking(&inner.root, &path)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_error(err)),
    }
    Ok(path)
}

fn read_clean(inner: &Inner, project: &str) -> Result<bool, AppError> {
    let project_dir = checked_project_path(inner, project)?;
    ensure_safe_project_root_blocking(&inner.root, &project_dir)?;
    let directory = inner.metadata_root.join(project);
    ensure_real_directory_chain(&directory)?;
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(directory.join("state"))
        .map_err(io_error)?;
    if !marker.metadata().map_err(io_error)?.is_file() {
        return Ok(false);
    }
    let mut bytes = Vec::with_capacity(CLEAN.len() + 1);
    marker
        .take((CLEAN.len() + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    Ok(bytes == CLEAN)
}

fn atomic_marker(directory: &Path, value: &[u8]) -> Result<(), AppError> {
    ensure_real_directory_chain(directory)?;
    let destination = directory.join("state");
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(AppError::BadRequest(
                "project state must be a regular file".to_string(),
            ));
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_error(err)),
    }

    let temporary = directory.join(format!(".state-{}", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(&temporary)?;
        file.write_all(value)?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        sync_directory(directory)
    })();
    if result.is_err() {
        // Only our uncommitted temporary file is removable. The state marker,
        // whether old or newly renamed, must never be guessed or rolled back.
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(io_error)
}

fn create_real_directory(path: &Path) -> Result<(), AppError> {
    let parent = path
        .parent()
        .ok_or_else(|| AppError::Internal("metadata directory has no parent".to_string()))?;
    ensure_real_directory_chain(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {
            sync_directory(path).map_err(io_error)?;
            sync_directory(parent).map_err(io_error)?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure_real_directory_chain(path)?;
        }
        Err(err) => return Err(io_error(err)),
    }
    Ok(())
}

fn open_directory(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(path)
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    open_directory(path)?.sync_all()
}

fn retry_interrupted(mut operation: impl FnMut() -> i32) -> std::io::Result<()> {
    loop {
        if operation() == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn io_error(error: std::io::Error) -> AppError {
    AppError::Internal(format!("project metadata I/O failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[tokio::test]
    async fn lock_lives_until_last_store_clone_is_dropped() {
        let temp = tempfile::tempdir().unwrap();
        let first = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        let clone = first.clone();
        drop(first);
        assert!(
            ProjectStore::initialize(temp.path().to_path_buf())
                .await
                .is_err()
        );
        drop(clone);
        assert!(
            ProjectStore::initialize(temp.path().to_path_buf())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn only_exact_clean_marker_and_real_project_allow_reuse_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let store = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        let project = store.project_path("demo").await.unwrap();
        assert!(!project.exists());
        assert!(!store.is_clean("demo").await.unwrap());
        store.mark_dirty("demo").await.unwrap();
        assert!(!project.exists());
        assert!(store.mark_clean("demo").await.is_err());
        fs::create_dir(&project).unwrap();
        fs::write(project.join("top.tcl"), b"puts hello").unwrap();
        store.mark_clean("demo").await.unwrap();
        assert!(store.is_clean("demo").await.unwrap());
        drop(store);

        let store = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(store.is_clean("demo").await.unwrap());
        store.mark_dirty("demo").await.unwrap();
        drop(store);
        let store = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(!store.is_clean("demo").await.unwrap());
        let marker = temp.path().join(".vivado-server/projects/demo/state");
        for value in [b"".as_slice(), b"clean", b"clean\nextra", b"unknown\n"] {
            fs::write(&marker, value).unwrap();
            assert!(!store.is_clean("demo").await.unwrap());
        }
        store.mark_clean("demo").await.unwrap();
        fs::remove_dir_all(&project).unwrap();
        assert!(!store.is_clean("demo").await.unwrap());
    }

    #[tokio::test]
    async fn marker_and_project_links_are_never_trusted_or_overwritten() {
        let temp = tempfile::tempdir().unwrap();
        let store = ProjectStore::initialize(temp.path().to_path_buf())
            .await
            .unwrap();
        let project = store.project_path("demo").await.unwrap();
        fs::create_dir(&project).unwrap();
        store.mark_clean("demo").await.unwrap();
        let marker = temp.path().join(".vivado-server/projects/demo/state");
        let external = temp.path().join("external-state");
        fs::write(&external, CLEAN).unwrap();
        fs::remove_file(&marker).unwrap();
        symlink(&external, &marker).unwrap();
        assert!(!store.is_clean("demo").await.unwrap());
        assert!(store.mark_dirty("demo").await.is_err());
        assert_eq!(fs::read(&external).unwrap(), CLEAN);
        fs::remove_file(&marker).unwrap();
        fs::remove_dir(&project).unwrap();
        symlink(temp.path(), &project).unwrap();
        assert!(store.project_path("demo").await.is_err());
        assert!(store.mark_dirty("demo").await.is_err());
        assert!(!store.is_clean("demo").await.unwrap());
    }

    #[tokio::test]
    async fn linked_metadata_and_instance_locks_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let internal = temp.path().join(INTERNAL_DIR);
        symlink(external.path(), &internal).unwrap();
        assert!(
            ProjectStore::initialize(temp.path().to_path_buf())
                .await
                .is_err()
        );
        fs::remove_file(&internal).unwrap();
        fs::create_dir(&internal).unwrap();
        let outside_lock = external.path().join("lock");
        fs::write(&outside_lock, b"untouched").unwrap();
        symlink(&outside_lock, internal.join("instance.lock")).unwrap();
        assert!(
            ProjectStore::initialize(temp.path().to_path_buf())
                .await
                .is_err()
        );
        assert_eq!(fs::read(&outside_lock).unwrap(), b"untouched");
    }
}
