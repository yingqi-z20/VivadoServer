//! Read-only, descriptor-relative access to untrusted runtime workspaces.
//!
//! Every component is opened with O_NOFOLLOW. Open descriptors, rather than
//! previously checked path strings, anchor subsequent access. Workspace reads
//! do not participate in the sync protocol and never imply an atomic snapshot.
use crate::{error::AppError, workspace::validate_project_name};
use axum::{
    body::Body,
    http::{HeaderValue, header},
    response::Response,
};
use nix::libc;
use serde::{Deserialize, Serialize};
use std::{
    ffi::{CStr, CString},
    fs::{File, Metadata},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::MetadataExt,
    },
    path::{Component, Path},
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use utoipa::ToSchema;

const PREVIEW_BYTES: u64 = 1024 * 1024;
const PREVIEW_LINES: usize = 20_000;
const MAX_ENTRIES: usize = 20_000;
const MAX_ARCHIVE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_TIME: Duration = Duration::from_secs(120);
static ARCHIVE_BUDGET: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceQuery {
    #[serde(default)]
    pub path: String,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkspaceEntry {
    pub path: String,
    pub name: String,
    pub kind: &'static str,
    pub size_bytes: u64,
    pub mtime_unix_ms: i64,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkspaceListing {
    pub path: String,
    pub entries: Vec<WorkspaceEntry>,
    pub exists: bool,
}
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkspacePreview {
    pub kind: &'static str,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

// Runtime-generated names are broader than the portable sync name subset.
// In particular Vivado creates files containing ':' and '?' on Linux.
fn valid_segment(name: &str) -> bool {
    !name.is_empty()
        && !matches!(name, "." | ".." | ".vivado-server")
        && name.len() <= 255
        && !name
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\'))
}
fn validate_path(path: &str, allow_root: bool) -> Result<(), AppError> {
    if path.len() > 4096
        || (!allow_root && path.is_empty())
        || (!path.is_empty() && !path.split('/').all(valid_segment))
        || path.split('/').count() > 128
    {
        return Err(AppError::BadRequest("invalid workspace path".into()));
    }
    Ok(())
}
fn io_error(error: std::io::Error) -> AppError {
    match error.raw_os_error() {
        Some(libc::ENOENT) => AppError::NotFound("workspace path".into()),
        Some(libc::ELOOP | libc::ENOTDIR | libc::EACCES | libc::EPERM) => AppError::BadRequest(
            "workspace path is not an accessible regular file or directory".into(),
        ),
        Some(libc::ENOSPC | libc::EDQUOT) => AppError::Conflict(
            "not enough workspace quota for a ZIP download; download individual files instead"
                .into(),
        ),
        _ => AppError::Internal(format!("workspace read failed: {error}")),
    }
}
pub(crate) fn open_at(parent: &File, name: &str, directory: bool) -> Result<File, AppError> {
    let name =
        CString::new(name).map_err(|_| AppError::BadRequest("invalid workspace path".into()))?;
    // Inspect leaf nodes with O_PATH first, so opening a device or FIFO can
    // never have side effects or block. The descriptor pins the inode across
    // the regular-file check and the eventual read open.
    let flags = libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | if directory {
            libc::O_RDONLY | libc::O_DIRECTORY
        } else {
            libc::O_PATH
        };
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(io_error(std::io::Error::last_os_error()));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_dir() && (!metadata.is_file() || metadata.nlink() != 1) {
        return Err(AppError::BadRequest("workspace path must be a regular file or directory; links and devices are not supported".into()));
    }
    if !directory && metadata.is_file() {
        // This proc link is generated solely from our live descriptor. It is
        // deliberately followed to open that pinned inode, never a user path.
        return File::open(format!("/proc/self/fd/{}", file.as_raw_fd())).map_err(io_error);
    }
    Ok(file)
}
pub(crate) fn open_root(root: &Path) -> Result<File, AppError> {
    let absolute = if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir().map_err(io_error)?.join(root)
    };
    let mut current = File::open("/").map_err(io_error)?;
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| AppError::BadRequest("invalid workspace root".into()))?;
                current = open_at(&current, name, true)?;
            }
            _ => return Err(AppError::BadRequest("invalid workspace root".into())),
        }
    }
    Ok(current)
}
fn open_project(root: &Path, project: &str) -> Result<File, AppError> {
    validate_project_name(project)?;
    open_at(&open_root(root)?, project, true)
}
fn open_path(mut project: File, path: &str, directory: bool) -> Result<File, AppError> {
    validate_path(path, directory)?;
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    for (index, part) in parts.iter().enumerate() {
        project = open_at(&project, part, directory || index + 1 < parts.len())?;
    }
    if !directory && !project.metadata().map_err(io_error)?.is_file() {
        return Err(AppError::BadRequest(
            "workspace path is not a regular file".into(),
        ));
    }
    Ok(project)
}

struct Directory(*mut libc::DIR);
impl Drop for Directory {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}
fn names(directory: &File) -> Result<Vec<String>, AppError> {
    // A new open description avoids sharing readdir's cursor with the caller.
    let duplicate = open_at(directory, ".", true)?;
    use std::os::fd::IntoRawFd;
    let fd = duplicate.into_raw_fd();
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        unsafe {
            libc::close(fd);
        }
        return Err(io_error(std::io::Error::last_os_error()));
    }
    let stream = Directory(stream);
    let mut names = Vec::new();
    let mut scanned = 0;
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(0) {
                return Err(io_error(error));
            }
            break;
        }
        scanned += 1;
        if scanned > MAX_ENTRIES + 2 {
            return Err(AppError::PayloadTooLarge);
        }
        let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if let Ok(name) = bytes.to_str()
            && valid_segment(name)
        {
            names.push(name.to_string());
        }
    }
    names.sort_unstable();
    Ok(names)
}
fn child_or_skip(directory: &File, name: &str) -> Result<Option<File>, AppError> {
    match open_at(directory, name, false) {
        Ok(file) => Ok(Some(file)),
        Err(AppError::BadRequest(_) | AppError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
fn mtime(metadata: &Metadata) -> i64 {
    metadata
        .mtime()
        .saturating_mul(1000)
        .saturating_add(metadata.mtime_nsec() / 1_000_000)
}
fn unchanged(before: &Metadata, after: &Metadata) -> bool {
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
        && after.nlink() == 1
}
pub(crate) fn list(root: &Path, project: &str, path: &str) -> Result<WorkspaceListing, AppError> {
    validate_project_name(project)?;
    validate_path(path, true)?;
    let directory = match open_project(root, project).and_then(|file| open_path(file, path, true)) {
        Ok(directory) => directory,
        Err(AppError::NotFound(_)) => {
            return Ok(WorkspaceListing {
                path: path.into(),
                entries: vec![],
                exists: false,
            });
        }
        Err(error) => return Err(error),
    };
    let mut entries = Vec::new();
    for name in names(&directory)? {
        let Some(file) = child_or_skip(&directory, &name)? else {
            continue;
        };
        let metadata = file.metadata().map_err(io_error)?;
        entries.push(WorkspaceEntry {
            path: if path.is_empty() {
                name.clone()
            } else {
                format!("{path}/{name}")
            },
            name,
            kind: if metadata.is_dir() { "dir" } else { "file" },
            size_bytes: if metadata.is_file() {
                metadata.len()
            } else {
                0
            },
            mtime_unix_ms: mtime(&metadata),
        });
    }
    entries.sort_by(|a, b| (a.kind, &a.name).cmp(&(b.kind, &b.name)));
    Ok(WorkspaceListing {
        path: path.into(),
        entries,
        exists: true,
    })
}
pub(crate) fn preview(
    root: &Path,
    project: &str,
    path: &str,
) -> Result<WorkspacePreview, AppError> {
    let mut file = open_path(open_project(root, project)?, path, false)?;
    let before = file.metadata().map_err(io_error)?;
    let mut result = WorkspacePreview {
        kind: "too_large",
        size_bytes: before.len(),
        text: None,
        reason: Some("preview is limited to 1 MiB and 20000 lines"),
    };
    if before.len() > PREVIEW_BYTES {
        return Ok(result);
    }
    let mut data = Vec::new();
    (&mut file)
        .take(PREVIEW_BYTES + 1)
        .read_to_end(&mut data)
        .map_err(io_error)?;
    if !unchanged(&before, &file.metadata().map_err(io_error)?) {
        return Err(AppError::Conflict(
            "file changed while reading; retry the preview".into(),
        ));
    }
    if data.len() as u64 > PREVIEW_BYTES
        || data.iter().filter(|&&byte| byte == b'\n').count() + 1 > PREVIEW_LINES
    {
        return Ok(result);
    }
    match String::from_utf8(data) {
        Ok(text)
            if !text
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) =>
        {
            result.kind = "text";
            result.text = Some(text);
            result.reason = None;
        }
        _ => {
            result.kind = "binary";
            result.reason = Some("file is not UTF-8 text or contains binary control characters");
        }
    }
    Ok(result)
}

pub(crate) fn download(root: &Path, project: &str, path: &str) -> Result<(File, u64), AppError> {
    let file = open_path(open_project(root, project)?, path, false)?;
    let size = file.metadata().map_err(io_error)?.len();
    Ok((file, size))
}
pub(crate) fn file_response(file: File, size: u64, name: &str, zip: bool) -> Response {
    // Live file reads are bounded to the size observed at open. Consumers should
    // stop Vivado first if they need a stable point-in-time result.
    let body = Body::from_stream(ReaderStream::new(
        tokio::fs::File::from_std(file).take(size),
    ));
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(if zip {
            "application/zip"
        } else {
            "application/octet-stream"
        }),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).expect("integer header"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "x-workspace-snapshot",
        HeaderValue::from_static("non-atomic"),
    );
    let encoded: String = name
        .as_bytes()
        .iter()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                (*byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect();
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"; filename*=UTF-8''{encoded}",
            if zip { "workspace.zip" } else { "download" }
        ))
        .expect("percent encoded attachment name"),
    );
    response
}

struct ArchiveBudget {
    entries: usize,
    bytes: u64,
    started: Instant,
    cancelled: tokio_util::sync::CancellationToken,
}
impl ArchiveBudget {
    fn check(&self) -> Result<(), AppError> {
        if self.cancelled.is_cancelled() {
            return Err(AppError::RequestTimeout);
        }
        if self.entries > MAX_ENTRIES || self.bytes > MAX_ARCHIVE_BYTES {
            return Err(AppError::PayloadTooLarge);
        }
        if self.started.elapsed() > MAX_ARCHIVE_TIME {
            return Err(AppError::RequestTimeout);
        }
        Ok(())
    }
}
fn zip_error(error: zip::result::ZipError) -> AppError {
    match error {
        zip::result::ZipError::Io(error) => io_error(error),
        _ => AppError::Internal(format!("ZIP creation failed: {error}")),
    }
}
fn pack(
    directory: &File,
    prefix: &str,
    zip: &mut zip::ZipWriter<File>,
    budget: &mut ArchiveBudget,
    depth: usize,
) -> Result<(), AppError> {
    if depth > 128 {
        return Err(AppError::PayloadTooLarge);
    }
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);
    for name in names(directory)? {
        budget.check()?;
        let Some(mut file) = child_or_skip(directory, &name)? else {
            continue;
        };
        let metadata = file.metadata().map_err(io_error)?;
        let path = format!("{prefix}{name}");
        if path.len() > 4096 {
            return Err(AppError::PayloadTooLarge);
        }
        budget.entries += 1;
        budget.check()?;
        if metadata.is_dir() {
            zip.add_directory(format!("{path}/"), options.unix_permissions(0o755))
                .map_err(zip_error)?;
            pack(&file, &format!("{path}/"), zip, budget, depth + 1)?;
        } else {
            budget.bytes = budget
                .bytes
                .checked_add(metadata.len())
                .ok_or(AppError::PayloadTooLarge)?;
            budget.check()?;
            zip.start_file(&path, options).map_err(zip_error)?;
            let mut remaining = metadata.len();
            let mut buffer = [0u8; 64 * 1024];
            while remaining > 0 {
                budget.check()?;
                let chunk = remaining.min(buffer.len() as u64) as usize;
                let count = file.read(&mut buffer[..chunk]).map_err(io_error)?;
                if count == 0 {
                    return Err(AppError::Conflict(
                        "workspace file changed during ZIP creation; stop Vivado or retry".into(),
                    ));
                }
                zip.write_all(&buffer[..count]).map_err(io_error)?;
                remaining -= count as u64;
            }
            if !unchanged(&metadata, &file.metadata().map_err(io_error)?) {
                return Err(AppError::Conflict(
                    "workspace file changed during ZIP creation; stop Vivado or retry".into(),
                ));
            }
        }
    }
    Ok(())
}
pub(crate) async fn archive(
    root: std::path::PathBuf,
    project: String,
    path: String,
) -> Result<(File, u64), AppError> {
    let permit = ARCHIVE_BUDGET
        .try_acquire()
        .map_err(|_| AppError::Capacity("another workspace ZIP download is in progress".into()))?;
    let cancelled = tokio_util::sync::CancellationToken::new();
    // Stop unused blocking work and close its quota allocation when the
    // waiting HTTP handler is dropped.
    let _cancel_on_drop = cancelled.clone().drop_guard();
    // O_TMPFILE gives an unnamed file on the quota-controlled runtime filesystem.
    // No client-controlled filename is created and closing the response releases it.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        validate_project_name(&project)?;
        validate_path(&path, true)?;
        let root = open_root(&root)?;
        let directory = open_path(open_at(&root, &project, true)?, &path, true)?;
        let dot = c".";
        let fd = unsafe {
            libc::openat(
                root.as_raw_fd(),
                dot.as_ptr(),
                libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io_error(std::io::Error::last_os_error()));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let mut zip = zip::ZipWriter::new(file);
        let mut budget = ArchiveBudget {
            entries: 0,
            bytes: 0,
            started: Instant::now(),
            cancelled,
        };
        pack(&directory, "", &mut zip, &mut budget, 0)?;
        let mut file = zip.finish().map_err(zip_error)?;
        let size = file.metadata().map_err(io_error)?.len();
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        Ok((file, size))
    })
    .await
    .map_err(|error| AppError::Internal(format!("workspace archive task failed: {error}")))?
}
