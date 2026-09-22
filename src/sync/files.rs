//! Shared manifest, upload, and path operations for the Linux sync engine.
use super::*;

#[derive(Debug)]
pub(super) struct SyncFilters {
    include: GlobSet,
    include_active: bool,
    exclude: GlobSet,
}

impl Clone for SyncFilters {
    fn clone(&self) -> Self {
        Self {
            include: self.include.clone(),
            include_active: self.include_active,
            exclude: self.exclude.clone(),
        }
    }
}

impl SyncFilters {
    pub(super) fn new(
        include_globs: &[String],
        exclude_globs: &[String],
    ) -> Result<Self, AppError> {
        if include_globs.len() + exclude_globs.len() > MAX_GLOBS {
            return Err(AppError::BadRequest(format!(
                "at most {MAX_GLOBS} include/exclude globs are allowed"
            )));
        }
        if include_globs
            .iter()
            .chain(exclude_globs)
            .any(|pattern| pattern.len() > MAX_GLOB_BYTES)
        {
            return Err(AppError::BadRequest(format!(
                "each glob must be at most {MAX_GLOB_BYTES} bytes"
            )));
        }
        tracing::debug!(
            include_globs = include_globs.len(),
            exclude_globs = exclude_globs.len(),
            "building sync filters"
        );
        let mut include = GlobSetBuilder::new();
        for pattern in include_globs {
            include.add(Glob::new(pattern).map_err(|err| {
                AppError::BadRequest(format!("invalid include glob {pattern}: {err}"))
            })?);
        }

        let mut exclude = GlobSetBuilder::new();
        for pattern in exclude_globs {
            exclude.add(Glob::new(pattern).map_err(|err| {
                AppError::BadRequest(format!("invalid exclude glob {pattern}: {err}"))
            })?);
        }
        Ok(Self {
            include: include
                .build()
                .map_err(|err| AppError::BadRequest(format!("invalid include globs: {err}")))?,
            include_active: !include_globs.is_empty(),
            exclude: exclude
                .build()
                .map_err(|err| AppError::BadRequest(format!("invalid exclude globs: {err}")))?,
        })
    }

    pub(super) fn matches_entry(&self, path: &str) -> bool {
        self.should_descend(path) && (!self.include_active || self.include.is_match(path))
    }

    pub(super) fn should_descend(&self, path: &str) -> bool {
        !is_internal_path(path)
            && !self.exclude.is_match(path)
            && !parent_paths(path)
                .iter()
                .any(|parent| self.exclude.is_match(parent))
    }
}

pub(super) fn scan_manifest_blocking(
    project_dir: PathBuf,
    filters: SyncFilters,
    max_entries: usize,
    max_file_bytes: u64,
) -> Result<ManifestResponse, AppError> {
    let started = Instant::now();
    let mut entries = Vec::new();
    let mut files = 0_usize;
    let mut dirs = 0_usize;
    let mut bytes = 0_u64;
    let mut skipped_by_filter = 0_usize;
    let walker = WalkDir::new(&project_dir)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            relative_path_from_disk(entry.path(), &project_dir)
                .map(|path| filters.should_descend(&path))
                .unwrap_or(true)
        });

    for entry in walker {
        let entry =
            entry.map_err(|err| AppError::Internal(format!("failed to walk project: {err}")))?;
        if entry.depth() == 0 {
            continue;
        }
        let path = relative_path_from_disk(entry.path(), &project_dir)?;
        if !filters.matches_entry(&path) {
            skipped_by_filter += 1;
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                "manifest entry skipped by filters"
            );
            continue;
        }
        if entries.len() >= max_entries {
            return Err(AppError::BadRequest(format!(
                "manifest exceeds sync_max_manifest_entries ({max_entries})"
            )));
        }
        let file_type = entry.file_type();
        let link_metadata = std::fs::symlink_metadata(entry.path()).map_err(|err| {
            AppError::Internal(format!("failed to inspect filesystem entry: {err}"))
        })?;
        if file_type.is_symlink() || metadata_is_link(&link_metadata) {
            return Err(AppError::BadRequest(format!(
                "symlink is not supported: {path}"
            )));
        }
        if file_type.is_dir() {
            dirs += 1;
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                "manifest directory accepted"
            );
            entries.push(ManifestEntry {
                path,
                kind: ManifestEntryKind::Dir,
                size_bytes: None,
                mtime_unix_ms: None,
                sha256: None,
                executable: false,
            });
        } else if file_type.is_file() {
            let (_file, transfer) =
                open_file_for_transfer_blocking(entry.path(), path.clone(), max_file_bytes)?;
            let size = transfer.size_bytes;
            files += 1;
            bytes = bytes.saturating_add(size);
            tracing::trace!(
                project_dir = %project_dir.display(),
                path = %path,
                size_bytes = size,
                "manifest file accepted"
            );
            entries.push(ManifestEntry {
                path: path.clone(),
                kind: ManifestEntryKind::File,
                size_bytes: Some(size),
                mtime_unix_ms: Some(transfer.mtime_unix_ms),
                sha256: Some(transfer.sha256),
                executable: transfer.executable,
            });
        } else {
            return Err(AppError::BadRequest(format!(
                "unsupported filesystem entry: {path}"
            )));
        }
    }

    synthesize_parent_dirs(&mut entries, max_entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    tracing::debug!(
        project_dir = %project_dir.display(),
        entries = entries.len(),
        files,
        dirs,
        bytes,
        skipped_by_filter,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "sync manifest scan completed"
    );
    Ok(ManifestResponse { entries })
}

pub(super) fn validate_client_manifest(
    entries: Vec<ManifestEntry>,
    filters: &SyncFilters,
    max_entries: usize,
    max_file_bytes: u64,
) -> Result<Vec<ManifestEntry>, AppError> {
    let input_entries = entries.len();
    tracing::debug!(
        input_entries,
        max_entries,
        max_file_bytes,
        "validating client sync manifest"
    );
    if entries.len() > max_entries {
        return Err(AppError::BadRequest(format!(
            "manifest exceeds sync_max_manifest_entries ({max_entries})"
        )));
    }

    let mut seen = HashSet::new();
    let mut validated = Vec::new();
    let mut skipped_by_filter = 0_usize;
    let mut files = 0_usize;
    let mut dirs = 0_usize;
    let mut bytes = 0_u64;
    for mut entry in entries {
        entry.path = normalize_sync_path(&entry.path)?;
        if !seen.insert(entry.path.clone()) {
            return Err(AppError::BadRequest(format!(
                "duplicate manifest path: {}",
                entry.path
            )));
        }
        if !filters.matches_entry(&entry.path) {
            skipped_by_filter += 1;
            tracing::trace!(
                path = %entry.path,
                "client manifest entry skipped by filters"
            );
            continue;
        }
        match entry.kind {
            ManifestEntryKind::File => {
                let size = entry.size_bytes.ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing size: {}", entry.path))
                })?;
                if size > max_file_bytes {
                    return Err(AppError::BadRequest(format!(
                        "file exceeds sync_max_file_bytes: {}",
                        entry.path
                    )));
                }
                files += 1;
                bytes = bytes.saturating_add(size);
                entry.mtime_unix_ms.ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing mtime: {}", entry.path))
                })?;
                let sha = entry.sha256.as_ref().ok_or_else(|| {
                    AppError::BadRequest(format!("file entry missing sha256: {}", entry.path))
                })?;
                if !is_sha256_hex(sha) {
                    return Err(AppError::BadRequest(format!(
                        "file entry has invalid sha256: {}",
                        entry.path
                    )));
                }
                // The wire format accepts either hex case; normalize once so
                // comparisons and upload verification use a canonical digest.
                entry.sha256 = Some(sha.to_ascii_lowercase());
            }
            ManifestEntryKind::Dir => {
                entry.size_bytes = None;
                entry.mtime_unix_ms = None;
                entry.sha256 = None;
                entry.executable = false;
                dirs += 1;
            }
        }
        validated.push(entry);
    }
    synthesize_parent_dirs(&mut validated, max_entries)?;
    validated.sort_by(|left, right| left.path.cmp(&right.path));
    tracing::debug!(
        input_entries,
        accepted_entries = validated.len(),
        skipped_by_filter,
        files,
        dirs,
        bytes,
        "client sync manifest validated"
    );
    Ok(validated)
}

pub(super) fn synthesize_parent_dirs(
    entries: &mut Vec<ManifestEntry>,
    max_entries: usize,
) -> Result<(), AppError> {
    let mut kinds: HashMap<String, ManifestEntryKind> = entries
        .iter()
        .map(|entry| (entry.path.clone(), entry.kind))
        .collect();
    let paths: Vec<String> = entries.iter().map(|entry| entry.path.clone()).collect();

    for path in paths {
        for parent in parent_paths(&path) {
            match kinds.get(&parent) {
                Some(ManifestEntryKind::File) => {
                    return Err(AppError::BadRequest(format!(
                        "file is an ancestor of another manifest entry: {parent}"
                    )));
                }
                Some(ManifestEntryKind::Dir) => {}
                None => {
                    if entries.len() >= max_entries {
                        return Err(AppError::BadRequest(format!(
                            "manifest exceeds sync_max_manifest_entries ({max_entries}) after adding parent directories"
                        )));
                    }
                    kinds.insert(parent.clone(), ManifestEntryKind::Dir);
                    entries.push(ManifestEntry {
                        path: parent,
                        kind: ManifestEntryKind::Dir,
                        size_bytes: None,
                        mtime_unix_ms: None,
                        sha256: None,
                        executable: false,
                    });
                }
            }
        }
    }
    Ok(())
}

pub(super) fn parent_paths(path: &str) -> Vec<String> {
    let mut parents = Vec::new();
    let mut current = path;
    while let Some((parent, _)) = current.rsplit_once('/') {
        parents.push(parent.to_string());
        current = parent;
    }
    parents.reverse();
    parents
}

pub(super) fn manifest_map(entries: Vec<ManifestEntry>) -> BTreeMap<String, ManifestEntry> {
    entries
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

pub(super) fn same_file(left: &ManifestEntry, right: &ManifestEntry) -> bool {
    left.kind == ManifestEntryKind::File
        && right.kind == ManifestEntryKind::File
        && left.size_bytes == right.size_bytes
        && left.mtime_unix_ms == right.mtime_unix_ms
        && left.sha256 == right.sha256
        && left.executable == right.executable
}

pub(super) fn file_transfer(entry: &ManifestEntry) -> Result<FileTransfer, AppError> {
    if entry.kind != ManifestEntryKind::File {
        return Err(AppError::BadRequest(format!("not a file: {}", entry.path)));
    }
    Ok(FileTransfer {
        path: entry.path.clone(),
        size_bytes: entry.size_bytes.ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing size: {}", entry.path))
        })?,
        mtime_unix_ms: entry.mtime_unix_ms.ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing mtime: {}", entry.path))
        })?,
        sha256: entry.sha256.clone().ok_or_else(|| {
            AppError::BadRequest(format!("file entry missing sha256: {}", entry.path))
        })?,
        executable: entry.executable,
    })
}

pub(super) async fn write_body_to_file(
    mut body: Body,
    tmp_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    max_file_bytes: u64,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> Result<(), AppError> {
    let started = Instant::now();
    tracing::debug!(
        tmp_path = %tmp_path.display(),
        expected_size,
        expected_sha256,
        max_file_bytes,
        "streaming sync upload body to staging file"
    );
    if expected_size > max_file_bytes {
        return Err(AppError::BadRequest(
            "file exceeds sync_max_file_bytes".to_string(),
        ));
    }
    let mut file = tokio::fs::File::create(tmp_path)
        .await
        .map_err(|err| AppError::Internal(format!("failed to create staged file: {err}")))?;
    let mut hasher = Sha256::new();
    let mut written = 0_u64;
    let mut chunks = 0_u64;
    let deadline = Instant::now() + total_timeout;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AppError::RequestTimeout);
        }
        let frame = time::timeout(idle_timeout.min(remaining), body.frame())
            .await
            .map_err(|_| AppError::RequestTimeout)?;
        let Some(frame) = frame else { break };
        let frame = frame.map_err(request_body_error)?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        chunks += 1;
        written = written
            .checked_add(data.len() as u64)
            .ok_or(AppError::PayloadTooLarge)?;
        tracing::trace!(
            tmp_path = %tmp_path.display(),
            chunk_bytes = data.len(),
            written,
            chunks,
            "received sync upload body chunk"
        );
        if written > expected_size || written > max_file_bytes {
            return Err(AppError::PayloadTooLarge);
        }
        hasher.update(data);
        file.write_all(data)
            .await
            .map_err(|err| AppError::Internal(format!("failed to write staged file: {err}")))?;
    }
    file.flush()
        .await
        .map_err(|err| AppError::Internal(format!("failed to flush staged file: {err}")))?;
    file.sync_data()
        .await
        .map_err(|err| AppError::Internal(format!("failed to sync staged file: {err}")))?;

    if written != expected_size {
        tracing::debug!(
            tmp_path = %tmp_path.display(),
            expected_size,
            actual_size = written,
            "sync upload size mismatch"
        );
        return Err(AppError::BadRequest(format!(
            "size mismatch: expected {expected_size}, got {written}"
        )));
    }
    let digest = hasher.finalize();
    let actual = hex_digest(&digest);
    if actual != expected_sha256 {
        tracing::debug!(
            tmp_path = %tmp_path.display(),
            expected_sha256,
            actual_sha256 = %actual,
            "sync upload sha256 mismatch"
        );
        return Err(AppError::BadRequest(format!(
            "sha256 mismatch: expected {expected_sha256}, got {actual}"
        )));
    }
    tracing::debug!(
        tmp_path = %tmp_path.display(),
        bytes = written,
        chunks,
        sha256 = %actual,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "sync upload body staged"
    );
    Ok(())
}

pub(super) fn request_body_error(error: axum::Error) -> AppError {
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if cause.is::<tower_http::timeout::TimeoutError>() {
            return AppError::RequestTimeout;
        }
        if cause.is::<http_body_util::LengthLimitError>() {
            return AppError::PayloadTooLarge;
        }
        match cause.source() {
            Some(source) => cause = source,
            None => break,
        }
    }
    AppError::BadRequest(format!("failed to read request body: {error}"))
}

pub(super) struct TempPathGuard {
    path: Option<PathBuf>,
}

impl TempPathGuard {
    pub(super) fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }
    pub(super) fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            // Drop is also run when an HTTP handler is cancelled. A small
            // synchronous unlink here prevents cancelled uploads from leaving
            // unbounded `.part` files behind.
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(super) async fn atomic_replace(source: PathBuf, target: PathBuf) -> Result<(), AppError> {
    tokio::fs::rename(source, target)
        .await
        .map_err(|error| AppError::Internal(format!("failed to install staged upload: {error}")))
}

pub(super) async fn open_file_for_transfer(
    path: PathBuf,
    rel_path: String,
    max_file_bytes: u64,
) -> Result<(File, FileTransfer), AppError> {
    tokio::task::spawn_blocking(move || {
        open_file_for_transfer_blocking(&path, rel_path, max_file_bytes)
    })
    .await
    .map_err(|error| AppError::Internal(format!("file snapshot task failed: {error}")))?
}

pub(super) fn open_file_for_transfer_blocking(
    path: &Path,
    rel_path: String,
    max_file_bytes: u64,
) -> Result<(File, FileTransfer), AppError> {
    let mut file = File::open(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => AppError::NotFound(format!("file not found: {rel_path}")),
        std::io::ErrorKind::PermissionDenied => {
            AppError::BadRequest(format!("permission denied reading file: {rel_path}"))
        }
        _ => AppError::Internal(format!("failed to open file for transfer: {error}")),
    })?;
    let before = file
        .metadata()
        .map_err(|error| AppError::Internal(format!("failed to read file metadata: {error}")))?;
    if !before.is_file()
        || metadata_is_link(&std::fs::symlink_metadata(path).map_err(|error| {
            AppError::Internal(format!("failed to inspect transfer path: {error}"))
        })?)
    {
        return Err(AppError::BadRequest(
            "path is not a regular file".to_string(),
        ));
    }
    if before.len() > max_file_bytes {
        return Err(AppError::PayloadTooLarge);
    }
    let digest = hash_reader(&mut file)?;
    let after = file
        .metadata()
        .map_err(|error| AppError::Internal(format!("failed to re-read file metadata: {error}")))?;
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || executable_from_metadata(&before) != executable_from_metadata(&after)
    {
        return Err(AppError::Conflict(format!(
            "file changed while it was being hashed: {rel_path}"
        )));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| AppError::Internal(format!("failed to rewind transfer file: {error}")))?;
    Ok((
        file,
        FileTransfer {
            path: rel_path,
            size_bytes: before.len(),
            mtime_unix_ms: system_time_to_ms(before.modified().unwrap_or(UNIX_EPOCH)),
            sha256: digest,
            executable: executable_from_metadata(&before),
        },
    ))
}

pub(super) async fn ensure_baseline_matches(
    target: &Path,
    baseline: Option<&ManifestEntry>,
) -> Result<(), AppError> {
    let baseline_path = baseline.map(|entry| entry.path.as_str()).unwrap_or("<new>");
    tracing::debug!(
        target = %target.display(),
        baseline_path,
        "checking sync baseline"
    );
    let target = target.to_path_buf();
    let baseline = baseline.cloned();
    tokio::task::spawn_blocking(move || match baseline {
        None => {
            if target.exists() {
                Err(AppError::Conflict(format!(
                    "target changed since plan: {}",
                    target.display()
                )))
            } else {
                Ok(())
            }
        }
        Some(entry) => match entry.kind {
            ManifestEntryKind::Dir => {
                if target.is_dir() {
                    Ok(())
                } else {
                    Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )))
                }
            }
            ManifestEntryKind::File => {
                if !target.is_file() {
                    return Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )));
                }
                let metadata = std::fs::metadata(&target).map_err(|error| {
                    AppError::Internal(format!("failed to inspect baseline file: {error}"))
                })?;
                let current_sha = hash_file_blocking(&target)?;
                let after = std::fs::metadata(&target).map_err(internal_io)?;
                if metadata.len() == after.len()
                    && metadata.modified().ok() == after.modified().ok()
                    && Some(metadata.len()) == entry.size_bytes
                    && Some(system_time_to_ms(metadata.modified().unwrap_or(UNIX_EPOCH)))
                        == entry.mtime_unix_ms
                    && Some(current_sha) == entry.sha256
                    && executable_from_metadata(&metadata) == entry.executable
                    && executable_from_metadata(&after) == entry.executable
                {
                    Ok(())
                } else {
                    Err(AppError::Conflict(format!(
                        "target changed since plan: {}",
                        entry.path
                    )))
                }
            }
        },
    })
    .await
    .map_err(|err| AppError::Internal(format!("baseline check task failed: {err}")))?
}

pub(super) async fn set_file_mtime(path: PathBuf, mtime_unix_ms: i64) -> Result<(), AppError> {
    tracing::trace!(
        path = %path.display(),
        mtime_unix_ms,
        "setting synced file mtime"
    );
    tokio::task::spawn_blocking(move || {
        let seconds = mtime_unix_ms.div_euclid(1_000);
        let nanos = (mtime_unix_ms.rem_euclid(1_000) * 1_000_000) as u32;
        let mtime = FileTime::from_unix_time(seconds, nanos);
        filetime::set_file_mtime(path, mtime)
            .map_err(|err| AppError::Internal(format!("failed to set file mtime: {err}")))
    })
    .await
    .map_err(|err| AppError::Internal(format!("mtime task failed: {err}")))?
}

pub(super) async fn set_file_attributes(
    path: PathBuf,
    mtime_unix_ms: Option<i64>,
    executable: bool,
) -> Result<(), AppError> {
    if let Some(mtime) = mtime_unix_ms {
        set_file_mtime(path.clone(), mtime).await?;
    }
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(&path).map_err(|error| {
            AppError::Internal(format!("failed to read file permissions: {error}"))
        })?;
        let mut mode = metadata.permissions().mode() & !0o111;
        if executable {
            mode |= 0o111;
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
            .map_err(|error| AppError::Internal(format!("failed to set executable bits: {error}")))
    })
    .await
    .map_err(|error| AppError::Internal(format!("permissions task failed: {error}")))??;
    Ok(())
}

pub(super) fn executable_from_metadata(metadata: &Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

pub(super) fn normalize_sync_path(raw: &str) -> Result<String, AppError> {
    let normalized = normalize_sync_path_allow_internal(raw)?;
    if is_internal_path(&normalized) {
        return Err(AppError::BadRequest(
            "internal sync path is reserved".to_string(),
        ));
    }
    Ok(normalized)
}

pub(super) fn normalize_sync_path_allow_internal(raw: &str) -> Result<String, AppError> {
    if raw.is_empty()
        || raw.len() > 4096
        || raw == "."
        || raw == ".."
        || raw.starts_with('/')
        || raw.starts_with('\\')
        || raw.contains('\\')
        || raw.contains(':')
        || raw.contains('\0')
    {
        return Err(AppError::BadRequest("invalid sync path".to_string()));
    }

    let mut parts = Vec::new();
    for part in raw.split('/') {
        if part.is_empty() || part == "." || part == ".." || !validate_portable_segment(part) {
            return Err(AppError::BadRequest("invalid sync path".to_string()));
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

pub(super) fn resolve_relative_path(
    root: &Path,
    normalized_path: &str,
) -> Result<PathBuf, AppError> {
    let normalized_path = normalize_sync_path(normalized_path)?;
    let mut path = root.to_path_buf();
    for part in normalized_path.split('/') {
        path.push(part);
    }
    Ok(path)
}

pub(super) fn relative_path_from_disk(path: &Path, root: &Path) -> Result<String, AppError> {
    let rel = path.strip_prefix(root).map_err(|err| {
        AppError::Internal(format!("failed to derive relative manifest path: {err}"))
    })?;
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => {
                let Some(part) = part.to_str() else {
                    return Err(AppError::BadRequest(
                        "non-UTF-8 filesystem path is not supported".to_string(),
                    ));
                };
                parts.push(part.to_string());
            }
            _ => {
                return Err(AppError::BadRequest(
                    "invalid filesystem path in project".to_string(),
                ));
            }
        }
    }
    normalize_sync_path_allow_internal(&parts.join("/"))
}

pub(super) fn is_internal_path(path: &str) -> bool {
    path.split('/').any(|part| part == ".vivado-server")
}

pub(super) fn hash_file_blocking(path: &Path) -> Result<String, AppError> {
    let file = File::open(path)
        .map_err(|err| AppError::Internal(format!("failed to open file for hashing: {err}")))?;
    let mut reader = BufReader::new(file);
    hash_reader(&mut reader)
}

pub(super) fn hash_reader(reader: &mut impl Read) -> Result<String, AppError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let len = reader
            .read(&mut buffer)
            .map_err(|err| AppError::Internal(format!("failed to hash file: {err}")))?;
        if len == 0 {
            break;
        }
        hasher.update(&buffer[..len]);
    }
    let digest = hasher.finalize();
    Ok(hex_digest(&digest))
}

pub(super) fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub(super) fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn system_time_to_ms(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

pub(super) fn sort_paths(paths: &mut Vec<String>) {
    paths.sort();
    paths.dedup();
}

pub(super) fn sort_paths_deepest_first(paths: &mut Vec<String>) {
    paths.sort_by(|left, right| {
        right
            .matches('/')
            .count()
            .cmp(&left.matches('/').count())
            .then_with(|| left.cmp(right))
    });
    paths.dedup();
}
