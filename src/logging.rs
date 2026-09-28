//! Process logging setup. Keep the guard alive until every service has drained.
use crate::{LogFormat, ObservabilityConfig};
use anyhow::Context;
use std::{
    ffi::{CString, OsStr},
    fs::File,
    io::{self, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing_appender::non_blocking::{ErrorCounter, WorkerGuard};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const DAY_SECONDS: u64 = 24 * 3600;
const RETENTION_SECONDS: u64 = 14 * DAY_SECONDS;

/// Owns the bounded log writer and flushes queued events when dropped.
pub struct LoggingGuard {
    _worker: WorkerGuard,
    pub(crate) counter: ErrorCounter,
    pub(crate) file_failures: Arc<AtomicU64>,
}

impl LoggingGuard {
    /// Events discarded when the log collector cannot keep up with the service.
    pub fn dropped_lines(&self) -> usize {
        self.counter.dropped_lines()
    }

    /// File writes, flushes or retention checks that failed since initialization.
    pub fn file_write_failures(&self) -> u64 {
        self.file_failures.load(Ordering::Relaxed)
    }
}

/// Install JSON or text logging, using `RUST_LOG` as an explicit filter override.
///
/// The 4096-line queue protects PTY handling from a blocked stdout collector.
/// Queue overflow is observable through the authenticated metrics endpoint.
pub fn initialize_logging(config: &ObservabilityConfig) -> anyhow::Result<LoggingGuard> {
    initialize_with_writer(config, std::io::stdout(), Arc::new(AtomicU64::new(0)))
}

/// Preserve stdout logging and retain at most five 20 MiB service log files.
///
/// Files live under `workspace_root/.vivado-server/service-logs`. Every path
/// component is opened without following links. The retained directory handle
/// also prevents later ancestor replacement from redirecting rotation writes.
/// The configured format applies to both destinations (JSON by default).
pub fn initialize_logging_with_root(
    config: &ObservabilityConfig,
    workspace_root: &Path,
) -> anyhow::Result<LoggingGuard> {
    // Validate the filter before creating any directories or rotating files.
    parse_filter(config, std::env::var("RUST_LOG").ok().as_deref())?;
    let file = RotatingLog::open(workspace_root, 20 * 1024 * 1024, 5)
        .context("failed to initialize bounded service log files")?;
    let failures = Arc::new(AtomicU64::new(0));
    initialize_with_writer(
        config,
        ServiceWriter {
            file,
            stdout: std::io::stdout(),
            warned: false,
            failures: failures.clone(),
        },
        failures,
    )
}

fn initialize_with_writer(
    config: &ObservabilityConfig,
    destination: impl Write + Send + 'static,
    file_failures: Arc<AtomicU64>,
) -> anyhow::Result<LoggingGuard> {
    let override_filter = std::env::var("RUST_LOG").ok();
    let filter = parse_filter(config, override_filter.as_deref())?;
    let (writer, worker) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .thread_name("vivado-log-writer")
        .finish(destination);
    let counter = writer.error_counter();
    let subscriber = tracing_subscriber::registry().with(filter);
    match config.log_format {
        LogFormat::Json => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer),
            )
            .try_init(),
        LogFormat::Text => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_target(true)
                    .with_writer(writer),
            )
            .try_init(),
    }
    .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(event = "process.panic", error = %info, "task panicked");
        // Preserve Rust's stderr/backtrace behavior, including before queue flush.
        previous(info);
    }));
    Ok(LoggingGuard {
        _worker: worker,
        counter,
        file_failures,
    })
}

/// File errors do not suppress stdout; stdout errors do not suppress files.
struct ServiceWriter<W> {
    file: RotatingLog,
    stdout: W,
    warned: bool,
    failures: Arc<AtomicU64>,
}

impl<W: Write> Write for ServiceWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let file_result = self.file.write_all(bytes);
        let stdout_result = self.stdout.write_all(bytes);
        if file_result.is_err() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        if file_result.is_err() && !self.warned {
            // Never print record bodies: they may contain operator diagnostics.
            eprintln!("Vivado service file logging failed; stdout logging remains enabled");
            self.warned = true;
        } else if file_result.is_ok() {
            self.warned = false;
        }
        // Keep the nonblocking worker alive even if one collector fails.
        file_result.or(stdout_result).map(|_| bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let file_result = self.file.flush();
        let stdout_result = self.stdout.flush();
        if file_result.is_err() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        file_result.or(stdout_result)
    }
}

struct RotatingLog {
    directory: File,
    _lock: File,
    current: File,
    bytes: u64,
    limit: u64,
    files: usize,
    current_day: u64,
    last_cleanup: Instant,
}

impl RotatingLog {
    fn open(root: &Path, limit: u64, files: usize) -> io::Result<Self> {
        if limit == 0 || files == 0 {
            return Err(io::Error::other("invalid service log limits"));
        }
        let workspace = open_directory_chain(root)?;
        // Older workspaces created this shared metadata directory with 0755.
        // Only service-logs itself needs private permissions.
        let internal = open_directory_at(&workspace, OsStr::new(".vivado-server"), false)?;
        let directory = open_directory_at(&internal, OsStr::new("service-logs"), true)?;
        let lock = open_regular_at(&directory, ".writer.lock")?;
        // Initialization happens before the workspace runtime takes its own lock.
        // A separate lock prevents two initializers from sharing rotation files.
        syscall(|| unsafe {
            nix::libc::flock(lock.as_raw_fd(), nix::libc::LOCK_EX | nix::libc::LOCK_NB)
        })
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "workspace is already in use or service log lock cannot be acquired: {error}"
                ),
            )
        })?;
        let current = open_regular_at(&directory, "service.log")?;
        let bytes = current.metadata()?.len();
        let current_day = unix_seconds(current.metadata()?.modified()?) / DAY_SECONDS;
        let mut result = Self {
            directory,
            _lock: lock,
            current,
            bytes,
            limit,
            files,
            current_day,
            last_cleanup: Instant::now(),
        };
        // Validate retained names too, before any rename can replace one.
        for index in 1..files {
            result.validate_existing(&format!("service.log.{index}"))?;
        }
        if bytes > limit {
            return Err(io::Error::other(
                "existing service log exceeds the file limit",
            ));
        }
        if bytes == limit {
            result.rotate()?;
        }
        result.prune_expired(SystemTime::now())?;
        Ok(result)
    }

    fn validate_existing(&self, name: &str) -> io::Result<()> {
        self.inspect_existing(name).map(|_| ())
    }

    fn inspect_existing(&self, name: &str) -> io::Result<Option<nix::libc::stat>> {
        let name = c_name(OsStr::new(name))?;
        let mut stat = std::mem::MaybeUninit::<nix::libc::stat>::uninit();
        let result = syscall(|| unsafe {
            nix::libc::fstatat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                nix::libc::AT_SYMLINK_NOFOLLOW,
            )
        });
        match result {
            Ok(_) => {
                // fstatat initialized the complete stat object on success.
                let stat = unsafe { stat.assume_init() };
                if stat.st_mode & nix::libc::S_IFMT != nix::libc::S_IFREG
                    || stat.st_nlink != 1
                    || stat.st_uid != unsafe { nix::libc::geteuid() }
                    || stat.st_mode & 0o077 != 0
                    || stat.st_size < 0
                    || stat.st_size as u64 > self.limit
                {
                    return Err(io::Error::other("unsafe retained service log"));
                }
                Ok(Some(stat))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn prune_expired(&mut self, now: SystemTime) -> io::Result<()> {
        // Validate all fixed names before deleting any. No directory enumeration
        // or user-controlled path ever reaches unlinkat.
        let mut expired = Vec::new();
        let now = unix_seconds(now);
        for index in 0..self.files {
            if let Some(stat) = self.inspect_existing(&log_name(index))?
                && now.saturating_sub((stat.st_mtime.max(0) as u64 / DAY_SECONDS) * DAY_SECONDS)
                    >= RETENTION_SECONDS
            {
                expired.push(index);
            }
        }
        for index in expired {
            let name = c_name(OsStr::new(&log_name(index)))?;
            let removed = syscall(|| unsafe {
                nix::libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0)
            });
            if let Err(error) = removed
                && error.kind() != io::ErrorKind::NotFound
            {
                return Err(error);
            }
            if index == 0 {
                self.current = open_regular_at(&self.directory, "service.log")?;
                self.bytes = self.current.metadata()?.len();
                self.current_day = now / DAY_SECONDS;
            }
        }
        self.last_cleanup = Instant::now();
        Ok(())
    }

    fn prepare_write(&mut self, now: SystemTime) -> io::Result<()> {
        if self.last_cleanup.elapsed() >= Duration::from_secs(60) {
            self.prune_expired(now)?;
        }
        let day = unix_seconds(now) / DAY_SECONDS;
        // Rotate at the UTC date boundary, including after restart (mtime tells
        // us the existing segment's date). This prevents continuous writes from
        // keeping a months-old segment alive by refreshing its mtime.
        if day != self.current_day {
            if self.bytes != 0 {
                self.rotate()?;
            }
            self.current_day = day;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.current.flush()?;
        for index in 0..self.files {
            self.validate_existing(&log_name(index))?;
        }
        let oldest = c_name(OsStr::new(&log_name(self.files - 1)))?;
        let removed = syscall(|| unsafe {
            nix::libc::unlinkat(self.directory.as_raw_fd(), oldest.as_ptr(), 0)
        });
        if let Err(error) = removed
            && error.kind() != io::ErrorKind::NotFound
        {
            return Err(error);
        }
        for index in (1..self.files).rev() {
            let source = c_name(OsStr::new(&log_name(index - 1)))?;
            let destination = c_name(OsStr::new(&log_name(index)))?;
            let renamed = syscall(|| unsafe {
                nix::libc::renameat(
                    self.directory.as_raw_fd(),
                    source.as_ptr(),
                    self.directory.as_raw_fd(),
                    destination.as_ptr(),
                )
            });
            if let Err(error) = renamed
                && error.kind() != io::ErrorKind::NotFound
            {
                return Err(error);
            }
        }
        self.current = open_regular_at(&self.directory, "service.log")?;
        self.bytes = self.current.metadata()?.len();
        self.current_day = unix_seconds(SystemTime::now()) / DAY_SECONDS;
        Ok(())
    }
}

impl Write for RotatingLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.prepare_write(SystemTime::now())?;
        let incoming = bytes.len() as u64;
        // Never split a structured record across files or exceed the disk cap.
        // An oversize record remains available on stdout.
        if incoming > self.limit {
            return Err(io::Error::other("service log record exceeds file limit"));
        }
        if self.bytes.saturating_add(incoming) > self.limit {
            self.rotate()?;
        }
        // Refresh the inode properties before appending; reject introduced links.
        let metadata = self.current.metadata()?;
        if metadata.nlink() != 1 || metadata.len() > self.limit {
            return Err(io::Error::other("service log changed unexpectedly"));
        }
        self.bytes = metadata.len();
        if self.bytes.saturating_add(incoming) > self.limit {
            self.rotate()?;
        }
        let count = self.current.write(bytes)?;
        self.bytes += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.current.flush()
    }
}

fn unix_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn log_name(index: usize) -> String {
    if index == 0 {
        "service.log".into()
    } else {
        format!("service.log.{index}")
    }
}

fn c_name(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| io::Error::other("NUL in log path"))
}

fn syscall(mut call: impl FnMut() -> i32) -> io::Result<i32> {
    loop {
        let result = call();
        if result >= 0 {
            return Ok(result);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn open_directory_chain(path: &Path) -> io::Result<File> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut directory = File::open("/")?;
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => directory = open_directory_at(&directory, name, false)?,
            _ => return Err(io::Error::other("workspace path contains parent traversal")),
        }
    }
    Ok(directory)
}

fn open_directory_at(parent: &File, name: &OsStr, private: bool) -> io::Result<File> {
    let name = c_name(name)?;
    let open = || {
        syscall(|| unsafe {
            nix::libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                nix::libc::O_RDONLY
                    | nix::libc::O_DIRECTORY
                    | nix::libc::O_NOFOLLOW
                    | nix::libc::O_CLOEXEC,
            )
        })
    };
    let fd = match open() {
        Ok(fd) => fd,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let created =
                syscall(|| unsafe { nix::libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) });
            if let Err(error) = created
                && error.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(error);
            }
            open()?
        }
        Err(error) => return Err(error),
    };
    // A successful openat gives this File sole ownership of the descriptor.
    let directory = unsafe { File::from_raw_fd(fd) };
    let metadata = directory.metadata()?;
    if private
        && (metadata.uid() != unsafe { nix::libc::geteuid() } || metadata.mode() & 0o077 != 0)
    {
        return Err(io::Error::other(
            "service log directory must be private and owned by this user",
        ));
    }
    Ok(directory)
}

fn open_regular_at(parent: &File, name: &str) -> io::Result<File> {
    let name = c_name(OsStr::new(name))?;
    let fd = syscall(|| unsafe {
        nix::libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            nix::libc::O_WRONLY
                | nix::libc::O_APPEND
                | nix::libc::O_CREAT
                | nix::libc::O_NOFOLLOW
                | nix::libc::O_NONBLOCK
                | nix::libc::O_CLOEXEC,
            0o600,
        )
    })?;
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { nix::libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(io::Error::other(
            "service log must be a private regular file with one link",
        ));
    }
    Ok(file)
}

fn parse_filter(config: &ObservabilityConfig, supplied: Option<&str>) -> anyhow::Result<EnvFilter> {
    let filter = supplied.unwrap_or(&config.log_filter);
    anyhow::ensure!(!filter.trim().is_empty(), "log filter cannot be empty");
    EnvFilter::try_new(filter)
        .context("invalid log filter (RUST_LOG overrides observability.log_filter)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    fn logs(root: &Path) -> std::path::PathBuf {
        root.join(".vivado-server/service-logs")
    }

    #[test]
    fn explicit_invalid_filter_is_not_silently_replaced() {
        let config = ObservabilityConfig::default();
        assert!(parse_filter(&config, None).is_ok());
        assert!(parse_filter(&config, Some("vivado_server=debug")).is_ok());
        assert!(parse_filter(&config, Some("vivado_server=invalid_level")).is_err());
        assert!(parse_filter(&config, Some("")).is_err());
    }

    #[test]
    fn rotates_complete_records_and_enforces_file_and_byte_caps() {
        let temp = tempfile::tempdir().unwrap();
        let mut writer = RotatingLog::open(temp.path(), 12, 5).unwrap();
        for number in 0..10 {
            writer
                .write_all(format!("{{\"n\":{number}}}\n").as_bytes())
                .unwrap();
        }
        writer.flush().unwrap();
        let directory = logs(temp.path());
        let entries: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap() != ".writer.lock")
            .collect();
        assert_eq!(entries.len(), 5);
        for index in 0..5 {
            let path = directory.join(log_name(index));
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                format!("{{\"n\":{}}}\n", 9 - index)
            );
            assert!(path.metadata().unwrap().len() <= 12);
            assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        }
        assert_eq!(
            directory.metadata().unwrap().permissions().mode() & 0o777,
            0o700
        );
        drop(writer);
        let mut reopened = RotatingLog::open(temp.path(), 12, 5).unwrap();
        reopened.write_all(b"{\"n\":10}\n").unwrap();
        assert_eq!(
            fs::read_to_string(directory.join("service.log")).unwrap(),
            "{\"n\":10}\n"
        );
    }

    #[test]
    fn rejects_links_and_non_regular_destinations_without_touching_target() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("external");
        fs::write(&target, b"preserve").unwrap();
        for name in ["service.log", "service.log.1", ".writer.lock"] {
            let workspace = tempfile::tempdir().unwrap();
            fs::create_dir_all(logs(workspace.path())).unwrap();
            fs::set_permissions(logs(workspace.path()), fs::Permissions::from_mode(0o700)).unwrap();
            symlink(&target, logs(workspace.path()).join(name)).unwrap();
            assert!(
                RotatingLog::open(workspace.path(), 12, 5).is_err(),
                "{name}"
            );
            assert_eq!(fs::read(&target).unwrap(), b"preserve");
        }
        let workspace = tempfile::tempdir().unwrap();
        fs::create_dir_all(logs(workspace.path())).unwrap();
        fs::set_permissions(logs(workspace.path()), fs::Permissions::from_mode(0o700)).unwrap();
        fs::hard_link(&target, logs(workspace.path()).join("service.log")).unwrap();
        assert!(RotatingLog::open(workspace.path(), 12, 5).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"preserve");
        fs::remove_file(logs(workspace.path()).join("service.log")).unwrap();
        let fifo = c_name(logs(workspace.path()).join("service.log").as_os_str()).unwrap();
        assert_eq!(unsafe { nix::libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(RotatingLog::open(workspace.path(), 12, 5).is_err());
    }

    #[test]
    fn rejects_linked_ancestors_and_concurrent_writer() {
        let temp = tempfile::tempdir().unwrap();
        let actual = temp.path().join("actual");
        fs::create_dir(&actual).unwrap();
        symlink(&actual, temp.path().join("alias")).unwrap();
        assert!(RotatingLog::open(&temp.path().join("alias/workspace"), 12, 5).is_err());
        assert!(!actual.join("workspace").exists());
        let writer = RotatingLog::open(&actual, 12, 5).unwrap();
        assert!(RotatingLog::open(&actual, 12, 5).is_err());
        drop(writer);
        assert!(RotatingLog::open(&actual, 12, 5).is_ok());
    }

    #[test]
    fn pinned_directory_cannot_be_redirected_by_ancestor_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let mut writer = RotatingLog::open(temp.path(), 8, 5).unwrap();
        writer.write_all(b"first\n").unwrap();
        let retained = temp.path().join("retained");
        fs::rename(logs(temp.path()), &retained).unwrap();
        let other = temp.path().join("other");
        fs::create_dir(&other).unwrap();
        symlink(&other, logs(temp.path())).unwrap();
        writer.write_all(b"second\n").unwrap();
        assert_eq!(fs::read_dir(&other).unwrap().count(), 0);
        assert_eq!(
            fs::read(retained.join("service.log.1")).unwrap(),
            b"first\n"
        );
        assert_eq!(fs::read(retained.join("service.log")).unwrap(), b"second\n");
    }

    #[test]
    fn stdout_survives_oversize_record_and_rotation_failure() {
        let temp = tempfile::tempdir().unwrap();
        let file = RotatingLog::open(temp.path(), 8, 5).unwrap();
        let mut writer = ServiceWriter {
            file,
            stdout: Vec::new(),
            warned: false,
            failures: Arc::new(AtomicU64::new(0)),
        };
        writer.write_all(b"oversized-record\n").unwrap();
        assert_eq!(writer.stdout, b"oversized-record\n");
        assert_eq!(
            fs::metadata(logs(temp.path()).join("service.log"))
                .unwrap()
                .len(),
            0
        );
        writer.write_all(b"first\n").unwrap();
        let target = temp.path().join("untouched");
        fs::write(&target, b"safe").unwrap();
        symlink(&target, logs(temp.path()).join("service.log.1")).unwrap();
        writer.write_all(b"second\n").unwrap();
        assert_eq!(writer.stdout, b"oversized-record\nfirst\nsecond\n");
        assert_eq!(fs::read(target).unwrap(), b"safe");
        assert_eq!(writer.failures.load(Ordering::Relaxed), 2);
        let telemetry = crate::observability::Observability::new();
        telemetry.attach_log_file_error_counter(writer.failures.clone());
        assert!(
            telemetry
                .render()
                .unwrap()
                .contains("vivado_server_log_file_write_failures 2\n")
        );
    }

    #[test]
    fn fourteen_day_pruning_removes_expired_segments_and_reopens_current() {
        let temp = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        let old =
            filetime::FileTime::from_system_time(now - Duration::from_secs(RETENTION_SECONDS + 1));
        let mut writer = RotatingLog::open(temp.path(), 8, 5).unwrap();
        writer.write_all(b"older\n").unwrap();
        writer.write_all(b"newer\n").unwrap();
        let directory = logs(temp.path());
        filetime::set_file_mtime(directory.join("service.log.1"), old).unwrap();
        writer.prune_expired(now).unwrap();
        assert!(!directory.join("service.log.1").exists());
        assert_eq!(fs::read(directory.join("service.log")).unwrap(), b"newer\n");
        filetime::set_file_mtime(directory.join("service.log"), old).unwrap();
        writer.prune_expired(now).unwrap();
        writer.write_all(b"fresh\n").unwrap();
        assert_eq!(fs::read(directory.join("service.log")).unwrap(), b"fresh\n");
        // Startup also applies expiry, even when no periodic write has occurred.
        filetime::set_file_mtime(directory.join("service.log"), old).unwrap();
        drop(writer);
        let _reopened = RotatingLog::open(temp.path(), 8, 5).unwrap();
        assert_eq!(
            fs::metadata(directory.join("service.log")).unwrap().len(),
            0
        );
    }

    #[test]
    fn daily_rotation_prevents_append_from_extending_segment_age_forever() {
        let temp = tempfile::tempdir().unwrap();
        let mut writer = RotatingLog::open(temp.path(), 64, 5).unwrap();
        writer.write_all(b"previous day\n").unwrap();
        let tomorrow = UNIX_EPOCH + Duration::from_secs((writer.current_day + 1) * DAY_SECONDS);
        writer.prepare_write(tomorrow).unwrap();
        assert_eq!(
            fs::read(logs(temp.path()).join("service.log.1")).unwrap(),
            b"previous day\n"
        );
        assert_eq!(
            fs::metadata(logs(temp.path()).join("service.log"))
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn retention_does_not_follow_links_or_delete_unrelated_files() {
        let temp = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        let old =
            filetime::FileTime::from_system_time(now - Duration::from_secs(RETENTION_SECONDS + 1));
        let mut writer = RotatingLog::open(temp.path(), 32, 5).unwrap();
        writer.write_all(b"current\n").unwrap();
        let unrelated = logs(temp.path()).join("unrelated.log");
        fs::write(&unrelated, b"preserve").unwrap();
        filetime::set_file_mtime(&unrelated, old).unwrap();
        symlink(&unrelated, logs(temp.path()).join("service.log.1")).unwrap();
        filetime::set_file_mtime(logs(temp.path()).join("service.log"), old).unwrap();
        assert!(writer.prune_expired(now).is_err());
        assert_eq!(fs::read(&unrelated).unwrap(), b"preserve");
        // Validate all targets first: the invalid retained link prevents partial
        // cleanup, so the active current file remains untouched as well.
        assert_eq!(
            fs::read(logs(temp.path()).join("service.log")).unwrap(),
            b"current\n"
        );
        fs::remove_file(logs(temp.path()).join("service.log.1")).unwrap();
        writer.prune_expired(now).unwrap();
        assert_eq!(fs::read(unrelated).unwrap(), b"preserve");
    }
}
