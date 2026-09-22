//! Linux process identity and nonblocking PTY primitives.

use nix::libc;
use portable_pty::{Child, CommandBuilder, PtySize, native_pty_system};
use std::{
    fs, io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::PathBuf,
};
use tokio::io::unix::AsyncFd;

pub(super) struct PtyProcess {
    child: Option<Box<dyn Child + Send + Sync>>,
    pub(super) pid: libc::pid_t,
    identity_owned: bool,
}

pub(super) fn spawn(
    path: PathBuf,
    args: Vec<String>,
    directory: PathBuf,
) -> anyhow::Result<(PtyProcess, OwnedFd)> {
    let pair = native_pty_system().openpty(PtySize {
        rows: 40,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let raw = pair
        .master
        .as_raw_fd()
        .ok_or_else(|| anyhow::anyhow!("PTY master has no fd"))?;
    // Duplicating before spawn means all fallible fd setup precedes child creation.
    let copied = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
    if copied < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let master = unsafe { OwnedFd::from_raw_fd(copied) };
    // This is a byte-stream API, not an interactive keyboard. Canonical mode
    // silently discards bytes past MAX_CANON on long Tcl lines. Raw mode also
    // disables kernel echo/newline conversion and control-character signals;
    // stopping is handled through our independent process-group control path.
    let mut terminal: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(copied, &mut terminal) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    unsafe { libc::cfmakeraw(&mut terminal) };
    terminal.c_cc[libc::VMIN] = 1;
    terminal.c_cc[libc::VTIME] = 0;
    if unsafe { libc::tcsetattr(copied, libc::TCSANOW, &terminal) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let flags = unsafe { libc::fcntl(copied, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(copied, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut command = CommandBuilder::new(path);
    command.args(["-mode", "tcl"]);
    command.args(args);
    command.cwd(directory);
    let child = pair.slave.spawn_command(command)?;
    // Unix portable-pty executes setsid before exec. Its child PID is our
    // process-group ID; tcgetpgrp would instead report a mutable foreground group.
    let pid = child
        .process_id()
        .expect("Unix children always have a process ID") as libc::pid_t;
    let process = PtyProcess {
        child: Some(child),
        pid,
        identity_owned: true,
    };
    drop(pair.slave);
    drop(pair.master);
    Ok((process, master))
}

impl PtyProcess {
    /// Observe, but do not reap, the leader. Its zombie reserves this PGID while
    /// we clean up descendants; no signal is sent to a numerically reused PID.
    pub(super) fn observe_exit(&mut self) -> io::Result<bool> {
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                self.pid as libc::id_t,
                &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                self.identity_owned = false;
            }
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(false);
            }
            return Err(error);
        }
        Ok(unsafe { status.si_pid() } != 0)
    }

    pub(super) fn signal_group(&self, signal: libc::c_int) -> io::Result<()> {
        if !self.identity_owned {
            return Err(io::Error::other(
                "child ownership was lost; refusing to signal a numeric PGID",
            ));
        }
        if unsafe { libc::kill(-self.pid, signal) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(error)
        }
    }

    /// Call only after WNOWAIT reported exit and the process group is quiescent.
    pub(super) fn reap(&mut self) -> io::Result<i32> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("child already reaped"))?;
        let status = match child.wait() {
            Ok(status) => status,
            Err(error) => {
                if error.raw_os_error() == Some(libc::ECHILD) {
                    self.identity_owned = false;
                }
                return Err(error);
            }
        };
        self.identity_owned = false;
        self.child.take();
        Ok(status.exit_code() as i32)
    }
}

impl Drop for PtyProcess {
    fn drop(&mut self) {
        // Emergency path for runtime teardown or a panic, not normal cleanup.
        // Never invoke portable-pty's killer: its Unix implementation can reap
        // early and invalidate the PGID before descendants have been handled.
        if self.child.is_some() && self.identity_owned {
            let _ = self.signal_group(libc::SIGKILL);
        }
    }
}

pub(super) async fn read(fd: &AsyncFd<OwnedFd>) -> io::Result<Vec<u8>> {
    loop {
        let mut ready = fd.readable().await?;
        match ready.try_io(|fd| {
            let mut bytes = vec![0; 4096];
            let size = unsafe {
                libc::read(
                    fd.get_ref().as_raw_fd(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            if size < 0 {
                let error = io::Error::last_os_error();
                // Linux reports slave closure as EIO, including normal exit.
                if error.raw_os_error() == Some(libc::EIO) {
                    return Ok(Vec::new());
                }
                return Err(error);
            }
            bytes.truncate(size as usize);
            Ok(bytes)
        }) {
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
            Ok(result) => return result,
            Err(_) => continue,
        }
    }
}

pub(super) async fn write(fd: &AsyncFd<OwnedFd>, bytes: &[u8]) -> io::Result<usize> {
    loop {
        let mut ready = fd.writable().await?;
        match ready.try_io(|fd| {
            let size = unsafe {
                libc::write(
                    fd.get_ref().as_raw_fd(),
                    bytes.as_ptr().cast(),
                    bytes.len().min(4096),
                )
            };
            if size < 0 {
                return Err(io::Error::last_os_error());
            }
            if size == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "PTY input closed"));
            }
            Ok(size as usize)
        }) {
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
            Ok(result) => return result,
            Err(_) => continue,
        }
    }
}

/// `/proc` failures cannot be interpreted as an empty process group. The
/// supported tools retain this PGID and stay visible to their service UID.
pub(super) fn group_has_live_members(pgid: libc::pid_t) -> io::Result<bool> {
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.as_encoded_bytes().iter().all(u8::is_ascii_digit) {
            continue;
        }
        let bytes = match fs::read(entry.path().join("stat")) {
            Ok(bytes) => bytes,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        let (group, live) = parse_stat(&bytes)?;
        if group == pgid && live {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_stat(bytes: &[u8]) -> io::Result<(libc::pid_t, bool)> {
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed /proc process stat");
    // comm is arbitrary bytes and may itself contain ')', spaces, or newlines.
    let end = bytes
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(malformed)?;
    let tail = std::str::from_utf8(&bytes[end + 1..]).map_err(|_| malformed())?;
    let fields: Vec<_> = tail.split_ascii_whitespace().collect();
    let state = fields.first().ok_or_else(malformed)?;
    let pgid = fields
        .get(2)
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    let threads: usize = fields
        .get(17)
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    // A thread-group leader may be a zombie while another thread is still
    // running after pthread_exit; do not release ownership in that case.
    let live = !matches!(*state, "Z" | "X" | "x") || threads > 1;
    Ok((pgid, live))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn stat(state: &str, threads: usize) -> Vec<u8> {
        format!("12 (odd ) name\n) {state} 1 12 12 0 0 0 0 0 0 0 0 0 0 0 0 0 {threads} 0\n")
            .into_bytes()
    }
    #[test]
    fn process_stat_handles_special_names_and_live_threads_of_zombie_leader() {
        assert_eq!(parse_stat(&stat("Z", 1)).unwrap(), (12, false));
        assert_eq!(parse_stat(&stat("Z", 2)).unwrap(), (12, true));
        assert_eq!(parse_stat(&stat("S", 1)).unwrap(), (12, true));
        let mut bytes = stat("S", 1);
        bytes[5] = 255;
        assert_eq!(parse_stat(&bytes).unwrap(), (12, true));
        assert!(parse_stat(b"broken").is_err());
    }

    #[test]
    fn lost_wait_ownership_disables_numeric_group_signals() {
        let mut process = PtyProcess {
            child: None,
            pid: unsafe { libc::getpid() },
            identity_owned: true,
        };
        assert_eq!(
            process.observe_exit().unwrap_err().raw_os_error(),
            Some(libc::ECHILD)
        );
        assert!(!process.identity_owned);
        // Signal 0 probes permissions only; the guard must reject even that.
        assert!(process.signal_group(0).is_err());
    }

    #[tokio::test]
    async fn a_full_nonblocking_writer_can_be_cancelled_and_used_again() {
        let mut handles = [-1; 2];
        assert_eq!(
            unsafe { libc::pipe2(handles.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) },
            0
        );
        let reader = unsafe { OwnedFd::from_raw_fd(handles[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(handles[1]) };
        let writer = AsyncFd::new(writer).unwrap();
        let bytes = [0_u8; 4096];
        loop {
            let size = unsafe {
                libc::write(
                    writer.get_ref().as_raw_fd(),
                    bytes.as_ptr().cast(),
                    bytes.len(),
                )
            };
            if size < 0 {
                assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
                break;
            }
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), write(&writer, &bytes))
                .await
                .is_err()
        );
        let mut drained = [0_u8; 4096];
        assert!(
            unsafe {
                libc::read(
                    reader.as_raw_fd(),
                    drained.as_mut_ptr().cast(),
                    drained.len(),
                )
            } > 0
        );
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), write(&writer, &bytes))
                .await
                .unwrap()
                .unwrap(),
            bytes.len()
        );
    }
}
