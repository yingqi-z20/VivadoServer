//! One owner arbitrates process control, bounded input, and final output.

use super::{
    CreateSessionRequest, Input, Manager, Session, SessionInfo, TerminationReason,
    output::Utf8StreamDecoder,
    process::{self, PtyProcess},
};
use crate::{error::AppError, workspace::ensure_project_dir};
use nix::libc;
use std::{
    io,
    os::fd::OwnedFd,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::{
    io::unix::AsyncFd,
    sync::{mpsc, oneshot},
};

const TERM_GRACE: Duration = Duration::from_secs(1);
const DRAIN_GRACE: Duration = Duration::from_secs(2);
const CLEANUP_DEADLINE: Duration = Duration::from_secs(8);

struct SupervisorGuard {
    manager: Arc<Manager>,
    session: Arc<Session>,
    complete: bool,
}
impl Drop for SupervisorGuard {
    fn drop(&mut self) {
        if !self.complete {
            // A panic/runtime teardown must not silently release the active slot.
            self.manager.degraded.store(true, Ordering::Release);
            tracing::error!(
                event = "session_supervisor_aborted",
                session_id = %self.session.id,
                project = %self.session.project,
                pid = self.session.pid.load(Ordering::Acquire),
                panicking = std::thread::panicking(),
                "Session supervisor ended without confirming process cleanup; capacity remains reserved"
            );
            self.session
                .telemetry
                .event("session_supervisor", "aborted");
            if !self.session.terminal.load(Ordering::Acquire) {
                // The manager still reserves this session until process death
                // can be confirmed. Keep the active-resource gauge elevated.
                self.session.telemetry.operation_finished(
                    "session",
                    "aborted",
                    self.session.started.elapsed(),
                );
            }
        }
    }
}

pub(super) async fn run(
    manager: Arc<Manager>,
    session: Arc<Session>,
    receiver: mpsc::Receiver<Input>,
    request: CreateSessionRequest,
    started: oneshot::Sender<Result<SessionInfo, AppError>>,
) {
    let mut guard = SupervisorGuard {
        manager: manager.clone(),
        session: session.clone(),
        complete: false,
    };
    let id = session.snapshot().await.session_id;
    let directory = match ensure_project_dir(&manager.config.workspace_root, &request.project).await
    {
        Ok(path) => path,
        Err(error) => {
            tracing::error!(event = "session_start_failed", session_id = %session.id, project = %session.project, %error, "Cannot prepare Vivado project directory");
            session
                .finish(TerminationReason::ProcessStartFailed, None, false)
                .await;
            session.begin_retention();
            manager.finished(id);
            guard.complete = true;
            let _ = started.send(Err(error));
            session.drain_archive().await;
            return;
        }
    };
    if started.is_closed() || session.stop.is_cancelled() {
        tracing::info!(event = "session_start_cancelled", session_id = %session.id, project = %session.project, "Session was stopped before process startup");
        session
            .finish(
                if session.stop.is_cancelled() {
                    session.reason()
                } else {
                    TerminationReason::ClientRequested
                },
                None,
                false,
            )
            .await;
        session.begin_retention();
        manager.finished(id);
        guard.complete = true;
        let _ = started.send(Err(AppError::SessionNotRunning));
        session.drain_archive().await;
        return;
    }
    let path = manager.config.vivado_path.clone();
    // This task is manager-owned. HTTP cancellation cannot detach the spawn
    // result or run cleanup before the blocking startup has actually finished.
    let startup_span = tracing::info_span!(
        "vivado_startup",
        session_id = %session.id,
        project = %session.project,
    );
    let spawned = tokio::task::spawn_blocking(move || {
        startup_span.in_scope(|| process::spawn(path, request.args, directory))
    })
    .await;
    let (process, master) = match spawned {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            tracing::error!(event = "session_start_failed", session_id = %session.id, project = %session.project, error = %format!("{error:#}"), "Cannot start Vivado process");
            session
                .finish(TerminationReason::ProcessStartFailed, None, false)
                .await;
            session.begin_retention();
            manager.finished(id);
            guard.complete = true;
            let _ = started.send(Err(AppError::Internal(format!(
                "failed to start Vivado: {error:#}"
            ))));
            session.drain_archive().await;
            return;
        }
        Err(error) => {
            tracing::error!(event = "session_start_task_failed", session_id = %session.id, project = %session.project, %error, "Vivado startup task failed before process ownership could be confirmed");
            // A panicking spawn has no trustworthy result. Keep the slot for
            // operator recovery rather than assume it never created a child.
            session
                .stopping(TerminationReason::ProcessStartFailed)
                .await;
            report_cleanup_error(
                &manager,
                &session,
                format!("Vivado spawn task failed: {error}"),
            )
            .await;
            let _ = started.send(Err(AppError::Internal(
                "Vivado startup ownership could not be confirmed".into(),
            )));
            return;
        }
    };
    session.pid.store(process.pid, Ordering::Release);
    session.history.record(crate::history::Event::new(
        "session.process_started",
        session.workflow_id,
        Some(session.id),
        Some(&session.project),
        serde_json::json!({"pid":process.pid}),
    ));
    tracing::info!(event = "session_process_started", session_id = %session.id, project = %session.project, pid = process.pid, "Vivado process started");
    let mut started = Some(started);
    let mut startup_error = None;
    let mut unclaimed = false;
    let fd = match AsyncFd::new(master) {
        Ok(fd) => {
            if started
                .take()
                .expect("startup response is sent once")
                .send(Ok(session.snapshot().await))
                .is_err()
            {
                unclaimed = true;
                tracing::info!(event = "session_start_unclaimed", session_id = %session.id, project = %session.project, pid = process.pid, "Startup caller disconnected; stopping unclaimed process");
                session.request_stop(TerminationReason::ClientRequested);
            }
            Some(fd)
        }
        Err(error) => {
            tracing::error!(event = "session_output_registration_failed", session_id = %session.id, project = %session.project, pid = process.pid, %error, "Cannot register Vivado PTY for asynchronous output");
            session
                .telemetry
                .event("session_output", "registration_failed");
            session.request_stop(TerminationReason::OutputReadFailed);
            startup_error = Some(AppError::Internal(format!(
                "failed to register PTY: {error}"
            )));
            None
        }
    };
    supervise(&manager, &session, process, fd, receiver).await;
    if unclaimed || startup_error.is_some() {
        session.begin_retention();
    }
    manager.finished(id);
    guard.complete = true;
    if let Some(started) = started {
        let _ = started.send(Err(
            startup_error.expect("unsent startup response has an error")
        ));
    }
    session.drain_archive().await;
}

struct Stop {
    reason: TerminationReason,
    since: Instant,
    term_sent: bool,
    last_kill: Option<Instant>,
}

impl Stop {
    fn new(reason: TerminationReason) -> Self {
        Self {
            reason,
            since: Instant::now(),
            term_sent: false,
            last_kill: None,
        }
    }
}

enum Event {
    Stop,
    Tick,
    Input(Option<Input>),
    Read(io::Result<Vec<u8>>),
    Written(io::Result<usize>),
}

async fn supervise(
    manager: &Manager,
    session: &Session,
    mut process: PtyProcess,
    mut fd: Option<AsyncFd<OwnedFd>>,
    mut receiver: mpsc::Receiver<Input>,
) {
    let mut stop: Option<Stop> = None;
    let mut pending: Option<Input> = None;
    let mut decoder = Utf8StreamDecoder::default();
    let mut reader_done = fd.is_none();
    let mut truncated = fd.is_none();
    let mut leader_exited = false;
    let mut reaped_at: Option<Instant> = None;
    let mut exit_code = None;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if reaped_at.is_some() && reader_done {
            break;
        }
        let user_bytes = pending.as_ref().map(|input| &input.bytes[input.offset..]);
        let write_bytes = user_bytes;
        // AsyncFd operations perform at most one short syscall. They are safe
        // to drop at any select boundary; no blocked writer can delay SIGKILL.
        let event = tokio::select! {
            biased;
            _ = session.stop.cancelled(), if stop.is_none() => Event::Stop,
            _ = tick.tick() => Event::Tick,
            input = receiver.recv(), if stop.is_none() && pending.is_none() && !leader_exited => Event::Input(input),
            result = async { process::write(fd.as_ref().expect("enabled writer has fd"), write_bytes.expect("enabled writer has bytes")).await },
                if fd.is_some() && write_bytes.is_some() && reaped_at.is_none() => Event::Written(result),
            result = async { process::read(fd.as_ref().expect("enabled reader has fd")).await }, if !reader_done => Event::Read(result),
        };
        match event {
            Event::Stop => {
                let reason = session.reason();
                stop = Some(Stop::new(reason));
                session.stopping(reason).await;
                reject_inputs(&mut receiver, &mut pending);
            }
            Event::Input(Some(input)) => {
                pending = Some(input);
            }
            Event::Input(None) => {
                session.request_stop(TerminationReason::ServiceShutdown);
            }
            Event::Read(Ok(bytes)) if bytes.is_empty() => {
                reader_done = true;
            }
            Event::Read(Ok(bytes)) => {
                session.push_output(decoder.push(&bytes)).await;
            }
            Event::Read(Err(error)) => {
                reader_done = true;
                truncated = true;
                tracing::warn!(event = "session_output_read_failed", session_id = %session.id, project = %session.project, %error, pid = process.pid, "PTY output failed");
                session.telemetry.event("session_output", "read_failed");
                session.request_stop(TerminationReason::OutputReadFailed);
                if let Some(stop) = stop.as_mut()
                    && stop.reason == TerminationReason::ProcessExited
                {
                    stop.reason = TerminationReason::OutputReadFailed;
                }
            }
            Event::Written(Ok(count)) => {
                session.telemetry.add_bytes("session_input", count as u64);
                if let Some(input) = pending.as_mut() {
                    input.offset += count;
                    if input.offset == input.bytes.len()
                        && let Some(input) = pending.take()
                    {
                        input.record_completion("written");
                        let _ = input.completion.send(Ok(()));
                    }
                }
            }
            Event::Written(Err(error)) => {
                tracing::warn!(event = "session_input_write_failed", session_id = %session.id, project = %session.project, pid = process.pid, %error, "PTY input failed");
                session.telemetry.event("session_input", "write_failed");
                if let Some(input) = pending.take() {
                    input.record_completion("write_failed");
                    let _ = input.completion.send(Err(AppError::Internal(format!(
                        "PTY input failed: {error}"
                    ))));
                    session.request_stop(TerminationReason::InputWriteFailed);
                }
            }
            Event::Tick => {
                if let Some(reaped) = reaped_at {
                    if !reader_done && reaped.elapsed() >= DRAIN_GRACE {
                        // Seal the stream explicitly; terminal output never grows.
                        tracing::warn!(event = "session_output_drain_timeout", session_id = %session.id, project = %session.project, pid = process.pid, grace_ms = DRAIN_GRACE.as_millis() as u64, "PTY output did not close before drain deadline; output is truncated");
                        session.telemetry.event("session_output", "drain_timeout");
                        truncated = true;
                        reader_done = true;
                        fd.take();
                    }
                    continue;
                }
                if !leader_exited {
                    match process.observe_exit() {
                        Ok(true) => {
                            tracing::debug!(event = "session_leader_exited", session_id = %session.id, project = %session.project, pid = process.pid, "Vivado leader exited; confirming process group cleanup");
                            leader_exited = true;
                            if stop.is_none() {
                                stop = Some(Stop::new(TerminationReason::ProcessExited));
                                session.stopping(TerminationReason::ProcessExited).await;
                                reject_inputs(&mut receiver, &mut pending);
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            session.request_stop(TerminationReason::ProcessWaitFailed);
                            report_cleanup_error(
                                manager,
                                session,
                                format!("cannot observe Vivado exit: {error}"),
                            )
                            .await;
                        }
                    }
                }
                if let Some(stop) = stop.as_mut() {
                    let elapsed = stop.since.elapsed();
                    // Natural leader exit still requires cleaning its remaining
                    // group before scanning and reaping; a single scan is not a
                    // substitute for asking descendants to stop.
                    if !stop.term_sent {
                        match process.signal_group(libc::SIGTERM) {
                            Ok(()) => {
                                stop.term_sent = true;
                                tracing::debug!(event = "session_group_term_sent", session_id = %session.id, project = %session.project, pid = process.pid, "Sent SIGTERM to Vivado process group");
                            }
                            Err(error) => {
                                report_cleanup_error(
                                    manager,
                                    session,
                                    format!("cannot terminate Vivado group: {error}"),
                                )
                                .await
                            }
                        }
                    }
                    if elapsed >= TERM_GRACE
                        && stop
                            .last_kill
                            .is_none_or(|last| last.elapsed() >= Duration::from_secs(1))
                    {
                        let first_kill = stop.last_kill.is_none();
                        stop.last_kill = Some(Instant::now());
                        if let Err(error) = process.signal_group(libc::SIGKILL) {
                            report_cleanup_error(
                                manager,
                                session,
                                format!("cannot kill Vivado group: {error}"),
                            )
                            .await;
                        } else if first_kill {
                            tracing::warn!(event = "session_group_kill_sent", session_id = %session.id, project = %session.project, pid = process.pid, elapsed_ms = elapsed.as_millis() as u64, "Vivado group did not stop during grace period; sent SIGKILL");
                            session.telemetry.event("session_cleanup", "kill_required");
                        }
                    }
                    if elapsed >= CLEANUP_DEADLINE {
                        report_cleanup_error(manager, session, "Vivado group death remains unconfirmed; workflow ownership is retained".into()).await;
                    }
                }
                if leader_exited && stop.as_ref().is_some_and(|stop| stop.term_sent) {
                    let pgid = process.pid;
                    let span = tracing::Span::current();
                    match tokio::task::spawn_blocking(move || {
                        span.in_scope(|| process::group_has_live_members(pgid))
                    })
                    .await
                    {
                        Ok(Ok(false)) => {
                            // /proc enumeration is not an atomic snapshot: a
                            // dying member could fork while it is being scanned.
                            // Quiesce the group with SIGKILL and confirm again
                            // before relinquishing the unreaped leader's PGID.
                            if let Err(error) = process.signal_group(libc::SIGKILL) {
                                report_cleanup_error(
                                    manager,
                                    session,
                                    format!("cannot finalize Vivado group cleanup: {error}"),
                                )
                                .await;
                                continue;
                            }
                            let span = tracing::Span::current();
                            match tokio::task::spawn_blocking(move || {
                                span.in_scope(|| process::group_has_live_members(pgid))
                            })
                            .await
                            {
                                Ok(Ok(false)) => match process.reap() {
                                    Ok(code) => {
                                        tracing::debug!(event = "session_process_reaped", session_id = %session.id, project = %session.project, pid = process.pid, exit_code = code, "Vivado process group is stopped and leader reaped");
                                        exit_code = Some(code);
                                        reaped_at = Some(Instant::now());
                                    }
                                    Err(error) => {
                                        report_cleanup_error(
                                            manager,
                                            session,
                                            format!("cannot reap Vivado: {error}"),
                                        )
                                        .await
                                    }
                                },
                                Ok(Ok(true)) => {}
                                Ok(Err(error)) => {
                                    report_cleanup_error(
                                        manager,
                                        session,
                                        format!("cannot confirm Vivado group cleanup: {error}"),
                                    )
                                    .await
                                }
                                Err(error) => {
                                    report_cleanup_error(
                                        manager,
                                        session,
                                        format!("process group confirmation failed: {error}"),
                                    )
                                    .await
                                }
                            }
                        }
                        Ok(Ok(true)) => {}
                        Ok(Err(error)) => {
                            report_cleanup_error(
                                manager,
                                session,
                                format!("cannot inspect Vivado process group: {error}"),
                            )
                            .await
                        }
                        Err(error) => {
                            report_cleanup_error(
                                manager,
                                session,
                                format!("process group inspection failed: {error}"),
                            )
                            .await
                        }
                    }
                }
            }
        }
    }
    reject_inputs(&mut receiver, &mut pending);
    fd.take();
    session.push_output(decoder.finish()).await;
    let reason = stop.map_or(TerminationReason::ProcessExited, |stop| stop.reason);
    if session.data.lock().await.info.cleanup_error.is_some() {
        tracing::info!(event = "session_cleanup_recovered", session_id = %session.id, project = %session.project, pid = session.pid.load(Ordering::Acquire), "Vivado cleanup recovered after an earlier failure");
        session.telemetry.event("session_cleanup", "recovered");
    }
    session.finish(reason, exit_code, truncated).await;
}

fn reject_inputs(receiver: &mut mpsc::Receiver<Input>, pending: &mut Option<Input>) {
    receiver.close();
    if let Some(input) = pending.take() {
        input.record_completion("interrupted");
        let _ = input.completion.send(Err(AppError::SessionNotRunning));
    }
    while let Ok(input) = receiver.try_recv() {
        input.record_completion("interrupted");
        let _ = input.completion.send(Err(AppError::SessionNotRunning));
    }
}

async fn report_cleanup_error(manager: &Manager, session: &Session, error: String) {
    manager.degraded.store(true, Ordering::Release);
    let mut data = session.data.lock().await;
    if data.info.cleanup_error.is_none() {
        tracing::error!(event = "session_cleanup_failed", session_id = %session.id, project = %session.project, pid = session.pid.load(Ordering::Acquire), %error, "Vivado cleanup is incomplete");
        session.telemetry.event("session_cleanup", "incomplete");
        data.info.cleanup_error = Some(error);
    }
    drop(data);
    session.changed.notify_waiters();
}
