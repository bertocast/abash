use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use abash_core::{ErrorKind, ExecutionRequest, ExecutionResult, SandboxError, SandboxSession};
use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;

use crate::observability::{AuditEvent, AuditEventKind, RunEvent, RunEventKind, RunStatus};

const MAX_RETAINED_EVENTS: usize = 64;
const MAX_RETAINED_AUDITS: usize = 64;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Default)]
pub struct RuntimeCallbacks {
    pub event_callback: Option<Arc<Py<PyAny>>>,
    pub audit_callback: Option<Arc<Py<PyAny>>>,
}

pub struct SandboxRuntime {
    session: Arc<Mutex<SandboxSession>>,
    cancel_flag: Arc<AtomicBool>,
    session_id: String,
    backend: String,
    profile: String,
    filesystem_mode: String,
    callbacks: RuntimeCallbacks,
    next_run_id: AtomicU64,
    active_run: Arc<Mutex<Option<Arc<RunState>>>>,
    audit_log: Arc<Mutex<VecDeque<AuditEvent>>>,
    next_audit_sequence: Arc<AtomicU64>,
}

#[derive(Debug)]
struct RunStateInner {
    status: RunStatus,
    result: Option<ExecutionResult>,
    events: VecDeque<RunEvent>,
    audits: VecDeque<AuditEvent>,
    next_event_sequence: u64,
}

#[derive(Debug)]
pub struct RunState {
    run_id: String,
    started_at_ms: u64,
    inner: Mutex<RunStateInner>,
    condvar: Condvar,
}

impl RunState {
    fn new(run_id: String, started_at_ms: u64) -> Self {
        Self {
            run_id,
            started_at_ms,
            inner: Mutex::new(RunStateInner {
                status: RunStatus::Pending,
                result: None,
                events: VecDeque::new(),
                audits: VecDeque::new(),
                next_event_sequence: 1,
            }),
            condvar: Condvar::new(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn started_at_ms(&self) -> u64 {
        self.started_at_ms
    }

    pub fn status(&self) -> RunStatus {
        self.inner.lock().status.clone()
    }

    pub fn wait(&self) -> ExecutionResult {
        let mut guard = self.inner.lock();
        while guard.result.is_none() {
            self.condvar.wait(&mut guard);
        }
        guard
            .result
            .clone()
            .expect("completed run must store a result")
    }

    pub fn stdout(&self) -> String {
        self.inner
            .lock()
            .result
            .as_ref()
            .map(|result| String::from_utf8_lossy(&result.stdout).to_string())
            .unwrap_or_default()
    }

    pub fn stderr(&self) -> String {
        self.inner
            .lock()
            .result
            .as_ref()
            .map(|result| String::from_utf8_lossy(&result.stderr).to_string())
            .unwrap_or_default()
    }

    pub fn output(&self) -> String {
        let guard = self.inner.lock();
        let Some(result) = guard.result.as_ref() else {
            return String::new();
        };
        format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        )
    }

    pub fn events(&self) -> Vec<RunEvent> {
        self.inner.lock().events.iter().cloned().collect()
    }

    pub fn audits(&self) -> Vec<AuditEvent> {
        self.inner.lock().audits.iter().cloned().collect()
    }

    fn set_status(&self, status: RunStatus) {
        self.inner.lock().status = status;
    }

    fn push_event(&self, event: RunEvent) {
        let mut guard = self.inner.lock();
        push_bounded(&mut guard.events, event, MAX_RETAINED_EVENTS);
    }

    fn push_audit(&self, audit: AuditEvent) {
        let mut guard = self.inner.lock();
        push_bounded(&mut guard.audits, audit, MAX_RETAINED_AUDITS);
    }

    fn next_event_sequence(&self) -> u64 {
        let mut guard = self.inner.lock();
        let sequence = guard.next_event_sequence;
        guard.next_event_sequence += 1;
        sequence
    }

    fn finish(&self, result: ExecutionResult, status: RunStatus) {
        let mut guard = self.inner.lock();
        guard.status = status;
        guard.result = Some(result);
        self.condvar.notify_all();
    }
}

impl SandboxRuntime {
    pub fn new(
        session: Arc<Mutex<SandboxSession>>,
        cancel_flag: Arc<AtomicBool>,
        callbacks: RuntimeCallbacks,
    ) -> Self {
        let metadata = session.lock().base_metadata();
        let backend = metadata
            .get("backend")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let profile = metadata
            .get("profile")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let filesystem_mode = metadata
            .get("filesystem_mode")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let session_id = format!("session-{}", NEXT_SESSION_ID.fetch_add(1, Ordering::SeqCst));
        let runtime = Self {
            session,
            cancel_flag,
            session_id,
            backend,
            profile,
            filesystem_mode,
            callbacks,
            next_run_id: AtomicU64::new(1),
            active_run: Arc::new(Mutex::new(None)),
            audit_log: Arc::new(Mutex::new(VecDeque::new())),
            next_audit_sequence: Arc::new(AtomicU64::new(1)),
        };
        runtime.record_session_audit(AuditEventKind::SessionOpened, None);
        runtime
    }

    pub fn exec_detached(&self, request: ExecutionRequest) -> Result<Arc<RunState>, SandboxError> {
        self.ensure_can_start_run()?;
        self.cancel_flag.store(false, Ordering::SeqCst);

        let sequence = self.next_run_id.fetch_add(1, Ordering::SeqCst);
        let run_id = format!("{}-run-{sequence}", self.session_id);
        let run = Arc::new(RunState::new(run_id, now_unix_ms()));

        {
            let mut active = self.active_run.lock();
            *active = Some(run.clone());
        }

        self.record_run_audit(&run, AuditEventKind::RunRequested, None);

        let session = self.session.clone();
        let active_run = self.active_run.clone();
        let runtime = self.clone_for_thread();
        let run_state = run.clone();

        thread::spawn(move || {
            run_state.set_status(RunStatus::Running);
            runtime.record_run_event(&run_state, RunEventKind::RunStarted, None, None, None, None);

            let result = session.lock().run(request);
            let terminal_status = terminal_status(&result);

            if !result.stdout.is_empty() {
                runtime.record_run_event(
                    &run_state,
                    RunEventKind::Stdout,
                    Some("stdout".to_string()),
                    Some(String::from_utf8_lossy(&result.stdout).to_string()),
                    None,
                    None,
                );
            }
            if !result.stderr.is_empty() {
                runtime.record_run_event(
                    &run_state,
                    RunEventKind::Stderr,
                    Some("stderr".to_string()),
                    Some(String::from_utf8_lossy(&result.stderr).to_string()),
                    None,
                    None,
                );
            }

            if let Some(error) = result.error.as_ref() {
                match error.kind {
                    ErrorKind::PolicyDenied => {
                        runtime.record_run_audit(
                            &run_state,
                            AuditEventKind::RunDenied,
                            Some(error.message.clone()),
                        );
                        runtime.record_run_audit(
                            &run_state,
                            classify_policy_audit(&error.message),
                            Some(error.message.clone()),
                        );
                    }
                    ErrorKind::Timeout => {
                        runtime.record_run_audit(
                            &run_state,
                            AuditEventKind::RunTimedOut,
                            Some(error.message.clone()),
                        );
                    }
                    ErrorKind::Cancellation => {
                        runtime.record_run_audit(
                            &run_state,
                            AuditEventKind::RunCancelled,
                            Some(error.message.clone()),
                        );
                    }
                    _ => {}
                }
            }

            let terminal_kind = match terminal_status {
                RunStatus::Completed => RunEventKind::RunCompleted,
                RunStatus::Cancelled => RunEventKind::RunCancelled,
                RunStatus::Failed => RunEventKind::RunFailed,
                RunStatus::Pending | RunStatus::Running => RunEventKind::RunFailed,
            };

            run_state.finish(result.clone(), terminal_status.clone());
            runtime.record_run_event(
                &run_state,
                terminal_kind,
                None,
                None,
                Some(result.exit_code),
                Some(result.termination_reason.as_str().to_string()),
            );

            let mut active = active_run.lock();
            if active
                .as_ref()
                .is_some_and(|active_run| active_run.run_id() == run_state.run_id())
            {
                *active = None;
            }
        });

        Ok(run)
    }

    pub fn run_sync(&self, request: ExecutionRequest) -> Result<ExecutionResult, SandboxError> {
        let run = self.exec_detached(request)?;
        Ok(run.wait())
    }

    pub fn cancel_active(&self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
    }

    pub fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        self.ensure_idle()?;
        self.session.lock().read_file(path)
    }

    pub fn write_file(
        &self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        self.ensure_idle()?;
        self.session
            .lock()
            .write_file(path, contents, create_parents)
    }

    pub fn mkdir(&self, path: &str, parents: bool) -> Result<(), SandboxError> {
        self.ensure_idle()?;
        self.session.lock().mkdir(path, parents)
    }

    pub fn exists(&self, path: &str) -> Result<bool, SandboxError> {
        self.ensure_idle()?;
        self.session.lock().exists(path)
    }

    pub fn close(&self) -> Result<(), SandboxError> {
        self.ensure_idle()?;
        self.session.lock().close()?;
        self.record_session_audit(AuditEventKind::SessionClosed, None);
        Ok(())
    }

    pub fn audit_events(&self) -> Vec<AuditEvent> {
        self.audit_log.lock().iter().cloned().collect()
    }

    fn ensure_can_start_run(&self) -> Result<(), SandboxError> {
        if self
            .active_run
            .lock()
            .as_ref()
            .is_some_and(|run| matches!(run.status(), RunStatus::Pending | RunStatus::Running))
        {
            return Err(SandboxError::InvalidRequest(
                "sandbox already has an active run".to_string(),
            ));
        }
        if self.session.lock().is_closed() {
            return Err(SandboxError::ClosedSession);
        }
        Ok(())
    }

    fn ensure_idle(&self) -> Result<(), SandboxError> {
        if self
            .active_run
            .lock()
            .as_ref()
            .is_some_and(|run| matches!(run.status(), RunStatus::Pending | RunStatus::Running))
        {
            return Err(SandboxError::InvalidRequest(
                "sandbox has an active run; wait or cancel it before file or lifecycle operations"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn clone_for_thread(&self) -> ThreadRuntime {
        ThreadRuntime {
            session_id: self.session_id.clone(),
            backend: self.backend.clone(),
            profile: self.profile.clone(),
            filesystem_mode: self.filesystem_mode.clone(),
            callbacks: RuntimeCallbacks {
                event_callback: self.callbacks.event_callback.clone(),
                audit_callback: self.callbacks.audit_callback.clone(),
            },
            audit_log: self.audit_log.clone(),
            next_audit_sequence: self.next_audit_sequence.clone(),
        }
    }

    fn record_session_audit(&self, kind: AuditEventKind, reason: Option<String>) {
        let audit = self.build_audit(None, kind, reason);
        {
            let mut log = self.audit_log.lock();
            push_bounded(&mut log, audit.clone(), MAX_RETAINED_AUDITS);
        }
        self.emit_audit_callback(&audit);
    }

    fn record_run_audit(&self, run: &RunState, kind: AuditEventKind, reason: Option<String>) {
        let audit = self.build_audit(Some(run), kind, reason);
        run.push_audit(audit.clone());
        {
            let mut log = self.audit_log.lock();
            push_bounded(&mut log, audit.clone(), MAX_RETAINED_AUDITS);
        }
        self.emit_audit_callback(&audit);
    }

    fn build_audit(
        &self,
        run: Option<&RunState>,
        kind: AuditEventKind,
        reason: Option<String>,
    ) -> AuditEvent {
        AuditEvent {
            session_id: self.session_id.clone(),
            run_id: run.map(|run| run.run_id().to_string()),
            sequence: self.next_audit_sequence.fetch_add(1, Ordering::SeqCst),
            timestamp_ms: now_unix_ms(),
            kind,
            backend: self.backend.clone(),
            profile: self.profile.clone(),
            filesystem_mode: self.filesystem_mode.clone(),
            reason,
        }
    }

    fn emit_audit_callback(&self, audit: &AuditEvent) {
        let Some(callback) = self.callbacks.audit_callback.as_ref() else {
            return;
        };
        Python::with_gil(|py| {
            let _ = callback
                .bind(py)
                .call1((crate::audit_event_to_python(py, audit),));
        });
    }
}

#[derive(Clone)]
struct ThreadRuntime {
    session_id: String,
    backend: String,
    profile: String,
    filesystem_mode: String,
    callbacks: RuntimeCallbacks,
    audit_log: Arc<Mutex<VecDeque<AuditEvent>>>,
    next_audit_sequence: Arc<AtomicU64>,
}

impl ThreadRuntime {
    fn record_run_event(
        &self,
        run: &RunState,
        kind: RunEventKind,
        stream: Option<String>,
        text: Option<String>,
        exit_code: Option<i32>,
        termination_reason: Option<String>,
    ) {
        let event = RunEvent {
            run_id: run.run_id().to_string(),
            sequence: run.next_event_sequence(),
            timestamp_ms: now_unix_ms(),
            kind,
            status: run.status(),
            stream,
            text,
            exit_code,
            termination_reason,
            metadata: default_event_metadata(&self.backend, &self.profile, &self.filesystem_mode),
        };
        run.push_event(event.clone());
        let Some(callback) = self.callbacks.event_callback.as_ref() else {
            return;
        };
        Python::with_gil(|py| {
            let _ = callback
                .bind(py)
                .call1((crate::run_event_to_python(py, &event),));
        });
    }

    fn record_run_audit(&self, run: &RunState, kind: AuditEventKind, reason: Option<String>) {
        let audit = AuditEvent {
            session_id: self.session_id.clone(),
            run_id: Some(run.run_id().to_string()),
            sequence: self.next_audit_sequence.fetch_add(1, Ordering::SeqCst),
            timestamp_ms: now_unix_ms(),
            kind,
            backend: self.backend.clone(),
            profile: self.profile.clone(),
            filesystem_mode: self.filesystem_mode.clone(),
            reason,
        };
        run.push_audit(audit.clone());
        {
            let mut log = self.audit_log.lock();
            push_bounded(&mut log, audit.clone(), MAX_RETAINED_AUDITS);
        }
        let Some(callback) = self.callbacks.audit_callback.as_ref() else {
            return;
        };
        Python::with_gil(|py| {
            let _ = callback
                .bind(py)
                .call1((crate::audit_event_to_python(py, &audit),));
        });
    }
}

fn default_event_metadata(
    backend: &str,
    profile: &str,
    filesystem_mode: &str,
) -> std::collections::BTreeMap<String, String> {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("backend".to_string(), backend.to_string());
    metadata.insert("profile".to_string(), profile.to_string());
    metadata.insert("filesystem_mode".to_string(), filesystem_mode.to_string());
    metadata
}

fn terminal_status(result: &ExecutionResult) -> RunStatus {
    match result.error.as_ref().map(|error| &error.kind) {
        Some(ErrorKind::Cancellation) => RunStatus::Cancelled,
        Some(_) => RunStatus::Failed,
        None => RunStatus::Completed,
    }
}

fn classify_policy_audit(reason: &str) -> AuditEventKind {
    let lower = reason.to_ascii_lowercase();
    if lower.contains("network")
        || lower.contains("http")
        || lower.contains("redirect")
        || lower.contains("origin")
    {
        AuditEventKind::NetworkPolicyDenied
    } else if lower.contains("workspace")
        || lower.contains("writable")
        || lower.contains("symlink")
        || lower.contains("filesystem")
        || lower.contains("sandbox path")
    {
        AuditEventKind::FilesystemPolicyDenied
    } else {
        AuditEventKind::RunDenied
    }
}

fn push_bounded<T>(buffer: &mut VecDeque<T>, item: T, max_len: usize) {
    if buffer.len() == max_len {
        buffer.pop_front();
    }
    buffer.push_back(item);
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}
