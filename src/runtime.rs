use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use abash_core::{
    ErrorKind, ExecutionRequest, ExecutionResult, ExtensionCommandResult, SandboxError,
    SandboxExtensions, SandboxSession,
};
use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;

use crate::observability::{
    AuditEvent, AuditEventKind, RunEvent, RunEventKind, RunStatus, RunSummary,
};

const MAX_RETAINED_EVENTS: usize = 64;
const MAX_RETAINED_AUDITS: usize = 64;
const MAX_RETAINED_RUNS: usize = 64;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Default)]
pub struct RuntimeCallbacks {
    pub event_callback: Option<Arc<Py<PyAny>>>,
    pub audit_callback: Option<Arc<Py<PyAny>>>,
    pub custom_command_callback: Option<Arc<Py<PyAny>>>,
    pub custom_command_names: std::collections::BTreeSet<String>,
    pub lazy_file_callback: Option<Arc<Py<PyAny>>>,
    pub lazy_mount_roots: std::collections::BTreeSet<String>,
    pub pre_exec_hook: Option<Arc<Py<PyAny>>>,
    pub post_exec_hook: Option<Arc<Py<PyAny>>>,
}

#[derive(Clone, Default)]
pub struct PythonExtensions {
    pub custom_command_callback: Option<Arc<Py<PyAny>>>,
    pub custom_command_names: std::collections::BTreeSet<String>,
    pub lazy_file_callback: Option<Arc<Py<PyAny>>>,
    pub lazy_mount_roots: std::collections::BTreeSet<String>,
}

impl SandboxExtensions for PythonExtensions {
    fn exec_custom_command(
        &self,
        request: &ExecutionRequest,
    ) -> Result<Option<ExtensionCommandResult>, SandboxError> {
        let Some(command) = request.argv.first() else {
            return Ok(None);
        };
        if !self.custom_command_names.contains(command) {
            return Ok(None);
        }
        let Some(callback) = self.custom_command_callback.as_ref() else {
            return Ok(None);
        };
        Python::with_gil(
            |py| -> Result<Option<ExtensionCommandResult>, SandboxError> {
                let request_payload = crate::execution_request_to_python(py, request)
                    .map_err(python_callback_error)?;
                let result_payload = callback
                    .bind(py)
                    .call1((request_payload,))
                    .map_err(python_callback_error)?;
                if let Ok(delegated_request) = result_payload.get_item("delegated_request") {
                    if !delegated_request.is_none() {
                        return crate::python_to_execution_request(&delegated_request)
                            .map(ExtensionCommandResult::Delegate)
                            .map(Some)
                            .map_err(python_callback_error);
                    }
                }
                let mut result = crate::python_to_execution_result(&result_payload)
                    .map_err(python_callback_error)?;
                result
                    .metadata
                    .entry("backend".to_string())
                    .or_insert_with(|| "custom".to_string());
                result
                    .metadata
                    .entry("command".to_string())
                    .or_insert_with(|| command.clone());
                Ok(Some(ExtensionCommandResult::Completed(result)))
            },
        )
    }

    fn read_lazy_file(&self, path: &str) -> Result<Option<Vec<u8>>, SandboxError> {
        if !self
            .lazy_mount_roots
            .iter()
            .any(|root| path == root || path.starts_with(&format!("{root}/")))
        {
            return Ok(None);
        }
        let Some(callback) = self.lazy_file_callback.as_ref() else {
            return Ok(None);
        };
        Python::with_gil(|py| -> Result<Option<Vec<u8>>, SandboxError> {
            let payload = callback
                .bind(py)
                .call1((path,))
                .map_err(python_callback_error)?;
            if payload.is_none() {
                return Ok(None);
            }
            if let Ok(bytes) = payload.extract::<Vec<u8>>() {
                return Ok(Some(bytes));
            }
            if let Ok(text) = payload.extract::<String>() {
                return Ok(Some(text.into_bytes()));
            }
            Err(python_callback_error(PyErr::new::<
                pyo3::exceptions::PyTypeError,
                _,
            >(
                "lazy file provider must return bytes, str, or None",
            )))
        })
    }
}

pub struct SandboxRuntime {
    session: Arc<Mutex<SandboxSession>>,
    session_id: String,
    backend: String,
    profile: String,
    filesystem_mode: String,
    callbacks: RuntimeCallbacks,
    next_run_id: AtomicU64,
    runs: Arc<Mutex<VecDeque<Arc<RunState>>>>,
    event_log: Arc<Mutex<VecDeque<RunEvent>>>,
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
    cancel_flag: Arc<AtomicBool>,
    inner: Mutex<RunStateInner>,
    condvar: Condvar,
}

impl RunState {
    fn new(run_id: String, started_at_ms: u64) -> Self {
        Self {
            run_id,
            started_at_ms,
            cancel_flag: Arc::new(AtomicBool::new(false)),
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

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status(),
            RunStatus::Completed | RunStatus::Cancelled | RunStatus::Failed
        )
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
        let mut guard = self.inner.lock();
        guard.status = status;
    }

    fn push_event(&self, event: RunEvent) {
        let mut guard = self.inner.lock();
        push_bounded(&mut guard.events, event, MAX_RETAINED_EVENTS);
        self.condvar.notify_all();
    }

    fn push_audit(&self, audit: AuditEvent) {
        let mut guard = self.inner.lock();
        push_bounded(&mut guard.audits, audit, MAX_RETAINED_AUDITS);
        self.condvar.notify_all();
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

    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        self.cancel_flag.clone()
    }

    pub fn wait_for_events(&self, after_sequence: u64, timeout_ms: Option<u64>) -> Vec<RunEvent> {
        let mut guard = self.inner.lock();
        let timeout = timeout_ms.map(Duration::from_millis);
        loop {
            let events: Vec<RunEvent> = guard
                .events
                .iter()
                .filter(|event| event.sequence > after_sequence)
                .cloned()
                .collect();
            if !events.is_empty() || terminal_status_locked(&guard.status) {
                return events;
            }
            match timeout {
                Some(duration) => {
                    if self.condvar.wait_for(&mut guard, duration).timed_out() {
                        return Vec::new();
                    }
                }
                None => self.condvar.wait(&mut guard),
            }
        }
    }

    pub fn summary(&self) -> RunSummary {
        let guard = self.inner.lock();
        RunSummary {
            run_id: self.run_id.clone(),
            started_at_ms: self.started_at_ms,
            status: guard.status.clone(),
            exit_code: guard.result.as_ref().map(|result| result.exit_code),
            termination_reason: guard
                .result
                .as_ref()
                .map(|result| result.termination_reason.as_str().to_string()),
        }
    }
}

impl SandboxRuntime {
    pub fn new(session: Arc<Mutex<SandboxSession>>, callbacks: RuntimeCallbacks) -> Self {
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
            session_id,
            backend,
            profile,
            filesystem_mode,
            callbacks,
            next_run_id: AtomicU64::new(1),
            runs: Arc::new(Mutex::new(VecDeque::new())),
            event_log: Arc::new(Mutex::new(VecDeque::new())),
            audit_log: Arc::new(Mutex::new(VecDeque::new())),
            next_audit_sequence: Arc::new(AtomicU64::new(1)),
        };
        runtime.record_session_audit(AuditEventKind::SessionOpened, None);
        runtime
    }

    pub fn exec_detached(&self, request: ExecutionRequest) -> Result<Arc<RunState>, SandboxError> {
        self.ensure_session_open()?;

        let sequence = self.next_run_id.fetch_add(1, Ordering::SeqCst);
        let run_id = format!("{}-run-{sequence}", self.session_id);
        let run = Arc::new(RunState::new(run_id, now_unix_ms()));

        {
            let mut runs = self.runs.lock();
            push_bounded(&mut runs, run.clone(), MAX_RETAINED_RUNS);
        }

        self.record_run_audit(&run, AuditEventKind::RunRequested, None);

        let session = self.session.clone();
        let runtime = self.clone_for_thread();
        let run_state = run.clone();
        let cancel_flag = run.cancel_flag();

        thread::spawn(move || {
            let result =
                runtime.execute_request(&session, request, &run_state, cancel_flag.as_ref());
            let terminal_status = terminal_status(&result);

            if !result.stdout.is_empty() {
                runtime.record_run_event(
                    &run_state,
                    RunEventKind::Stdout,
                    None,
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
                    None,
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

            runtime.record_run_event(
                &run_state,
                terminal_kind,
                Some(terminal_status.clone()),
                None,
                None,
                Some(result.exit_code),
                Some(result.termination_reason.as_str().to_string()),
            );
            run_state.finish(result.clone(), terminal_status.clone());
        });

        Ok(run)
    }

    pub fn run_sync(&self, request: ExecutionRequest) -> Result<ExecutionResult, SandboxError> {
        let run = self.exec_detached(request)?;
        Ok(run.wait())
    }

    pub fn cancel_all_runs(&self) {
        let runs: Vec<_> = self.runs.lock().iter().cloned().collect();
        for run in runs {
            if !run.is_terminal() {
                run.cancel();
            }
        }
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

    pub fn run_events(&self) -> Vec<RunEvent> {
        self.event_log.lock().iter().cloned().collect()
    }

    pub fn run_summaries(&self) -> Vec<RunSummary> {
        self.runs.lock().iter().map(|run| run.summary()).collect()
    }

    fn ensure_session_open(&self) -> Result<(), SandboxError> {
        if self.session.lock().is_closed() {
            return Err(SandboxError::ClosedSession);
        }
        Ok(())
    }

    fn ensure_idle(&self) -> Result<(), SandboxError> {
        if self
            .runs
            .lock()
            .iter()
            .any(|run| matches!(run.status(), RunStatus::Pending | RunStatus::Running))
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
                custom_command_callback: self.callbacks.custom_command_callback.clone(),
                custom_command_names: self.callbacks.custom_command_names.clone(),
                lazy_file_callback: self.callbacks.lazy_file_callback.clone(),
                lazy_mount_roots: self.callbacks.lazy_mount_roots.clone(),
                pre_exec_hook: self.callbacks.pre_exec_hook.clone(),
                post_exec_hook: self.callbacks.post_exec_hook.clone(),
            },
            event_log: self.event_log.clone(),
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
    event_log: Arc<Mutex<VecDeque<RunEvent>>>,
    audit_log: Arc<Mutex<VecDeque<AuditEvent>>>,
    next_audit_sequence: Arc<AtomicU64>,
}

impl ThreadRuntime {
    fn execute_request(
        &self,
        session: &Arc<Mutex<SandboxSession>>,
        request: ExecutionRequest,
        run: &RunState,
        cancel_flag: &AtomicBool,
    ) -> ExecutionResult {
        let mut effective_request = match self.apply_pre_exec_hook(&request) {
            Ok(request) => request,
            Err(error) => return ExecutionResult::failure(error, session.lock().base_metadata()),
        };
        decorate_request_metadata(
            &mut effective_request,
            &self.session_id,
            run.run_id(),
            &self.backend,
            &self.profile,
            &self.filesystem_mode,
        );
        run.set_status(RunStatus::Running);
        self.record_run_event(run, RunEventKind::RunStarted, None, None, None, None, None);
        let mut result = match self.run_custom_command(&effective_request) {
            Ok(Some(ExtensionCommandResult::Completed(result))) => result,
            Ok(Some(ExtensionCommandResult::Delegate(delegated_request))) => session
                .lock()
                .run_with_cancel(delegated_request, cancel_flag),
            Ok(None) => session
                .lock()
                .run_with_cancel(effective_request.clone(), cancel_flag),
            Err(error) => ExecutionResult::failure(error, session.lock().base_metadata()),
        };
        self.apply_post_exec_hook(&effective_request, &mut result);
        result
    }

    fn run_custom_command(
        &self,
        request: &ExecutionRequest,
    ) -> Result<Option<ExtensionCommandResult>, SandboxError> {
        if request.mode != abash_core::ExecutionMode::Argv {
            return Ok(None);
        }
        let extensions = PythonExtensions {
            custom_command_callback: self.callbacks.custom_command_callback.clone(),
            custom_command_names: self.callbacks.custom_command_names.clone(),
            lazy_file_callback: self.callbacks.lazy_file_callback.clone(),
            lazy_mount_roots: self.callbacks.lazy_mount_roots.clone(),
        };
        let extension_result = match extensions.exec_custom_command(request)? {
            Some(result) => result,
            None => return Ok(None),
        };
        Ok(Some(match extension_result {
            ExtensionCommandResult::Completed(mut result) => {
                result
                    .metadata
                    .entry("profile".to_string())
                    .or_insert_with(|| self.profile.clone());
                result
                    .metadata
                    .entry("filesystem_mode".to_string())
                    .or_insert_with(|| self.filesystem_mode.clone());
                ExtensionCommandResult::Completed(result)
            }
            ExtensionCommandResult::Delegate(mut request) => {
                request
                    .metadata
                    .entry("profile".to_string())
                    .or_insert_with(|| self.profile.clone());
                request
                    .metadata
                    .entry("filesystem_mode".to_string())
                    .or_insert_with(|| self.filesystem_mode.clone());
                ExtensionCommandResult::Delegate(request)
            }
        }))
    }

    fn apply_pre_exec_hook(
        &self,
        request: &ExecutionRequest,
    ) -> Result<ExecutionRequest, SandboxError> {
        let Some(callback) = self.callbacks.pre_exec_hook.as_ref() else {
            return Ok(request.clone());
        };
        Python::with_gil(|py| {
            let payload =
                crate::execution_request_to_python(py, request).map_err(python_callback_error)?;
            let updated = callback
                .bind(py)
                .call1((payload,))
                .map_err(python_callback_error)?;
            if updated.is_none() {
                return Ok(request.clone());
            }
            crate::python_to_execution_request(&updated).map_err(python_callback_error)
        })
    }

    fn apply_post_exec_hook(&self, request: &ExecutionRequest, result: &mut ExecutionResult) {
        let Some(callback) = self.callbacks.post_exec_hook.as_ref() else {
            return;
        };
        Python::with_gil(|py| {
            let request_payload = match crate::execution_request_to_python(py, request) {
                Ok(payload) => payload,
                Err(_) => return,
            };
            let result_payload = match crate::execution_result_to_python(py, result.clone()) {
                Ok(payload) => payload,
                Err(_) => return,
            };
            let Ok(updated) = callback.bind(py).call1((request_payload, result_payload)) else {
                return;
            };
            if updated.is_none() {
                return;
            }
            if let Ok(next) = crate::python_to_execution_result(&updated) {
                *result = next;
            }
        });
    }

    fn record_run_event(
        &self,
        run: &RunState,
        kind: RunEventKind,
        status_override: Option<RunStatus>,
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
            status: status_override.unwrap_or_else(|| run.status()),
            stream,
            text,
            exit_code,
            termination_reason,
            metadata: default_event_metadata(&self.backend, &self.profile, &self.filesystem_mode),
        };
        run.push_event(event.clone());
        {
            let mut log = self.event_log.lock();
            push_bounded(&mut log, event.clone(), MAX_RETAINED_EVENTS);
        }
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

fn python_callback_error(error: PyErr) -> SandboxError {
    SandboxError::BackendFailure(format!("python callback failed: {error}"))
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

fn decorate_request_metadata(
    request: &mut ExecutionRequest,
    session_id: &str,
    run_id: &str,
    backend: &str,
    profile: &str,
    filesystem_mode: &str,
) {
    request
        .metadata
        .entry("session_id".to_string())
        .or_insert_with(|| session_id.to_string());
    request
        .metadata
        .entry("run_id".to_string())
        .or_insert_with(|| run_id.to_string());
    request
        .metadata
        .entry("backend".to_string())
        .or_insert_with(|| backend.to_string());
    request
        .metadata
        .entry("profile".to_string())
        .or_insert_with(|| profile.to_string());
    request
        .metadata
        .entry("filesystem_mode".to_string())
        .or_insert_with(|| filesystem_mode.to_string());
    request
        .metadata
        .entry("request_mode".to_string())
        .or_insert_with(|| match request.mode {
            abash_core::ExecutionMode::Argv => "argv".to_string(),
            abash_core::ExecutionMode::Script => "script".to_string(),
        });
}

fn terminal_status(result: &ExecutionResult) -> RunStatus {
    match result.error.as_ref().map(|error| &error.kind) {
        Some(ErrorKind::Cancellation) => RunStatus::Cancelled,
        Some(_) => RunStatus::Failed,
        None => RunStatus::Completed,
    }
}

fn terminal_status_locked(status: &RunStatus) -> bool {
    matches!(
        status,
        RunStatus::Completed | RunStatus::Cancelled | RunStatus::Failed
    )
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
