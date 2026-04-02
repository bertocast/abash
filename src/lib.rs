use std::collections::{BTreeMap, BTreeSet};
use std::sync::{atomic::AtomicBool, Arc};

use abash_core::{
    default_cwd_for_mode, normalize_sandbox_path as core_normalize_sandbox_path,
    parse_network_policy_json, ExecutionMode, ExecutionProfile, ExecutionRequest, FilesystemMode,
    SandboxConfig, SandboxError, SandboxSession, SessionBackend, SessionState,
};
use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

mod observability;
mod runtime;

use observability::{AuditEvent, RunEvent};
use runtime::{RunState, RuntimeCallbacks, SandboxRuntime};

#[pyclass(module = "abash._native")]
struct NativeSandbox {
    runtime: Arc<SandboxRuntime>,
}

#[pyclass(module = "abash._native")]
struct NativeRun {
    runtime: Arc<SandboxRuntime>,
    state: Arc<RunState>,
}

#[pymethods]
impl NativeSandbox {
    #[new]
    #[pyo3(signature = (
        profile,
        filesystem_mode,
        allowlisted_commands,
        session_state="persistent",
        workspace_root=None,
        writable_roots=None,
        network_policy_json=None,
        event_callback=None,
        audit_callback=None,
        custom_command_names=None,
        custom_command_callback=None,
        pre_exec_hook=None,
        post_exec_hook=None
    ))]
    fn new(
        profile: String,
        filesystem_mode: String,
        allowlisted_commands: Vec<String>,
        session_state: &str,
        workspace_root: Option<String>,
        writable_roots: Option<Vec<String>>,
        network_policy_json: Option<String>,
        event_callback: Option<Py<PyAny>>,
        audit_callback: Option<Py<PyAny>>,
        custom_command_names: Option<Vec<String>>,
        custom_command_callback: Option<Py<PyAny>>,
        pre_exec_hook: Option<Py<PyAny>>,
        post_exec_hook: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let profile = parse_profile(&profile)?;
        let filesystem_mode = parse_filesystem_mode(&filesystem_mode)?;
        let session_state = parse_session_state(session_state)?;
        let allowlisted_commands = if allowlisted_commands.is_empty() {
            default_allowlisted_commands()
                .into_iter()
                .collect::<BTreeSet<_>>()
        } else {
            allowlisted_commands.into_iter().collect::<BTreeSet<_>>()
        };

        let config = SandboxConfig {
            profile: profile.clone(),
            filesystem_mode: filesystem_mode.clone(),
            session_state,
            allowlisted_commands,
            default_cwd: default_cwd_for_mode(&filesystem_mode).to_string(),
            workspace_root: workspace_root.map(Into::into),
            writable_roots: writable_roots
                .unwrap_or_default()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            network_policy: match network_policy_json {
                Some(json) => Some(parse_network_policy_json(&json).map_err(to_py_err)?),
                None => None,
            },
        };
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let backend = build_backend(&config)?;
        let session = Arc::new(Mutex::new(SandboxSession::new(
            config,
            backend,
            cancel_flag.clone(),
        )));
        let runtime = Arc::new(SandboxRuntime::new(
            session,
            cancel_flag,
            RuntimeCallbacks {
                event_callback: event_callback.map(Arc::new),
                audit_callback: audit_callback.map(Arc::new),
                custom_command_callback: custom_command_callback.map(Arc::new),
                custom_command_names: custom_command_names
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                pre_exec_hook: pre_exec_hook.map(Arc::new),
                post_exec_hook: post_exec_hook.map(Arc::new),
            },
        ));

        Ok(Self { runtime })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (mode, argv=None, script=None, cwd=None, env=None, stdin=None, timeout_ms=None, metadata=None, network_enabled=None, filesystem_mode=None))]
    fn run(
        &self,
        py: Python<'_>,
        mode: String,
        argv: Option<Vec<String>>,
        script: Option<String>,
        cwd: Option<String>,
        env: Option<BTreeMap<String, String>>,
        stdin: Option<Vec<u8>>,
        timeout_ms: Option<u64>,
        metadata: Option<BTreeMap<String, String>>,
        network_enabled: Option<bool>,
        filesystem_mode: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let request = build_request(
            &mode,
            argv,
            script,
            cwd,
            env,
            stdin,
            timeout_ms,
            metadata,
            network_enabled,
            filesystem_mode,
        )?;
        let runtime = self.runtime.clone();
        let result = py.allow_threads(move || runtime.run_sync(request));
        execution_result_to_python(py, result.map_err(to_py_err)?)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (mode, argv=None, script=None, cwd=None, env=None, stdin=None, timeout_ms=None, metadata=None, network_enabled=None, filesystem_mode=None))]
    fn exec_detached(
        &self,
        py: Python<'_>,
        mode: String,
        argv: Option<Vec<String>>,
        script: Option<String>,
        cwd: Option<String>,
        env: Option<BTreeMap<String, String>>,
        stdin: Option<Vec<u8>>,
        timeout_ms: Option<u64>,
        metadata: Option<BTreeMap<String, String>>,
        network_enabled: Option<bool>,
        filesystem_mode: Option<String>,
    ) -> PyResult<Py<NativeRun>> {
        let request = build_request(
            &mode,
            argv,
            script,
            cwd,
            env,
            stdin,
            timeout_ms,
            metadata,
            network_enabled,
            filesystem_mode,
        )?;
        let runtime = self.runtime.clone();
        let run = py
            .allow_threads(move || runtime.exec_detached(request))
            .map_err(to_py_err)?;
        Py::new(
            py,
            NativeRun {
                runtime: self.runtime.clone(),
                state: run,
            },
        )
    }

    fn read_file(&self, py: Python<'_>, path: String) -> PyResult<Py<PyAny>> {
        let runtime = self.runtime.clone();
        let bytes = py
            .allow_threads(move || runtime.read_file(&path))
            .map_err(to_py_err)?;
        Ok(PyBytes::new_bound(py, &bytes).into_any().unbind())
    }

    #[pyo3(signature = (path, contents, create_parents=false))]
    fn write_file(
        &self,
        py: Python<'_>,
        path: String,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> PyResult<()> {
        let runtime = self.runtime.clone();
        py.allow_threads(move || runtime.write_file(&path, contents, create_parents))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (path, parents=false))]
    fn mkdir(&self, py: Python<'_>, path: String, parents: bool) -> PyResult<()> {
        let runtime = self.runtime.clone();
        py.allow_threads(move || runtime.mkdir(&path, parents))
            .map_err(to_py_err)
    }

    fn exists(&self, py: Python<'_>, path: String) -> PyResult<bool> {
        let runtime = self.runtime.clone();
        py.allow_threads(move || runtime.exists(&path))
            .map_err(to_py_err)
    }

    fn cancel(&self) {
        self.runtime.cancel_active();
    }

    fn audit_events(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        audit_events_to_python(py, &self.runtime.audit_events())
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = self.runtime.clone();
        py.allow_threads(move || runtime.close()).map_err(to_py_err)
    }
}

#[pymethods]
impl NativeRun {
    #[getter]
    fn run_id(&self) -> String {
        self.state.run_id().to_string()
    }

    #[getter]
    fn started_at_ms(&self) -> u64 {
        self.state.started_at_ms()
    }

    fn status(&self) -> String {
        self.state.status().as_str().to_string()
    }

    fn wait(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.state.clone();
        let result = py.allow_threads(move || state.wait());
        execution_result_to_python(py, result)
    }

    fn cancel(&self) {
        self.runtime.cancel_active();
    }

    fn stdout(&self) -> String {
        self.state.stdout()
    }

    fn stderr(&self) -> String {
        self.state.stderr()
    }

    fn output(&self) -> String {
        self.state.output()
    }

    fn events(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        run_events_to_python(py, &self.state.events())
    }

    fn audit_events(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        audit_events_to_python(py, &self.state.audits())
    }
}

#[pyfunction]
fn default_allowlisted_commands() -> Vec<String> {
    vec![
        "echo".to_string(),
        "env".to_string(),
        "which".to_string(),
        "dirname".to_string(),
        "basename".to_string(),
        "cd".to_string(),
        "export".to_string(),
        "expr".to_string(),
        "time".to_string(),
        "timeout".to_string(),
        "whoami".to_string(),
        "hostname".to_string(),
        "help".to_string(),
        "clear".to_string(),
        "history".to_string(),
        "alias".to_string(),
        "unalias".to_string(),
        "bash".to_string(),
        "sh".to_string(),
        "tree".to_string(),
        "stat".to_string(),
        "file".to_string(),
        "readlink".to_string(),
        "ln".to_string(),
        "curl".to_string(),
        "pwd".to_string(),
        "printenv".to_string(),
        "du".to_string(),
        "cat".to_string(),
        "grep".to_string(),
        "egrep".to_string(),
        "fgrep".to_string(),
        "wc".to_string(),
        "sort".to_string(),
        "uniq".to_string(),
        "head".to_string(),
        "tail".to_string(),
        "cut".to_string(),
        "tr".to_string(),
        "paste".to_string(),
        "sed".to_string(),
        "join".to_string(),
        "awk".to_string(),
        "jq".to_string(),
        "yq".to_string(),
        "find".to_string(),
        "ls".to_string(),
        "rev".to_string(),
        "nl".to_string(),
        "tac".to_string(),
        "strings".to_string(),
        "fold".to_string(),
        "expand".to_string(),
        "unexpand".to_string(),
        "rm".to_string(),
        "cp".to_string(),
        "mv".to_string(),
        "tee".to_string(),
        "printf".to_string(),
        "seq".to_string(),
        "date".to_string(),
        "gzip".to_string(),
        "html-to-markdown".to_string(),
        "gunzip".to_string(),
        "zcat".to_string(),
        "tar".to_string(),
        "sqlite3".to_string(),
        "mkdir".to_string(),
        "touch".to_string(),
        "rmdir".to_string(),
        "comm".to_string(),
        "diff".to_string(),
        "column".to_string(),
        "chmod".to_string(),
        "python".to_string(),
        "python3".to_string(),
        "js-exec".to_string(),
        "xan".to_string(),
        "xargs".to_string(),
        "rg".to_string(),
        "split".to_string(),
        "od".to_string(),
        "base64".to_string(),
        "md5sum".to_string(),
        "sha1sum".to_string(),
        "sha256sum".to_string(),
        "true".to_string(),
        "false".to_string(),
        "sleep".to_string(),
    ]
}

#[pyfunction]
fn normalize_sandbox_path(path: &str) -> PyResult<String> {
    core_normalize_sandbox_path(path).map_err(to_py_err)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeSandbox>()?;
    module.add_class::<NativeRun>()?;
    module.add_function(wrap_pyfunction!(default_allowlisted_commands, module)?)?;
    module.add_function(wrap_pyfunction!(normalize_sandbox_path, module)?)?;
    Ok(())
}

pub(crate) fn execution_request_to_python(
    py: Python<'_>,
    request: &ExecutionRequest,
) -> PyResult<Py<PyAny>> {
    let payload = PyDict::new_bound(py);
    payload.set_item(
        "mode",
        match request.mode {
            ExecutionMode::Argv => "argv",
            ExecutionMode::Script => "script",
        },
    )?;
    payload.set_item("argv", request.argv.clone())?;
    payload.set_item("script", request.script.clone())?;
    payload.set_item("cwd", request.cwd.clone())?;
    payload.set_item("env", request.env.clone())?;
    payload.set_item("stdin", PyBytes::new_bound(py, &request.stdin))?;
    payload.set_item("timeout_ms", request.timeout_ms)?;
    payload.set_item("network_enabled", request.network_enabled)?;
    payload.set_item("filesystem_mode", request.filesystem_mode.as_str())?;
    payload.set_item("metadata", request.metadata.clone())?;
    Ok(payload.into_any().unbind())
}

fn build_backend(config: &SandboxConfig) -> PyResult<Box<dyn SessionBackend>> {
    match config.profile {
        ExecutionProfile::Safe | ExecutionProfile::Workspace => {
            abash_backend_virtual::create_session(config.clone()).map_err(to_py_err)
        }
        ExecutionProfile::RealShell => {
            abash_backend_nsjail::create_session(config.clone()).map_err(to_py_err)
        }
    }
}

fn build_request(
    mode: &str,
    argv: Option<Vec<String>>,
    script: Option<String>,
    cwd: Option<String>,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<Vec<u8>>,
    timeout_ms: Option<u64>,
    metadata: Option<BTreeMap<String, String>>,
    network_enabled: Option<bool>,
    filesystem_mode: Option<String>,
) -> PyResult<ExecutionRequest> {
    Ok(ExecutionRequest {
        mode: parse_execution_mode(mode)?,
        argv: argv.unwrap_or_default(),
        script,
        cwd: cwd.unwrap_or_default(),
        env: env.unwrap_or_default(),
        stdin: stdin.unwrap_or_default(),
        timeout_ms,
        network_enabled: network_enabled.unwrap_or(false),
        filesystem_mode: match filesystem_mode {
            Some(mode) => parse_filesystem_mode(&mode)?,
            None => FilesystemMode::Memory,
        },
        metadata: metadata.unwrap_or_default(),
    })
}

fn parse_execution_mode(value: &str) -> PyResult<ExecutionMode> {
    match value {
        "argv" => Ok(ExecutionMode::Argv),
        "script" => Ok(ExecutionMode::Script),
        _ => Err(PyValueError::new_err(format!(
            "unsupported execution mode: {value}"
        ))),
    }
}

fn parse_profile(value: &str) -> PyResult<ExecutionProfile> {
    match value {
        "safe" => Ok(ExecutionProfile::Safe),
        "workspace" => Ok(ExecutionProfile::Workspace),
        "real_shell" => Ok(ExecutionProfile::RealShell),
        _ => Err(PyValueError::new_err(format!(
            "unsupported execution profile: {value}"
        ))),
    }
}

fn parse_filesystem_mode(value: &str) -> PyResult<FilesystemMode> {
    match value {
        "memory" => Ok(FilesystemMode::Memory),
        "host_readonly" => Ok(FilesystemMode::HostReadonly),
        "host_cow" => Ok(FilesystemMode::HostCow),
        "host_readwrite" => Ok(FilesystemMode::HostReadwrite),
        _ => Err(PyValueError::new_err(format!(
            "unsupported filesystem mode: {value}"
        ))),
    }
}

fn parse_session_state(value: &str) -> PyResult<SessionState> {
    match value {
        "persistent" => Ok(SessionState::Persistent),
        "per_exec" => Ok(SessionState::PerExec),
        _ => Err(PyValueError::new_err(format!(
            "unsupported session state: {value}"
        ))),
    }
}

fn to_py_err(error: SandboxError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

pub(crate) fn execution_result_to_python(
    py: Python<'_>,
    result: abash_core::ExecutionResult,
) -> PyResult<Py<PyAny>> {
    let payload = PyDict::new_bound(py);
    payload.set_item(
        "stdout",
        String::from_utf8_lossy(&result.stdout).to_string(),
    )?;
    payload.set_item(
        "stderr",
        String::from_utf8_lossy(&result.stderr).to_string(),
    )?;
    payload.set_item("exit_code", result.exit_code)?;
    payload.set_item("termination_reason", result.termination_reason.as_str())?;
    payload.set_item("metadata", result.metadata)?;
    if let Some(error) = result.error {
        let error_payload = PyDict::new_bound(py);
        error_payload.set_item("kind", error.kind.as_str())?;
        error_payload.set_item("message", error.message)?;
        payload.set_item("error", error_payload)?;
    } else {
        payload.set_item("error", py.None())?;
    }
    Ok(payload.into_any().unbind())
}

pub(crate) fn python_to_execution_request(
    payload: &Bound<'_, PyAny>,
) -> PyResult<ExecutionRequest> {
    let mode = payload.get_item("mode")?.extract::<String>()?;
    let filesystem_mode = payload.get_item("filesystem_mode")?.extract::<String>()?;
    let stdin = payload.get_item("stdin")?.extract::<Vec<u8>>()?;
    Ok(ExecutionRequest {
        mode: parse_execution_mode(&mode)?,
        argv: payload.get_item("argv")?.extract::<Vec<String>>()?,
        script: payload.get_item("script")?.extract::<Option<String>>()?,
        cwd: payload
            .get_item("cwd")?
            .extract::<Option<String>>()?
            .unwrap_or_default(),
        env: payload
            .get_item("env")?
            .extract::<BTreeMap<String, String>>()?,
        stdin,
        timeout_ms: payload.get_item("timeout_ms")?.extract::<Option<u64>>()?,
        network_enabled: payload
            .get_item("network_enabled")?
            .extract::<Option<bool>>()?
            .unwrap_or(false),
        filesystem_mode: parse_filesystem_mode(&filesystem_mode)?,
        metadata: payload
            .get_item("metadata")?
            .extract::<BTreeMap<String, String>>()?,
    })
}

pub(crate) fn python_to_execution_result(
    payload: &Bound<'_, PyAny>,
) -> PyResult<abash_core::ExecutionResult> {
    let stdout_value = payload.get_item("stdout")?;
    let stdout = if let Ok(text) = stdout_value.extract::<String>() {
        text.into_bytes()
    } else {
        stdout_value.extract::<Vec<u8>>()?
    };
    let stderr_value = payload.get_item("stderr")?;
    let stderr = if let Ok(text) = stderr_value.extract::<String>() {
        text.into_bytes()
    } else {
        stderr_value.extract::<Vec<u8>>()?
    };
    let error_payload = payload.get_item("error")?;
    let error = if error_payload.is_none() {
        None
    } else {
        let kind = error_payload.get_item("kind")?.extract::<String>()?;
        let message = error_payload.get_item("message")?.extract::<String>()?;
        Some(abash_core::SanitizedError {
            kind: match kind.as_str() {
                "policy_denied" => abash_core::ErrorKind::PolicyDenied,
                "timeout" => abash_core::ErrorKind::Timeout,
                "cancellation" => abash_core::ErrorKind::Cancellation,
                "unsupported_feature" => abash_core::ErrorKind::UnsupportedFeature,
                "internal_error" => abash_core::ErrorKind::InternalError,
                "backend_failure" => abash_core::ErrorKind::BackendFailure,
                "invalid_request" => abash_core::ErrorKind::InvalidRequest,
                "closed_session" => abash_core::ErrorKind::ClosedSession,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unsupported error kind: {other}"
                    )))
                }
            },
            message,
        })
    };
    let termination_reason = payload
        .get_item("termination_reason")?
        .extract::<String>()?;
    Ok(abash_core::ExecutionResult {
        stdout,
        stderr,
        exit_code: payload.get_item("exit_code")?.extract::<i32>()?,
        termination_reason: match termination_reason.as_str() {
            "exited" => abash_core::TerminationReason::Exited,
            "timeout" => abash_core::TerminationReason::Timeout,
            "cancelled" => abash_core::TerminationReason::Cancelled,
            "denied" => abash_core::TerminationReason::Denied,
            "unsupported" => abash_core::TerminationReason::Unsupported,
            "failed" => abash_core::TerminationReason::Failed,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unsupported termination reason: {other}"
                )))
            }
        },
        error,
        metadata: payload
            .get_item("metadata")?
            .extract::<BTreeMap<String, String>>()?,
    })
}

pub(crate) fn run_event_to_python(py: Python<'_>, event: &RunEvent) -> Py<PyAny> {
    let payload = PyDict::new_bound(py);
    payload
        .set_item("run_id", event.run_id.clone())
        .expect("run event run_id");
    payload
        .set_item("sequence", event.sequence)
        .expect("run event sequence");
    payload
        .set_item("timestamp_ms", event.timestamp_ms)
        .expect("run event timestamp");
    payload
        .set_item("kind", event.kind.as_str())
        .expect("run event kind");
    payload
        .set_item("status", event.status.as_str())
        .expect("run event status");
    payload
        .set_item("stream", event.stream.clone())
        .expect("run event stream");
    payload
        .set_item("text", event.text.clone())
        .expect("run event text");
    payload
        .set_item("exit_code", event.exit_code)
        .expect("run event exit_code");
    payload
        .set_item("termination_reason", event.termination_reason.clone())
        .expect("run event termination");
    payload
        .set_item("metadata", event.metadata.clone())
        .expect("run event metadata");
    payload.into_any().unbind()
}

pub(crate) fn audit_event_to_python(py: Python<'_>, audit: &AuditEvent) -> Py<PyAny> {
    let payload = PyDict::new_bound(py);
    payload
        .set_item("session_id", audit.session_id.clone())
        .expect("audit session_id");
    payload
        .set_item("run_id", audit.run_id.clone())
        .expect("audit run_id");
    payload
        .set_item("sequence", audit.sequence)
        .expect("audit sequence");
    payload
        .set_item("timestamp_ms", audit.timestamp_ms)
        .expect("audit timestamp");
    payload
        .set_item("kind", audit.kind.as_str())
        .expect("audit kind");
    payload
        .set_item("backend", audit.backend.clone())
        .expect("audit backend");
    payload
        .set_item("profile", audit.profile.clone())
        .expect("audit profile");
    payload
        .set_item("filesystem_mode", audit.filesystem_mode.clone())
        .expect("audit filesystem_mode");
    payload
        .set_item("reason", audit.reason.clone())
        .expect("audit reason");
    payload.into_any().unbind()
}

fn run_events_to_python(py: Python<'_>, events: &[RunEvent]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty_bound(py);
    for event in events {
        list.append(run_event_to_python(py, event))?;
    }
    Ok(list.into_any().unbind())
}

fn audit_events_to_python(py: Python<'_>, events: &[AuditEvent]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty_bound(py);
    for event in events {
        list.append(audit_event_to_python(py, event))?;
    }
    Ok(list.into_any().unbind())
}
