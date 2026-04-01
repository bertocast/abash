use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunEventKind {
    RunStarted,
    Stdout,
    Stderr,
    RunCompleted,
    RunCancelled,
    RunFailed,
}

impl RunEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::RunCompleted => "run_completed",
            Self::RunCancelled => "run_cancelled",
            Self::RunFailed => "run_failed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditEventKind {
    SessionOpened,
    SessionClosed,
    RunRequested,
    RunDenied,
    NetworkPolicyDenied,
    FilesystemPolicyDenied,
    RunCancelled,
    RunTimedOut,
}

impl AuditEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SessionOpened => "session_opened",
            Self::SessionClosed => "session_closed",
            Self::RunRequested => "run_requested",
            Self::RunDenied => "run_denied",
            Self::NetworkPolicyDenied => "network_policy_denied",
            Self::FilesystemPolicyDenied => "filesystem_policy_denied",
            Self::RunCancelled => "run_cancelled",
            Self::RunTimedOut => "run_timed_out",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunEvent {
    pub run_id: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: RunEventKind,
    pub status: RunStatus,
    pub stream: Option<String>,
    pub text: Option<String>,
    pub exit_code: Option<i32>,
    pub termination_reason: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEvent {
    pub session_id: String,
    pub run_id: Option<String>,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: AuditEventKind,
    pub backend: String,
    pub profile: String,
    pub filesystem_mode: String,
    pub reason: Option<String>,
}
