from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ExecutionMode(str, Enum):
    ARGV = "argv"
    SCRIPT = "script"


class ExecutionProfile(str, Enum):
    SAFE = "safe"
    WORKSPACE = "workspace"
    REAL_SHELL = "real_shell"


class FilesystemMode(str, Enum):
    MEMORY = "memory"
    HOST_READONLY = "host_readonly"
    HOST_COW = "host_cow"
    HOST_READWRITE = "host_readwrite"


class ErrorKind(str, Enum):
    POLICY_DENIED = "policy_denied"
    TIMEOUT = "timeout"
    CANCELLATION = "cancellation"
    UNSUPPORTED_FEATURE = "unsupported_feature"
    INTERNAL_ERROR = "internal_error"
    BACKEND_FAILURE = "backend_failure"
    INVALID_REQUEST = "invalid_request"
    CLOSED_SESSION = "closed_session"


class TerminationReason(str, Enum):
    EXITED = "exited"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    DENIED = "denied"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(slots=True)
class ResourceLimits:
    timeout_ms: int | None = None
    max_output_bytes: int = 65_536


@dataclass(slots=True)
class SanitizedError:
    kind: ErrorKind
    message: str


@dataclass(slots=True)
class NetworkOrigin:
    origin: str
    path_prefix: str = "/"
    injected_headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class NetworkPolicy:
    allowed_origins: list[NetworkOrigin]
    allowed_methods: list[str] = field(default_factory=lambda: ["GET"])
    allowed_schemes: list[str] = field(default_factory=lambda: ["https"])
    request_timeout_ms: int = 30_000
    max_response_bytes: int = 65_536
    block_private_ranges: bool = True
    dns_rebinding_protection: bool = True


@dataclass(slots=True)
class ExecutionRequest:
    mode: ExecutionMode = ExecutionMode.ARGV
    argv: list[str] | None = None
    script: str | None = None
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    stdin: bytes | str | None = None
    timeout_ms: int | None = None
    filesystem_mode: FilesystemMode | None = None
    network_enabled: bool = False
    metadata: dict[str, str] = field(default_factory=dict)

    def stdin_bytes(self) -> bytes | None:
        if self.stdin is None:
            return None
        if isinstance(self.stdin, bytes):
            return self.stdin
        return self.stdin.encode("utf-8")


@dataclass(slots=True)
class ExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    termination_reason: TerminationReason
    error: SanitizedError | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class BashOptions:
    profile: ExecutionProfile = ExecutionProfile.SAFE
    filesystem_mode: FilesystemMode = FilesystemMode.MEMORY
    workspace_root: str | None = None
    writable_roots: list[str] = field(default_factory=list)
    allowlisted_commands: list[str] = field(default_factory=list)
    network_policy: NetworkPolicy | None = None
    event_callback: Callable[["RunEvent"], None] | None = None
    audit_callback: Callable[["AuditEvent"], None] | None = None


@dataclass(slots=True)
class RunEvent:
    run_id: str
    sequence: int
    timestamp: datetime
    kind: str
    status: RunStatus
    stream: str | None = None
    text: str | None = None
    exit_code: int | None = None
    termination_reason: TerminationReason | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class AuditEvent:
    session_id: str
    run_id: str | None
    sequence: int
    timestamp: datetime
    kind: str
    backend: str
    profile: ExecutionProfile | None
    filesystem_mode: FilesystemMode | None
    reason: str | None = None
