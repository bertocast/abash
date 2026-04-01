from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TypedDict, cast

from anyio import to_thread

from ._models import (
    AuditEvent,
    BashOptions,
    ErrorKind,
    ExecutionMode,
    ExecutionProfile,
    ExecutionRequest,
    ExecutionResult,
    FilesystemMode,
    NetworkPolicy,
    RunEvent,
    RunStatus,
    SanitizedError,
    TerminationReason,
)
from ._native import (
    NativeRun,
    NativeSandbox,
    default_allowlisted_commands,
    normalize_sandbox_path,
)


class _NativeErrorPayload(TypedDict):
    kind: str
    message: str


class _NativeExecutionPayload(TypedDict):
    stdout: str
    stderr: str
    exit_code: int
    termination_reason: str
    error: _NativeErrorPayload | None
    metadata: dict[str, str]


class _NativeRunEventPayload(TypedDict):
    run_id: str
    sequence: int
    timestamp_ms: int
    kind: str
    status: str
    stream: str | None
    text: str | None
    exit_code: int | None
    termination_reason: str | None
    metadata: dict[str, str]


class _NativeAuditEventPayload(TypedDict):
    session_id: str
    run_id: str | None
    sequence: int
    timestamp_ms: int
    kind: str
    backend: str
    profile: str
    filesystem_mode: str
    reason: str | None


def _timestamp_from_ms(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1_000, tz=UTC)


def _maybe_profile(value: str | None) -> ExecutionProfile | None:
    if value is None:
        return None
    try:
        return ExecutionProfile(value)
    except ValueError:
        return None


def _maybe_filesystem_mode(value: str | None) -> FilesystemMode | None:
    if value is None:
        return None
    try:
        return FilesystemMode(value)
    except ValueError:
        return None


def _maybe_termination_reason(value: str | None) -> TerminationReason | None:
    if value is None:
        return None
    try:
        return TerminationReason(value)
    except ValueError:
        return None


def _coerce_result(payload: dict[str, object]) -> ExecutionResult:
    typed_payload = cast(_NativeExecutionPayload, payload)
    error_payload = typed_payload.get("error")
    error = None
    if error_payload is not None:
        error = SanitizedError(
            kind=ErrorKind(str(error_payload["kind"])),
            message=str(error_payload["message"]),
        )
    return ExecutionResult(
        stdout=str(typed_payload["stdout"]),
        stderr=str(typed_payload["stderr"]),
        exit_code=int(typed_payload["exit_code"]),
        termination_reason=TerminationReason(str(typed_payload["termination_reason"])),
        error=error,
        metadata=dict(typed_payload.get("metadata", {})),
    )


def _coerce_run_event(payload: dict[str, object]) -> RunEvent:
    typed_payload = cast(_NativeRunEventPayload, payload)
    return RunEvent(
        run_id=str(typed_payload["run_id"]),
        sequence=int(typed_payload["sequence"]),
        timestamp=_timestamp_from_ms(int(typed_payload["timestamp_ms"])),
        kind=str(typed_payload["kind"]),
        status=RunStatus(str(typed_payload["status"])),
        stream=typed_payload.get("stream"),
        text=typed_payload.get("text"),
        exit_code=typed_payload.get("exit_code"),
        termination_reason=_maybe_termination_reason(typed_payload.get("termination_reason")),
        metadata=dict(typed_payload.get("metadata", {})),
    )


def _coerce_audit_event(payload: dict[str, object]) -> AuditEvent:
    typed_payload = cast(_NativeAuditEventPayload, payload)
    return AuditEvent(
        session_id=str(typed_payload["session_id"]),
        run_id=typed_payload.get("run_id"),
        sequence=int(typed_payload["sequence"]),
        timestamp=_timestamp_from_ms(int(typed_payload["timestamp_ms"])),
        kind=str(typed_payload["kind"]),
        backend=str(typed_payload["backend"]),
        profile=_maybe_profile(typed_payload.get("profile")),
        filesystem_mode=_maybe_filesystem_mode(typed_payload.get("filesystem_mode")),
        reason=typed_payload.get("reason"),
    )


def _build_request(
    argv: list[str],
    *,
    mode: ExecutionMode = ExecutionMode.ARGV,
    script: str | None = None,
    cwd: str | None,
    env: dict[str, str] | None,
    stdin: str | bytes | None,
    timeout_ms: int | None,
    metadata: dict[str, str] | None,
    network_enabled: bool,
    filesystem_mode: FilesystemMode | None,
) -> ExecutionRequest:
    return ExecutionRequest(
        mode=mode,
        argv=list(argv),
        script=script,
        cwd=cwd,
        env=dict(env or {}),
        stdin=stdin,
        timeout_ms=timeout_ms,
        filesystem_mode=filesystem_mode,
        network_enabled=network_enabled,
        metadata=dict(metadata or {}),
    )


def _network_policy_json(policy: NetworkPolicy | None) -> str | None:
    if policy is None:
        return None
    return json.dumps(asdict(policy))


def _wrap_event_callback(
    callback: Callable[[RunEvent], None] | None,
) -> Callable[[dict[str, object]], None] | None:
    if callback is None:
        return None

    def _bridge(payload: dict[str, object]) -> None:
        callback(_coerce_run_event(payload))

    return _bridge


def _wrap_audit_callback(
    callback: Callable[[AuditEvent], None] | None,
) -> Callable[[dict[str, object]], None] | None:
    if callback is None:
        return None

    def _bridge(payload: dict[str, object]) -> None:
        callback(_coerce_audit_event(payload))

    return _bridge


class BashRun:
    def __init__(self, native: NativeRun) -> None:
        self._native = native

    @property
    def run_id(self) -> str:
        return self._native.run_id

    @property
    def started_at(self) -> datetime:
        return _timestamp_from_ms(self._native.started_at_ms)

    def status(self) -> RunStatus:
        return RunStatus(self._native.status())

    async def wait(self) -> ExecutionResult:
        payload = await to_thread.run_sync(self._native.wait)
        return _coerce_result(payload)

    def cancel(self) -> None:
        self._native.cancel()

    def stdout(self) -> str:
        return self._native.stdout()

    def stderr(self) -> str:
        return self._native.stderr()

    def output(self) -> str:
        return self._native.output()

    def events(self) -> list[RunEvent]:
        return [_coerce_run_event(payload) for payload in self._native.events()]

    def audit_events(self) -> list[AuditEvent]:
        return [_coerce_audit_event(payload) for payload in self._native.audit_events()]


class Bash:
    def __init__(
        self,
        *,
        profile: ExecutionProfile = ExecutionProfile.SAFE,
        filesystem_mode: FilesystemMode = FilesystemMode.MEMORY,
        workspace_root: str | None = None,
        writable_roots: Iterable[str] | None = None,
        allowlisted_commands: Iterable[str] | None = None,
        network_policy: NetworkPolicy | None = None,
        event_callback: Callable[[RunEvent], None] | None = None,
        audit_callback: Callable[[AuditEvent], None] | None = None,
        options: BashOptions | None = None,
    ) -> None:
        options = options or BashOptions(
            profile=profile,
            filesystem_mode=filesystem_mode,
            workspace_root=workspace_root,
            writable_roots=list(writable_roots or ()),
            allowlisted_commands=list(allowlisted_commands or ()),
            network_policy=network_policy,
            event_callback=event_callback,
            audit_callback=audit_callback,
        )
        commands = list(options.allowlisted_commands or default_allowlisted_commands())
        self._event_callback_bridge = _wrap_event_callback(options.event_callback)
        self._audit_callback_bridge = _wrap_audit_callback(options.audit_callback)
        self._native = NativeSandbox(
            options.profile.value,
            options.filesystem_mode.value,
            commands,
            options.workspace_root,
            list(options.writable_roots),
            _network_policy_json(options.network_policy),
            self._event_callback_bridge,
            self._audit_callback_bridge,
        )
        self.options = BashOptions(
            profile=options.profile,
            filesystem_mode=options.filesystem_mode,
            workspace_root=options.workspace_root,
            writable_roots=list(options.writable_roots),
            allowlisted_commands=commands,
            network_policy=options.network_policy,
            event_callback=options.event_callback,
            audit_callback=options.audit_callback,
        )
        self._closed = False

    @classmethod
    async def open(
        cls,
        *,
        profile: ExecutionProfile = ExecutionProfile.SAFE,
        filesystem_mode: FilesystemMode = FilesystemMode.MEMORY,
        workspace_root: str | None = None,
        writable_roots: Iterable[str] | None = None,
        allowlisted_commands: Iterable[str] | None = None,
        network_policy: NetworkPolicy | None = None,
        event_callback: Callable[[RunEvent], None] | None = None,
        audit_callback: Callable[[AuditEvent], None] | None = None,
    ) -> "Bash":
        return cls(
            profile=profile,
            filesystem_mode=filesystem_mode,
            workspace_root=workspace_root,
            writable_roots=writable_roots,
            allowlisted_commands=allowlisted_commands,
            network_policy=network_policy,
            event_callback=event_callback,
            audit_callback=audit_callback,
        )

    async def exec_detached(
        self,
        argv: Iterable[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> BashRun:
        if self._closed:
            raise RuntimeError("Bash session is closed")

        request = _build_request(
            list(argv),
            mode=ExecutionMode.ARGV,
            cwd=cwd,
            env=env,
            stdin=stdin,
            timeout_ms=timeout_ms,
            metadata=metadata,
            network_enabled=network_enabled,
            filesystem_mode=filesystem_mode,
        )
        selected_filesystem_mode = request.filesystem_mode or self.options.filesystem_mode
        native_run = await to_thread.run_sync(
            self._native.exec_detached,
            request.mode.value,
            request.argv,
            request.script,
            request.cwd,
            request.env,
            request.stdin_bytes(),
            request.timeout_ms,
            request.metadata,
            request.network_enabled,
            selected_filesystem_mode.value,
        )
        return BashRun(native_run)

    async def exec(
        self,
        argv: Iterable[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> ExecutionResult:
        run = await self.exec_detached(
            argv,
            cwd=cwd,
            env=env,
            stdin=stdin,
            timeout_ms=timeout_ms,
            metadata=metadata,
            network_enabled=network_enabled,
            filesystem_mode=filesystem_mode,
        )
        return await run.wait()

    async def exec_detached_script(
        self,
        script: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> BashRun:
        if self._closed:
            raise RuntimeError("Bash session is closed")

        request = _build_request(
            [],
            mode=ExecutionMode.SCRIPT,
            script=script,
            cwd=cwd,
            env=env,
            stdin=stdin,
            timeout_ms=timeout_ms,
            metadata=metadata,
            network_enabled=network_enabled,
            filesystem_mode=filesystem_mode,
        )
        selected_filesystem_mode = request.filesystem_mode or self.options.filesystem_mode
        native_run = await to_thread.run_sync(
            self._native.exec_detached,
            request.mode.value,
            request.argv,
            request.script,
            request.cwd,
            request.env,
            request.stdin_bytes(),
            request.timeout_ms,
            request.metadata,
            request.network_enabled,
            selected_filesystem_mode.value,
        )
        return BashRun(native_run)

    async def exec_script(
        self,
        script: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> ExecutionResult:
        run = await self.exec_detached_script(
            script,
            cwd=cwd,
            env=env,
            stdin=stdin,
            timeout_ms=timeout_ms,
            metadata=metadata,
            network_enabled=network_enabled,
            filesystem_mode=filesystem_mode,
        )
        return await run.wait()

    async def read_file(self, path: str, *, binary: bool = False) -> str | bytes:
        if self._closed:
            raise RuntimeError("Bash session is closed")

        data = await to_thread.run_sync(self._native.read_file, path)
        if binary:
            return data
        return data.decode("utf-8")

    async def write_file(
        self,
        path: str,
        data: str | bytes,
        *,
        binary: bool = False,
        create_parents: bool = False,
    ) -> None:
        if self._closed:
            raise RuntimeError("Bash session is closed")

        payload = data if isinstance(data, bytes) else data.encode("utf-8")
        if not binary and isinstance(data, bytes):
            payload = data
        await to_thread.run_sync(self._native.write_file, path, payload, create_parents)

    async def mkdir(self, path: str, *, parents: bool = False) -> None:
        if self._closed:
            raise RuntimeError("Bash session is closed")
        await to_thread.run_sync(self._native.mkdir, path, parents)

    async def exists(self, path: str) -> bool:
        if self._closed:
            raise RuntimeError("Bash session is closed")
        return await to_thread.run_sync(self._native.exists, path)

    async def audit_events(self) -> list[AuditEvent]:
        if self._closed:
            raise RuntimeError("Bash session is closed")
        payloads = await to_thread.run_sync(self._native.audit_events)
        return [_coerce_audit_event(payload) for payload in payloads]

    async def close(self) -> None:
        if self._closed:
            return
        await to_thread.run_sync(self._native.close)
        self._closed = True

    def cancel(self) -> None:
        self._native.cancel()

    async def __aenter__(self) -> "Bash":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


__all__ = ["Bash", "BashRun", "normalize_sandbox_path"]
