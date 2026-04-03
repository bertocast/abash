from __future__ import annotations

import json
from inspect import Parameter, signature
from collections.abc import Callable, Iterable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TypedDict, cast

from anyio import to_thread

from ._models import (
    AuditEvent,
    BashOptions,
    CustomCommandContext,
    DelegatedExecution,
    ErrorKind,
    ExecutionMode,
    ExecutionProfile,
    ExecutionRequest,
    ExecutionResult,
    FilesystemMode,
    HostMount,
    LazyMountProvider,
    LazyPathEntry,
    NetworkPolicy,
    RunEvent,
    RunSummary,
    RunStatus,
    SanitizedError,
    SessionState,
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


class _NativeRequestPayload(TypedDict):
    mode: str
    argv: list[str]
    script: str | None
    cwd: str
    env: dict[str, str]
    replace_env: bool
    stdin: bytes
    timeout_ms: int | None
    network_enabled: bool
    filesystem_mode: str
    metadata: dict[str, str]


class _NativeRunSummaryPayload(TypedDict):
    run_id: str
    started_at_ms: int
    status: str
    exit_code: int | None
    termination_reason: str | None


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


def _maybe_mode(value: str | None) -> ExecutionMode | None:
    if value is None:
        return None
    try:
        return ExecutionMode(value)
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


def _coerce_request(payload: dict[str, object]) -> ExecutionRequest:
    typed_payload = cast(_NativeRequestPayload, payload)
    return ExecutionRequest(
        mode=ExecutionMode(str(typed_payload["mode"])),
        argv=list(typed_payload.get("argv", [])),
        script=typed_payload.get("script"),
        cwd=str(typed_payload.get("cwd", "")),
        env=dict(typed_payload.get("env", {})),
        replace_env=bool(typed_payload.get("replace_env", False)),
        stdin=bytes(typed_payload.get("stdin", b"")),
        timeout_ms=typed_payload.get("timeout_ms"),
        filesystem_mode=FilesystemMode(str(typed_payload["filesystem_mode"])),
        network_enabled=bool(typed_payload.get("network_enabled", False)),
        metadata=dict(typed_payload.get("metadata", {})),
    )


def _result_to_payload(result: ExecutionResult | str | bytes) -> dict[str, object]:
    if isinstance(result, str):
        result = ExecutionResult(
            stdout=result,
            stderr="",
            exit_code=0,
            termination_reason=TerminationReason.EXITED,
        )
    elif isinstance(result, bytes):
        result = ExecutionResult(
            stdout=result.decode("utf-8"),
            stderr="",
            exit_code=0,
            termination_reason=TerminationReason.EXITED,
        )

    error_payload: dict[str, str] | None = None
    if result.error is not None:
        error_payload = {
            "kind": result.error.kind.value,
            "message": result.error.message,
        }
    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "exit_code": result.exit_code,
        "termination_reason": result.termination_reason.value,
        "error": error_payload,
        "metadata": dict(result.metadata),
    }


def _request_to_payload(request: ExecutionRequest) -> dict[str, object]:
    return {
        "mode": request.mode.value,
        "argv": list(request.argv or []),
        "script": request.script,
        "cwd": request.cwd or "",
        "env": dict(request.env),
        "replace_env": request.replace_env,
        "stdin": request.stdin_bytes() or b"",
        "timeout_ms": request.timeout_ms,
        "network_enabled": request.network_enabled,
        "filesystem_mode": (request.filesystem_mode or FilesystemMode.MEMORY).value,
        "metadata": dict(request.metadata),
    }


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


def _coerce_run_summary(payload: dict[str, object]) -> RunSummary:
    typed_payload = cast(_NativeRunSummaryPayload, payload)
    return RunSummary(
        run_id=str(typed_payload["run_id"]),
        started_at=_timestamp_from_ms(int(typed_payload["started_at_ms"])),
        status=RunStatus(str(typed_payload["status"])),
        exit_code=typed_payload.get("exit_code"),
        termination_reason=_maybe_termination_reason(typed_payload.get("termination_reason")),
    )


def _command_context_from_request(request: ExecutionRequest) -> CustomCommandContext:
    metadata = request.metadata
    return CustomCommandContext(
        session_id=metadata.get("session_id"),
        run_id=metadata.get("run_id"),
        backend=metadata.get("backend"),
        profile=_maybe_profile(metadata.get("profile")),
        filesystem_mode=_maybe_filesystem_mode(metadata.get("filesystem_mode")),
        request_mode=_maybe_mode(metadata.get("request_mode")) or request.mode,
        command_name=request.argv[0] if request.argv else "",
        cwd=request.cwd,
    )


def _build_request(
    argv: list[str],
    *,
    mode: ExecutionMode = ExecutionMode.ARGV,
    script: str | None = None,
    cwd: str | None,
    env: dict[str, str] | None,
    replace_env: bool = False,
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
        replace_env=replace_env,
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


def _native_host_mounts(host_mounts: list[HostMount]) -> list[tuple[str, str]] | None:
    if not host_mounts:
        return None
    return [(mount.sandbox_path, mount.host_path) for mount in host_mounts]


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


def _wrap_custom_command_callback(
    callbacks: dict[str, Callable[..., ExecutionResult | str | bytes | DelegatedExecution]]
    | None,
) -> Callable[[dict[str, object]], dict[str, object]] | None:
    if not callbacks:
        return None

    accepts_context: dict[str, bool] = {}
    for name, callback in callbacks.items():
        positional = [
            parameter
            for parameter in signature(callback).parameters.values()
            if parameter.kind
            in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
        ]
        accepts_context[name] = len(positional) >= 2

    def _bridge(payload: dict[str, object]) -> dict[str, object]:
        request = _coerce_request(payload)
        command = request.argv[0] if request.argv else ""
        callback = callbacks[command]
        if accepts_context.get(command, False):
            result = callback(request, _command_context_from_request(request))
        else:
            result = callback(request)
        if isinstance(result, DelegatedExecution):
            return {"delegated_request": _request_to_payload(result.request)}
        return _result_to_payload(result)

    return _bridge


def _wrap_lazy_file_callback(
    callbacks: dict[str, Callable[[str], str | bytes | None] | LazyMountProvider] | None,
) -> Callable[[str], str | bytes | None] | None:
    if not callbacks:
        return None

    roots = sorted(callbacks, key=len, reverse=True)

    def _bridge(path: str) -> str | bytes | None:
        for root in roots:
            if path == root or path.startswith(f"{root}/"):
                provider = callbacks[root]
                if isinstance(provider, LazyMountProvider) or hasattr(provider, "read_file"):
                    return provider.read_file(path)
                return provider(path)
        return None

    return _bridge


def _wrap_lazy_paths_callback(
    callbacks: dict[str, Callable[[str], str | bytes | None] | LazyMountProvider] | None,
) -> Callable[[], list[tuple[str, bool]]] | None:
    if not callbacks:
        return None

    def _normalize_lazy_path(root: str, entry: str | LazyPathEntry) -> tuple[str, bool]:
        if isinstance(entry, LazyPathEntry):
            raw_path = entry.path
            is_dir = entry.is_dir
        else:
            raw_path = entry
            is_dir = raw_path.endswith("/")
            if is_dir and raw_path != "/":
                raw_path = raw_path.rstrip("/")

        if not raw_path:
            raw_path = root

        if raw_path.startswith("/"):
            normalized = normalize_sandbox_path(raw_path)
        else:
            base = root.rstrip("/") or "/"
            normalized = normalize_sandbox_path(f"{base}/{raw_path.lstrip('/')}")

        if normalized != root and not normalized.startswith(f"{root}/"):
            raise ValueError(f"lazy provider path escapes root {root}: {normalized}")
        return normalized, is_dir

    def _bridge() -> list[tuple[str, bool]]:
        merged: dict[str, bool] = {}
        for root, provider in callbacks.items():
            list_paths = (
                provider.list_paths
                if isinstance(provider, LazyMountProvider) or hasattr(provider, "list_paths")
                else None
            )
            if list_paths is None:
                continue
            for entry in list_paths():
                path, is_dir = _normalize_lazy_path(root, entry)
                merged[path] = merged.get(path, False) or is_dir
        return sorted(merged.items())

    return _bridge


def _wrap_pre_exec_hook(
    callback: Callable[[ExecutionRequest], ExecutionRequest | None] | None,
) -> Callable[[dict[str, object]], dict[str, object] | None] | None:
    if callback is None:
        return None

    def _bridge(payload: dict[str, object]) -> dict[str, object] | None:
        request = _coerce_request(payload)
        updated = callback(request)
        if updated is None:
            return None
        return {
            "mode": updated.mode.value,
            "argv": list(updated.argv or []),
            "script": updated.script,
            "cwd": updated.cwd or "",
            "env": dict(updated.env),
            "replace_env": updated.replace_env,
            "stdin": updated.stdin_bytes() or b"",
            "timeout_ms": updated.timeout_ms,
            "network_enabled": updated.network_enabled,
            "filesystem_mode": (updated.filesystem_mode or FilesystemMode.MEMORY).value,
            "metadata": dict(updated.metadata),
        }

    return _bridge


def _wrap_post_exec_hook(
    callback: Callable[[ExecutionRequest, ExecutionResult], ExecutionResult | None] | None,
) -> Callable[[dict[str, object], dict[str, object]], dict[str, object] | None] | None:
    if callback is None:
        return None

    def _bridge(
        request_payload: dict[str, object],
        result_payload: dict[str, object],
    ) -> dict[str, object] | None:
        request = _coerce_request(request_payload)
        result = _coerce_result(result_payload)
        updated = callback(request, result)
        if updated is None:
            return None
        return _result_to_payload(updated)

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

    async def stream_events(self, *, timeout_ms: int = 100):
        sequence = 0
        while True:
            payloads = await to_thread.run_sync(
                self._native.wait_for_events,
                sequence,
                timeout_ms,
            )
            events = [_coerce_run_event(payload) for payload in payloads]
            for event in events:
                sequence = max(sequence, event.sequence)
                yield event
            if self.status() in {
                RunStatus.COMPLETED,
                RunStatus.CANCELLED,
                RunStatus.FAILED,
            } and not events:
                final_events = [event for event in self.events() if event.sequence > sequence]
                for event in final_events:
                    sequence = max(sequence, event.sequence)
                    yield event
                break

    async def stream_output(
        self,
        *,
        timeout_ms: int = 100,
        streams: tuple[str, ...] = ("stdout", "stderr"),
    ):
        async for event in self.stream_events(timeout_ms=timeout_ms):
            if event.stream in streams and event.text:
                yield event.stream, event.text


class Bash:
    def __init__(
        self,
        *,
        profile: ExecutionProfile = ExecutionProfile.SAFE,
        filesystem_mode: FilesystemMode = FilesystemMode.MEMORY,
        session_state: SessionState = SessionState.PERSISTENT,
        workspace_root: str | None = None,
        host_mounts: Iterable[HostMount] | None = None,
        writable_roots: Iterable[str] | None = None,
        allowlisted_commands: Iterable[str] | None = None,
        network_policy: NetworkPolicy | None = None,
        event_callback: Callable[[RunEvent], None] | None = None,
        audit_callback: Callable[[AuditEvent], None] | None = None,
        custom_commands: dict[
            str,
            Callable[..., ExecutionResult | str | bytes | DelegatedExecution],
        ]
        | None = None,
        lazy_file_providers: dict[
            str, Callable[[str], str | bytes | None] | LazyMountProvider
        ]
        | None = None,
        pre_exec_hook: Callable[[ExecutionRequest], ExecutionRequest | None] | None = None,
        post_exec_hook: Callable[
            [ExecutionRequest, ExecutionResult], ExecutionResult | None
        ]
        | None = None,
        options: BashOptions | None = None,
    ) -> None:
        options = options or BashOptions(
            profile=profile,
            filesystem_mode=filesystem_mode,
            session_state=session_state,
            workspace_root=workspace_root,
            host_mounts=list(host_mounts or ()),
            writable_roots=list(writable_roots or ()),
            allowlisted_commands=list(allowlisted_commands or ()),
            network_policy=network_policy,
            event_callback=event_callback,
            audit_callback=audit_callback,
            custom_commands=dict(custom_commands or {}),
            lazy_file_providers=dict(lazy_file_providers or {}),
            pre_exec_hook=pre_exec_hook,
            post_exec_hook=post_exec_hook,
        )
        commands = list(options.allowlisted_commands or default_allowlisted_commands())
        self._event_callback_bridge = _wrap_event_callback(options.event_callback)
        self._audit_callback_bridge = _wrap_audit_callback(options.audit_callback)
        self._custom_command_bridge = _wrap_custom_command_callback(options.custom_commands)
        self._lazy_file_bridge = _wrap_lazy_file_callback(options.lazy_file_providers)
        self._lazy_paths_bridge = _wrap_lazy_paths_callback(options.lazy_file_providers)
        self._pre_exec_hook_bridge = _wrap_pre_exec_hook(options.pre_exec_hook)
        self._post_exec_hook_bridge = _wrap_post_exec_hook(options.post_exec_hook)
        self._native = NativeSandbox(
            options.profile.value,
            options.filesystem_mode.value,
            commands,
            options.session_state.value,
            options.workspace_root,
            _native_host_mounts(options.host_mounts),
            list(options.writable_roots),
            _network_policy_json(options.network_policy),
            self._event_callback_bridge,
            self._audit_callback_bridge,
            sorted(options.custom_commands),
            self._custom_command_bridge,
            sorted(options.lazy_file_providers),
            self._lazy_file_bridge,
            self._lazy_paths_bridge,
            self._pre_exec_hook_bridge,
            self._post_exec_hook_bridge,
        )
        self.options = BashOptions(
            profile=options.profile,
            filesystem_mode=options.filesystem_mode,
            session_state=options.session_state,
            workspace_root=options.workspace_root,
            host_mounts=list(options.host_mounts),
            writable_roots=list(options.writable_roots),
            allowlisted_commands=commands,
            network_policy=options.network_policy,
            event_callback=options.event_callback,
            audit_callback=options.audit_callback,
            custom_commands=dict(options.custom_commands),
            lazy_file_providers=dict(options.lazy_file_providers),
            pre_exec_hook=options.pre_exec_hook,
            post_exec_hook=options.post_exec_hook,
        )
        self._closed = False

    @classmethod
    async def open(
        cls,
        *,
        profile: ExecutionProfile = ExecutionProfile.SAFE,
        filesystem_mode: FilesystemMode = FilesystemMode.MEMORY,
        session_state: SessionState = SessionState.PERSISTENT,
        workspace_root: str | None = None,
        host_mounts: Iterable[HostMount] | None = None,
        writable_roots: Iterable[str] | None = None,
        allowlisted_commands: Iterable[str] | None = None,
        network_policy: NetworkPolicy | None = None,
        event_callback: Callable[[RunEvent], None] | None = None,
        audit_callback: Callable[[AuditEvent], None] | None = None,
        custom_commands: dict[
            str,
            Callable[..., ExecutionResult | str | bytes | DelegatedExecution],
        ]
        | None = None,
        lazy_file_providers: dict[
            str, Callable[[str], str | bytes | None] | LazyMountProvider
        ]
        | None = None,
        pre_exec_hook: Callable[[ExecutionRequest], ExecutionRequest | None] | None = None,
        post_exec_hook: Callable[
            [ExecutionRequest, ExecutionResult], ExecutionResult | None
        ]
        | None = None,
    ) -> "Bash":
        return cls(
            profile=profile,
            filesystem_mode=filesystem_mode,
            session_state=session_state,
            workspace_root=workspace_root,
            host_mounts=host_mounts,
            writable_roots=writable_roots,
            allowlisted_commands=allowlisted_commands,
            network_policy=network_policy,
            event_callback=event_callback,
            audit_callback=audit_callback,
            custom_commands=custom_commands,
            lazy_file_providers=lazy_file_providers,
            pre_exec_hook=pre_exec_hook,
            post_exec_hook=post_exec_hook,
        )

    async def exec_detached(
        self,
        argv: Iterable[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        replace_env: bool = False,
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
            replace_env=replace_env,
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
            request.replace_env,
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
        replace_env: bool = False,
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
            replace_env=replace_env,
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
        argv: list[str] | None = None,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        replace_env: bool = False,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> BashRun:
        if self._closed:
            raise RuntimeError("Bash session is closed")

        request = _build_request(
            list(argv or []),
            mode=ExecutionMode.SCRIPT,
            script=script,
            cwd=cwd,
            env=env,
            replace_env=replace_env,
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
            request.replace_env,
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
        argv: list[str] | None = None,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        replace_env: bool = False,
        stdin: str | bytes | None = None,
        timeout_ms: int | None = None,
        metadata: dict[str, str] | None = None,
        network_enabled: bool = False,
        filesystem_mode: FilesystemMode | None = None,
    ) -> ExecutionResult:
        run = await self.exec_detached_script(
            script,
            argv=argv,
            cwd=cwd,
            env=env,
            replace_env=replace_env,
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

    async def run_events(self) -> list[RunEvent]:
        if self._closed:
            raise RuntimeError("Bash session is closed")
        payloads = await to_thread.run_sync(self._native.run_events)
        return [_coerce_run_event(payload) for payload in payloads]

    async def runs(self) -> list[RunSummary]:
        if self._closed:
            raise RuntimeError("Bash session is closed")
        payloads = await to_thread.run_sync(self._native.runs)
        return [_coerce_run_summary(payload) for payload in payloads]

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
