# Requirements Specification

## Document Status

This document defines the product and security requirements for a Python library that provides a safe bash-like execution environment as a tool for agents.

This is a requirements specification only. It does not prescribe implementation details, code structure, or a specific runtime architecture beyond what is required for correctness and safety.

## Purpose

The library SHALL provide agents with a constrained shell execution environment suitable for text processing, file manipulation, and scripted workflows without exposing unrestricted access to the host machine.

The library SHALL be designed for zero-trust execution. Scripts, command inputs, file paths, environment variables, stdin, and remote data MUST be treated as untrusted.

## Design Goals

The library MUST:

- provide a safe execution environment for agent-driven shell tasks
- default to least privilege
- expose a tool-friendly Python API
- make security boundaries explicit
- provide deterministic and inspectable execution behavior
- support the common shell workflows agents rely on

The library SHOULD:

- preserve a familiar bash-like user model for supported features
- allow controlled persistence of virtual workspace state across runs
- provide observability suitable for debugging, auditing, and policy enforcement

The library MUST NOT:

- rely on trust in the calling agent
- silently grant host filesystem or network access
- claim full bash compatibility unless verified and documented

## Threat Model

The primary threat actor is an untrusted script author, including an LLM agent or end user that can submit arbitrary shell input.

Secondary threat actors include:

- malicious stdin or file content
- malicious remote responses when network access is enabled
- malicious filenames, environment variables, or path fragments

The embedding application is trusted. Host-provided extensions, plugins, policy hooks, and storage backends are considered outside the sandbox trust boundary unless explicitly re-sandboxed by the host.

## Security Baseline

The library MUST implement a zero-trust default configuration.

By default:

- network access MUST be disabled
- host filesystem access MUST be disabled
- host environment variables MUST NOT be exposed
- host process identity and machine details MUST NOT be exposed
- only explicitly enabled commands and capabilities MAY be available

Security controls MUST be enforced by the execution environment itself, not only by caller convention or documentation.

The library MUST define and document its trust boundaries, residual risks, and known limitations.

## Execution Model

The library MUST provide an execution model with clearly defined state behavior.

At minimum, it MUST specify:

- whether environment variables persist between executions
- whether working directory changes persist between executions
- whether shell functions and aliases persist between executions
- whether filesystem changes persist between executions

The default behavior SHOULD isolate shell state between executions.

Filesystem persistence MAY be supported across executions within the same tool session, but this behavior MUST be explicit and configurable.

Each execution MUST return a structured result containing at least:

- `stdout`
- `stderr`
- `exit_code`

The library SHOULD support optional execution metadata, including command-level metadata and tracing information.

## Execution Modes

The library MUST support two distinct invocation models:

### Argv Mode

Argv mode MUST execute a command plus arguments without shell parsing.

In argv mode:

- shell metacharacters MUST be treated as literal data
- the caller MUST be able to pass arguments without quoting concerns
- this mode SHOULD be the default for tool-driven command execution where possible

### Script Mode

Script mode MUST execute a shell script string using the supported shell grammar.

In script mode:

- shell parsing semantics MUST be explicit
- the result MUST remain subject to the same filesystem, network, and resource controls as argv mode

## Filesystem Requirements

The library MUST provide a virtual filesystem.

### Default Filesystem

The default execution environment MUST use a virtual filesystem with no implicit access to the host disk.

### Filesystem Modes

The library SHOULD support the following filesystem modes:

- in-memory virtual filesystem
- read-only or copy-on-write view over a host directory
- explicitly enabled read-write workspace root
- multiple mounted roots with independent policies

If host-backed filesystem access is supported:

- access MUST be scoped to explicit root directories
- path traversal outside the allowed root MUST be blocked
- symlink-based escape MUST be blocked by default
- writes MUST be restricted to explicitly writable roots

The library MUST document the difference between:

- virtual-only storage
- host-backed read access
- host-backed write access

### File APIs

The library MUST expose tool-friendly file operations sufficient for agent workflows.

At minimum, it SHOULD support:

- writing files into the sandbox
- reading files from the sandbox
- creating directories
- checking file existence

The library SHOULD support binary-safe file transfer semantics, such as explicit encoding modes.

## Environment Requirements

The library MUST allow controlled per-execution environment configuration.

It MUST support:

- setting environment variables for a single execution
- choosing whether those variables merge with or replace the default execution environment
- setting the working directory for a single execution
- passing stdin explicitly

Host environment variables MUST NOT be exposed unless explicitly allowlisted by the embedding application.

## Command Surface Requirements

The library MUST define a supported command surface.

It SHOULD prioritize commands commonly used by agents for:

- file inspection
- text processing
- structured data processing
- searching and filtering
- shell composition

The library MUST support command allowlisting or equivalent capability restriction so the embedding application can expose only approved commands.

If optional runtimes are supported, such as Python, JavaScript, or SQLite:

- they MUST be opt-in
- they MUST be documented as additional security surface
- they MUST be governed by the same or stricter resource and sandbox controls

## Shell Compatibility Requirements

The library SHOULD support the subset of shell behavior most valuable to agent workflows, including:

- pipes
- redirections
- command chaining
- variables
- globbing
- control flow
- functions

The library MUST publish a compatibility statement and known limitations.

The library MUST NOT imply full bash parity unless validated and maintained as such.

TTY-dependent features, job control, interactive shell history, and arbitrary host binary execution MAY be out of scope. If they are out of scope, this MUST be stated explicitly.

## Resource Control Requirements

The library MUST enforce execution limits sufficient to prevent runaway compute and memory abuse.

At minimum, configurable limits MUST exist for:

- total commands executed
- loop iterations
- recursion or call depth
- command substitution depth if applicable
- output size
- string or buffer growth
- number of open file descriptors if applicable
- wall-clock execution time

The library SHOULD support command-specific or runtime-specific limits where needed.

Exceeding a limit MUST fail safely and return a sanitized, user-visible error.

## Cancellation and Timeouts

The library MUST support cooperative or enforced cancellation for individual executions.

It MUST support:

- per-execution timeout
- external cancellation signal or equivalent control

Cancellation MUST terminate the logical execution and prevent continued sandbox activity after the call has been reported as stopped.

## Network Requirements

Network access MUST be disabled by default.

If network access is enabled, the library MUST support a policy model that includes:

- URL allowlisting by origin and optional path prefix
- allowed HTTP method restrictions
- redirect validation on every hop
- request timeout
- maximum response size
- protocol restrictions

The library SHOULD support controls to reduce SSRF risk, including:

- blocking loopback and private IP ranges
- DNS rebinding resistance

If the host application needs to inject credentials or headers:

- secrets MUST be injected at the host boundary
- secrets MUST NOT be exposed as plain sandbox environment variables unless explicitly intended
- host-injected headers MUST take precedence over conflicting user-supplied values where policy requires it

Any “allow full internet access” mode MUST be explicitly marked dangerous.

## Information Disclosure Requirements

The library MUST minimize host information disclosure.

It MUST NOT expose, by default:

- host filesystem paths outside the sandbox
- host environment variables
- host PID, UID, username, or machine identity
- stack traces or runtime internals that materially aid escape attempts

If process-like information is exposed for compatibility, it MUST be virtualized or sanitized.

Error messages MUST be sanitized before being returned to the caller.

## Injection and Escape Resistance

The library MUST defend against common sandbox escape and misuse classes relevant to its execution model.

These include, at minimum:

- command injection through string interpolation in tool APIs
- path traversal
- symlink escape
- unsafe parsing of paths, environment data, or headers
- denial of service through excessive expansion, recursion, or output generation
- information disclosure through unsanitized errors

If the system is not implemented as a pure virtual interpreter and instead executes a real host shell, then OS-level isolation becomes mandatory.

In that case, the product MUST use a real isolation boundary appropriate for untrusted code execution, such as a container, jail, namespace-based sandbox, seccomp-based restriction, or virtual machine.

Python-level validation alone MUST NOT be treated as sufficient isolation for real `/bin/bash` execution.

## Observability Requirements

The library SHOULD provide observability hooks suitable for agents and host applications.

These SHOULD include:

- structured execution logs
- trace or profiling callbacks
- optional command collection metadata
- audit events for security-relevant policy decisions

Observability features MUST NOT weaken sandbox guarantees or leak secrets by default.

## Tool-Facing API Requirements

The library MUST expose an API suitable for use as an agent tool.

The API SHOULD support:

- creating a sandbox or session
- running a command
- writing files
- reading files
- creating directories
- stopping or disposing of a sandbox

If asynchronous or detached execution is supported, the API SHOULD also support:

- waiting for completion
- fetching `stdout` and `stderr`
- killing a running command
- reading execution logs

The behavior of each API entry point MUST be defined in terms of security semantics, persistence semantics, and failure modes.

## Extensibility Requirements

The library MAY support host-defined custom commands, plugins, or transforms.

If extensibility is supported:

- the trust boundary between trusted host extensions and untrusted scripts MUST be explicit
- extension points MUST be documented as privileged surfaces
- extension failures and policy violations MUST be reported safely

Instrumentation or transform hooks SHOULD be allowed for use cases such as:

- metadata collection
- audit logging
- output capture
- policy inspection

## Testing and Assurance Requirements

The project MUST include security-focused testing.

The test strategy SHOULD include:

- unit tests for policy enforcement
- path traversal and symlink escape tests
- injection-resistance tests for tool-facing APIs
- timeout and cancellation tests
- output and memory limit tests
- network allowlist and redirect tests
- regression tests for known attack patterns

The project SHOULD maintain:

- a threat model
- a list of known limitations
- explicit residual risk documentation

## Non-Goals

Unless explicitly added later, the following are non-goals:

- unrestricted execution of host binaries
- unrestricted host filesystem access
- unrestricted internet access
- full interactive terminal emulation
- complete bash feature parity
- security claims stronger than the actual isolation boundary

## Phase Structure

The library SHOULD be delivered in phases. Each phase defines the minimum scope required to unlock the next one.

No phase is complete unless all listed acceptance criteria are satisfied.

Phase documents:

- [Phase 1](/Users/alberto/repos/abash/docs/specs/phase-1-minimal-safe-execution-core.md)
- [Phase 2](/Users/alberto/repos/abash/docs/specs/phase-2-workspace-and-filesystem-policy.md)
- [Phase 3](/Users/alberto/repos/abash/docs/specs/phase-3-network-policy-and-data-access.md)
- [Phase 4](/Users/alberto/repos/abash/docs/specs/phase-4-agent-tooling-and-observability.md)
- [Phase 5](/Users/alberto/repos/abash/docs/specs/phase-5-compatibility-expansion.md)
- [Phase 6](/Users/alberto/repos/abash/docs/specs/phase-6-extended-runtimes-and-privileged-extensions.md)

## Acceptance Criteria

The library is acceptable only if all of the following are true:

- zero-trust defaults are enforced
- host access is opt-in and policy-bound
- executions return structured results
- timeouts and cancellation are supported
- resource limits are enforced
- filesystem scope is controlled and documented
- network policy is explicit and safe by default
- errors are sanitized
- supported shell behavior and known limitations are documented
- the isolation story matches the actual execution architecture

## Reference Basis

This specification is informed by the design principles, feature set, threat model, and explicit limitations observed in Vercel Labs’ `just-bash` project, while adapting them into implementation-agnostic requirements for a Python library.
