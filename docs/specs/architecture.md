# Architecture Specification

## Status

This document defines the target architecture for the project.

This document is normative. It describes subsystem boundaries, trust boundaries, major interfaces, and execution flows. It does not define implementation code.

## Purpose

The architecture MUST support a Python library that exposes a safe bash-oriented execution environment for agents while preserving a real security boundary for untrusted execution.

The architecture MUST align with the technology decisions defined in [tech-stack.md](/Users/alberto/repos/abash/docs/specs/tech-stack.md) and the product/security requirements defined in [requirements.md](/Users/alberto/repos/abash/docs/specs/requirements.md).

## Architectural Principles

The architecture MUST:

- separate the public Python API from the security-critical execution core
- make trust boundaries explicit
- support multiple execution backends behind a stable core contract
- ensure that policy enforcement occurs in the execution core or lower
- support a safe default execution profile

The architecture MUST NOT:

- treat Python as the primary isolation boundary
- tie the product to a single execution backend
- allow policy bypass through optional integration layers

## Top-Level System Model

The system SHALL consist of:

- a Python public API layer
- a Rust core runtime
- one or more Rust backend adapters
- an optional host isolation layer for real shell execution
- a documentation and policy layer

At a high level, execution flows as follows:

1. the embedding application calls the Python API
2. the Python API validates user-facing configuration shape and forwards requests into the Rust core
3. the Rust core resolves policy, filesystem view, execution limits, and backend selection
4. the selected backend executes under the applicable isolation model
5. the Rust core normalizes and sanitizes the result
6. the Python API returns a structured result to the caller

## Trust Boundaries

### Untrusted Inputs

The following inputs MUST be treated as untrusted:

- shell scripts
- command names and argv values
- stdin
- file contents
- file paths
- environment variables supplied by callers
- remote responses
- data returned from guest runtimes

### Trusted Components

The following components are trusted by design:

- embedding application
- Python package code
- Rust core code
- configured host-side policy definitions
- explicitly trusted custom extensions added by the host

### Conditional Trust

The following surfaces are trusted only when explicitly enabled by the host:

- custom commands
- plugins or transforms
- host-backed filesystems
- real shell backends
- network credentials injected by the host

## Subsystems

### Python API Layer

### Responsibilities

The Python layer MUST:

- expose the user-facing package API
- define session and execution entry points
- define typed request and response models
- provide async orchestration and lifecycle management
- translate Python exceptions and cancellation signals into core-compatible requests

### Non-Responsibilities

The Python layer MUST NOT:

- be the primary security boundary
- directly enforce host filesystem containment for execution
- directly sandbox real shell execution
- define the sole source of truth for resource limits or path policy

### Rust Core Runtime

### Responsibilities

The Rust core is the architectural center of the system.

It MUST:

- define canonical execution requests and results
- define policy objects
- define filesystem and mount abstractions
- resolve backend selection
- enforce path and workspace policy
- enforce resource limit accounting where backend-independent
- sanitize errors and normalize results
- coordinate cancellation and timeout behavior

### Non-Responsibilities

The Rust core MUST NOT:

- expose backend-specific internals directly to the Python layer
- assume one execution model
- assume that all backends provide equivalent compatibility

### Execution Backends

Backends MUST implement a stable internal contract defined by the Rust core.

The initial architecture MUST support:

- a virtual backend
- a Linux real-bash backend

### Virtual Backend

The virtual backend MUST provide:

- safe default execution profile
- virtual filesystem semantics
- no implicit host filesystem dependency
- no implicit network access
- bounded execution semantics

The virtual backend is the default backend for the safe profile.

### Real-Bash Backend

The real-bash backend MUST:

- be Linux-first
- run only behind OS-level isolation
- use `nsjail` as the initial isolation mechanism
- operate under explicit policy from the Rust core

The real-bash backend MUST NOT be the default safe profile.

### Optional WASM Backend

If a WASM backend or WASM guest layer is introduced, it MUST be treated as an optional capability runtime.

It MAY be used for:

- plugins
- helper commands
- portable guest execution

It MUST NOT be the primary architecture for implementing bash execution semantics.

## Core Contracts

### Execution Request

The core execution request model MUST include, at minimum:

- execution mode
  - argv mode or script mode
- current working directory
- environment variables
- stdin
- backend selection or execution profile
- resource limits
- network policy reference
- filesystem policy reference
- cancellation token or equivalent

### Execution Result

The core execution result model MUST include, at minimum:

- stdout
- stderr
- exit code
- termination reason if not a normal completion
- optional metadata
- sanitized error information when applicable

### Filesystem Contract

The core filesystem contract MUST support:

- path resolution
- read
- write
- mkdir
- existence checks
- mount composition
- explicit read/write policy

The filesystem contract MUST use sandbox path semantics, not raw host-path assumptions.

### Policy Contract

The core policy contract MUST cover:

- command allowlisting
- filesystem roots and writable scopes
- environment exposure
- network policy
- execution limits
- backend selection constraints

## Execution Profiles

The architecture MUST support explicit execution profiles.

At minimum:

- **safe profile**
  - virtual backend
  - no host filesystem access by default
  - no network by default
  - minimal enabled capabilities

- **workspace profile**
  - virtual backend or constrained host-backed filesystem mode
  - explicit project root policy
  - still no implicit real-shell execution

- **real-shell profile**
  - Linux-only
  - `nsjail` backend
  - explicit host isolation configuration
  - explicit opt-in

Profiles MUST be visible and auditable. The caller MUST know which profile is active.

## Filesystem Architecture

The filesystem architecture MUST separate:

- logical sandbox paths
- mount and storage policy
- host-backed path mapping

The system MUST support:

- in-memory roots
- read-only host-backed roots
- copy-on-write host-backed roots
- explicitly writable host-backed roots

Path enforcement MUST occur in the Rust core or backend boundary, not only in Python.

Symlink handling MUST be explicit. The default architecture assumes symlink escape prevention rather than permissive traversal.

## Network Architecture

Network capabilities MUST be mediated by policy, not by direct guest access.

The architecture MUST support:

- explicit enablement
- allowlist-based request evaluation
- redirect revalidation
- timeout and response size limits
- optional private-range blocking
- host-side credential injection

Backends MUST NOT access the network outside the core-approved network policy path.

## Cancellation and Timeout Architecture

The architecture MUST provide cancellation propagation from Python through the Rust core to the selected backend.

Timeout handling MUST exist at two levels where applicable:

- core-level lifecycle timeout
- backend-level execution enforcement

Cancellation MUST be observable and result in a deterministic terminal state.

## Error Handling Architecture

Errors MUST be classified before being exposed to the caller.

At minimum, the system SHOULD distinguish:

- policy denial
- execution timeout
- cancellation
- unsupported feature
- internal error
- backend failure

The caller-facing result MUST contain sanitized error information. Internal error detail MAY be retained in logs or diagnostics, but MUST NOT be exposed by default through the normal API.

## Observability Architecture

Observability MUST be layered so that it does not weaken security boundaries.

The architecture SHOULD support:

- structured event logs
- trace hooks
- execution metadata
- backend and policy audit records

Observability data MUST be associated with:

- session identity
- execution identity
- selected backend
- policy decisions
- termination reason

## Extension Architecture

Extensions MUST be treated as privileged code unless otherwise sandboxed.

The architecture MAY support:

- custom commands
- transform plugins
- metadata plugins

If extensions are supported:

- they MUST integrate through defined extension points
- their trust status MUST be explicit
- they MUST NOT bypass core policy silently

## Backend Interface Rules

Every backend MUST:

- accept a canonical execution request
- return a canonical execution result
- respect the core policy contract
- support cancellation semantics
- report unsupported features explicitly

No backend MAY redefine:

- trust boundaries
- caller-visible result shape
- baseline policy semantics

Backends MAY differ in:

- compatibility level
- performance characteristics
- isolation strength
- supported feature subsets

Those differences MUST be documented.

## Repository and Module Boundaries

The architecture assumes the following code organization:

- `python/abash/`
  - public package
  - Python-facing models
  - session and orchestration API

- `crates/core/`
  - canonical request/result contracts
  - policy engine
  - filesystem abstractions
  - backend dispatch

- `crates/backend-virtual/`
  - virtual execution backend

- `crates/backend-nsjail/`
  - Linux real-shell backend

- `crates/backend-wasm/`
  - optional WASM guest execution support

This module split is required to keep backend concerns from leaking into the public package API.

## Evolution Constraints

The architecture MAY evolve, but the following constraints remain fixed unless the specifications are explicitly revised:

- Python is the public package surface
- Rust is the security-sensitive core
- real host-shell execution requires OS isolation
- the safe profile is not the real-shell profile
- WASM is optional capability infrastructure, not the foundational shell strategy

## Acceptance Conditions

The architecture is acceptable only if:

- subsystem responsibilities are clearly separated
- trust boundaries are explicit
- policy is enforced below the Python layer
- the safe profile does not depend on real host shell execution
- real-shell execution is isolated by the OS, not by convention
- all backends conform to the same canonical request/result model
- extension points cannot silently redefine security semantics

## Related Specifications

- [requirements.md](/Users/alberto/repos/abash/docs/specs/requirements.md)
- [tech-stack.md](/Users/alberto/repos/abash/docs/specs/tech-stack.md)
- [phase-1-minimal-safe-execution-core.md](/Users/alberto/repos/abash/docs/specs/phase-1-minimal-safe-execution-core.md)
- [phase-2-workspace-and-filesystem-policy.md](/Users/alberto/repos/abash/docs/specs/phase-2-workspace-and-filesystem-policy.md)
- [phase-3-network-policy-and-data-access.md](/Users/alberto/repos/abash/docs/specs/phase-3-network-policy-and-data-access.md)
- [phase-4-agent-tooling-and-observability.md](/Users/alberto/repos/abash/docs/specs/phase-4-agent-tooling-and-observability.md)
- [phase-5-compatibility-expansion.md](/Users/alberto/repos/abash/docs/specs/phase-5-compatibility-expansion.md)
- [phase-6-extended-runtimes-and-privileged-extensions.md](/Users/alberto/repos/abash/docs/specs/phase-6-extended-runtimes-and-privileged-extensions.md)
