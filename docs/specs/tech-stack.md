# Technology Stack Decision

## Status

This document defines the required and avoided technology choices for the project.

This document is normative. Items listed as `required` are part of the target stack. Items listed as `avoid` are excluded unless this document is explicitly revised.

## Stack Principles

The stack MUST optimize for:

- real security boundaries, not only input validation
- explicit isolation semantics
- strong packaging and distribution for Python users
- high-performance implementation of security-critical paths
- long-term maintainability for a shell-oriented execution product

The stack MUST NOT optimize for:

- fastest initial prototype at the expense of security architecture
- pure-Python implementation of security-critical execution boundaries
- false portability claims that weaken the isolation model

## Required

### Language and Packaging

- **Python 3.13+**
  - Python is the public library surface.
  - Python is used for user-facing APIs, orchestration, configuration, and integration.

- **Rust core**
  - Rust is required for the execution core, policy enforcement, and security-sensitive runtime logic.
  - Security-critical components MUST NOT rely on Python as the primary enforcement boundary.

- **PyO3**
  - PyO3 is required for Python bindings to the Rust core.

- **maturin**
  - maturin is required for building and publishing the Python package with Rust extensions.

- **uv**
  - `uv` is required for project environment management, dependency installation, and developer workflows.

- **ruff**
  - `ruff` is required for Python linting and formatting.
  - The project MUST use a single canonical Python formatter and linter configuration based on `ruff`.

- **ty**
  - `ty` is required for Python type checking.
  - Python type-checking workflows MUST use `ty` as the canonical checker.

### Core Runtime Architecture

- **Hybrid architecture: Python control plane + Rust execution core**
  - Python owns the package UX.
  - Rust owns the security-sensitive engine.

- **Pluggable backend model**
  - The architecture MUST support multiple execution backends behind a stable interface.
  - At minimum, the design MUST accommodate:
    - a virtual execution backend
    - a real-bash backend behind OS isolation

- **Virtual filesystem abstraction**
  - The core runtime MUST expose a virtual filesystem abstraction independent of host filesystem APIs.

- **Linux-first host isolation for real bash**
  - If real `/bin/bash` execution is supported, it MUST run behind a real OS-level isolation boundary.
  - The initial production-grade host isolation target MUST be Linux.

- **nsjail for real-bash sandboxing**
  - `nsjail` is required as the day-1 isolation technology for any real host-shell backend.
  - The real-bash backend MUST rely on namespace, rlimit, and related kernel-enforced isolation rather than Python-only or application-only guards.

### WASM Positioning

- **WASM is required as an optional capability layer, not as the core shell architecture**
  - WASM MAY be used for isolated helper runtimes, plugins, or portable guest execution.
  - WASM MUST NOT be the primary strategy for implementing bash semantics or shell sandboxing.

- **Wasmtime is the required WASM runtime if a WASM layer is introduced**
  - If WASM guest execution is added, Wasmtime is the required engine.

### Concurrency and API Behavior

- **AnyIO**
  - AnyIO is required for async orchestration, cancellation, and timeout handling at the Python layer.

- **Structured execution API**
  - The package MUST expose structured command results and sandbox/session lifecycle operations rather than a thin subprocess wrapper.

### Parsing and Compatibility

- **tree-sitter-bash as the required parser foundation if an external parser is adopted**
  - If the project uses an external bash grammar instead of a custom parser, `tree-sitter-bash` is the required choice.
  - Shell compatibility work MUST be explicit and test-backed.

### Testing and Verification

- **pytest**
  - `pytest` is required for the main test suite.

- **Hypothesis**
  - Hypothesis is required for adversarial, property-based, and fuzz-adjacent testing of parsing, path handling, limits, and policy behavior.

- **Dedicated security and regression suites**
  - The repository MUST include distinct tests for:
    - path traversal
    - symlink escape
    - API injection resistance
    - timeout and cancellation behavior
    - resource limit enforcement
    - network policy enforcement
    - sandbox escape regressions

### Data Modeling

- **Rust-native domain models in the core**
  - Core policy, execution, and filesystem models MUST be native Rust types.

- **Typed Python-facing models**
  - Python-facing config and result objects MUST be typed and explicit.
  - Lightweight Python typing primitives are preferred unless a stronger schema layer is clearly required by the public API.

## Avoid

### Architecture

- **Pure-Python sandbox core**
  - Do not implement the execution boundary, policy engine, or path safety model as a pure-Python core.

- **Python validation as the main security boundary**
  - Do not treat Python-side path checks, argument filtering, or command sanitization as sufficient protection for untrusted shell execution.

- **Running host `/bin/bash` directly from Python without OS isolation**
  - Do not execute real bash by wrapping `subprocess`, `asyncio.create_subprocess_exec`, or similar mechanisms and calling the result a sandbox.

- **Single-backend architecture that assumes one execution model forever**
  - Do not hard-wire the system around only one backend if the product scope includes both safe virtual execution and real shell compatibility.

### WASM Misuse

- **Using WASM as the primary shell implementation strategy**
  - Do not make “bash in WASM” the foundational architecture for the product.

- **Using WASM to avoid building a real isolation story**
  - Do not treat WASM alone as a substitute for OS-level isolation when real host shell or host binaries are involved.

- **Making Python bindings to a WASM runtime the deepest mandatory dependency of the core shell product**
  - Do not center the architecture on Python-to-WASM bindings for the main shell engine.

### Sandboxing Choices

- **Bubblewrap as the primary production sandbox choice**
  - Do not make `bubblewrap` the primary day-1 isolation layer for this project.
  - It is lower-level and leaves more policy composition burden on the application.

- **Containers as the only isolation primitive**
  - Do not assume Docker or container runtime availability is a sufficient or universal day-1 library strategy.

- **Cross-platform claims for real-shell sandboxing without equivalent isolation guarantees**
  - Do not present macOS or Windows support for real bash as equivalent to the Linux isolation model unless equivalent guarantees actually exist.

### Python Stack Choices

- **Multiple overlapping Python lint and format stacks**
  - Do not adopt parallel formatter and linter stacks for the same concerns.
  - `ruff` is the required Python formatter and linter.

- **Multiple overlapping Python type-checking stacks**
  - Do not adopt parallel Python type-checking tools as co-equal project standards.
  - `ty` is the required Python type checker.

- **Making Pydantic the mandatory deep core dependency**
  - Do not make Pydantic the foundational dependency for the deepest runtime layers.
  - It is not part of the required core stack.

- **Heavy framework-first design**
  - Do not build the library around FastAPI, Django, or other service frameworks.
  - This project is a library/runtime first, not a web service first.

- **Generic filesystem abstraction libraries as the security boundary**
  - Do not outsource the core filesystem safety model to a generic abstraction library.

### Parsing Choices

- **Regex- or string-splitting-based shell parsing**
  - Do not implement shell parsing with ad hoc regex parsing or token splitting.

- **Undocumented partial compatibility**
  - Do not add shell features opportunistically without documenting semantics and limitations.

## Repository Layout

The repository SHOULD follow a Python-package-first layout with a Rust core.

Target structure:

- `python/abash/`
  - public Python package
  - session API
  - config models
  - backend selection
  - async orchestration

- `crates/core/`
  - Rust execution core
  - policy engine
  - VFS
  - path normalization
  - execution results

- `crates/backend-virtual/`
  - virtual shell backend

- `crates/backend-nsjail/`
  - Linux real-bash backend
  - nsjail integration

- `crates/backend-wasm/`
  - optional WASM-backed helper runtime integration

- `tests/`
  - Python integration tests

- `rust-tests/` or crate-local test suites
  - Rust core and backend tests

- `docs/specs/`
  - requirements
  - phases
  - tech stack

## Decision Summary

The required day-1 stack is:

- Python 3.13+
- Rust core
- PyO3
- maturin
- uv
- ruff
- ty
- AnyIO
- pytest
- Hypothesis
- tree-sitter-bash if an external parser is used
- nsjail for any real host-bash backend
- Wasmtime only for optional WASM capability layers

The architecture to avoid is:

- pure Python sandbox core
- Python-only protection around host bash
- WASM as the primary bash strategy
- bubblewrap as the primary day-1 isolation layer
- framework-first design

## Reference Basis

This decision reflects the project requirements and the constraint that untrusted shell execution requires a real security boundary.

It is also informed by:

- the packaging model supported by PyO3 and maturin
- the use of structured async orchestration in Python
- the distinction between OS sandboxing and guest-runtime sandboxing
- the fact that Wasmtime’s Python bindings are external to the main Wasmtime repository and may lag the main release cadence
