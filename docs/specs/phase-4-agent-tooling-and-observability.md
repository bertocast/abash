# Phase 4: Agent Tooling and Observability

## Goal

- turn the execution core into a robust agent tool surface
- make runs inspectable, debuggable, and operationally manageable

## Required Scope

- sandbox or session lifecycle API
- file management API
- detached or asynchronous execution support
- wait, stop, and output retrieval operations
- structured logging
- trace or profiling hooks
- execution metadata support
- policy and security audit events where applicable

## Acceptance Criteria

- an embedding application can create, use, and dispose of a sandbox through documented APIs
- detached execution can be awaited, canceled, and inspected safely
- output retrieval behaves consistently for completed and canceled commands
- logs and trace hooks do not require exposing host internals
- metadata and observability features do not weaken security boundaries
- API behavior is documented in terms of persistence, isolation, and failure semantics
