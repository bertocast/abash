# Phase 2: Workspace and Filesystem Policy

## Goal

- support realistic agent work on project files without losing containment
- make persistence and host-backed storage explicit and safe

## Required Scope

- in-memory filesystem session persistence
- host-backed read-only or copy-on-write workspace mode
- optional scoped read-write workspace mode
- explicit writable roots
- file read/write/mkdir/existence APIs
- binary-safe file transfer semantics
- documented filesystem mode matrix

## Acceptance Criteria

- filesystem modes are clearly distinguishable in configuration and behavior
- copy-on-write mode never mutates host files
- read-write mode cannot access paths outside configured writable roots
- path normalization prevents directory traversal
- host-backed access cannot escape through symlinks by default
- file APIs operate on sandbox paths without shell injection side effects
- binary-safe reads and writes preserve content fidelity
- persistence semantics across executions and sessions are documented and verified
