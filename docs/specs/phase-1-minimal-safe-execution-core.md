# Phase 1: Minimal Safe Execution Core

## Goal

- establish the zero-trust baseline
- provide a usable execution primitive for agent workflows
- ensure the isolation story is explicit from the start

## Required Scope

- structured command execution result
- argv mode execution
- script mode execution
- virtual filesystem as the default storage model
- per-execution `cwd`, `env`, and `stdin`
- isolated shell state between executions
- explicit filesystem persistence semantics
- command allowlisting
- per-execution timeout and cancellation
- core resource limits
- sanitized error handling
- threat model and known limitations documents

## Acceptance Criteria

- the default configuration has no host filesystem access
- the default configuration has no network access
- argv mode treats shell metacharacters as literal data
- script mode executes within the same policy boundaries as argv mode
- every execution returns `stdout`, `stderr`, and `exit_code`
- timeouts terminate execution and return a safe failure result
- cancellation stops further sandbox activity for the canceled execution
- resource limit violations fail safely and surface sanitized errors
- path traversal outside the sandbox root is blocked
- symlink escape is blocked or explicitly unsupported in this phase
- command allowlisting is enforced by the runtime, not caller convention
- the product documentation clearly states supported behavior and known gaps
