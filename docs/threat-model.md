# Threat Model

## Primary Threats

- untrusted shell input authored by an LLM agent or end user
- malicious argv values, environment variables, file paths, and stdin
- denial-of-service attempts through output growth, recursion, or long-running commands
- information disclosure through unsanitized errors or host-backed execution shortcuts

## Trusted Components

- the embedding application
- the Rust core
- the Python package surface
- explicit host-side policy configuration

## Explicitly Untrusted Inputs

- command names and arguments
- script strings
- per-execution environment variables
- sandbox paths
- command stdin
- future remote data and optional runtime outputs

## Filesystem Guarantees

- default profile is virtual and host-isolated by construction
- network access is denied by default
- host filesystem access is available only through explicit workspace policy
- host-backed access is restricted to one configured workspace root mounted at `/workspace`
- writable host access is restricted to explicit writable sandbox roots
- symlink-based escape outside the workspace root is blocked by default
- backend selection is explicit
- unsupported capabilities fail with sanitized, typed errors

## Residual Risks

- bootstrap compatibility is intentionally narrow and not a full shell
- script mode is a constrained interpreter layer, not a pass-through to host bash
- copy-on-write overlay does not yet model deletion semantics
- detached execution exists only as an in-process, single-active-run handle model with buffered observability
- Linux real-shell execution now exists only through a narrow `nsjail` argv path; broader shell/script/network semantics still must not be inferred from crate presence alone
