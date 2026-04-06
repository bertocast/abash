# Roadmap

Command-name parity with `just-bash` is done. The remaining work is mostly behavior depth, runtime shape, backend maturity.

## Already In `just-bash`

- richer custom commands with first-class shell composition
- nested command execution from custom-command context
- AST transform/plugin surface
- multi-mount filesystem as a first-class public model
- lazy files participating in direct reads and directory listings
- broader shell control flow: `case`, `return`, `break`, `continue`
- command substitution and subshell execution
- deeper builtin behavior across `awk`, `jq`, `yq`, `xan`, `curl`, `sqlite3`, `tar`, `python3`
- per-exec shell reset as the default model
- stronger JavaScript isolation direction through QuickJS/WASM

## Partial In `just-bash`

- detached command handles exist, but logs are still buffered after completion rather than truly live
- sandbox command APIs cover `wait`, `kill`, `stdout`, `stderr`, `output`, `logs`, but not a session-owned retained event/audit model
- network tooling is broader, but still policy-driven rather than a full unrestricted runtime
- shell breadth is much higher, but still not complete GNU/bash fidelity

## `abash`-Specific Next Work

### Tier 1: Runtime And Embedding

- [x] add live stdout/stderr/event streaming instead of buffered snapshots only
- [x] broaden detached execution beyond one active run per `Bash` session
- [x] retain a stronger session-owned event/audit model as runs grow more capable
- [x] deepen custom-command context with explicit supported runtime metadata
- [x] let custom commands invoke nested sandbox work through a narrow stable helper surface

### Tier 2: Filesystem And Providers

- [x] make listing-capable lazy file providers visible to directory-oriented operations too: `find`, `ls`, `tree`, Python file helpers
- [x] keep broader mount adapter types out of the main product line for now; explicit `host_mounts=[HostMount(...)]` stays the supported host model
- [x] keep `host_cow` delete semantics non-whiteout; deleting host-backed paths remains unsupported by design

### Tier 3: Shell And Builtins

- [x] add `case`
- [x] add `return` inside script functions
- [x] add `break`
- [x] add `continue`
- [x] evaluate command substitution
- [x] evaluate subshell execution
- [x] deepen `awk`, `jq`, `yq`, `xan`, `curl`, `sqlite3`, `tar`, `python3`, `js-exec` where narrow behavior still blocks real workflows

### Tier 4: Backend Maturity

- [x] activate the Linux real-shell backend behind the intended isolation model
- [x] define the long-term story for `nsjail` vs any alternative Linux sandbox strategy
- [x] decide whether a stronger JavaScript isolation mode is worth adding alongside host-runtime `js-exec`

### Tier 5: Product Defaults To Revisit Only With Demand

- [x] re-evaluate the default session model if most embedders prefer `per_exec` over persistent shell state
- [x] re-evaluate AST/plugin rewrite hooks if custom-command demand grows past the current top-level hooks
- [x] re-evaluate full GNU/bash fidelity only when narrow behavior keeps blocking real workflows

## Notes

- `docs/known-limitations.md` remains the honest source for current behavior.
- this roadmap tracks the next valuable lifts, not every missing flag
- `just-bash` is a useful reference point, but not every upstream choice should become a default in `abash`
- Tier 4 decision: Linux real-shell work stays centered on `nsjail` for now; no second Linux sandbox backend is planned until the current path blocks a real deployment need.
- Tier 4 decision: `js-exec` stays host-runtime based for now; a stronger isolated JavaScript runtime is deferred unless threat-model or embedding demand changes materially.
- Tier 5 decision: session-persistent shell state stays the default; `session_state="per_exec"` remains the explicit opt-in for embedders that want reset-on-each-call semantics.
- Tier 5 decision: top-level hooks plus current custom-command composition remain the supported extension model; AST/plugin rewrite hooks are deferred until real embedder demand appears.
- Tier 5 decision: `abash` stays workflow-first rather than chasing full GNU/bash fidelity; shell and builtin depth should continue to grow only where narrow behavior blocks real use cases.
