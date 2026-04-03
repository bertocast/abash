# Roadmap

Comparison baseline: `just-bash` from Vercel Labs.

This comparison pass is complete. Command-name parity is done. The remaining differences are now explicit product decisions, not active roadmap work.

## Closed Decisions

### Custom Commands

- custom commands run in argv mode and inside script-mode pipelines and redirections
- request payloads already carry command argv, cwd, env, stdin, timeout, metadata, filesystem mode, and network flag
- nested sandbox execution from inside a custom command callback is not part of the current extension model
- AST rewrite plugins remain out of scope

Current product line:

- host callbacks stay small and predictable
- top-level pre/post hooks remain boundary hooks, not shell-internal rewrites

### Filesystem Shape

- explicit multi-mount host configuration is supported
- legacy `workspace_root="/workspace"` remains compatibility sugar
- lazy file providers are supported for command-time direct reads
- directory-aware provider adapters are not part of the current line

Current product line:

- path guarantees stay strict
- writable policy stays sandbox-path based
- lazy providers are intentionally narrower than real host mounts

### JavaScript Runtime

- `js-exec` stays host-Node based in the current line
- no isolated QuickJS/WASM mode is planned in this repo right now

Current product line:

- the trust model is explicit in docs
- parity with `just-bash` stops at command shape here, not runtime isolation

### Execution Model

- default shell state remains session-persistent
- `session_state="per_exec"` stays the opt-in reset mode
- `replace_env=True` stays the narrow per-call env reset control

Current product line:

- filesystem persistence is separate from shell-state persistence
- the default will not flip unless clear product demand shows up

### Builtin Depth

- builtin deepening is no longer tracked as a parity roadmap item
- future work is workflow-driven

Current product line:

- broad command surface
- intentionally narrow behavior where documented

### Shell Surface

- the current shell subset is the intended surface for now
- future shell growth is blocked-workflow driven, not parity-count driven

Current product line:

- loops and narrow functions are enough for the current target workflows
- `case`, subshells, command substitution, `return`, `break`, and `continue` stay out of scope unless a concrete need changes that call

## Working Rule

When new work resumes:

1. prioritize embedder value over parity theater
2. keep host-trust tradeoffs explicit
3. only broaden semantics when a real workflow justifies the extra complexity

## Status

No active comparison-roadmap items remain from the `just-bash` pass.
