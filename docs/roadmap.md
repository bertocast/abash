# Roadmap

Comparison baseline: `just-bash` from Vercel Labs.

Command-name parity is complete. The remaining work is not about adding more command names. It is about making the current surface behave more like `just-bash` where that improves real workflows.

## Priority 1: Custom Command Integration

This is the biggest remaining product difference.

Current `abash` state:

- argv-mode custom commands are supported
- top-level pre/post execution hooks are supported

Current `just-bash` shape:

- custom commands behave like real shell commands
- they compose with pipes, redirects, and shell features
- they receive richer execution context

Recommended work:

1. let custom commands participate inside script-mode pipelines and redirections
2. expose a richer execution context to custom commands
3. support nested command execution from custom commands

Why first:

- highest embedder value
- clearest remaining parity difference
- stronger product payoff than deepening one more builtin

## Priority 2: Filesystem Breadth

This is the next largest architectural difference.

Current `abash` state:

- explicit multi-mount host configuration is now supported, while legacy `workspace_root="/workspace"` remains as compat sugar
- narrow lazy file-provider hooks now exist for command-time direct reads
- current host-backed modes are still deliberate and narrow

Current `just-bash` shape:

- multi-mount composition
- lazy file providers
- more filesystem adapter surface

Recommended work:

1. broaden lazy providers beyond direct reads into directory/listing-aware adapters
2. keep current path and policy guarantees intact while broadening mount shape

Why second:

- unlocks more realistic embedding scenarios
- materially changes what can be modeled, not just syntax

## Priority 3: JavaScript Runtime Direction

This is the strongest trust-model difference.

Current `abash` state:

- `js-exec` uses host Node.js with workspace shims

Current `just-bash` shape:

- `js-exec` runs through QuickJS/WASM isolation

Recommended work:

1. decide whether host-Node remains the intended default long-term
2. if parity matters, add an isolated JavaScript runtime mode
3. if both modes exist, make the tradeoff explicit in configuration and docs

Why third:

- important architectural choice
- larger implementation cost than the first two tracks
- should be a deliberate decision, not incremental drift

## Priority 4: Execution Model Alignment

This is now mostly an API and ergonomics difference.

Current `abash` state:

- session-persistent shell state by default
- opt-in `session_state="per_exec"` for reset semantics

Current `just-bash` shape:

- per-exec shell reset by default
- more per-exec controls like `replaceEnv`, raw-script handling, and direct argv injection

Recommended work:

1. keep the current default unless product direction changes
2. consider adding narrow convenience flags comparable to `replaceEnv`
3. only revisit the default if strong user demand shows up

Why fourth:

- the major semantic choice is already explicit
- lower urgency than custom-command and filesystem work

## Priority 5: Builtin Behavior Depth

This is now the long tail.

Current `abash` state:

- broad command surface
- many commands intentionally narrow

Most useful follow-ups:

1. `awk`
2. `xan`
3. `yq`
4. `jq`
5. `curl`

Recommended rule:

- deepen commands only when a concrete workflow needs it
- avoid chasing GNU-complete behavior for its own sake

Why fifth:

- many workflows are already covered
- most remaining work here is incremental, not architectural

## Priority 6: Shell Semantics Beyond The Current Narrow Layer

Current `abash` state:

- loops and narrow functions are landed
- no `case`
- no `return`, `break`, `continue`
- no subshells or command substitution

Recommended work:

1. only expand this area if a concrete workflow is blocked
2. prefer a small, predictable shell surface over broad but brittle emulation

Why sixth:

- lower product payoff than the items above
- higher parser/runtime complexity

## Operating Rule

For the next phase of work:

1. prioritize embedder value over command-count growth
2. prefer architecture wins over one-off builtin deepening
3. keep docs honest whenever behavior stays intentionally narrow

## Suggested Sequence

If work resumes immediately, this is the best order:

1. richer custom command composition
2. multi-mount filesystem support
3. JavaScript runtime decision
4. execution-model/API refinement
5. targeted builtin deepening driven by real workflows
