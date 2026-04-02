# Roadmap

Comparison baseline: `just-bash` from Vercel Labs.

This document turns the remaining comparison work into implementation tracks. Command-name parity is complete; the larger body of work now sits in shell language, command behavior, runtime behavior, and extension surface.

## Command Surface

Published command-name parity with the `just-bash` README is complete.

Recommended focus:

1. keep the command list in sync as upstream adds or removes names
2. spend implementation time on behavior depth instead of more names

## Shell Language

`abash` currently covers simple commands, chaining, pipes, redirections, basic variable expansion, globbing, and minimal `if`.

`just-bash` advertises a broader shell language:

- `${VAR:-default}`
- positional parameters: `$1`, `$2`, `$@`, `$#`
- `elif`
- functions
- `local`
- loops: `for`, `while`, `until`

Recommended order:

1. `${VAR:-default}`
2. positional parameters
3. `elif`
4. `while`
5. `for`
6. functions and `local`
7. `until`

Rationale:

- default expansion and positional params unlock many practical agent scripts
- `elif` and `while` raise script usefulness faster than jumping straight to functions
- functions and `local` should come after loop/control-flow semantics are stable

## Execution Semantics

`just-bash` resets shell state on every `exec()` call while keeping the filesystem shared.

`abash` currently persists selected session state across calls:

- working directory
- exported environment
- history
- aliases

This is a meaningful behavioral difference. It is not automatically wrong, but it should become an explicit product choice rather than accidental drift.

Decision track:

1. keep session-persistent semantics and document the divergence more strongly
2. add a `just-bash`-style reset mode
3. move default behavior closer to per-exec reset semantics

Recommended next step:

- define the intended long-term execution-state model before deepening functions and positional parameters

## Command Behavior

Many commands now exist by name in both projects, but `just-bash` is still broader in behavior.

Highest-priority work:

- `grep`: regex, recursive modes, richer flags
- `awk`: broader language surface
- `jq`: larger filter language and builtin coverage
- `yq`: more formats and broader transcoding surface
- `xan`: additional CSV subcommands beyond the current narrow slice

Recommended order:

1. `grep`
2. `jq`
3. `yq`
4. `xan`
5. `awk`

Rationale:

- `grep` is common and still very narrow in `abash`
- `jq` and `yq` affect high-value agent data workflows
- `xan` and `awk` are larger interpreter-style expansions and should follow clearer wins

## JavaScript Runtime

`just-bash` uses a QuickJS/WASM sandbox for `js-exec`.

`abash` currently uses host Node.js with path and filesystem shims.

This is both a behavior difference and a trust-model difference.

Workstream options:

1. keep host-Node execution and document it as an intentional runtime choice
2. add a more isolated embedded JavaScript runtime
3. support both modes behind configuration

Recommended next step:

- decide whether `abash` wants host-runtime pragmatism or closer runtime isolation semantics

## Filesystem Model

`just-bash` exposes a wider filesystem story:

- multi-mount composition
- lazy file providers
- direct compatibility-oriented filesystem adapters
- hard links in `ln`

`abash` still centers on one `/workspace` mount and only supports narrow symlink creation.

Recommended order:

1. hard links in `ln`
2. multi-mount filesystem composition
3. lazy file providers

Rationale:

- hard-link support closes a concrete command-behavior mismatch
- multi-mount composition is a larger architecture step and should be deliberate

## Extension Surface

`just-bash` exposes two major developer-facing surfaces that `abash` does not yet match:

- custom command registration
- AST transform plugins

These are not polish items. They change how embedders extend and instrument the system.

Recommended order:

1. custom command registration
2. lightweight execution hooks or transform pipeline

Rationale:

- custom commands have immediate value for embedders
- transform infrastructure should follow only if there is a clear instrumentation use case

## Network Work

`abash` already has a stronger explicit zero-trust policy story than the original comparison target in some areas, but the published surface is still narrower.

Main follow-up items:

- broader `curl` behavior if parity matters

Recommended order:

1. targeted `curl` improvements driven by real workflows

## Suggested Sequence

If the goal is to move closer to `just-bash` with high payoff and controlled scope:

1. `${VAR:-default}`
2. positional parameters
3. `elif`
4. `while`
5. `grep` depth
6. hard links in `ln`
7. execution-state decision
8. custom command registration
