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

- functions
- `local`
- loops: `for`, `until`

Recommended order:

1. `for`
2. functions and `local`
3. `until`

Rationale:

- `for` is the next smallest control-flow step after `while`
- functions and `local` should come after loop/control-flow semantics are stable

## Execution Semantics

`just-bash` resets shell state on every `exec()` call while keeping the filesystem shared.

`abash` currently persists selected session state across calls:

- working directory
- exported environment
- history
- aliases

This is a meaningful behavioral difference. It is not automatically wrong, but it should become an explicit product choice rather than accidental drift.

Current state:

1. default session-persistent semantics remain in place
2. opt-in `session_state="per_exec"` now provides a `just-bash`-style reset mode

Recommended next step:

- decide whether the long-term default should stay persistent or move closer to per-exec reset semantics before deepening functions and local-variable semantics

## Command Behavior

Many commands now exist by name in both projects, but `just-bash` is still broader in behavior.

Highest-priority work:

- `yq`: richer in-place/editing semantics beyond the current narrow file-rewrite surface
- `awk`: control-flow/runtime surface beyond the current richer regex/`printf` plus array core
- `xan`: aggregation/data-conversion subcommands beyond the current reshape plus initial `frequency`/`stats` slice

Recommended order:

1. `yq`
2. `awk`
3. `xan`

Rationale:

- `grep`, `ln`, execution reset-mode, a broader `jq` slice with direct path assignment, YAML/JSON/TOML/CSV/INI/XML/front-matter `yq`, narrow `yq -i`, a second `xan` row-shaping wave (`behead`, `cat`, `dedup`, `top`), a third `xan` aggregation wave (`frequency`, `stats`), and two `awk` lifts (`regex`/`printf`, then associative-array reads and writes) are landed
- `jq` and `yq` affect high-value agent data workflows
- `yq` now has the clearest remaining workflow payoff, `awk` still wants control-flow and array-iteration depth, and `xan` aggregation follow-ups like `groupby` or `agg` can follow after that

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

1. multi-mount filesystem composition
2. lazy file providers

Rationale:

- hard-link support is landed
- multi-mount composition is the next larger filesystem step and should be deliberate

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

1. execution-state decision
2. custom command registration
3. `jq` depth
4. multi-mount filesystem composition
