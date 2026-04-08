# Roadmap

Fresh compare basis: `just-bash` `origin/main` at `305c833` (`2026-04-06`).

Command-name parity is still closed. The remaining work is mostly shell depth, exec/API surface, command behavior, and hardening.

## Main Differences From `just-bash`

- shell language: `just-bash` already has heredocs, `rawScript`, broader function syntax, and deeper redirection/parser coverage
- exec/API surface: `just-bash` already exposes `args`, `rawScript`, `signal`, richer execution limits, logger hooks, and trace hooks
- extension surface: `just-bash` already has AST transform plugins and a first-class transform pipeline
- builtin depth: `find`, `curl`, `awk`, `jq`, `yq`, `ls`, `printf`, and archive/compression behavior are all materially deeper upstream
- security/hardening: `just-bash` already ships a larger defense-in-depth, security-logging, and fuzzing surface
- runtime isolation: `just-bash` still has the stronger `js-exec` direction through QuickJS instead of the current host-runtime path in `abash`

## Tier 1: Shell And Exec Surface

- [ ] add heredocs and here-strings, plus the parser/runtime plumbing they need
- [ ] add `raw_script=True` so multi-line scripts can preserve leading whitespace when needed
- [ ] add argv-bypass `args=[...]` support for first-command execution without shell parsing
- [ ] expose richer execution limits beyond timeout/output size: call depth, command count, loop iterations, and heredoc/input size
- [ ] add structured logger/trace hooks on the Python API so embedders can observe execution without scraping run events
- [ ] make binary-output handling explicit at the API boundary instead of treating everything as UTF-8 text

## Tier 2: High-Value Builtin Depth

- [ ] deepen `find` toward real workflow coverage: boolean operators, more predicates, `-exec`, and `-printf`
- [ ] deepen `curl`: forms, cookies, uploads, verbose output, and `--write-out`
- [ ] deepen `awk`: user functions, array iteration, `getline`, `nextfile`, ternary, and broader `printf` formatting
- [ ] deepen `jq`: reducers, variables, richer string/date/control builtins, and broader update semantics
- [ ] deepen `yq` alongside `jq`, especially edit behavior and broader function coverage
- [ ] deepen formatting/file-inspection commands where upstream already has better agent ergonomics: `ls`, `printf`, `date`, `du`, `tree`
- [ ] evaluate bzip2 archive/compression support now that upstream has started expanding there too

## Tier 3: Extension And Instrumentation

- [ ] add an AST transform pipeline, not only top-level pre/post request hooks
- [ ] ship one or two built-in transforms first, likely command collection and tee-style output instrumentation
- [ ] let custom commands participate in richer shell-level composition without relying only on top-level hooks
- [ ] define how transform metadata should flow into `ExecutionResult`, run events, and audit events

## Tier 4: Security And Hardening

- [ ] add a configurable defense-in-depth layer for higher-risk runtimes and extension paths
- [ ] add security-violation logging/callbacks so embedders can observe blocked behavior
- [ ] broaden attack-regression coverage around custom commands, nested exec, parser edge cases, and high-risk builtins
- [ ] evaluate a public fuzzing / coverage story for the shell parser and builtin surface

## Tier 5: Runtime Direction

- [ ] re-evaluate `js-exec` isolation against the current upstream QuickJS direction
- [ ] decide whether `python3` should stay host-runtime only or gain a stronger isolation track
- [ ] keep `nsjail` real-shell work separate from this parity track unless it starts blocking the higher-value virtual-shell work above

## Deliberate Differences To Keep Unless Demand Changes

- [ ] keep session-persistent shell state as the default unless embedder feedback shifts clearly toward `per_exec`
- [ ] keep workflow-first compatibility as the product bar, not full GNU/bash fidelity
- [ ] keep explicit host mount policy as the public filesystem model rather than copying every upstream filesystem abstraction

## Notes

- `docs/known-limitations.md` remains the honest source for current behavior.
- this roadmap is about the highest-value remaining differences, not every missing flag.
- some upstream behavior is useful reference, not automatic product direction.
