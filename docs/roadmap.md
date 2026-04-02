# Roadmap

Comparison baseline: `just-bash` from Vercel Labs.

This parity pass is complete. There are no active roadmap items left from the current `just-bash` comparison run.

## Command Surface

Status: complete.

- published command-name parity with the `just-bash` README is landed
- future command additions, if any, should follow upstream changes or concrete workflow demand rather than this parity plan

## Shell Language

Status: complete for the intended narrow shell layer.

- simple commands, pipes, chaining, redirects, globbing, and variable expansion are landed
- `if` / `elif`, `while`, `until`, `for`, and narrow `name() { ... }` functions with `local` are landed
- larger shell constructs such as `case`, `return`, `break`, and `continue` remain explicit compatibility limits rather than active roadmap work

## Execution Semantics

Status: decided.

- default behavior stays session-persistent
- `session_state="per_exec"` remains the opt-in reset mode for closer `just-bash` behavior
- filesystem persistence stays shared across both modes

## Command Behavior

Status: complete for the current parity target.

- `jq`, `yq`, `xan`, `awk`, `grep`, `ln`, and the broader text/file toolkit were expanded to the planned narrow-but-useful surface
- remaining behavioral differences are documented in [`docs/compatibility.md`](/Users/alberto/repos/abash/docs/compatibility.md) and [`docs/known-limitations.md`](/Users/alberto/repos/abash/docs/known-limitations.md), not tracked as active roadmap work

## JavaScript Runtime

Status: decided.

- `js-exec` intentionally uses host Node.js with workspace shims
- closer QuickJS/WASM-style isolation is not an active roadmap item in this pass

## Filesystem Model

Status: decided.

- current host-backed design stays centered on one `/workspace` mount
- additional mounts and lazy file providers are not active roadmap work for this pass
- current filesystem boundaries and limits stay documented in compatibility/limitations docs

## Extension Surface

Status: complete for the current embedding target.

- argv-mode custom command registration is landed
- top-level pre/post execution hooks are landed
- AST rewrite plugins are intentionally out of scope for now

## Network Work

Status: complete for the current parity target.

- policy-gated `curl` is the supported network surface
- broader `curl` fidelity is not active roadmap work in this pass

## Closeout

No active items remain in this roadmap. Future work should start from concrete product needs, upstream drift, or new phase docs rather than reopening this comparison checklist.
