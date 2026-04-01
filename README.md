# abash

`abash` is a hybrid Python/Rust project for safe, bash-oriented execution in agent workflows.

## Status

This repository is initialized through **Phase 5 v1** for workspace-aware filesystem policy, explicit network policy, in-process detached execution, and a narrow script-mode compatibility layer on the virtual backend.

- The public API is the Python package in `python/abash/`.
- The security-sensitive execution core lives in Rust.
- The default execution profile is the **virtual safe profile**.
- Host-backed filesystem access is available only through the **workspace profile**.
- Real shell execution is **Linux-only by design** and **not active in this bootstrap**.

## Trust Boundary

Python is the user-facing control plane. It is **not** the primary security boundary.

The Rust core owns canonical execution contracts, policy checks, path normalization, and backend dispatch. Any future real-shell support must remain behind OS-level isolation rather than Python-only checks.

## Current Capabilities

The current implementation provides:

- Python package packaging through `maturin`
- PyO3 bindings into the Rust core
- a virtual execution backend
- structured execution results
- explicit allowlist enforcement
- timeout and cooperative cancellation plumbing
- sanitized error propagation
- shell-first file and text workflows through `env`, `which`, `dirname`, `basename`, `tree`, `stat`, `file`, `readlink`, `ln`, `cat`, `grep`, `wc`, `sort`, `uniq`, `head`, `tail`, `cut`, `tr`, `paste`, `sed`, `join`, `awk`, `find`, `ls`, `rev`, `nl`, `tac`, `strings`, `fold`, `expand`, `unexpand`, `rm`, `rmdir`, `cp`, `mv`, `tee`, `printf`, `seq`, `date`, `comm`, `diff`, `column`, `xargs`, `rg`, `split`, `od`, `base64`, `md5sum`, `sha1sum`, `sha256sum`, `mkdir`, and `touch`
- typed network-policy configuration kept for future network commands
- host-side embedding APIs for reading, writing, creating, and checking sandbox files
- workspace-aware filesystem policy for `memory`, `host_readonly`, `host_cow`, and `host_readwrite`
- in-process detached execution through `Bash.exec_detached()`
- buffered `BashRun` inspection for status, wait, cancel, stdout/stderr/output, and retained events
- buffered session audit records plus optional event/audit callbacks
- script execution through `Bash.exec_script()` and `Bash.exec_detached_script()`
- safe shell composition for `|`, `<`, `>`, `>>`, `;`, `&&`, and `||`

## Filesystem Modes

| Mode | Read Visibility | Write Behavior | Persistence | Host Mutation |
| --- | --- | --- | --- | --- |
| `memory` | sandbox-only virtual files | read/write inside sandbox | persists for one `Bash` session | never |
| `host_readonly` | `/workspace` maps to one host directory | writes denied | reflects host state live | never |
| `host_cow` | reads host files, overlay wins when modified | writes go to sandbox overlay only | persists for one `Bash` session | never |
| `host_readwrite` | `/workspace` maps to one host directory | writes allowed only under explicit writable roots | reflects host state live | only under writable roots |

## Workspace Policy

- Host-backed modes mount exactly one host directory at `/workspace`.
- Path traversal outside the sandbox root is blocked.
- Host-backed access outside `/workspace` is blocked.
- Symlink resolution that escapes the configured workspace root is blocked by default.
- Python file helpers are embedding APIs; the intended agent workflow remains shell-first.

## Network Policy

- Network access remains disabled unless the sandbox is configured with a `NetworkPolicy`.
- No public network-capable builtin currently ships.
- Each request is checked against explicit scheme, origin, path-prefix, method, timeout, and response-size policy.
- Host-injected headers are attached outside the sandbox boundary and are not exposed through environment variables.

## Detached Execution

- `Bash.exec_detached()` starts one in-process background run owned by the current `Bash` session.
- `BashRun.wait()` returns the same structured `ExecutionResult` shape as `Bash.exec()`.
- `BashRun.events()` returns buffered lifecycle/output events after or during the run.
- `Bash.audit_events()` returns buffered sanitized audit records for session and policy activity.
- Output is buffered; there is no live streaming guarantee in Phase 4 v1.
- Only one active run is allowed per `Bash` session.

## Script Compatibility

- Script mode is intentionally partial, not bash-complete.
- Supported today: simple commands, quoting, comments, `|`, `<`, `>`, `>>`, `2>`, `2>>`, `2>&1`, `;`, `&&`, `||`, `if ...; then ...; fi`, `if ...; then ...; else ...; fi`, command-local assignment prefixes, `$NAME`, `${NAME}` expansion, and argument globbing with `*`, `?`, and bracket classes.
- Pipeline execution is buffered and sequential inside the virtual backend; it is not a streaming process graph.
- Variable expansion applies only in script mode, only for explicit request env plus command-local assignments, and does not expose host env.
- Globbing currently applies only to expanded script arguments; command names and redirection targets stay literal.
- Text builtins are intentionally narrow: `env` supports only `-i`, inline `KEY=VALUE`, and optional command execution, `which` checks only the sandbox allowlist, `dirname`/`basename` are path-string transforms, `tree` supports only `-a` and `-L`, `stat` reports only narrow type/size-or-entry metadata, `file` distinguishes only directory, symlink, empty, UTF-8 text, and data, `readlink` returns sanitized workspace targets, `ln` supports only `ln -s TARGET LINK_NAME` and only where the filesystem mode supports symlink creation, `grep` is literal line filtering, `wc` returns aggregate counts, `sort` is lexical line sorting, `uniq` deduplicates adjacent lines, `head`/`tail` support only `-n`, `cut` supports only delimiter-based field selection, `tr` supports only literal equal-length transliteration plus `-d` deletion on UTF-8 text, `paste` joins line columns with an optional single-character delimiter, `sed` supports only literal `s/old/new/` substitution with optional `g`, `join` supports only two pre-sorted inputs with optional `-t`, `-1`, and `-2`, `awk` supports only `print`, optional `-F`, `$0/$N`, `NF/NR/FNR`, and simple `==` / literal `~` filters, `find` supports only path roots plus `-name`, `-type`, and `-maxdepth`, `ls` supports only immediate listings plus optional `-a` and `-l`, `rev` reverses each input line, `nl` numbers every line, `tac` reverses line order, `strings` extracts printable ASCII runs with optional `-n`, `fold` wraps text with optional `-w`, `expand`/`unexpand` support only tabstop conversion with optional `-t` and `unexpand -a`, `rmdir` supports only empty-directory removal plus optional `-p`, `rm` supports only path deletion plus optional `-f` and `-r`, `cp` supports only plain copies plus optional recursive directory copy with `-r`, `mv` supports only path moves without flags, `comm` supports only two UTF-8 sorted inputs plus optional `-1`, `-2`, `-3`, `diff` emits a narrow line-oriented unified diff for two UTF-8 files, `column` aligns only plain text tables with optional `-t` and `-s`, `xargs` supports only whitespace tokenization plus optional `-n`, `rg` is recursive literal search with optional `-n`, `-l`, and `-i`, `split` supports only line-count splitting with `-l`, `od` emits only a narrow hex dump, `base64` supports only encode plus `-d`, `md5sum`/`sha1sum`/`sha256sum` emit standard checksum lines, `tee` supports only stdin passthrough plus optional `-a`, `printf` supports only `%s`, `%%`, and basic escapes, `seq` supports only integer sequences in 1-, 2-, or 3-argument form, and `date` supports only default local timestamp output plus narrow `+FORMAT` tokens.
- Unsupported today: global shell variables, broader control flow beyond `if ... then ... [else] ... fi`, functions, subshells, broader fd juggling beyond `2>`, `2>>`, `2>&1`, job control, and TTY semantics.

## Current Limitations

- multi-mount filesystem composition is deferred
- unrestricted network access remains unavailable by design
- detached runs are in-process only and do not survive interpreter or process restart
- output and event retrieval are buffered snapshots, not live streaming logs
- only one active run is allowed per `Bash` session
- script compatibility is intentionally narrow and not full bash parity
- the `nsjail` backend is reserved for later Linux integration and currently returns explicit unsupported errors

## Repository Layout

- `python/abash/`: public package and async orchestration
- `crates/core/`: canonical contracts, policy, path rules, session model
- `crates/backend-virtual/`: default safe backend
- `crates/backend-nsjail/`: Linux real-shell backend stub
- `docs/specs/`: normative product and architecture specs
- `tests/`: Python integration and property tests

## Phase Map

- Phase 1: minimal safe execution core
- Phase 2: workspace and filesystem policy
- Phase 3: network policy and data access
- Phase 4: agent tooling and observability
- Phase 5: compatibility expansion
- Phase 6: extended runtimes and privileged extensions

## Development

Use `uv` for Python workflows and `cargo` for Rust workflows.

Typical commands:

```bash
uv sync --group dev --python 3.13
uv run maturin develop
cargo test --workspace
uv run pytest
```
