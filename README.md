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
- shell-first file and text workflows through a narrow builtin command set
- typed network-policy configuration plus policy-gated network execution for `curl`
- host-side embedding APIs for reading, writing, creating, and checking sandbox files
- workspace-aware filesystem policy for `memory`, `host_readonly`, `host_cow`, and `host_readwrite`
- in-process detached execution through `Bash.exec_detached()`
- buffered `BashRun` inspection for status, wait, cancel, stdout/stderr/output, and retained events
- buffered session audit records plus optional event/audit callbacks
- argv-mode custom command registration plus top-level pre/post execution hooks
- script execution through `Bash.exec_script()` and `Bash.exec_detached_script()`, including optional script `argv`
- safe shell composition for `|`, `<`, `>`, `>>`, `;`, `&&`, and `||`

## Supported Commands

Implemented today on the virtual backend:

- shell/env: `cd`, `export`, `alias`, `unalias`, `history`, `help`, `clear`, `whoami`, `hostname`, `bash`, `sh`, `env`, `which`, `dirname`, `basename`, `expr`, `time`, `timeout`, `printf`, `seq`, `date`
- file/text: `cat`, `grep`, `wc`, `sort`, `uniq`, `head`, `tail`, `cut`, `tr`, `paste`, `sed`, `join`, `awk`, `jq`, `yq`, `sqlite3`, `find`, `ls`, `tree`, `stat`, `du`, `file`, `readlink`, `html-to-markdown`
- workspace/filesystem mutation: `ln`, `rm`, `rmdir`, `cp`, `mv`, `mkdir`, `touch`, `tee`, `gzip`
- network/data: `curl`
- inspection/formatting: `rev`, `nl`, `tac`, `strings`, `fold`, `expand`, `unexpand`, `column`, `comm`, `diff`, `rg`, `split`, `od`
- encoding/checksums: `base64`, `md5sum`, `sha1sum`, `sha256sum`

Most commands are intentionally partial implementations. `abash` aims for safe, useful workflows first, not full GNU/bash parity.
Recent behavior lifts: `grep` now supports regex search plus narrow `-E`, `-F`, `-i`, `-n`, `-v`, `-c`, `-l`, and `-r`; `ln` now creates hard links by default and still supports `-s` for symlinks; `jq` now supports literals, binary ops, `select`, `map`, `length`, `type`, `keys`, `has`, array/object construction, and direct path assignment; `yq` now carries that jq-lite surface across YAML/JSON/TOML/CSV/INI/XML, plus front-matter extraction and broader `-i` rewrites that preserve source format across multiple files; `xan` now covers row/column operations like `head`, `tail`, `slice`, `reverse`, `behead`, `cat`, `drop`, `rename`, `enum`, `dedup`, `top`, `frequency`, `stats`, `agg`, and narrow `groupby`; `awk` now supports `BEGIN`/`END`, `-v`, variables, scalar and array assignments, statement-level `if/else`, `delete`, `next`, arithmetic, regex literals, `printf`, and basic comparisons.

Command-name parity history is tracked in [docs/pending_commands.md](docs/pending_commands.md). The closed comparison pass and its final decisions are tracked in [docs/roadmap.md](docs/roadmap.md).

## Filesystem Modes

| Mode | Read Visibility | Write Behavior | Persistence | Host Mutation |
| --- | --- | --- | --- | --- |
| `memory` | sandbox-only virtual files | read/write inside sandbox | persists for one `Bash` session | never |
| `host_readonly` | one or more sandbox mount points map to host directories | writes denied | reflects host state live | never |
| `host_cow` | reads host files across configured mounts, overlay wins when modified | writes go to sandbox overlay only | persists for one `Bash` session | never |
| `host_readwrite` | one or more sandbox mount points map to host directories | writes allowed only under explicit writable roots | reflects host state live | only under writable roots |

## Workspace Policy

- Host-backed modes can use legacy `workspace_root="/workspace"` or explicit `host_mounts=[HostMount(...)]`.
- `lazy_file_providers={"/mount": callback}` can materialize file bytes on demand during command execution.
- Writable roots still use sandbox paths, so multi-mount write policy stays explicit.
- Path traversal outside the sandbox root is blocked.
- Host-backed access outside configured mount paths is blocked.
- Symlink resolution that escapes the configured host mount is blocked by default.
- Python file helpers are embedding APIs; the intended agent workflow remains shell-first.

## Network Policy

- Network access remains disabled unless the sandbox is configured with a `NetworkPolicy`.
- `curl` is the public network-capable builtin on the virtual backend.
- Each request is checked against explicit scheme, origin, path-prefix, method, timeout, and response-size policy.
- Host-injected headers are attached outside the sandbox boundary and are not exposed through environment variables.

## Detached Execution

- `Bash.exec_detached()` starts one in-process background run owned by the current `Bash` session.
- `BashRun.wait()` returns the same structured `ExecutionResult` shape as `Bash.exec()`.
- `BashRun.events()` returns buffered lifecycle/output events after or during the run.
- `Bash.audit_events()` returns buffered sanitized audit records for session and policy activity.
- Output is buffered; there is no live streaming guarantee in Phase 4 v1.
- Only one active run is allowed per `Bash` session.
- `Bash(session_state="per_exec")` opts into `just-bash`-style shell-state reset between calls while keeping the filesystem shared.

## Extension Surface

- `Bash(custom_commands={...})` registers host-side commands that can run in argv mode or inside script-mode pipelines and redirections.
- `pre_exec_hook` can rewrite top-level requests before dispatch.
- `post_exec_hook` can observe or replace top-level results after dispatch.
- Hooks remain intentionally narrow; they operate on the top-level request/result boundary, not on the internal script AST.

## Script Compatibility

- Script mode is intentionally partial, not bash-complete.
- Supported today: simple commands, quoting, comments, `|`, `<`, `>`, `>>`, `2>`, `2>>`, `2>&1`, `;`, `&&`, `||`, `if ...; then ...; fi`, `if ...; then ...; elif ...; then ...; else ...; fi`, `while ...; do ...; done`, `until ...; do ...; done`, `for ...; do ...; done`, narrow `name() { ...; }` functions, narrow `local`, command-local assignment prefixes, `$NAME`, `${NAME}`, `${NAME:-default}`, `$1`, `$2`, `$@`, `$#` expansion, and argument globbing with `*`, `?`, and bracket classes.
- Pipeline execution is buffered and sequential inside the virtual backend; it is not a streaming process graph.
- Variable expansion applies only in script mode, only for explicit request env plus command-local assignments, and does not expose host env.
- Globbing currently applies only to expanded script arguments; command names and redirection targets stay literal.
- Command behavior is intentionally narrow even when a command name exists. Examples: `env` supports only `-i` plus inline assignments, `tree` only `-a` and `-L`, `sed` only literal `s/old/new/` with optional `g`, `find` only `-name`, `-type`, and `-maxdepth`, `ls` only `-a` and `-l`, `rm` only `-f` and `-r`, `rg` only `-n`, `-l`, and `-i`, `html-to-markdown` only supports file-or-stdin conversion plus `--bullet`, `--code`, `--hr`, and `--heading-style`, `base64` only encode plus `-d`, and `date` only default output plus narrow `+FORMAT` tokens.
- Unsupported today: global shell variables, `case`, subshells, broader fd juggling beyond `2>`, `2>>`, `2>&1`, job control, `return`/`break`/`continue`, and TTY semantics.

## Current Limitations

- multi-mount filesystem composition is not part of the current product line
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
- `docs/`: project docs and command coverage notes
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
