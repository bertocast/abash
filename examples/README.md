# Examples

These examples use the current Python API exposed by `abash.Bash`.

The main examples are `.exec()`-driven and show the shell-first workflow the project is aiming for.

## Setup

From the repository root:

```bash
uv sync --group dev --python 3.13
uv run maturin develop
```

## Run

Run any example with:

```bash
uv run python examples/basic_memory.py
uv run python examples/workspace_host_cow.py
uv run python examples/workspace_host_readwrite.py
uv run python examples/file_helpers.py
uv run python examples/detached_run.py
uv run python examples/script_mode.py
```

## Notes

- The main examples stay shell-first; `script_mode.py` shows the current script subset when composition matters.
- Host-backed examples mount the current repository at `/workspace`.
- `host_cow` never mutates the host filesystem.
- `host_readwrite` only writes inside explicitly configured writable roots.
- `file_helpers.py` is intentionally secondary; the project direction is shell-first through `.exec()`.
- `detached_run.py` shows the Phase 4 `Bash.exec_detached()` handle and buffered events.
- `script_mode.py` shows the current script-mode subset with redirects, pipes, and chaining.
