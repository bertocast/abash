# Compatibility Statement

## Implemented

- structured `argv` execution
- narrow `script` execution
- per-execution `cwd`, `env`, `stdin`, and timeout
- command allowlisting
- sanitized error kinds
- cooperative cancellation for long-running virtual commands
- host-side file APIs for read/write/mkdir/exists
- shell-first file and text workflows via `cd`, `export`, `expr`, `time`, `timeout`, `whoami`, `hostname`, `help`, `clear`, `history`, `alias`, `unalias`, `bash`, `sh`, `env`, `which`, `dirname`, `basename`, `curl`, `tree`, `stat`, `du`, `file`, `readlink`, `ln`, `cat`, `grep`, `egrep`, `fgrep`, `wc`, `sort`, `uniq`, `head`, `tail`, `cut`, `tr`, `paste`, `sed`, `join`, `awk`, `jq`, `yq`, `sqlite3`, `find`, `ls`, `rev`, `nl`, `tac`, `strings`, `fold`, `expand`, `unexpand`, `rm`, `rmdir`, `cp`, `mv`, `tee`, `printf`, `seq`, `date`, `comm`, `diff`, `column`, `chmod`, `python`, `python3`, `js-exec`, `xan`, `xargs`, `rg`, `split`, `od`, `base64`, `md5sum`, `sha1sum`, `sha256sum`, `gzip`, `tar`, `mkdir`, and `touch`
- typed `NetworkPolicy` configuration surface
- in-process detached execution via `Bash.exec_detached()`
- one-active-run session handles with `wait`, `cancel`, `status`, and buffered output accessors
- buffered run events plus buffered session audit records
- optional Python event and audit callbacks with sanitized payloads
- filesystem session persistence for `memory` and `host_cow`
- host-backed workspace modes with one `/workspace` mount
- scoped writable roots for `host_readwrite`

## Script Compatibility Matrix

- simple commands: supported
- single and double quotes: supported
- `#` comments: supported
- pipes (`|`): supported, buffered and sequential
- redirects (`<`, `>`, `>>`, `2>`, `2>>`, `2>&1`): supported
- command chaining (`;`, `&&`, `||`): supported
- minimal `if` control flow (`if ...; then ...; fi`, optional `else`): supported
- command-local assignment prefixes (`FOO=bar cmd`): supported
- `$NAME` and `${NAME}` expansion: supported in script mode
- argument globbing with `*`, `?`, and bracket classes: supported in script mode
- literal `grep` with `-n` / `-v`: supported
- aggregate `wc` with `-l`, `-w`, `-c`: supported
- lexical line `sort` with optional `-r`: supported
- adjacent-line `uniq` with optional `-c`: supported
- `head` / `tail` with `-n`: supported
- delimiter-field `cut` with `-d` and `-f`: supported
- literal `tr` transliteration plus `-d` deletion: supported
- `paste` with optional `-d`: supported
- literal `sed` substitution `s/old/new/` with optional `g`: supported
- `join` with exactly two pre-sorted inputs plus optional `-t`, `-1`, `-2`: supported
- narrow `awk` with optional `-F`, `print`, `$0/$N`, `NF/NR/FNR`, and simple `==` / literal `~` filters: supported
- narrow `env` with optional `-i`, inline `KEY=VALUE`, and optional command exec: supported
- narrow `which` against the sandbox allowlist: supported
- `dirname` / `basename` path transforms: supported
- persistent session `cd`: supported
- persistent `export`: supported
- narrow `expr` with one binary operator or one literal value: supported
- narrow `time` and `timeout` wrappers around one nested command: supported
- narrow `whoami`, `hostname`, `help`, and `clear`: supported
- buffered per-session `history`: supported
- narrow `alias` / `unalias`: supported
- narrow `bash` / `sh` with `-c <script>` or one script path: supported, child-shell state stays isolated
- narrow `curl` with policy-gated `-X`, `-d`, `-I`, `-i`, `-o`, and `-L`: supported
- narrow `tree` with optional `-a` and `-L`: supported
- narrow `stat` for type, mode bits, and file-size-or-entry-count metadata: supported
- narrow `du` with `-a`, `-h`, `-s`, `-c`, and `--max-depth=N`: supported
- narrow `file` detection for symlink/directory/empty/text/data: supported
- narrow `readlink` with sanitized workspace-relative targets: supported
- narrow `ln` with only `-s` and filesystem-mode-dependent symlink creation: supported
- narrow `jq` with `.`, `.key`, `.[index]`, `.[start:end]`, `.[]`, `|`, `,`, and `-r` / `-c` / `-e` / `-s` / `-n`: supported
- narrow `yq` with YAML default input/output, optional `-p json`, `-o json`, and jq-lite filters: supported
- narrow `sqlite3` with `:memory:` or file-backed databases, SQL from arg or stdin, and `-json` / `-csv` / `-header`: supported
- narrow `gzip` with stdin/stdout plus `-c` / `-d` / `-k` / `-f` / `-S`: supported
- narrow `gunzip` as `gzip -d`: supported
- narrow `zcat` as `gzip -d -c`: supported
- narrow `tar` with `-c` / `-x` / `-t`, optional `-f`, `-C`, `-O`, and `-z`, plus safe extraction: supported
- narrow `chmod` with numeric or symbolic modes plus optional `-R` and `-v`: supported
- `python` alias to `python3`: supported
- narrow host-backed `python3` with `-c`, `-m`, script-file, `stdin`, and workspace file sync: supported
- narrow host-backed `js-exec` with `-c`, script-file, `stdin`, and workspace file sync: supported
- narrow `xan` CSV toolkit with `headers`, `count`, `select`, `search`, `sort`, and `filter`: supported
- `egrep` alias to narrow `grep`: supported
- `fgrep` alias to narrow `grep`: supported
- narrow `find` with path roots plus `-name`, `-type`, and `-maxdepth`: supported
- narrow `ls` with immediate listings plus optional `-a` and `-l`: supported
- narrow `rev`, `nl`, and `tac`: supported
- narrow `strings`, `fold`, `expand`, and `unexpand`: supported
- narrow `rmdir` with empty-directory removal plus optional `-p`: supported
- narrow `rm` with path deletion plus optional `-f` and `-r`: supported
- narrow `cp` with plain copies plus optional recursive directory copy via `-r`: supported
- narrow `mv` with path moves and no flag support: supported
- narrow `comm` with exactly two sorted UTF-8 inputs plus optional `-1`, `-2`, `-3`: supported
- narrow line-oriented `diff` across exactly two UTF-8 files: supported
- narrow `column` table alignment with optional `-t` and `-s`: supported
- narrow `xargs` with whitespace tokenization and optional `-n`: supported
- narrow literal recursive `rg` with optional `-n`, `-l`, `-i`: supported
- narrow `split` with only `-l`: supported
- narrow `od` hex dump: supported
- narrow `base64` with optional `-d`: supported
- `md5sum`, `sha1sum`, `sha256sum`: supported
- narrow `tee` with stdin passthrough plus optional `-a`: supported
- narrow `printf` with `%s`, `%%`, and basic escapes: supported
- narrow integer `seq` with 1, 2, or 3 arguments: supported
- narrow `date` with default local timestamp output plus limited `+FORMAT`: supported
- unmatched glob patterns: preserved literally
- persistent shell variables: not implemented
- command-name globbing: not implemented
- broader control flow (`while`, `for`, `case`): not implemented
- functions: not implemented
- subshells / command substitution: not implemented
- broader fd juggling beyond `2>`, `2>>`, `2>&1`: not implemented

## Filesystem Matrix

- `memory`: sandbox-only reads and writes, session-persistent, no host mutation
- `host_readonly`: host-backed reads under `/workspace`, writes denied, no host mutation
- `host_cow`: host-backed reads plus overlay writes, session-persistent overlay, no host mutation
- `host_readwrite`: host-backed reads and scoped writes under explicit writable roots

## Network

- network remains disabled by default
- narrow policy-gated `curl` ships on the virtual backend
- policy covers schemes, origins, optional path prefixes, methods, redirects, timeout, response size, and optional private-range blocking
- host-injected headers are configured per allowed origin and do not surface in metadata by default

## Modeled but Not Implemented

- real-shell backend execution
- multi-mount host filesystem composition

## Not Claimed

- full bash compatibility
- unrestricted host binary execution
- unrestricted host filesystem access
- unrestricted internet access
- cross-process detached-run persistence or resume
- live streaming stdout/stderr logs
- TTY emulation or job control
