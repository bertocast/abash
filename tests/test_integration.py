from __future__ import annotations

import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import tarfile
import threading

import anyio
import pytest

from abash import (
    AuditEvent,
    Bash,
    RunEvent,
    ErrorKind,
    ExecutionProfile,
    ExecutionResult,
    FilesystemMode,
    NetworkOrigin,
    NetworkPolicy,
    RunStatus,
    SessionState,
)


class _CurlTestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(head_only=True)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def _handle(self, head_only: bool = False) -> None:
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/hello")
            self.end_headers()
            return

        if self.path == "/hello":
            body = b"hello from curl\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head_only:
                self.wfile.write(body)
            return

        if self.path == "/echo":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            self.server.last_body = body  # type: ignore[attr-defined]
            self.server.last_header = self.headers.get("X-Injected", "")  # type: ignore[attr-defined]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head_only:
                self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()


class _CurlServer:
    def __enter__(self) -> "_CurlServer":
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _CurlTestHandler)
        self.httpd.last_body = b""  # type: ignore[attr-defined]
        self.httpd.last_header = ""  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.httpd.server_port}"
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=1)


@pytest.mark.anyio
async def test_allowed_command_round_trip() -> None:
    async with Bash() as bash:
        result = await bash.exec(["echo", "hello", "world"], env={"DEMO": "1"})

    assert result.exit_code == 0
    assert result.stdout == "hello world\n"
    assert result.error is None
    assert result.metadata["backend"] == "virtual"


@pytest.mark.anyio
async def test_argv_metacharacters_are_literal() -> None:
    async with Bash() as bash:
        result = await bash.exec(["echo", ";", "&&", "$(uname -a)"])

    assert result.exit_code == 0
    assert result.stdout == "; && $(uname -a)\n"


@pytest.mark.anyio
async def test_script_mode_runs_simple_commands_and_reports_metadata() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("echo hello; echo world")

    assert result.exit_code == 0
    assert result.stdout == "hello\nworld\n"
    assert result.metadata["mode"] == "script"
    assert result.metadata["commands_executed"] == "2"
    assert result.metadata["last_command"] == "echo"


@pytest.mark.anyio
async def test_script_mode_supports_pipes_and_redirections() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "echo hello > /workspace/demo.txt; cat < /workspace/demo.txt | cat"
        )

    assert result.exit_code == 0
    assert result.stdout == "hello\n"


@pytest.mark.anyio
async def test_script_mode_supports_append_and_short_circuiting() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "echo one > /workspace/log.txt; false && echo skip; false || echo two >> /workspace/log.txt; cat /workspace/log.txt"
        )

    assert result.exit_code == 0
    assert result.stdout == "one\ntwo\n"


@pytest.mark.anyio
async def test_script_mode_supports_stderr_redirect_truncate_and_append() -> None:
    async with Bash() as bash:
        first = await bash.exec_script("missing 2> /workspace/err.txt")
        first_err = await bash.read_file("/workspace/err.txt")
        second = await bash.exec_script("missing 2>> /workspace/err.txt")
        combined = await bash.read_file("/workspace/err.txt")

    assert first.error is not None
    assert first.error.kind is ErrorKind.POLICY_DENIED
    assert first.stderr == ""
    assert first_err == "command is not allowlisted: missing"
    assert second.error is not None
    assert second.error.kind is ErrorKind.POLICY_DENIED
    assert second.stderr == ""
    assert combined == ("command is not allowlisted: missingcommand is not allowlisted: missing")


@pytest.mark.anyio
async def test_script_mode_supports_stderr_to_stdout_redirection() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("missing 2>&1")

    assert result.error is not None
    assert result.error.kind is ErrorKind.POLICY_DENIED
    assert result.stdout == "command is not allowlisted: missing"
    assert result.stderr == ""


@pytest.mark.anyio
async def test_script_mode_supports_if_then_fi() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("if true; then echo yes; fi")

    assert result.exit_code == 0
    assert result.stdout == "yes\n"


@pytest.mark.anyio
async def test_script_mode_supports_if_then_else_fi() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("if false; then echo no; else echo yes; fi")

    assert result.exit_code == 0
    assert result.stdout == "yes\n"


@pytest.mark.anyio
async def test_script_mode_nested_if_blocks_chain_like_commands() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "if true; then if false; then echo no; else echo inner; fi; fi && echo done"
        )

    assert result.exit_code == 0
    assert result.stdout == "inner\ndone\n"


@pytest.mark.anyio
async def test_script_mode_if_without_else_succeeds_when_condition_is_false() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("if false; then echo no; fi && echo done")

    assert result.exit_code == 0
    assert result.stdout == "done\n"


@pytest.mark.anyio
async def test_script_mode_supports_if_elif_else_fi() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "if false; then echo no; elif true; then echo yes; else echo later; fi"
        )

    assert result.exit_code == 0
    assert result.stdout == "yes\n"


@pytest.mark.anyio
async def test_script_mode_supports_while_do_done() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/loop", parents=True)
        await bash.write_file("/workspace/loop/run.txt", "run")
        result = await bash.exec_script(
            "while find /workspace/loop -name run.txt | grep run.txt; do echo tick; rm /workspace/loop/run.txt; done"
        )
        remaining = await bash.exists("/workspace/loop/run.txt")

    assert result.exit_code == 0
    assert result.stdout == "/workspace/loop/run.txt\ntick\n"
    assert remaining is False


@pytest.mark.anyio
async def test_argv_mode_text_builtins_support_stdin_and_exit_codes() -> None:
    async with Bash() as bash:
        grep_hit = await bash.exec(["grep", "beta"], stdin="alpha\nbeta\n")
        grep_miss = await bash.exec(["grep", "gamma"], stdin="alpha\nbeta\n")
        egrep_hit = await bash.exec(["egrep", "beta"], stdin="alpha\nbeta\n")
        fgrep_hit = await bash.exec(["fgrep", "beta"], stdin="alpha\nbeta\n")
        wc_result = await bash.exec(["wc", "-l", "-w", "-c"], stdin="one two\nthree\n")

    assert grep_hit.exit_code == 0
    assert grep_hit.stdout == "beta\n"
    assert grep_miss.exit_code == 1
    assert grep_miss.stdout == ""
    assert egrep_hit.exit_code == 0
    assert egrep_hit.stdout == "beta\n"
    assert fgrep_hit.exit_code == 0
    assert fgrep_hit.stdout == "beta\n"
    assert wc_result.stdout == "2 3 14\n"


@pytest.mark.anyio
async def test_argv_mode_grep_supports_regex_fixed_case_and_count() -> None:
    async with Bash() as bash:
        regex_result = await bash.exec(["grep", "-in", "be.a"], stdin="Alpha\nBETA\nbeta\n")
        fixed_result = await bash.exec(["fgrep", "-i", "be.a"], stdin="be.a\nbeta\n")
        count_result = await bash.exec(
            ["grep", "-c", "warn"],
            stdin="warn\nok\nwarn\n",
        )

    assert regex_result.exit_code == 0
    assert regex_result.stdout == "2:BETA\n3:beta\n"
    assert fixed_result.exit_code == 0
    assert fixed_result.stdout == "be.a\n"
    assert count_result.exit_code == 0
    assert count_result.stdout == "2\n"


@pytest.mark.anyio
async def test_argv_mode_grep_supports_recursive_search_and_file_reporting() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/src/nested", parents=True)
        await bash.write_file("/workspace/src/app.txt", "alpha\nbeta\n")
        await bash.write_file("/workspace/src/nested/lib.txt", "beta\n")
        await bash.write_file("/workspace/src/nested/skip.txt", "gamma\n")

        recursive_result = await bash.exec(["grep", "-rn", "be.a", "/workspace/src"])
        listed_result = await bash.exec(["grep", "-rl", "beta", "/workspace/src"])
        counted_result = await bash.exec(
            ["grep", "-rc", "beta", "/workspace/src/app.txt", "/workspace/src/nested/lib.txt"],
        )

    assert recursive_result.exit_code == 0
    assert recursive_result.stdout == (
        "/workspace/src/app.txt:2:beta\n"
        "/workspace/src/nested/lib.txt:1:beta\n"
    )
    assert listed_result.exit_code == 0
    assert listed_result.stdout == "/workspace/src/app.txt\n/workspace/src/nested/lib.txt\n"
    assert counted_result.exit_code == 0
    assert counted_result.stdout == "/workspace/src/app.txt:1\n/workspace/src/nested/lib.txt:1\n"


@pytest.mark.anyio
async def test_script_mode_text_builtins_support_pipelines_and_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/text.txt", "pear\napple\npear\nbanana\n")
        result = await bash.exec_script("cat /workspace/text.txt | grep a | sort -r | uniq | wc -l")

    assert result.exit_code == 0
    assert result.stdout == "3\n"


@pytest.mark.anyio
async def test_script_mode_uniq_count_and_grep_numbered_output() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/log.txt", "ok\nok\nwarn\nwarn\nwarn\n")
        result = await bash.exec_script(
            "cat /workspace/log.txt | uniq -c > /workspace/uniq.txt; grep -n warn /workspace/uniq.txt"
        )

    assert result.exit_code == 0
    assert result.stdout == "2:3 warn\n"


@pytest.mark.anyio
async def test_script_mode_grep_works_inside_if_control_flow() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "alpha\nbeta\n")
        result = await bash.exec_script(
            "if grep beta /workspace/demo.txt; then echo hit; else echo miss; fi"
        )

    assert result.exit_code == 0
    assert result.stdout == "beta\nhit\n"


@pytest.mark.anyio
async def test_script_mode_grep_keeps_pattern_args_literal_before_file_globbing() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/patterns.txt", "*.rs\nmain.rs\n")
        result = await bash.exec_script("grep -F '*.rs' /workspace/patterns.txt")

    assert result.exit_code == 0
    assert result.stdout == "*.rs\n"


@pytest.mark.anyio
async def test_argv_mode_head_tail_and_cut_support_stdin() -> None:
    async with Bash() as bash:
        head_result = await bash.exec(["head", "-n", "2"], stdin="a\nb\nc\nd\n")
        tail_result = await bash.exec(["tail", "-n", "2"], stdin="a\nb\nc\nd\n")
        cut_result = await bash.exec(
            ["cut", "-d", ",", "-f", "2,3"],
            stdin="name,role,team\nbert,eng,core\n",
        )

    assert head_result.stdout == "a\nb\n"
    assert tail_result.stdout == "c\nd\n"
    assert cut_result.stdout == "role,team\neng,core\n"


@pytest.mark.anyio
async def test_script_mode_head_tail_and_cut_work_in_pipelines() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/people.csv",
            "name,role,team\nbert,eng,core\nana,pm,product\nleo,eng,infra\n",
        )
        result = await bash.exec_script(
            "cat /workspace/people.csv | tail -n 2 | cut -d , -f 1,3 | head -n 1"
        )

    assert result.exit_code == 0
    assert result.stdout == "ana,product\n"


@pytest.mark.anyio
async def test_argv_mode_tr_supports_translation_and_delete() -> None:
    async with Bash() as bash:
        translated = await bash.exec(["tr", "abc", "xyz"], stdin="cab\n")
        deleted = await bash.exec(["tr", "-d", "aeiou"], stdin="hello world\n")

    assert translated.stdout == "zxy\n"
    assert deleted.stdout == "hll wrld\n"


@pytest.mark.anyio
async def test_script_mode_tr_works_in_text_pipelines() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/raw.txt", "bert,core\nana,product\n")
        result = await bash.exec_script(
            "cat /workspace/raw.txt | tr , : | cut -d : -f 2 | head -n 1"
        )

    assert result.exit_code == 0
    assert result.stdout == "core\n"


@pytest.mark.anyio
async def test_argv_mode_paste_and_sed_support_text_transforms() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/left.txt", "bert\nana\n")
        await bash.write_file("/workspace/right.txt", "core\nproduct\n")
        pasted = await bash.exec(
            ["paste", "-d", ",", "/workspace/left.txt", "/workspace/right.txt"]
        )
        sed_result = await bash.exec(["sed", "s/o/O/g"], stdin="core\nproduct\n")

    assert pasted.stdout == "bert,core\nana,product\n"
    assert sed_result.stdout == "cOre\nprOduct\n"


@pytest.mark.anyio
async def test_script_mode_paste_and_sed_work_in_pipelines() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/names.txt", "bert\nana\n")
        await bash.write_file("/workspace/teams.txt", "core\nproduct\n")
        result = await bash.exec_script(
            "paste -d , /workspace/names.txt /workspace/teams.txt | sed s/product/growth/ | head -n 1"
        )

    assert result.exit_code == 0
    assert result.stdout == "bert,core\n"


@pytest.mark.anyio
async def test_argv_mode_join_supports_default_and_custom_fields() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/people.txt", "1 bert\n2 ana\n")
        await bash.write_file("/workspace/teams.txt", "1 core\n2 product\n")
        await bash.write_file("/workspace/names.csv", "bert,1\nana,2\n")
        await bash.write_file("/workspace/orgs.csv", "core,1\ngrowth,2\n")
        default_join = await bash.exec(["join", "/workspace/people.txt", "/workspace/teams.txt"])
        field_join = await bash.exec(
            [
                "join",
                "-t",
                ",",
                "-1",
                "2",
                "-2",
                "2",
                "/workspace/names.csv",
                "/workspace/orgs.csv",
            ]
        )

    assert default_join.stdout == "1 bert core\n2 ana product\n"
    assert field_join.stdout == "1,bert,core\n2,ana,growth\n"


@pytest.mark.anyio
async def test_script_mode_join_works_in_text_pipelines() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/names.csv", "bert,1\nana,2\n")
        await bash.write_file("/workspace/orgs.csv", "core,1\ngrowth,2\n")
        result = await bash.exec_script(
            "join -t , -1 2 -2 2 /workspace/names.csv /workspace/orgs.csv | sed s/growth/product/ | tail -n 1"
        )

    assert result.exit_code == 0
    assert result.stdout == "2,ana,product\n"


@pytest.mark.anyio
async def test_argv_mode_awk_supports_field_filters_and_counters() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/people_a.csv", "bert,core\nana,product\n")
        await bash.write_file("/workspace/people_b.csv", "cami,core\n")
        result = await bash.exec(
            [
                "awk",
                "-F",
                ",",
                '$2 == "core" { print $1, NR, FNR, NF }',
                "/workspace/people_a.csv",
                "/workspace/people_b.csv",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout == "bert 1 1 2\ncami 3 1 2\n"


@pytest.mark.anyio
async def test_script_mode_awk_works_in_text_pipelines() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/people.csv", "bert,core\nana,product\n")
        result = await bash.exec_script(
            """cat /workspace/people.csv | awk -F , '$2 == "product" { print $1 }' | head -n 1"""
        )

    assert result.exit_code == 0
    assert result.stdout == "ana\n"


@pytest.mark.anyio
async def test_argv_mode_awk_supports_begin_end_vars_and_accumulators() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/sales.csv", "bert,core,2\nana,product,9\ncami,core,3\n")
        result = await bash.exec(
            [
                "awk",
                "-F",
                ",",
                "-v",
                "greeting=hello",
                'BEGIN { total = 0; print greeting } $2 == "core" { total += $3 } END { print total, FILENAME }',
                "/workspace/sales.csv",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout == "hello\n5 /workspace/sales.csv\n"


@pytest.mark.anyio
async def test_argv_mode_awk_supports_regex_literals_and_printf() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/people.csv", "bert,core,2\nana,product,9\n")
        result = await bash.exec(
            [
                "awk",
                "-F",
                ",",
                '$2 ~ /core/ { printf "%s:%d", $1, $3 }',
                "/workspace/people.csv",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout == "bert:2"


@pytest.mark.anyio
async def test_argv_mode_awk_supports_array_accumulators() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/sales.csv", "bert,core,2\nana,product,9\ncami,core,3\n")
        result = await bash.exec(
            [
                "awk",
                "-F",
                ",",
                '{ totals[$2] += $3 } END { print totals["core"], totals["product"] }',
                "/workspace/sales.csv",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout == "5 9\n"


@pytest.mark.anyio
async def test_argv_mode_jq_supports_paths_slices_and_raw_output() -> None:
    async with Bash() as bash:
        result = await bash.exec(
            ["jq", "-r", ".items[] | .name, .tags[-2:]", "/workspace/data.json"]
        )

    assert result.error is not None
    assert result.error.kind is ErrorKind.INVALID_REQUEST

    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.json",
            '{"items":[{"name":"bert","tags":["ops","ml","core"]}]}',
        )
        result = await bash.exec(
            ["jq", "-r", ".items[] | .name, .tags[-2:]", "/workspace/data.json"]
        )

    assert result.exit_code == 0
    assert result.stdout == 'bert\n[\n  "ml",\n  "core"\n]\n'


@pytest.mark.anyio
async def test_script_mode_jq_works_in_pipelines_without_globbing_filter() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            """echo '{"items":[{"name":"bert"},{"name":"ana"}]}' | jq -r '.items[] | .name' | tail -n 1"""
        )

    assert result.exit_code == 0
    assert result.stdout == "ana\n"


@pytest.mark.anyio
async def test_argv_mode_jq_supports_compact_slurp_and_exit_status() -> None:
    async with Bash() as bash:
        compact = await bash.exec(["jq", "-c", "-s", ".", "/workspace/stream.json"])

    assert compact.error is not None
    assert compact.error.kind is ErrorKind.INVALID_REQUEST

    async with Bash() as bash:
        await bash.write_file("/workspace/stream.json", '{"a":1}\n{"b":2}\n')
        compact = await bash.exec(["jq", "-c", "-s", ".", "/workspace/stream.json"])
        status = await bash.exec(["jq", "-e", ".missing"], stdin='{"a":1}')

    assert compact.exit_code == 0
    assert compact.stdout == '[{"a":1},{"b":2}]\n'
    assert status.exit_code == 1
    assert status.stdout == "null\n"


@pytest.mark.anyio
async def test_argv_mode_jq_supports_construction_builtins_and_operators() -> None:
    async with Bash() as bash:
        builtins = await bash.exec(
            ["jq", "-c", "{keys: keys, len: (.items | length), ok: has(\"name\")}"],
            stdin='{"name":"bert","items":[1,2,3],"z":1}\n',
        )
        mapping = await bash.exec(
            ["jq", "-c", "[.[] | select(. > 2)] | map(. * 10)"],
            stdin="[1,2,3,4]\n",
        )
        fallback = await bash.exec(
            ["jq", "-r", ".missing // \"fallback\""],
            stdin='{"name":"bert"}\n',
        )

    assert builtins.exit_code == 0
    assert builtins.stdout == '{"keys":["items","name","z"],"len":3,"ok":true}\n'
    assert mapping.stdout == "[30.0,40.0]\n"
    assert fallback.stdout == "fallback\n"


@pytest.mark.anyio
async def test_argv_mode_yq_reads_yaml_and_can_emit_json() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.yaml",
            "config:\n  host: localhost\n  port: 5432\nitems:\n  - name: bert\n  - name: ana\n",
        )
        host = await bash.exec(["yq", ".config.host", "/workspace/data.yaml"])
        config = await bash.exec(["yq", "-o", "json", "-c", ".config", "/workspace/data.yaml"])

    assert host.exit_code == 0
    assert host.stdout == "localhost\n"
    assert config.stdout == '{"host":"localhost","port":5432}\n'


@pytest.mark.anyio
async def test_argv_mode_yq_inherits_expanded_jq_filter_surface() -> None:
    async with Bash() as bash:
        result = await bash.exec(
            ["yq", "-o", "json", "-c", "{name, count: (.items | length)}"],
            stdin="name: bert\nitems:\n  - a\n  - b\n",
        )

    assert result.exit_code == 0
    assert result.stdout == '{"count":2,"name":"bert"}\n'


@pytest.mark.anyio
async def test_argv_mode_yq_supports_json_input_and_exit_status() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/data.json", '{"name":"bert","active":true}\n')
        result = await bash.exec(["yq", "-p", "json", ".name", "/workspace/data.json"])
        status = await bash.exec(["yq", "-p", "json", "-e", ".missing", "/workspace/data.json"])

    assert result.exit_code == 0
    assert result.stdout == "bert\n"
    assert status.exit_code == 1
    assert status.stdout == "null\n"


@pytest.mark.anyio
async def test_argv_mode_yq_auto_detects_toml_and_supports_toml_output() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/Cargo.toml",
            '[package]\nname = "abash"\nversion = "0.1.0"\n',
        )
        name = await bash.exec(["yq", "-r", ".package.name", "/workspace/Cargo.toml"])
        package = await bash.exec(
            ["yq", "-o", "toml", ".package", "/workspace/Cargo.toml"]
        )

    assert name.exit_code == 0
    assert name.stdout == "abash\n"
    assert package.stdout == 'name = "abash"\nversion = "0.1.0"\n'


@pytest.mark.anyio
async def test_argv_mode_yq_supports_csv_input_and_output() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/users.csv",
            "name,age\nbert,34\nana,29\n",
        )
        selected = await bash.exec(
            ["yq", "-p", "csv", "-o", "json", "-c", "[.[] | select(.age > 30) | .name]", "/workspace/users.csv"],
        )
        rendered = await bash.exec(
            ["yq", "-p", "json", "-o", "csv", ".users"],
            stdin='{"users":[{"name":"bert","age":34},{"name":"ana","age":29}]}\n',
        )

    assert selected.exit_code == 0
    assert selected.stdout == '["bert"]\n'
    assert rendered.stdout == "age,name\n34,bert\n29,ana\n"


@pytest.mark.anyio
async def test_argv_mode_yq_supports_ini_input_and_output() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/config.ini",
            "name=abash\n[server]\nport=8080\ndebug=true\n",
        )
        port = await bash.exec(["yq", "-p", "ini", ".server.port", "/workspace/config.ini"])
        rendered = await bash.exec(
            ["yq", "-p", "json", "-o", "ini", "."],
            stdin='{"name":"abash","server":{"port":8080,"debug":true}}\n',
        )

    assert port.exit_code == 0
    assert port.stdout == "8080\n"
    assert rendered.stdout == "name=abash\n\n[server]\ndebug=true\nport=8080\n"


@pytest.mark.anyio
async def test_argv_mode_yq_supports_front_matter_extraction() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/post.md",
            "---\ntitle: Roadmap\ncount: 2\n---\n# body\n",
        )
        title = await bash.exec(["yq", "--front-matter", ".title", "/workspace/post.md"])
        count = await bash.exec(
            ["yq", "--front-matter", "-o", "json", "-c", ".count", "/workspace/post.md"]
        )

    assert title.exit_code == 0
    assert title.stdout == "Roadmap\n"
    assert count.stdout == "2\n"


@pytest.mark.anyio
async def test_argv_mode_yq_supports_xml_input_and_output() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.xml",
            '<root><user id="7"><name>bert</name></user></root>\n',
        )
        name = await bash.exec(["yq", "-p", "xml", "-r", ".root.user.name", "/workspace/data.xml"])
        identifier = await bash.exec(
            ["yq", "-p", "xml", "-r", '.root.user["+@id"]', "/workspace/data.xml"]
        )
        rendered = await bash.exec(
            ["yq", "-p", "json", "-o", "xml", "."],
            stdin='{"root":{"user":{"name":"bert","+@id":"7"}}}\n',
        )

    assert name.exit_code == 0
    assert name.stdout == "bert\n"
    assert identifier.stdout == "7\n"
    assert "<root>" in rendered.stdout
    assert 'id="7"' in rendered.stdout
    assert "<name>bert</name>" in rendered.stdout


@pytest.mark.anyio
async def test_argv_mode_yq_supports_inplace_writes() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/data.yaml", "name: bert\ncount: 1\n")
        result = await bash.exec(["yq", "-i", '.name = "ana"', "/workspace/data.yaml"])
        updated = await bash.read_file("/workspace/data.yaml")

    assert result.exit_code == 0
    assert result.stdout == ""
    assert "name: ana" in updated


@pytest.mark.anyio
async def test_argv_mode_yq_inplace_preserves_source_format_and_updates_multiple_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/a.json", '{"name":"bert"}\n')
        await bash.write_file("/workspace/b.json", '{"name":"ana"}\n')
        result = await bash.exec(
            ["yq", "-i", '.name = "core"', "/workspace/a.json", "/workspace/b.json"]
        )
        a_updated = await bash.read_file("/workspace/a.json")
        b_updated = await bash.read_file("/workspace/b.json")

    assert result.exit_code == 0
    assert result.stdout == ""
    assert json.loads(a_updated) == {"name": "core"}
    assert json.loads(b_updated) == {"name": "core"}


@pytest.mark.anyio
async def test_argv_mode_yq_inplace_requires_real_file() -> None:
    async with Bash() as bash:
        result = await bash.exec(["yq", "-i", ".name"], stdin="name: bert\n")

    assert result.exit_code == 1
    assert result.error is not None
    assert result.error.kind.value == "invalid_request"
    assert "requires at least one file argument" in result.error.message


@pytest.mark.anyio
async def test_argv_mode_yq_inplace_rejects_front_matter_rewrites() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/post.md", "---\ntitle: bert\n---\nbody\n")
        result = await bash.exec(
            ["yq", "-i", "--front-matter", '.title = "ana"', "/workspace/post.md"]
        )

    assert result.exit_code == 1
    assert result.error is not None
    assert result.error.kind.value == "invalid_request"
    assert "does not support --front-matter rewrites" in result.error.message


@pytest.mark.anyio
async def test_script_mode_yq_works_in_pipelines_without_globbing_filter() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.yaml",
            "items:\n  - name: bert\n  - name: ana\n",
        )
        result = await bash.exec_script(
            """cat /workspace/data.yaml | yq -r '.items[] | .name' | tail -n 1"""
        )

    assert result.exit_code == 0
    assert result.stdout == "ana\n"


@pytest.mark.anyio
async def test_argv_mode_sqlite3_supports_memory_db_and_json_output() -> None:
    async with Bash() as bash:
        rows = await bash.exec(
            [
                "sqlite3",
                ":memory:",
                "CREATE TABLE t(x INT); INSERT INTO t VALUES(1),(2); SELECT * FROM t",
            ]
        )
        rendered = await bash.exec(
            [
                "sqlite3",
                "-json",
                ":memory:",
                "CREATE TABLE t(x INT, y TEXT); INSERT INTO t VALUES(1,'bert'); SELECT * FROM t",
            ]
        )

    assert rows.exit_code == 0
    assert rows.stdout == "1\n2\n"
    assert rendered.stdout == '[{"x":1,"y":"bert"}]\n'


@pytest.mark.anyio
async def test_argv_mode_sqlite3_persists_file_backed_databases() -> None:
    async with Bash() as bash:
        await bash.exec(
            [
                "sqlite3",
                "/workspace/demo.db",
                "CREATE TABLE users(id INT, name TEXT); INSERT INTO users VALUES(1,'ana')",
            ]
        )
        result = await bash.exec(["sqlite3", "/workspace/demo.db", "SELECT * FROM users"])

    assert result.exit_code == 0
    assert result.stdout == "1|ana\n"


@pytest.mark.anyio
async def test_script_mode_sqlite3_reads_sql_from_stdin() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            """echo "CREATE TABLE t(x); INSERT INTO t VALUES(42); SELECT * FROM t" | sqlite3 :memory:"""
        )

    assert result.exit_code == 0
    assert result.stdout == "42\n"


@pytest.mark.anyio
async def test_argv_mode_gzip_compresses_files_and_respects_keep_flag() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello gzip")
        compressed = await bash.exec(["gzip", "-k", "/workspace/demo.txt"])
        original = await bash.read_file("/workspace/demo.txt")
        listed = await bash.exec(["ls", "/workspace"])

    assert compressed.exit_code == 0
    assert original == "hello gzip"
    assert "demo.txt.gz\n" in listed.stdout


@pytest.mark.anyio
async def test_script_mode_gzip_works_in_binary_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("""echo "hello" | gzip | base64""")

    assert result.exit_code == 0
    assert result.stdout != ""


@pytest.mark.anyio
async def test_argv_mode_gunzip_restores_gzip_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello gunzip")
        await bash.exec(["gzip", "/workspace/demo.txt"])
        result = await bash.exec(["gunzip", "/workspace/demo.txt.gz"])
        restored = await bash.read_file("/workspace/demo.txt")

    assert result.exit_code == 0
    assert restored == "hello gunzip"


@pytest.mark.anyio
async def test_script_mode_zcat_decompresses_to_stdout() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello zcat")
        await bash.exec(["gzip", "/workspace/demo.txt"])
        result = await bash.exec_script("zcat /workspace/demo.txt.gz")

    assert result.exit_code == 0
    assert result.stdout == "hello zcat"


@pytest.mark.anyio
async def test_script_mode_tar_creates_lists_and_extracts_gzip_archives() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/src", parents=True)
        await bash.write_file("/workspace/src/demo.txt", "hello tar")
        created = await bash.exec_script(
            "tar -czf /workspace/demo.tar.gz -C /workspace/src demo.txt"
        )
        listed = await bash.exec_script("tar -tzf /workspace/demo.tar.gz")
        await bash.mkdir("/workspace/out", parents=True)
        extracted = await bash.exec_script("tar -xzf /workspace/demo.tar.gz -C /workspace/out")
        restored = await bash.read_file("/workspace/out/demo.txt")

    assert created.exit_code == 0
    assert listed.stdout == "demo.txt\n"
    assert extracted.exit_code == 0
    assert restored == "hello tar"


@pytest.mark.anyio
async def test_argv_mode_tar_extracts_to_workspace() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/src", parents=True)
        await bash.write_file("/workspace/src/data.txt", "hello extract")
        await bash.exec(["tar", "-cf", "/workspace/data.tar", "-C", "/workspace/src", "data.txt"])
        await bash.mkdir("/workspace/dest", parents=True)
        result = await bash.exec(["tar", "-xf", "/workspace/data.tar", "-C", "/workspace/dest"])
        restored = await bash.read_file("/workspace/dest/data.txt")

    assert result.exit_code == 0
    assert restored == "hello extract"


@pytest.mark.anyio
async def test_argv_mode_tar_blocks_parent_traversal_members() -> None:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        info = tarfile.TarInfo("../escape.txt")
        body = b"bad"
        info.size = len(body)
        archive.addfile(info, io.BytesIO(body))

    async with Bash() as bash:
        await bash.write_file("/workspace/escape.tar", payload.getvalue())
        await bash.mkdir("/workspace/out", parents=True)
        result = await bash.exec(["tar", "-xf", "/workspace/escape.tar", "-C", "/workspace/out"])

    assert result.exit_code == 1
    assert "Path contains '..'" in result.stderr


@pytest.mark.anyio
async def test_argv_mode_chmod_updates_stat_mode_bits() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello chmod")
        changed = await bash.exec(["chmod", "755", "/workspace/demo.txt"])
        result = await bash.exec(["stat", "/workspace/demo.txt"])

    assert changed.exit_code == 0
    assert "Mode: 0755" in result.stdout


@pytest.mark.anyio
async def test_script_mode_chmod_recursive_updates_nested_paths() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/tree/nested", parents=True)
        await bash.write_file("/workspace/tree/nested/file.txt", "hello recursive chmod")
        changed = await bash.exec_script("chmod -R 700 /workspace/tree")
        result = await bash.exec(
            ["stat", "/workspace/tree", "/workspace/tree/nested", "/workspace/tree/nested/file.txt"]
        )

    assert changed.exit_code == 0
    assert result.stdout.count("Mode: 0700") == 3


@pytest.mark.anyio
async def test_argv_mode_python3_executes_inline_code_and_reads_workspace_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello python3")
        result = await bash.exec(
            ["python3", "-c", "print(open('/workspace/demo.txt').read().strip())"]
        )

    assert result.exit_code == 0
    assert result.stdout == "hello python3\n"


@pytest.mark.anyio
async def test_argv_mode_python3_syncs_workspace_mutations_back() -> None:
    async with Bash() as bash:
        result = await bash.exec(
            [
                "python3",
                "-c",
                "from pathlib import Path; Path('/workspace/out.txt').write_text('from python3'); print(Path('/workspace/out.txt').read_text())",
            ]
        )
        restored = await bash.read_file("/workspace/out.txt")

    assert result.exit_code == 0
    assert result.stdout == "from python3\n"
    assert restored == "from python3"


@pytest.mark.anyio
async def test_argv_mode_python_alias_forwards_to_python3() -> None:
    async with Bash() as bash:
        result = await bash.exec(["python", "-c", "print('alias ok')"])

    assert result.exit_code == 0
    assert result.stdout == "alias ok\n"


@pytest.mark.anyio
async def test_argv_mode_js_exec_executes_inline_code_and_reads_workspace_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello js-exec")
        result = await bash.exec(
            [
                "js-exec",
                "-c",
                "const fs = require('fs'); console.log(fs.readFileSync('/workspace/demo.txt', 'utf8').trim())",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout == "hello js-exec\n"


@pytest.mark.anyio
async def test_argv_mode_js_exec_syncs_workspace_mutations_back() -> None:
    async with Bash() as bash:
        result = await bash.exec(
            [
                "js-exec",
                "-c",
                "const fs = require('fs'); fs.writeFileSync('/workspace/out.txt', 'from js-exec'); console.log(fs.readFileSync('/workspace/out.txt', 'utf8'))",
            ]
        )
        restored = await bash.read_file("/workspace/out.txt")

    assert result.exit_code == 0
    assert result.stdout == "from js-exec\n"
    assert restored == "from js-exec"


@pytest.mark.anyio
async def test_argv_mode_xan_headers_and_count() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.csv",
            "name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\nana,28,porto,3,4\n",
        )
        headers = await bash.exec(["xan", "headers", "-j", "/workspace/data.csv"])
        count = await bash.exec(["xan", "count", "/workspace/data.csv"])

    assert headers.exit_code == 0
    assert headers.stdout == "name\nage\ncity\nvec_1\nvec_2\n"
    assert count.exit_code == 0
    assert count.stdout == "2\n"


@pytest.mark.anyio
async def test_argv_mode_xan_select_search_sort_and_filter() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.csv",
            "name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\nana,28,porto,3,4\nzoe,41,rome,5,6\n",
        )
        selected = await bash.exec(["xan", "select", "name,vec_*", "/workspace/data.csv"])
        searched = await bash.exec(["xan", "search", "-s", "city", "^m", "/workspace/data.csv"])
        sorted_result = await bash.exec(
            ["xan", "sort", "-s", "age", "-N", "-R", "/workspace/data.csv"]
        )
        filtered = await bash.exec(["xan", "filter", "age > 30", "/workspace/data.csv"])

    assert selected.exit_code == 0
    assert selected.stdout == "name,vec_1,vec_2\nbert,1,2\nana,3,4\nzoe,5,6\n"
    assert searched.exit_code == 0
    assert searched.stdout == "name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\n"
    assert sorted_result.exit_code == 0
    assert sorted_result.stdout.startswith("name,age,city,vec_1,vec_2\nzoe,41,rome,5,6\n")
    assert filtered.exit_code == 0
    assert filtered.stdout == ("name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\nzoe,41,rome,5,6\n")


@pytest.mark.anyio
async def test_argv_mode_xan_head_tail_slice_reverse_and_column_ops() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.csv",
            "name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\nana,28,porto,3,4\nzoe,41,rome,5,6\n",
        )
        head = await bash.exec(["xan", "head", "-n", "1", "/workspace/data.csv"])
        tail = await bash.exec(["xan", "tail", "-n", "1", "/workspace/data.csv"])
        sliced = await bash.exec(
            ["xan", "slice", "-s", "1", "-l", "1", "/workspace/data.csv"]
        )
        reversed_rows = await bash.exec(["xan", "reverse", "/workspace/data.csv"])
        dropped = await bash.exec(["xan", "drop", "vec_*", "/workspace/data.csv"])
        renamed = await bash.exec(
            ["xan", "rename", "person,years", "-s", "name,age", "/workspace/data.csv"]
        )
        enumerated = await bash.exec(["xan", "enum", "-c", "row_id", "/workspace/data.csv"])

    assert head.exit_code == 0
    assert head.stdout == "name,age,city,vec_1,vec_2\nbert,32,madrid,1,2\n"
    assert tail.exit_code == 0
    assert tail.stdout == "name,age,city,vec_1,vec_2\nzoe,41,rome,5,6\n"
    assert sliced.exit_code == 0
    assert sliced.stdout == "name,age,city,vec_1,vec_2\nana,28,porto,3,4\n"
    assert reversed_rows.exit_code == 0
    assert reversed_rows.stdout.startswith("name,age,city,vec_1,vec_2\nzoe,41,rome,5,6\n")
    assert dropped.exit_code == 0
    assert dropped.stdout == "name,age,city\nbert,32,madrid\nana,28,porto\nzoe,41,rome\n"
    assert renamed.exit_code == 0
    assert renamed.stdout.startswith("person,years,city,vec_1,vec_2\nbert,32,madrid,1,2\n")
    assert enumerated.exit_code == 0
    assert enumerated.stdout.startswith("row_id,name,age,city,vec_1,vec_2\n0,bert,32,madrid,1,2\n")


@pytest.mark.anyio
async def test_argv_mode_xan_behead_cat_dedup_and_top() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/a.csv", "name,score,team\nbert,10,a\nana,30,a\n")
        await bash.write_file("/workspace/b.csv", "name,score,team\nzoe,20,b\nmia,30,b\n")
        beheaded = await bash.exec(["xan", "behead", "/workspace/a.csv"])
        catted = await bash.exec(["xan", "cat", "/workspace/a.csv", "/workspace/b.csv"])
        deduped = await bash.exec(["xan", "dedup", "-s", "score", "/workspace/b.csv"])
        top = await bash.exec(["xan", "top", "score", "-l", "2", "/workspace/a.csv"])

    assert beheaded.exit_code == 0
    assert beheaded.stdout == "bert,10,a\nana,30,a\n"
    assert catted.exit_code == 0
    assert catted.stdout == "name,score,team\nbert,10,a\nana,30,a\nzoe,20,b\nmia,30,b\n"
    assert deduped.exit_code == 0
    assert deduped.stdout == "name,score,team\nzoe,20,b\nmia,30,b\n"
    assert top.exit_code == 0
    assert top.stdout == "name,score,team\nana,30,a\nbert,10,a\n"


@pytest.mark.anyio
async def test_argv_mode_xan_cat_supports_padded_headers() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/a.csv", "name,score\nbert,10\n")
        await bash.write_file("/workspace/b.csv", "name,team\nana,a\n")
        result = await bash.exec(["xan", "cat", "-p", "/workspace/a.csv", "/workspace/b.csv"])

    assert result.exit_code == 0
    assert result.stdout == "name,score,team\nbert,10,\nana,,a\n"


@pytest.mark.anyio
async def test_argv_mode_xan_frequency_and_stats() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.csv",
            "team,score\ncore,10\ncore,30\ngrowth,30\n",
        )
        frequency = await bash.exec(["xan", "frequency", "-s", "score", "-A", "/workspace/data.csv"])
        stats = await bash.exec(["xan", "stats", "-s", "score", "/workspace/data.csv"])

    assert frequency.exit_code == 0
    assert frequency.stdout == "field,value,count\nscore,30,2\nscore,10,1\n"
    assert stats.exit_code == 0
    assert stats.stdout == "field,type,count,min,max,mean\nscore,Number,3,10,30,23.333333333333332\n"


@pytest.mark.anyio
async def test_script_mode_xan_preserves_column_globs() -> None:
    async with Bash() as bash:
        await bash.write_file(
            "/workspace/data.csv",
            "name,vec_1,vec_2\nbert,1,2\nana,3,4\n",
        )
        result = await bash.exec_script("xan select vec_* /workspace/data.csv | tail -n 1")

    assert result.exit_code == 0
    assert result.stdout == "3,4\n"


@pytest.mark.anyio
async def test_argv_mode_find_supports_name_type_and_depth() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs", parents=True)
        await bash.write_file("/workspace/demo.txt", "root")
        await bash.write_file("/workspace/docs/readme.txt", "nested")
        await bash.write_file("/workspace/docs/data.csv", "nested")
        result = await bash.exec(
            ["find", "/workspace", "-name", "*.txt", "-type", "f", "-maxdepth", "1"]
        )

    assert result.exit_code == 0
    assert result.stdout == "/workspace/demo.txt\n"


@pytest.mark.anyio
async def test_script_mode_find_works_in_text_pipelines() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs", parents=True)
        await bash.write_file("/workspace/docs/readme.txt", "bert")
        await bash.write_file("/workspace/docs/data.csv", "ana")
        result = await bash.exec_script('find /workspace/docs -name "*.txt" -type f | tail -n 1')

    assert result.exit_code == 0
    assert result.stdout == "/workspace/docs/readme.txt\n"


@pytest.mark.anyio
async def test_argv_mode_ls_supports_a_and_l() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs", parents=True)
        await bash.write_file("/workspace/demo.txt", "root")
        await bash.write_file("/workspace/.env", "secret")
        plain = await bash.exec(["ls", "/workspace"])
        detailed = await bash.exec(["ls", "-a", "-l", "/workspace"])

    assert plain.stdout == "demo.txt\ndocs\n"
    assert detailed.stdout == "- .env\n- demo.txt\nd docs\n"


@pytest.mark.anyio
async def test_argv_mode_du_supports_summary_depth_and_total() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs/nested", parents=True)
        await bash.write_file("/workspace/root.txt", "abcd")
        await bash.write_file("/workspace/docs/a.txt", "xy")
        await bash.write_file("/workspace/docs/nested/b.txt", "12345")
        summary = await bash.exec(["du", "/workspace"])
        shallow = await bash.exec(["du", "--max-depth=1", "/workspace"])
        total = await bash.exec(["du", "-s", "-c", "/workspace/docs", "/workspace/root.txt"])

    assert summary.exit_code == 0
    assert summary.stdout == "5\t/workspace/docs/nested\n7\t/workspace/docs\n11\t/workspace\n"
    assert shallow.exit_code == 0
    assert shallow.stdout == "7\t/workspace/docs\n11\t/workspace\n"
    assert total.exit_code == 0
    assert total.stdout == "7\t/workspace/docs\n4\t/workspace/root.txt\n11\ttotal\n"


@pytest.mark.anyio
async def test_argv_mode_du_supports_all_files_and_human_sizes() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/bin", parents=True)
        await bash.write_file("/workspace/bin/blob.bin", "x" * 1536)
        result = await bash.exec(["du", "-a", "-h", "/workspace/bin"])

    assert result.exit_code == 0
    assert result.stdout == "1.5K\t/workspace/bin/blob.bin\n1.5K\t/workspace/bin\n"


@pytest.mark.anyio
async def test_argv_mode_html_to_markdown_supports_stdin_and_options() -> None:
    async with Bash() as bash:
        result = await bash.exec(
            [
                "html-to-markdown",
                "--bullet=+",
                "--code=~~~",
                "--hr=***",
                "--heading-style=setext",
            ],
            stdin=(
                "<h1>Title</h1><ul><li>One</li></ul><pre><code>x = 1;</code></pre>"
                "<hr><script>alert(1)</script><style>body{}</style><footer>bye</footer>"
            ),
        )

    assert result.exit_code == 0
    assert result.stdout.startswith("Title\n=====")
    assert "+ One" in result.stdout
    assert "~~~" in result.stdout
    assert "***" in result.stdout
    assert "alert" not in result.stdout
    assert "body{}" not in result.stdout
    assert "bye" not in result.stdout


@pytest.mark.anyio
async def test_argv_mode_html_to_markdown_supports_file_input_and_help() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/page.html", "<h2>From File</h2>")
        file_result = await bash.exec(["html-to-markdown", "/workspace/page.html"])
        help_result = await bash.exec(["html-to-markdown", "--help"])

    assert file_result.exit_code == 0
    assert file_result.stdout == "## From File\n"
    assert help_result.exit_code == 0
    assert "html-to-markdown" in help_result.stdout
    assert "--bullet" in help_result.stdout
    assert "curl -s https://example.com | html-to-markdown" in help_result.stdout


@pytest.mark.anyio
async def test_argv_mode_html_to_markdown_reports_missing_file() -> None:
    async with Bash() as bash:
        result = await bash.exec(["html-to-markdown", "/workspace/missing.html"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == (
        "html-to-markdown: /workspace/missing.html: No such file or directory\n"
    )


@pytest.mark.anyio
async def test_script_mode_ls_works_in_text_pipelines() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs", parents=True)
        await bash.write_file("/workspace/demo.txt", "root")
        result = await bash.exec_script("ls -l /workspace | tail -n 1")

    assert result.exit_code == 0
    assert result.stdout == "d docs\n"


@pytest.mark.anyio
async def test_argv_mode_rm_supports_force_and_recursive_delete() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs/nested", parents=True)
        await bash.write_file("/workspace/demo.txt", "root")
        await bash.write_file("/workspace/docs/nested/readme.txt", "nested")
        removed_file = await bash.exec(["rm", "/workspace/demo.txt"])
        removed_dir = await bash.exec(["rm", "-r", "/workspace/docs"])
        ignored_missing = await bash.exec(["rm", "-f", "/workspace/missing.txt"])
        exists_file = await bash.exists("/workspace/demo.txt")
        exists_dir = await bash.exists("/workspace/docs")

    assert removed_file.exit_code == 0
    assert removed_dir.exit_code == 0
    assert ignored_missing.exit_code == 0
    assert exists_file is False
    assert exists_dir is False


@pytest.mark.anyio
async def test_script_mode_rm_works_with_followup_listing() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/docs", parents=True)
        await bash.write_file("/workspace/docs/readme.txt", "bert")
        result = await bash.exec_script("rm /workspace/docs/readme.txt; ls /workspace/docs")

    assert result.exit_code == 0
    assert result.stdout == ""


@pytest.mark.anyio
async def test_argv_mode_cp_supports_multiple_files_and_recursive_dirs() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/src/docs", parents=True)
        await bash.mkdir("/workspace/out", parents=True)
        await bash.write_file("/workspace/src/demo.txt", "root")
        await bash.write_file("/workspace/src/docs/readme.txt", "nested")
        copied_file = await bash.exec(["cp", "/workspace/src/demo.txt", "/workspace/out"])
        copied_dir = await bash.exec(["cp", "-r", "/workspace/src/docs", "/workspace/out"])
        file_contents = await bash.read_file("/workspace/out/demo.txt")
        dir_contents = await bash.read_file("/workspace/out/docs/readme.txt")

    assert copied_file.exit_code == 0
    assert copied_dir.exit_code == 0
    assert file_contents == "root"
    assert dir_contents == "nested"


@pytest.mark.anyio
async def test_script_mode_cp_works_with_followup_read() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/source.txt", "bert")
        result = await bash.exec_script(
            "cp /workspace/source.txt /workspace/copied.txt; cat /workspace/copied.txt"
        )

    assert result.exit_code == 0
    assert result.stdout == "bert"


@pytest.mark.anyio
async def test_argv_mode_mv_supports_files_and_directories() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/src/docs", parents=True)
        await bash.mkdir("/workspace/out", parents=True)
        await bash.write_file("/workspace/src/demo.txt", "root")
        await bash.write_file("/workspace/src/docs/readme.txt", "nested")
        moved_file = await bash.exec(["mv", "/workspace/src/demo.txt", "/workspace/out"])
        moved_dir = await bash.exec(["mv", "/workspace/src/docs", "/workspace/out"])
        file_contents = await bash.read_file("/workspace/out/demo.txt")
        dir_contents = await bash.read_file("/workspace/out/docs/readme.txt")
        source_file_exists = await bash.exists("/workspace/src/demo.txt")
        source_dir_exists = await bash.exists("/workspace/src/docs")

    assert moved_file.exit_code == 0
    assert moved_dir.exit_code == 0
    assert file_contents == "root"
    assert dir_contents == "nested"
    assert source_file_exists is False
    assert source_dir_exists is False


@pytest.mark.anyio
async def test_script_mode_mv_works_with_followup_listing() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/source.txt", "bert")
        result = await bash.exec_script(
            "mv /workspace/source.txt /workspace/moved.txt; ls /workspace | tail -n 1"
        )

    assert result.exit_code == 0
    assert result.stdout == "moved.txt\n"


@pytest.mark.anyio
async def test_argv_mode_tee_supports_passthrough_and_append() -> None:
    async with Bash() as bash:
        first = await bash.exec(["tee", "/workspace/log.txt"], stdin="bert\n")
        second = await bash.exec(["tee", "-a", "/workspace/log.txt"], stdin="ana\n")
        contents = await bash.read_file("/workspace/log.txt")

    assert first.stdout == "bert\n"
    assert second.stdout == "ana\n"
    assert contents == "bert\nana\n"


@pytest.mark.anyio
async def test_script_mode_tee_works_in_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("echo bert | tee /workspace/tee.txt | tr abc xyz")
        contents = await bash.read_file("/workspace/tee.txt")

    assert result.exit_code == 0
    assert result.stdout == "yert\n"
    assert contents == "bert\n"


@pytest.mark.anyio
async def test_argv_mode_printf_supports_percent_s_and_percent_escape() -> None:
    async with Bash() as bash:
        rendered = await bash.exec(["printf", "%s\\n", "bert", "ana"])
        literal = await bash.exec(["printf", "%% done\\n"])

    assert rendered.stdout == "bert\nana\n"
    assert literal.stdout == "% done\n"


@pytest.mark.anyio
async def test_script_mode_printf_works_in_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "printf '%s\\n' bert | tee /workspace/printf.txt | tail -n 1"
        )
        contents = await bash.read_file("/workspace/printf.txt")

    assert result.exit_code == 0
    assert result.stdout == "bert\n"
    assert contents == "bert\n"


@pytest.mark.anyio
async def test_argv_mode_seq_supports_default_and_explicit_steps() -> None:
    async with Bash() as bash:
        default = await bash.exec(["seq", "3"])
        explicit = await bash.exec(["seq", "2", "2", "6"])

    assert default.stdout == "1\n2\n3\n"
    assert explicit.stdout == "2\n4\n6\n"


@pytest.mark.anyio
async def test_script_mode_seq_works_in_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("seq 1 4 | tail -n 1")

    assert result.exit_code == 0
    assert result.stdout == "4\n"


@pytest.mark.anyio
async def test_argv_mode_date_supports_default_and_format_output() -> None:
    async with Bash() as bash:
        default = await bash.exec(["date"])
        formatted = await bash.exec(["date", "+%F"])

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}\n", default.stdout)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}\n", formatted.stdout)


@pytest.mark.anyio
async def test_script_mode_date_works_in_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script("date +%F | tail -n 1")

    assert result.exit_code == 0
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}\n", result.stdout)


@pytest.mark.anyio
async def test_argv_mode_env_supports_clear_assign_and_exec() -> None:
    async with Bash() as bash:
        listed = await bash.exec(["env", "-i", "FOO=bar", "BAR=baz"])
        executed = await bash.exec(["env", "-i", "FOO=bar", "printenv", "FOO", "BAR"])

    assert listed.exit_code == 0
    assert listed.stdout == "BAR=baz\nFOO=bar\n"
    assert executed.exit_code == 0
    assert executed.stdout == "bar\n"


@pytest.mark.anyio
async def test_argv_mode_which_reports_found_and_missing() -> None:
    async with Bash() as bash:
        result = await bash.exec(["which", "echo", "rg", "missing"])

    assert result.exit_code == 1
    assert result.stdout == "echo\nrg\n"


@pytest.mark.anyio
async def test_argv_mode_dirname_and_basename_support_multiple_paths() -> None:
    async with Bash() as bash:
        dirname_result = await bash.exec(["dirname", "/workspace/docs/readme.txt", "demo"])
        basename_result = await bash.exec(["basename", "/workspace/docs/readme.txt", "demo/"])

    assert dirname_result.stdout == "/workspace/docs\n.\n"
    assert basename_result.stdout == "readme.txt\ndemo\n"


@pytest.mark.anyio
async def test_argv_mode_rmdir_supports_parent_cleanup() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/demo/nested", parents=True)
        result = await bash.exec(["rmdir", "-p", "/workspace/demo/nested"])

        nested_exists = await bash.exists("/workspace/demo/nested")
        parent_exists = await bash.exists("/workspace/demo")

    assert result.exit_code == 0
    assert nested_exists is False
    assert parent_exists is False


@pytest.mark.anyio
async def test_argv_mode_comm_and_diff_compare_two_files() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/left.txt", "alpha\ncommon\n")
        await bash.write_file("/workspace/right.txt", "beta\ncommon\n")
        comm_result = await bash.exec(["comm", "-3", "/workspace/left.txt", "/workspace/right.txt"])
        diff_result = await bash.exec(["diff", "/workspace/left.txt", "/workspace/right.txt"])

    assert comm_result.exit_code == 0
    assert comm_result.stdout == "alpha\n\tbeta\n"
    assert diff_result.exit_code == 1
    assert diff_result.stdout == (
        "--- /workspace/left.txt\n+++ /workspace/right.txt\n@@\n-alpha\n+beta\n common\n"
    )


@pytest.mark.anyio
async def test_script_mode_column_and_xargs_work_in_pipelines() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "printf 'name role\\nbert eng\\n' | column -t | tail -n 1; "
            "printf 'a b c' | xargs -n 2 echo"
        )

    assert result.exit_code == 0
    assert result.stdout == "bert  eng\na b\nc\n"


@pytest.mark.anyio
async def test_argv_mode_rg_searches_files_and_stdin() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/search", parents=True)
        await bash.write_file("/workspace/search/a.txt", "alpha\nbert\n")
        await bash.write_file("/workspace/search/b.txt", "BERT\nbeta\n")

        files = await bash.exec(["rg", "-n", "bert", "/workspace/search"])
        listed = await bash.exec(["rg", "-l", "-i", "bert", "/workspace/search"])
        piped = await bash.exec(["rg", "-n", "bert"], stdin="ana\nbert\n")

    assert files.exit_code == 0
    assert files.stdout == "/workspace/search/a.txt:2:bert\n"
    assert listed.exit_code == 0
    assert listed.stdout == "/workspace/search/a.txt\n/workspace/search/b.txt\n"
    assert piped.exit_code == 0
    assert piped.stdout == "2:bert\n"


@pytest.mark.anyio
async def test_argv_mode_tree_stat_and_file_report_paths() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/demo/nested", parents=True)
        await bash.write_file("/workspace/demo/nested/readme.txt", "hello")

        tree_result = await bash.exec(["tree", "-L", "2", "/workspace/demo"])
        stat_result = await bash.exec(["stat", "/workspace/demo/nested/readme.txt"])
        file_result = await bash.exec(["file", "/workspace/demo/nested/readme.txt"])

    assert tree_result.exit_code == 0
    assert "/workspace/demo" in tree_result.stdout
    assert "nested/" in tree_result.stdout
    assert stat_result.stdout == (
        "File: /workspace/demo/nested/readme.txt\nMode: 0644\nType: regular file\nSize: 5\n"
    )
    assert file_result.stdout == "/workspace/demo/nested/readme.txt: UTF-8 text\n"


@pytest.mark.anyio
async def test_host_readwrite_ln_and_readlink_work_for_workspace_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "links-workspace"
    workspace.mkdir()
    (workspace / "docs").mkdir()
    (workspace / "docs" / "guide.txt").write_text("hello", encoding="utf-8")

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_READWRITE,
        workspace_root=str(workspace),
        writable_roots=["/workspace/docs"],
    ) as bash:
        linked = await bash.exec(
            ["ln", "-s", "/workspace/docs/guide.txt", "/workspace/docs/guide-link.txt"]
        )
        readlink_result = await bash.exec(["readlink", "/workspace/docs/guide-link.txt"])
        cat_result = await bash.exec(["cat", "/workspace/docs/guide-link.txt"])

    assert linked.exit_code == 0
    assert readlink_result.stdout == "/workspace/docs/guide.txt\n"
    assert cat_result.stdout == "hello"


@pytest.mark.anyio
async def test_memory_mode_ln_creates_narrow_hard_links() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/original.txt", "hello")
        linked = await bash.exec(["ln", "/workspace/original.txt", "/workspace/hardlink.txt"])
        cat_result = await bash.exec(["cat", "/workspace/hardlink.txt"])

    assert linked.exit_code == 0
    assert cat_result.stdout == "hello"


@pytest.mark.anyio
async def test_host_readwrite_ln_creates_real_hard_links(tmp_path: Path) -> None:
    workspace = tmp_path / "hardlink-workspace"
    workspace.mkdir()
    (workspace / "docs").mkdir()
    (workspace / "docs" / "guide.txt").write_text("hello", encoding="utf-8")

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_READWRITE,
        workspace_root=str(workspace),
        writable_roots=["/workspace/docs"],
    ) as bash:
        linked = await bash.exec(
            ["ln", "/workspace/docs/guide.txt", "/workspace/docs/guide-hard.txt"]
        )
        cat_result = await bash.exec(["cat", "/workspace/docs/guide-hard.txt"])

    assert linked.exit_code == 0
    assert cat_result.stdout == "hello"
    assert (workspace / "docs" / "guide.txt").stat().st_ino == (
        workspace / "docs" / "guide-hard.txt"
    ).stat().st_ino


@pytest.mark.anyio
async def test_script_mode_rev_nl_tac_strings_fold_expand_and_unexpand_work() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "printf 'abc\\ndef\\n' | tac | rev; "
            "printf 'x\\ny\\n' | nl -ba; "
            "printf 'bcdef\\n' | strings -n 3; "
            "printf 'abcdef' | fold -w 3; "
            "printf 'a\\tb\\n' | expand -t 4 | unexpand -a -t 4"
        )

    assert result.exit_code == 0
    assert result.stdout == "fed\ncba\n     1\tx\n     2\ty\nbcdef\nabc\ndef\na\tb\n"


@pytest.mark.anyio
async def test_argv_mode_split_od_base64_and_hashes_work() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/input.txt", "one\ntwo\nthree\n")
        split_result = await bash.exec(
            ["split", "-l", "2", "/workspace/input.txt", "/workspace/chunk-"]
        )
        first_chunk = await bash.read_file("/workspace/chunk-aa")
        second_chunk = await bash.read_file("/workspace/chunk-ab")
        od_result = await bash.exec(["od", "-An", "-tx1", "/workspace/chunk-aa"])
        encoded = await bash.exec(["base64", "/workspace/chunk-aa"])
        decoded = await bash.exec(["base64", "-d"], stdin=encoded.stdout)
        md5_result = await bash.exec(["md5sum", "/workspace/chunk-aa"])
        sha1_result = await bash.exec(["sha1sum", "/workspace/chunk-aa"])
        sha256_result = await bash.exec(["sha256sum", "/workspace/chunk-aa"])

    assert split_result.exit_code == 0
    assert first_chunk == "one\ntwo\n"
    assert second_chunk == "three\n"
    assert od_result.stdout == "6f 6e 65 0a 74 77 6f 0a\n"
    assert decoded.stdout == "one\ntwo\n"
    assert md5_result.stdout == "2094b601daac3d68f5aed51d3c20f7cd  /workspace/chunk-aa\n"
    assert sha1_result.stdout == "c708d7ef841f7e1748436b8ef5670d0b2de1a227  /workspace/chunk-aa\n"
    assert sha256_result.stdout == (
        "c3f9c8c283a2b1f2f1896f27a01cbe3cddc0c9d93f752e4639035a0f5b36f6e8  /workspace/chunk-aa\n"
    )


@pytest.mark.anyio
async def test_script_mode_detached_handle_buffers_events() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached_script("echo detached; echo script")
        result = await run.wait()

    assert result.stdout == "detached\nscript\n"
    assert [event.kind for event in run.events()] == [
        "run_started",
        "stdout",
        "run_completed",
    ]


@pytest.mark.anyio
async def test_script_mode_expands_request_env_variables() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "echo $GREETING \"${TARGET}\" '$TARGET'",
            env={"GREETING": "hello", "TARGET": "world"},
        )

    assert result.exit_code == 0
    assert result.stdout == "hello world $TARGET\n"


@pytest.mark.anyio
async def test_script_mode_expands_default_variables() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "echo ${SET:-fallback} ${EMPTY:-fallback} ${MISSING:-$SET} ${MISSING:-literal}",
            env={"SET": "hello", "EMPTY": ""},
        )

    assert result.exit_code == 0
    assert result.stdout == "hello fallback hello literal\n"


@pytest.mark.anyio
async def test_script_mode_expands_positional_parameters() -> None:
    async with Bash() as bash:
        result = await bash.exec_script(
            "echo $1 $2 $# $@ ${MISSING:-$2}",
            argv=["bert", "core"],
        )

    assert result.exit_code == 0
    assert result.stdout == "bert core 2 bert core core\n"


@pytest.mark.anyio
async def test_script_mode_supports_command_local_assignments() -> None:
    async with Bash() as bash:
        local = await bash.exec_script("FOO=hello BAR=${FOO}-world printenv FOO BAR")
        leaked = await bash.exec_script("printenv FOO BAR")

    assert local.exit_code == 0
    assert local.stdout == "hello\nhello-world\n"
    assert leaked.stdout == ""


@pytest.mark.anyio
async def test_script_mode_globbing_expands_matches() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/glob", parents=True)
        await bash.write_file("/workspace/glob/a1.txt", "a1")
        await bash.write_file("/workspace/glob/a2.txt", "a2")
        await bash.write_file("/workspace/glob/b1.txt", "b1")
        await bash.write_file("/workspace/glob/notes.md", "md")

        result = await bash.exec_script(
            "echo *.txt a?.txt [ab]1.txt",
            cwd="/workspace/glob",
        )

    assert result.exit_code == 0
    assert result.stdout == "a1.txt a2.txt b1.txt a1.txt a2.txt a1.txt b1.txt\n"


@pytest.mark.anyio
async def test_script_mode_globbing_leaves_unmatched_patterns_literal() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/glob", parents=True)
        await bash.write_file("/workspace/glob/demo.txt", "demo")
        result = await bash.exec_script("echo *.png", cwd="/workspace/glob")

    assert result.exit_code == 0
    assert result.stdout == "*.png\n"


@pytest.mark.anyio
async def test_script_mode_globbing_works_with_env_expansion() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/glob", parents=True)
        await bash.write_file("/workspace/glob/app.log", "app")
        await bash.write_file("/workspace/glob/web.log", "web")
        result = await bash.exec_script(
            "echo $PATTERN",
            cwd="/workspace/glob",
            env={"PATTERN": "*.log"},
        )

    assert result.exit_code == 0
    assert result.stdout == "app.log web.log\n"


@pytest.mark.anyio
async def test_cooperative_cancellation() -> None:
    async with Bash() as bash:
        result_holder: dict[str, ExecutionResult] = {}

        async def run_sleep() -> None:
            result_holder["result"] = await bash.exec(["sleep", "0.25"], timeout_ms=1_000)

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(run_sleep)
            await anyio.sleep(0.05)
            bash.cancel()

        result = result_holder["result"]

    assert result.error is not None
    assert result.error.kind is ErrorKind.CANCELLATION


@pytest.mark.anyio
async def test_exec_detached_returns_handle_and_waits_successfully() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached(["echo", "hello", "detached"])
        assert run.run_id.startswith("session-")
        assert run.status() in {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.COMPLETED}
        result = await run.wait()

    assert result.exit_code == 0
    assert result.stdout == "hello detached\n"
    assert run.status() is RunStatus.COMPLETED
    assert run.stdout() == "hello detached\n"
    assert run.output() == "hello detached\n"
    assert [event.kind for event in run.events()] == [
        "run_started",
        "stdout",
        "run_completed",
    ]


@pytest.mark.anyio
async def test_detached_cancelled_run_has_stable_terminal_state() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached(["sleep", "0.25"], timeout_ms=1_000)
        await anyio.sleep(0.05)
        run.cancel()
        result = await run.wait()

    assert result.error is not None
    assert result.error.kind is ErrorKind.CANCELLATION
    assert run.status() is RunStatus.CANCELLED
    assert run.stdout() == ""
    assert run.stderr()
    assert [event.kind for event in run.events()] == [
        "run_started",
        "stderr",
        "run_cancelled",
    ]


@pytest.mark.anyio
async def test_active_run_blocks_second_exec_and_file_helpers() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached(["sleep", "0.25"], timeout_ms=1_000)

        with pytest.raises(ValueError):
            await bash.exec_detached(["echo", "nope"])

        with pytest.raises(ValueError):
            await bash.exists("/workspace/demo.txt")

        run.cancel()
        await run.wait()


@pytest.mark.anyio
async def test_close_fails_while_active_run_exists() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached(["sleep", "0.25"], timeout_ms=1_000)

        with pytest.raises(ValueError):
            await bash.close()

        run.cancel()
        await run.wait()


@pytest.mark.anyio
async def test_wait_remains_valid_after_session_closes_post_completion() -> None:
    bash = Bash()
    run = await bash.exec_detached(["echo", "after-close"])
    first = await run.wait()
    await bash.close()
    second = await run.wait()

    assert first.stdout == "after-close\n"
    assert second.stdout == "after-close\n"


@pytest.mark.anyio
async def test_callbacks_receive_events_and_audits() -> None:
    events: list[RunEvent] = []
    audits: list[AuditEvent] = []
    async with Bash(
        event_callback=events.append,
        audit_callback=audits.append,
    ) as bash:
        run = await bash.exec_detached(["echo", "callback-check"])
        result = await run.wait()

    assert result.exit_code == 0
    assert any(audit.kind == "session_opened" for audit in audits)
    assert any(audit.kind == "run_requested" for audit in audits)
    assert [event.kind for event in events if event.run_id == run.run_id] == [
        "run_started",
        "stdout",
        "run_completed",
    ]


@pytest.mark.anyio
async def test_memory_file_helpers_and_shell_commands_share_state() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/demo", parents=True)
        await bash.write_file("/workspace/demo/notes.txt", "hello")

        exists = await bash.exists("/workspace/demo/notes.txt")
        shell_result = await bash.exec(["cat", "/workspace/demo/notes.txt"])

    assert exists is True
    assert shell_result.stdout == "hello"


@pytest.mark.anyio
async def test_host_readonly_allows_reads_and_denies_writes(tmp_path: Path) -> None:
    workspace = tmp_path / "readonly-workspace"
    workspace.mkdir()
    (workspace / "docs").mkdir()
    (workspace / "docs" / "guide.txt").write_text("hello", encoding="utf-8")

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_READONLY,
        workspace_root=str(workspace),
    ) as bash:
        contents = await bash.read_file("/workspace/docs/guide.txt")
        with pytest.raises(ValueError):
            await bash.write_file("/workspace/docs/guide.txt", "updated")

    assert contents == "hello"
    assert (workspace / "docs" / "guide.txt").read_text(encoding="utf-8") == "hello"


@pytest.mark.anyio
async def test_host_cow_preserves_host_files(tmp_path: Path) -> None:
    workspace = tmp_path / "cow-workspace"
    workspace.mkdir()
    (workspace / "docs").mkdir()
    (workspace / "docs" / "guide.txt").write_text("host", encoding="utf-8")

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_COW,
        workspace_root=str(workspace),
    ) as bash:
        await bash.write_file("/workspace/docs/guide.txt", "overlay")
        shell_result = await bash.exec(["cat", "/workspace/docs/guide.txt"])

    assert shell_result.stdout == "overlay"
    assert (workspace / "docs" / "guide.txt").read_text(encoding="utf-8") == "host"


@pytest.mark.anyio
async def test_host_readwrite_respects_writable_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "rw-workspace"
    workspace.mkdir()
    (workspace / "allowed").mkdir()
    (workspace / "blocked").mkdir()

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_READWRITE,
        workspace_root=str(workspace),
        writable_roots=["/workspace/allowed"],
    ) as bash:
        await bash.write_file("/workspace/allowed/demo.txt", "ok")
        with pytest.raises(ValueError):
            await bash.write_file("/workspace/blocked/demo.txt", "nope")

    assert (workspace / "allowed" / "demo.txt").read_text(encoding="utf-8") == "ok"
    assert not (workspace / "blocked" / "demo.txt").exists()


@pytest.mark.anyio
async def test_memory_state_resets_after_reopen() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/reset", parents=True)
        await bash.write_file("/workspace/reset/demo.txt", "hello")
        assert await bash.exists("/workspace/reset/demo.txt")

    async with Bash() as bash:
        assert await bash.exists("/workspace/reset/demo.txt") is False


@pytest.mark.anyio
async def test_binary_file_helpers_preserve_content() -> None:
    payload = b"\x00\xffhello\x10"

    async with Bash() as bash:
        await bash.mkdir("/workspace/bin", parents=True)
        await bash.write_file("/workspace/bin/blob.dat", payload, binary=True)
        loaded = await bash.read_file("/workspace/bin/blob.dat", binary=True)

    assert loaded == payload


@pytest.mark.anyio
async def test_tier3_cd_persists_only_after_cd_builtin() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/demo/inner", parents=True)

        scoped = await bash.exec(["pwd"], cwd="/workspace/demo")
        before = await bash.exec(["pwd"])
        changed = await bash.exec_script("cd /workspace/demo/inner; pwd")
        after = await bash.exec(["pwd"])

    assert scoped.stdout == "/workspace/demo\n"
    assert before.stdout == "/\n"
    assert changed.stdout == "/workspace/demo/inner\n"
    assert after.stdout == "/workspace/demo/inner\n"


@pytest.mark.anyio
async def test_tier3_export_and_history_persist_across_session() -> None:
    async with Bash() as bash:
        exported = await bash.exec(["export", "TEAM=core"])
        env_result = await bash.exec(["printenv", "TEAM"])
        history = await bash.exec(["history"])

    assert exported.exit_code == 0
    assert env_result.stdout == "core\n"
    assert "export TEAM=core" in history.stdout
    assert "printenv TEAM" in history.stdout


@pytest.mark.anyio
async def test_per_exec_session_state_resets_shell_state_between_calls() -> None:
    async with Bash(session_state=SessionState.PER_EXEC) as bash:
        exported = await bash.exec(["export", "TEAM=core"])
        env_result = await bash.exec(["printenv", "TEAM"])
        aliased = await bash.exec(["alias", "ll=ls -l"])
        alias_use = await bash.exec(["ll"])
        changed = await bash.exec_script("mkdir -p /workspace/demo/inner; cd /workspace/demo/inner; pwd")
        after = await bash.exec(["pwd"])
        history = await bash.exec(["history"])

    assert exported.exit_code == 0
    assert env_result.stdout == ""
    assert aliased.stdout == "ll='ls -l'\n"
    assert alias_use.error is not None
    assert alias_use.error.kind is ErrorKind.POLICY_DENIED
    assert changed.stdout == "/workspace/demo/inner\n"
    assert after.stdout == "/\n"
    assert history.stdout.strip() == "1  history"


@pytest.mark.anyio
async def test_tier3_alias_and_identity_commands_work() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/demo.txt", "hello")
        alias_result = await bash.exec(["alias", "ll=ls -l"])
        listed = await bash.exec(["ll", "/workspace"])
        help_result = await bash.exec(["help"])
        whoami = await bash.exec(["whoami"], env={"USER": "berto"})
        hostname = await bash.exec(["hostname"])
        cleared = await bash.exec(["clear"])
        unaliased = await bash.exec(["unalias", "ll"])
        missing = await bash.exec(["ll"])

    assert alias_result.stdout == "ll='ls -l'\n"
    assert "demo.txt" in listed.stdout
    assert "alias" in help_result.stdout
    assert "timeout" in help_result.stdout
    assert whoami.stdout == "berto\n"
    assert hostname.stdout == "abash\n"
    assert cleared.stdout == "\x1b[H\x1b[2J"
    assert unaliased.exit_code == 0
    assert missing.error is not None
    assert missing.error.kind is ErrorKind.POLICY_DENIED


@pytest.mark.anyio
async def test_tier3_expr_time_timeout_and_nested_shells_work() -> None:
    async with Bash() as bash:
        await bash.write_file("/workspace/run.sh", "echo from-file\n")
        expr_result = await bash.exec(["expr", "2", "+", "3"])
        timed = await bash.exec(["time", "echo", "hello"])
        timed_out = await bash.exec(["timeout", "0.01", "sleep", "0.05"])
        inline_bash = await bash.exec(["bash", "-c", "export TEAM=core; printenv TEAM"])
        after_inline = await bash.exec(["printenv", "TEAM"])
        file_sh = await bash.exec(["sh", "/workspace/run.sh"])

    assert expr_result.stdout == "5\n"
    assert timed.stdout == "hello\n"
    assert re.search(r"real \d+\.\d{3}s\n$", timed.stderr)
    assert timed_out.error is not None
    assert timed_out.error.kind is ErrorKind.TIMEOUT
    assert inline_bash.stdout == "core\n"
    assert after_inline.stdout == ""
    assert file_sh.stdout == "from-file\n"


@pytest.mark.anyio
async def test_curl_requires_explicit_network_enablement() -> None:
    with _CurlServer() as server:
        policy = NetworkPolicy(
            allowed_origins=[NetworkOrigin(origin=server.base_url)],
            allowed_methods=["GET"],
            allowed_schemes=["http"],
            block_private_ranges=False,
        )
        async with Bash(network_policy=policy) as bash:
            result = await bash.exec(["curl", f"{server.base_url}/hello"])

    assert result.error is not None
    assert result.error.kind is ErrorKind.POLICY_DENIED


@pytest.mark.anyio
async def test_curl_respects_policy_and_returns_metadata() -> None:
    with _CurlServer() as server:
        policy = NetworkPolicy(
            allowed_origins=[NetworkOrigin(origin=server.base_url)],
            allowed_methods=["GET"],
            allowed_schemes=["http"],
            block_private_ranges=False,
        )
        async with Bash(network_policy=policy) as bash:
            result = await bash.exec(
                ["curl", "-L", f"{server.base_url}/redirect"], network_enabled=True
            )

    assert result.exit_code == 0
    assert result.stdout == "hello from curl\n"
    assert result.metadata["http_status"] == "200"
    assert result.metadata["http_method"] == "GET"
    assert result.metadata["http_final_url"].endswith("/hello")
    assert result.metadata["http_content_type"] == "text/plain"


@pytest.mark.anyio
async def test_curl_supports_post_output_file_and_injected_headers() -> None:
    with _CurlServer() as server:
        policy = NetworkPolicy(
            allowed_origins=[
                NetworkOrigin(
                    origin=server.base_url,
                    injected_headers={"X-Injected": "secret"},
                )
            ],
            allowed_methods=["POST"],
            allowed_schemes=["http"],
            block_private_ranges=False,
        )
        async with Bash(network_policy=policy) as bash:
            result = await bash.exec(
                [
                    "curl",
                    "-o",
                    "/workspace/echo.txt",
                    "-d",
                    "posted",
                    f"{server.base_url}/echo",
                ],
                network_enabled=True,
            )
            written = await bash.read_file("/workspace/echo.txt")

    assert result.stdout == ""
    assert written == "posted"
    assert server.httpd.last_body == b"posted"  # type: ignore[attr-defined]
    assert server.httpd.last_header == "secret"  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_curl_blocks_private_ranges_by_default() -> None:
    with _CurlServer() as server:
        policy = NetworkPolicy(
            allowed_origins=[NetworkOrigin(origin=server.base_url)],
            allowed_methods=["GET"],
            allowed_schemes=["http"],
        )
        async with Bash(network_policy=policy) as bash:
            result = await bash.exec(["curl", f"{server.base_url}/hello"], network_enabled=True)

    assert result.error is not None
    assert result.error.kind is ErrorKind.POLICY_DENIED
