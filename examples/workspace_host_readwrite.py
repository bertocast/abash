from __future__ import annotations

from pathlib import Path

import anyio

from abash import Bash, ExecutionProfile, FilesystemMode


async def main() -> None:
    workspace_root = str(Path(".").resolve())

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_READWRITE,
        workspace_root=workspace_root,
        writable_roots=["/workspace/examples/output"],
    ) as bash:
        await bash.mkdir("/workspace/examples/output", parents=True)
        await bash.write_file(
            "/workspace/examples/output/hello.txt",
            "written through helper",
        )
        commands = [
            ["mkdir", "-p", "examples/output"],
            ["cat", "examples/output/hello.txt"],
        ]

        for argv in commands:
            result = await bash.exec(argv, cwd="/workspace")
            print("argv:", argv)
            print("stdout:", repr(result.stdout))
            print("exit_code:", result.exit_code)
            print("---")

        try:
            await bash.write_file("/workspace/README.md", "this should be denied")
        except ValueError as error:
            print("denied_write_error:", error)


if __name__ == "__main__":
    anyio.run(main)
