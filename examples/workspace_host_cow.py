from __future__ import annotations

from pathlib import Path

import anyio

from abash import Bash, ExecutionProfile, FilesystemMode


async def main() -> None:
    workspace_root = str(Path(".").resolve())
    host_path = Path("examples/output/cow-demo.txt")

    async with Bash(
        profile=ExecutionProfile.WORKSPACE,
        filesystem_mode=FilesystemMode.HOST_COW,
        workspace_root=workspace_root,
    ) as bash:
        await bash.mkdir("/workspace/examples/output", parents=True)
        await bash.write_file(
            "/workspace/examples/output/cow-demo.txt",
            "copy-on-write overlay",
        )
        print("exists:", await bash.exists("/workspace/examples/output/cow-demo.txt"))
        commands = [
            ["mkdir", "-p", "examples/output"],
            ["cat", "examples/output/cow-demo.txt"],
        ]

        for argv in commands:
            result = await bash.exec(argv, cwd="/workspace")
            print("argv:", argv)
            print("stdout:", repr(result.stdout))
            print("exit_code:", result.exit_code)
            print("---")

    print("host_file_exists_after_session:", host_path.exists())
    print("host file is unchanged because host_cow never writes back.")


if __name__ == "__main__":
    anyio.run(main)
