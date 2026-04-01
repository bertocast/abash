from __future__ import annotations

import anyio

from abash import Bash


async def main() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/demo", parents=True)
        await bash.write_file("/workspace/demo/hello.txt", "hello from memory mode")
        print("exists:", await bash.exists("/workspace/demo/hello.txt"))
        commands = [
            ["mkdir", "-p", "demo"],
            ["cat", "demo/hello.txt"],
            ["pwd"],
        ]

        for argv in commands:
            result = await bash.exec(argv, cwd="/workspace")
            print("argv:", argv)
            print("stdout:", repr(result.stdout))
            print("exit_code:", result.exit_code)
            print("metadata:", result.metadata)
            print("---")


if __name__ == "__main__":
    anyio.run(main)
