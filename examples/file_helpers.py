from __future__ import annotations

import anyio

from abash import Bash


async def main() -> None:
    async with Bash() as bash:
        await bash.mkdir("/workspace/notes", parents=True)
        await bash.write_file("/workspace/notes/todo.txt", "ship examples\n")

        exists = await bash.exists("/workspace/notes/todo.txt")
        contents = await bash.read_file("/workspace/notes/todo.txt")

        print("exists:", exists)
        print("contents:")
        print(contents, end="")


if __name__ == "__main__":
    anyio.run(main)
