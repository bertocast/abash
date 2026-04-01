from __future__ import annotations

import anyio

from abash import Bash


async def main() -> None:
    async with Bash() as bash:
        run = await bash.exec_detached(["echo", "hello", "from", "detached"])
        print("run_id:", run.run_id)
        print("initial_status:", run.status().value)

        result = await run.wait()
        print("final_status:", run.status().value)
        print("stdout:", repr(result.stdout))
        print("events:", [event.kind for event in run.events()])


if __name__ == "__main__":
    anyio.run(main)
