import argparse
import asyncio
import os
import sys
from typing import List, Tuple

try:
    from asyncio import run as async_run  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    # disable coverage for this block. it should be a copy-n-paste from
    # from newer libs for compatibilty on older python versions
    def async_run(coro):  # type: ignore
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


def call(command: List[str], timeout: int) -> Tuple[str, str, int]:
    timeout = int(timeout)

    async def run_with_timeout() -> Tuple[str, str, int]:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )
        assert process.stdout
        assert process.stderr
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout,
            )
        except asyncio.TimeoutError:
            # try to terminate the process assuming it is still running.  It's
            # possible that even after killing the process it will not
            # complete, particularly if it is D-state.  If that happens the
            # process.wait call will block, but we're no worse off than before
            # when the timeout did not work.  Additionally, there are other
            # corner-cases we could try and handle here but we decided to start
            # simple.
            process.kill()
            await process.wait()
            print("Hit timeout")
            return "", "", 124
        else:
            assert process.returncode is not None
            return (
                stdout.decode("utf-8"),
                stderr.decode("utf-8"),
                process.returncode,
            )

    stdout, stderr, returncode = async_run(run_with_timeout())
    return stdout, stderr, returncode


def command_call(args):
    out, err, rc = call(args.command, args.timeout)
    print(f"out: {out}\nerr: {err}\nrc: {rc}")


def main():
    parser = argparse.ArgumentParser(
        description='cephadm "call" function',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=command_call)
    parser.add_argument(
        "--timeout", help="timeout to pass to call", required=False, default=-1
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="command")

    args = parser.parse_args()
    if "func" not in args:
        print("No command specified")
        sys.exit()
    print(args)
    r = args.func(args)

    if not r:
        r = 0
    sys.exit()


if __name__ == "__main__":
    main()
