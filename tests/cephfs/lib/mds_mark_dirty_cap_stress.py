#!/usr/bin/env python3
"""Concurrent metadata / cap-update stress for CDir::mark_dirty assert class.

Stimulates get_version() < pv under dense create/write/append/truncate/
unlink/rename/chmod on shared dirs.
"""
import argparse
import os
import random
import string
import sys
import time
from multiprocessing import Process, Value

OPS = ("create", "write", "append", "truncate", "unlink", "rename", "chmod")


def rnd_name(n=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def worker(wid, hot, ndirs, stop_at, counters, err):
    dirs = [os.path.join(hot, f"d{i:02d}") for i in range(ndirs)]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    local = 0
    while time.time() < stop_at:
        d = random.choice(dirs)
        op = random.choice(OPS)
        path = os.path.join(d, f"w{wid}_{rnd_name()}")
        try:
            if op == "create":
                with open(path, "wb") as f:
                    f.write(os.urandom(random.randint(1, 4096)))
            elif op == "write":
                with open(path, "wb") as f:
                    f.write(b"x" * random.randint(1, 8192))
            elif op == "append":
                with open(path, "ab") as f:
                    f.write(b"a" * random.randint(1, 2048))
            elif op == "truncate":
                with open(path, "wb") as f:
                    f.write(b"t" * 4096)
                os.truncate(path, random.randint(0, 2048))
            elif op == "unlink":
                with open(path, "wb") as f:
                    f.write(b"u")
                os.unlink(path)
            elif op == "rename":
                with open(path, "wb") as f:
                    f.write(b"r")
                dst = path + ".ren"
                os.rename(path, dst)
                try:
                    os.unlink(dst)
                except FileNotFoundError:
                    pass
            elif op == "chmod":
                with open(path, "wb") as f:
                    f.write(b"c")
                os.chmod(path, random.choice([0o644, 0o600, 0o755, 0o444]))
            local += 1
            if local % 200 == 0:
                with counters.get_lock():
                    counters.value += 200
        except OSError as e:
            # concurrent unlink/rename races are expected; count hard errors only
            if e.errno not in (2, 17, 39):  # ENOENT, EEXIST, ENOTEMPTY
                with err.get_lock():
                    err.value += 1
    with counters.get_lock():
        counters.value += local % 200


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hot", default="/mnt/cephfs/test_mark_dirty/hot")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dirs", type=int, default=24)
    ap.add_argument("--seconds", type=int, default=300)
    args = ap.parse_args()

    os.makedirs(args.hot, exist_ok=True)
    for i in range(args.dirs):
        os.makedirs(os.path.join(args.hot, f"d{i:02d}"), exist_ok=True)

    stop_at = time.time() + args.seconds
    counters = Value("L", 0)
    err = Value("L", 0)
    procs = []
    print(
        f"START hot={args.hot} workers={args.workers} dirs={args.dirs} "
        f"seconds={args.seconds} pid={os.getpid()}",
        flush=True,
    )
    for w in range(args.workers):
        p = Process(
            target=worker, args=(w, args.hot, args.dirs, stop_at, counters, err)
        )
        p.start()
        procs.append(p)

    try:
        while any(p.is_alive() for p in procs):
            time.sleep(60)
            left = max(0, int(stop_at - time.time()))
            print(
                f"{time.strftime('%Y-%m-%dT%H:%M:%S')} ops~={counters.value} "
                f"hard_err={err.value} remaining_s={left}",
                flush=True,
            )
            # Stop waiting once soak window elapsed; workers should exit on stop_at.
            if left == 0:
                break
    except KeyboardInterrupt:
        print("INTERRUPTED — stopping workers", flush=True)

    for p in procs:
        p.join(timeout=30)
        if p.is_alive():
            p.terminate()
            p.join(timeout=10)
            if p.is_alive():
                p.kill()
                p.join(timeout=5)
    print(f"DONE ops~={counters.value} hard_err={err.value}", flush=True)
    return 0 if err.value == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
