#!/usr/bin/env python3
"""
Hold an O_RDONLY fd open until SIGTERM/SIGINT.

Used by test_mds_replay_cap_reconnect to keep client caps on an inode across
MDS fail / clientreplay (upstream TestReplayCapReconnect pattern).

Usage:
  python3 mds_replay_cap_hold_open.py <path> <marker_file>
"""
import os
import signal
import sys
import time

path = sys.argv[1]
marker = sys.argv[2]
stop = False


def _stop(signum, frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)

fd = os.open(path, os.O_RDONLY)
with open(marker, "w") as fh:
    fh.write("opened\n")
    fh.flush()

while not stop:
    time.sleep(1)

os.close(fd)
