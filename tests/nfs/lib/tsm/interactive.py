"""Driver for tests/nfs/scripts_tools/common_interactive.c over SSH.

Keeps a long-lived process with open FDs via a FIFO (manual interactive flow).
"""

import os
import time
import uuid

from utility.log import Log

log = Log(__name__)

SRC_C = "tests/nfs/scripts_tools/common_interactive.c"
REMOTE_C = "/tmp/common_interactive.c"
REMOTE_BIN = "/tmp/common_interactive"

OPEN_MODE = {
    "WRITE_ONLY": "w",
    "READ_ONLY": "r",
    "READ_WRITE": "rw",
}


def _local_src():
    if os.path.isfile(SRC_C):
        return SRC_C
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "scripts_tools",
            "common_interactive.c",
        )
    )


def ensure_interactive_binary(client):
    """Upload and compile common_interactive on ``client``. Return 0 ok, 1 fail."""
    try:
        client.upload_file(sudo=True, src=_local_src(), dst=REMOTE_C)
    except Exception as exc:
        log.error("upload common_interactive.c to %s failed: %s", client.hostname, exc)
        return 1

    client.exec_command(
        sudo=True,
        cmd=(
            "dnf install -y gcc >/dev/null 2>&1 || "
            "yum install -y gcc >/dev/null 2>&1; "
            f"gcc -O2 -o {REMOTE_BIN} {REMOTE_C}"
        ),
        check_ec=False,
    )
    out, _ = client.exec_command(
        sudo=True, cmd=f"test -x {REMOTE_BIN} && echo ok", check_ec=False
    )
    if "ok" not in (out or ""):
        log.error("Failed to compile common_interactive on %s", client.hostname)
        return 1
    log.info("common_interactive ready on %s", client.hostname)
    return 0


class InteractiveSession:
    """Remote FIFO-driven common_interactive process."""

    def __init__(self, client, base_prefix, tag=None):
        self.client = client
        self.base_prefix = base_prefix.rstrip("/")
        self.tag = tag or uuid.uuid4().hex[:8]
        self.fifo = f"/tmp/tsm_int_{self.tag}.in"
        self.outfile = f"/tmp/tsm_int_{self.tag}.out"
        self.pid = None
        self._offset = 0

    def start(self):
        """Start binary; return True on success. Calls stop() if banner missing."""
        c = self.client
        c.exec_command(
            sudo=True,
            cmd=f"rm -f {self.fifo} {self.outfile}; mkfifo {self.fifo}; : > {self.outfile}",
            check_ec=False,
        )
        # tail -f keeps the FIFO open between commands
        out, _ = c.exec_command(
            sudo=True,
            cmd=(
                f"nohup bash -c 'tail -f {self.fifo} | "
                f"{REMOTE_BIN} {self.base_prefix} > {self.outfile} 2>&1' "
                f">/dev/null 2>&1 & echo $!"
            ),
            check_ec=False,
        )
        pid = (out or "").strip().splitlines()[-1] if out else ""
        if not pid.isdigit():
            log.error("Failed to start interactive on %s: %r", c.hostname, out)
            return False
        self.pid = pid
        if not self._wait_output("Commands:", timeout=15):
            log.error("Interactive banner missing on %s", c.hostname)
            self.stop()
            return False
        log.info(
            "Interactive pid=%s on %s base=%s",
            self.pid,
            c.hostname,
            self.base_prefix,
        )
        return True

    def _log_out(self, text, label="out"):
        body = (text or "").rstrip() or "<empty>"
        log.info(
            "[interactive:%s] %s %s on %s:\n%s",
            self.tag,
            label,
            self.outfile,
            self.client.hostname,
            body,
        )

    def _cat_out(self):
        out, _ = self.client.exec_command(
            sudo=True, cmd=f"cat {self.outfile}", check_ec=False
        )
        return out or ""

    def _read_new(self):
        text = self._cat_out()
        if len(text) < self._offset:
            self._offset = 0
        chunk = text[self._offset :]
        self._offset = len(text)
        if chunk.strip():
            self._log_out(chunk, label="new")
        return chunk, text

    def _wait_output(self, substr, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, full = self._read_new()
            if substr in full:
                self._log_out(full, label="matched")
                return True
            time.sleep(0.5)
        _, full = self._read_new()
        self._log_out(full, label="timeout")
        return False

    def _delta(self, full, before):
        if before is None:
            return full
        return full[len(before) :] if full.startswith(before) else full

    def _poll_open(self, index, timeout, before=None):
        """Poll for open result. Returns success|blocked|denied|error."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, full = self._read_new()
            chunk = self._delta(full, before)
            if f"Opened index {index}" in chunk:
                self._log_out(chunk, label=f"open {index} success")
                return "success"
            low = chunk.lower()
            if "permission denied" in low or "share.denied" in low:
                self._log_out(chunk, label=f"open {index} denied")
                return "denied"
            if "open:" in low or "No such file" in chunk:
                self._log_out(chunk, label=f"open {index} error")
                return "error"
            time.sleep(0.5)
        _, full = self._read_new()
        chunk = self._delta(full, before)
        self._log_out(chunk, label=f"open {index} blocked")
        return "blocked"

    def _pid_alive(self):
        if not self.pid:
            return False
        alive, _ = self.client.exec_command(
            sudo=True,
            cmd=f"kill -0 {self.pid} 2>/dev/null && echo alive || echo dead",
            check_ec=False,
        )
        return "alive" in (alive or "")

    def send(self, line):
        """Write one command line into the FIFO."""
        safe = line.replace("'", "'\\''")
        log.info(
            "[interactive:%s] send %r -> %s on %s",
            self.tag,
            line,
            self.fifo,
            self.client.hostname,
        )
        self.client.exec_command(
            sudo=True,
            cmd=f"printf '%s\\n' '{safe}' > {self.fifo}",
            check_ec=False,
        )

    def open_file(self, index, mode, timeout=30):
        """Issue ``open <index> <mode>``. Returns success|blocked|denied|error."""
        token = OPEN_MODE.get(mode, mode)
        before = self._cat_out()
        self._offset = len(before)
        self.send(f"open {index} {token}")
        return self._poll_open(index, timeout, before=before)

    def wait_opened(self, index, timeout=90):
        """Wait for a hanging open to complete — no new send."""
        return self._poll_open(index, timeout, before=None)

    def lock_file(self, index, kind, timeout=15):
        """Issue ``nrlock|nwlock|rlock|wlock <index>``.

        Returns success|denied|error|blocked.
        """
        before = self._cat_out()
        self._offset = len(before)
        self.send(f"{kind} {index}")
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, full = self._read_new()
            chunk = self._delta(full, before)
            low = chunk.lower()
            if f"locked index {index}" in low:
                self._log_out(chunk, label=f"{kind} {index} success")
                return "success"
            if "lock failed" in low:
                self._log_out(chunk, label=f"{kind} {index} denied")
                return "denied"
            if "not open" in low or "already has" in low:
                self._log_out(chunk, label=f"{kind} {index} error")
                return "error"
            time.sleep(0.5)
        _, full = self._read_new()
        chunk = self._delta(full, before)
        self._log_out(chunk, label=f"{kind} {index} blocked")
        return "blocked"

    def stop(self, wait_quit_sec=10):
        """Quit so fds CLOSE; kill only if still alive.

        Do not pkill all common_interactive on the same base — sibling
        sessions must stay alive.
        """
        c = self.client
        if self.pid:
            try:
                self.send("quit")
            except Exception:
                pass
            deadline = time.time() + wait_quit_sec
            while time.time() < deadline and self._pid_alive():
                time.sleep(0.5)
            if self._pid_alive():
                log.warning(
                    "[interactive:%s] quit timed out; killing pid=%s",
                    self.tag,
                    self.pid,
                )
                c.exec_command(
                    sudo=True,
                    cmd=(
                        f"kill {self.pid} 2>/dev/null; "
                        f"pkill -f 'tail -f {self.fifo}' 2>/dev/null; true"
                    ),
                    check_ec=False,
                )
            self.pid = None
        c.exec_command(
            sudo=True,
            cmd=f"rm -f {self.fifo} {self.outfile}",
            check_ec=False,
        )
