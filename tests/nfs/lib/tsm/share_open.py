"""SSH driver for tests/nfs/scripts_tools/open_share.py.

``open_hold``: background OPEN (+ optional LOCK) with SEQUENCE renew until stop().
"""

import os
import time
import uuid

from utility.log import Log

log = Log(__name__)

SRC_PY = "tests/nfs/scripts_tools/open_share.py"
REMOTE_PY = "/tmp/open_share.py"

RESULT_MAP = {
    "NFS4_OK": "success",
    "NFS4ERR_GRACE": "blocked",
    "NFS4ERR_SHARE_DENIED": "denied",
    "NFS4ERR_DENIED": "denied",
}


def _local_src():
    if os.path.isfile(SRC_PY):
        return SRC_PY
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts_tools", "open_share.py"
        )
    )


def ensure_open_share_script(client):
    """Upload open_share.py to ``client``. Return 0 ok, 1 fail."""
    try:
        client.upload_file(sudo=True, src=_local_src(), dst=REMOTE_PY)
    except Exception as exc:
        log.error("upload open_share.py to %s failed: %s", client.hostname, exc)
        return 1
    out, _ = client.exec_command(
        sudo=True, cmd=f"test -f {REMOTE_PY} && echo ok", check_ec=False
    )
    if "ok" not in (out or ""):
        log.error("open_share.py missing on %s after upload", client.hostname)
        return 1
    log.info("open_share.py ready on %s", client.hostname)
    return 0


def _parse_result(text, key="RESULT"):
    outcome = None
    for ln in (text or "").splitlines():
        if ln.startswith(key + "="):
            outcome = RESULT_MAP.get(ln.split("=", 1)[1].strip(), "error")
    return outcome


class ShareOpenSession:
    """Remote open_share.py hold session."""

    def __init__(self, client, server, port, export, filename, tag=None):
        self.client = client
        self.server = server
        self.port = int(port)
        self.export = export
        self.filename = filename
        self.tag = tag or uuid.uuid4().hex[:8]
        self.outfile = f"/tmp/tsm_share_{self.tag}.out"
        self.stop_file = f"/tmp/tsm_share_{self.tag}.stop"
        self.pid = None
        self._holding = False

    def _base_cmd(self, access, deny, lock=None, want_deleg=False, create=False):
        parts = [
            f"python3 {REMOTE_PY}",
            f"--server {self.server}",
            f"--port {self.port}",
            f"--export {self.export}",
            f"--file {self.filename}",
            f"--access {access}",
            f"--deny {deny}",
        ]
        if lock:
            parts.append(f"--lock {lock}")
        if want_deleg:
            parts.append("--want-deleg")
        if create:
            parts.append("--create")
        return " ".join(parts)

    def _log_out(self, text, label="out"):
        log.info(
            "[share_open:%s] %s on %s:\n%s",
            self.tag,
            label,
            self.client.hostname,
            (text or "").rstrip() or "<empty>",
        )

    def _cat_out(self):
        out, _ = self.client.exec_command(
            sudo=True, cmd=f"cat {self.outfile} 2>/dev/null", check_ec=False
        )
        return out or ""

    def _pid_alive(self):
        if not self.pid:
            return False
        alive, _ = self.client.exec_command(
            sudo=True,
            cmd=f"kill -0 {self.pid} 2>/dev/null && echo alive || echo dead",
            check_ec=False,
        )
        return "alive" in (alive or "")

    def open_hold(
        self, access, deny="n", lock=None, want_deleg=False, create=False, timeout=60
    ):
        """Background OPEN (+ optional LOCK) with lease renew. Returns outcome."""
        if self._holding:
            log.error("[%s] already holding", self.tag)
            return "error"
        c = self.client
        c.exec_command(
            sudo=True,
            cmd=f"rm -f {self.outfile} {self.stop_file}; : > {self.outfile}",
            check_ec=False,
        )
        cmd = (
            self._base_cmd(access, deny, lock, want_deleg, create)
            + f" --hold --stop-file {self.stop_file} --renew 15"
        )
        log.info(
            "[share_open:%s] hold access=%s deny=%s lock=%s on %s",
            self.tag,
            access,
            deny,
            lock,
            c.hostname,
        )
        out, _ = c.exec_command(
            sudo=True,
            cmd=(
                f"nohup bash -c '{cmd} > {self.outfile} 2>&1' "
                f">/dev/null 2>&1 & echo $!"
            ),
            check_ec=False,
        )
        pid = (out or "").strip().splitlines()[-1] if out else ""
        if not pid.isdigit():
            log.error("[%s] failed to start hold process: %r", self.tag, out)
            return "error"
        self.pid = pid

        deadline = time.time() + timeout
        open_outcome = None
        while time.time() < deadline:
            text = self._cat_out()
            if open_outcome is None and "RESULT=" in text:
                open_outcome = _parse_result(text, "RESULT")
                self._log_out(text, label=f"hold open {access}/{deny}")
                if open_outcome != "success":
                    self.stop()
                    return open_outcome
            if open_outcome == "success":
                if lock:
                    if "LOCK_RESULT=" in text:
                        lock_outcome = _parse_result(text, "LOCK_RESULT")
                        if lock_outcome != "success":
                            self._log_out(text, label=f"hold lock {lock}")
                            self.stop()
                            return lock_outcome or "error"
                        if "READY" in text:
                            if want_deleg and "deleg_stateid=" not in text:
                                log.warning(
                                    "[%s] --want-deleg but no deleg_stateid in OPEN",
                                    self.tag,
                                )
                            self._holding = True
                            self._log_out(text, label="hold ready")
                            return "success"
                elif "READY" in text:
                    if want_deleg and "deleg_stateid=" not in text:
                        log.warning(
                            "[%s] --want-deleg but no deleg_stateid in OPEN",
                            self.tag,
                        )
                    self._holding = True
                    self._log_out(text, label="hold ready")
                    return "success"
            time.sleep(0.5)

        self._log_out(self._cat_out(), label="hold timeout")
        self.stop()
        return open_outcome or "error"

    def stop(self, wait_close_sec=25):
        """Signal hold process to CLOSE; wait for clean exit before kill.

        Idempotent: already-stopped sessions return True.
        """
        c = self.client
        if not self.pid and not self._holding:
            c.exec_command(
                sudo=True,
                cmd=f"rm -f {self.outfile} {self.stop_file}",
                check_ec=False,
            )
            return True
        clean = False
        c.exec_command(sudo=True, cmd=f"touch {self.stop_file}", check_ec=False)
        deadline = time.time() + wait_close_sec
        while time.time() < deadline:
            text = self._cat_out()
            if "CLEAN_CLOSE=OK" in text or "CLOSE -> NFS4_OK" in text:
                clean = True
            if self.pid:
                if not self._pid_alive():
                    break
            elif clean:
                break
            time.sleep(1)
        text = self._cat_out()
        if text.strip():
            self._log_out(text, label="stop final")
        if self.pid and self._pid_alive():
            log.warning(
                "[share_open:%s] clean close timed out; killing pid=%s",
                self.tag,
                self.pid,
            )
            c.exec_command(
                sudo=True,
                cmd=(
                    f"kill {self.pid} 2>/dev/null; "
                    f"pkill -f '{REMOTE_PY}.*--stop-file {self.stop_file}' "
                    f"2>/dev/null; true"
                ),
                check_ec=False,
            )
        elif clean:
            log.info("[share_open:%s] clean CLOSE completed", self.tag)
        self.pid = None
        self._holding = False
        c.exec_command(
            sudo=True,
            cmd=f"rm -f {self.outfile} {self.stop_file}",
            check_ec=False,
        )
        return clean
