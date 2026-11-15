"""
Shared helpers for NFS + CephFS subvolume snapshot/clone parity tests.
"""

from __future__ import annotations

import hashlib
import time

from utility.log import Log

log = Log(__name__)


def checksum_file(client, path):
    """Return sha256 hex digest of a remote file."""
    out, _ = client.exec_command(
        sudo=True, cmd=f"sha256sum {path} | awk '{{print $1}}'"
    )
    return out.strip()


def write_marker_file(client, path, content, size_mb=8):
    """Create a file with known content prefix, then pad with dd."""
    client.exec_command(sudo=True, cmd=f"mkdir -p $(dirname {path})")
    client.exec_command(
        sudo=True,
        cmd=(
            f"bash -c 'printf %s {content!r} > {path}; "
            f"dd if=/dev/urandom bs=1M count={size_mb} status=none >> {path}'"
        ),
    )


def wait_clone_complete(fs_util, client, vol_name, clone_name, group_name=None, timeout=600):
    """Poll clone status until complete or timeout."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            kwargs = {}
            if group_name:
                kwargs["group_name"] = group_name
            out, _ = fs_util.get_clone_status(
                client, vol_name, clone_name, **kwargs
            )
            status = out
            if isinstance(out, str):
                try:
                    import json

                    status = json.loads(out)
                except Exception:  # noqa: BLE001
                    status = out
        except Exception as exc:  # noqa: BLE001
            log.debug("clone status poll error: %s", exc)
            status = None
        state = None
        if isinstance(status, dict):
            state = status.get("status", {}).get("state") or status.get("state")
        elif status:
            state = str(status)
        log.info("Clone %s state=%s", clone_name, state)
        if state and "complete" in str(state).lower():
            return True
        if state and "failed" in str(state).lower():
            raise RuntimeError(f"Clone {clone_name} failed: {status}")
        time.sleep(10)
    raise TimeoutError(f"Clone {clone_name} not complete within {timeout}s")


def sha256_local_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
