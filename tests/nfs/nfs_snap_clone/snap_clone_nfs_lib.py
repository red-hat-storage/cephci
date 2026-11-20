"""
Shared helpers for NFS + CephFS subvolume snapshot/clone parity tests.
"""

from __future__ import annotations

import hashlib
import json
import time

from ceph.ceph import CommandFailed
from utility.log import Log

log = Log(__name__)

# One NFS Ganesha per host uses fixed ports (2049/…). Suite cases must share
# a single cluster name instead of creating a new one per test.
DEFAULT_NFS_NAME = "cephfs-nfs"


def list_nfs_clusters(client):
    """Return list of existing NFS cluster names."""
    out, _ = client.exec_command(sudo=True, cmd="ceph nfs cluster ls")
    text = (out or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [str(x) for x in data]
    except Exception:  # noqa: BLE001
        pass
    return [line.strip().strip('"').strip(",") for line in text.splitlines() if line.strip()]


def ensure_nfs_cluster(client, ceph_cluster, nfs_name=DEFAULT_NFS_NAME, fs_util=None):
    """
    Reuse an existing NFS cluster; create only if none exist.

    If ``nfs_name`` is already present, reuse it. If another cluster exists
    (e.g. left by a prior suite case), reuse that instead of creating a second
    daemon that would fail with port-already-in-use on 2049.
    """
    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    if not nfs_servers:
        raise CommandFailed("No NFS-labeled nodes in cluster")
    nfs_server = nfs_servers[0].node.hostname
    existing = list_nfs_clusters(client)

    if nfs_name in existing:
        log.info("Reusing existing NFS cluster %s (already present)", nfs_name)
        return {"nfs_name": nfs_name, "nfs_server": nfs_server}

    if existing:
        reused = existing[0]
        log.info(
            "NFS cluster %s not found; reusing already-deployed %s "
            "instead of creating another (avoids port 2049 clash)",
            nfs_name,
            reused,
        )
        return {"nfs_name": reused, "nfs_server": nfs_server}

    log.info("No NFS cluster present; creating %s on %s", nfs_name, nfs_server)
    if fs_util is None:
        raise CommandFailed("fs_util required to create NFS cluster")
    fs_util.create_nfs(
        client, nfs_cluster_name=nfs_name, nfs_server_name=nfs_server
    )
    # brief wait for daemon
    end = time.time() + 180
    while time.time() < end:
        if nfs_name in list_nfs_clusters(client):
            log.info("Created NFS cluster %s", nfs_name)
            return {"nfs_name": nfs_name, "nfs_server": nfs_server}
        time.sleep(5)
    raise CommandFailed(f"Failed to create nfs cluster {nfs_name}")


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
