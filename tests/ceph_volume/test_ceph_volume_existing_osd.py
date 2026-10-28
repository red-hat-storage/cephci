"""Verify activation and recovery of an existing prepared OSD."""

from tests.ceph_volume.test_ceph_volume_sanity import run as run_lifecycle


def run(ceph_cluster, **kw):
    """Reuse checked activation, OSD state polling and verified I/O assertions."""
    config = {**(kw.get("config") or {}), "operation": "prepare", "reboot": True}
    return run_lifecycle(ceph_cluster, **{**kw, "config": config})
