"""Validate an OSD using pre-created data, DB and WAL logical volumes."""

from tests.ceph_volume.test_ceph_volume_sanity import run as run_lifecycle


def run(ceph_cluster, **kw):
    """Require exact LV mappings, encryption, I/O and cleanup for the new OSD."""
    config = dict(kw.get("config") or {})
    specs = config.get("specs", {})
    config.update(
        precreated_lvs=True,
        layout="data-db-wal",
        encrypted=config.get("encrypted", specs.get("encrypted", False)),
    )
    return run_lifecycle(ceph_cluster, **{**kw, "config": config})
