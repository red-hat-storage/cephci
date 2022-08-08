from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    log.info("Starting mirroring configuration")
    config = kw.get("config")

    mirror1 = rbdmirror.RbdMirror(kw.get("ceph_cluster_dict").get("ceph-rbd1"), config)
    mirror2 = rbdmirror.RbdMirror(kw.get("ceph_cluster_dict").get("ceph-rbd2"), config)
    kw.get("test_data").update({"mirror1": mirror1, "mirror2": mirror2})

    # Handling of clusters with same name
    if mirror1.cluster_name == mirror2.cluster_name:
        mirror1.handle_same_name("primary")
        if "two-way" in config.get("way", ""):
            mirror2.handle_same_name("secondary")

    if "one-way" in config.get("way", ""):
        mirror2.setup_mirror(mirror1)
    else:
        mirror1.setup_mirror(
            mirror2, daemon_configured=config.get("used_ceph-ansible", False)
        )
        mirror2.setup_mirror(
            mirror1, daemon_configured=config.get("used_ceph-ansible", False)
        )

    return 0
