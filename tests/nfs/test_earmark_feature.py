from time import sleep

from cli.ceph.ceph import Ceph
from tests.nfs.nfs_operations import check_nfs_daemons_removed
from tests.nfs.nfs_test_multiple_filesystem_exports import create_nfs_export
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    nfs_node = nfs_nodes[0]
    operation = config.get("operation")
    fs_name = config.get("cephfs_volume", "ceph_fs")
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_server_name = nfs_node.hostname
    subvolume_group = "ganeshagroup"
    subvolume_name = "subvolume"
    earmark = config.get("earmark")
    ceph_fs_obj = Ceph(clients[0]).fs

    ceph_fs_obj.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    ceph_fs_obj.sub_volume.create(
        volume=fs_name, subvolume=subvolume_name, group_name=subvolume_group
    )

    ceph_fs_obj.sub_volume.earmark.set(
        volume=fs_name,
        subvolume_name=subvolume_name,
        earmark=earmark,
        group_name=subvolume_group,
    )

    subvolume_earmark = ceph_fs_obj.sub_volume.earmark.get(
        volume=fs_name, subvolume_name=subvolume_name, group_name=subvolume_group
    )
    if operation == "verify_earmark":
        if earmark not in subvolume_earmark:
            log.error(f'earmark "{earmark}" not found on subvolume {subvolume_name}')
            # raise OperationFailedError(f"earmark \"{earmark}\" not found on subvolume {subvolume_name}")
            return 1

        log.info(f'earmark "{earmark}" found on subvolume {subvolume_name}')
        return 0

    if operation == "rename_earmark":
        earmark2 = "nfs"
        ceph_fs_obj.sub_volume.earmark.remove(
            volume=fs_name, subvolume_name=subvolume_name, group_name=subvolume_group
        )
        ceph_fs_obj.sub_volume.earmark.set(
            volume=fs_name,
            subvolume_name=subvolume_name,
            group_name=subvolume_group,
            earmark=earmark2,
        )
    try:
        # Setup nfs cluster
        Ceph(clients[0]).nfs.cluster.create(
            name=nfs_name, nfs_server=nfs_server_name, ha=False, vip=None
        )
        sleep(3)

        if operation == "override_earmark":
            earmark2 = "smb"
            ceph_fs_obj.sub_volume.earmark.set(
                volume=fs_name,
                subvolume_name=subvolume_name,
                group_name=subvolume_group,
                earmark=earmark2,
            )

        # re-verifying the earmark
        subvolume_earmark = ceph_fs_obj.sub_volume.earmark.get(
            volume=fs_name, subvolume_name=subvolume_name, group_name=subvolume_group
        )

        log.info(f"subvolume earmark is {subvolume_earmark}")

        sub_volume_path = ceph_fs_obj.sub_volume.getpath(
            volume=fs_name, subvolume=subvolume_name, group_name=subvolume_group
        )

        create_nfs_export(
            clients[0], fs_name, nfs_name, nfs_export, sub_volume_path, ""
        )
        log.info(
            f"nfs export {nfs_export} has been created for subvolume path {nfs_export}"
        )

        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export)
        log.info(
            f"nfs export {nfs_export} has been deleted for subvolume path {nfs_export}"
        )
        return 0

    except Exception as e:
        if "earmark has already been set by smb" in e.args[0] and operation in [
            "override_earmark",
            "wrong_earmark",
        ]:
            log.info(f"expected failure, earmark has already been set by smb {e}")
            return 0
        else:
            log.error(f"Unexpected {e}")

        log.error(f"unable to create nfs cluster {nfs_name} with error {e}")
        return 1
    finally:
        log.info("Cleaning up in progress")
        ceph_fs_obj.sub_volume.rm(
            volume=fs_name, subvolume=subvolume_name, group=subvolume_group
        )
        log.info(f"Removed the subvolume {subvolume_name} from group {subvolume_group}")
        Ceph(clients[0]).nfs.cluster.delete(nfs_name)
        sleep(30)
        check_nfs_daemons_removed(clients[0])
