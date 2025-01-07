from time import sleep

from cli.ceph.ceph import Ceph
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.nfs.nfs_operations import check_nfs_daemons_removed
from tests.nfs.nfs_test_multiple_filesystem_exports import create_nfs_export
from utility.log import Log
log = Log(__name__)

def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    fs_util = FsUtils(ceph_cluster)
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

    fs_util.create_subvolumegroup(client=clients[0],
                                  vol_name=fs_name,
                                  group_name=subvolume_group)
    fs_util.create_subvolume(client=clients[0],
                             vol_name=fs_name,
                             subvol_name=subvolume_name, validate=True,
                             group_name=subvolume_group)
    fs_util.set_subvolume_earmark(client=clients[0],
                                  vol_name=fs_name,
                                  subvol_name=subvolume_name,
                                  group_name=subvolume_group,
                                  earmark=earmark)

    subvolume_earmark = fs_util.get_subvolume_earmark(client=clients[0],
                                                      vol_name=fs_name,
                                                      subvol_name=subvolume_name,
                                                      group_name=subvolume_group)

    if operation == "verify_earmark":
        if earmark not in subvolume_earmark:
            log.error(f"earmark \"{earmark}\" not found on subvolume {subvolume_name}")
            # raise OperationFailedError(f"earmark \"{earmark}\" not found on subvolume {subvolume_name}")
            return 1

        log.info(f"earmark \"{earmark}\" found on subvolume {subvolume_name}")
        return 0

    if operation == "rename_earmark":
        earmark2 = "nfs"
        fs_util.remove_subvolume_earmark(client=clients[0],
                                         vol_name=fs_name,
                                         subvol_name=subvolume_name,
                                         group_name=subvolume_group)

        fs_util.set_subvolume_earmark(client=clients[0],
                                      vol_name=fs_name,
                                      subvol_name=subvolume_name,
                                      group_name=subvolume_group,
                                      earmark=earmark2)

    try:
        # Setup nfs cluster
        Ceph(clients[0]).nfs.cluster.create(
            name=nfs_name, nfs_server=nfs_server_name, ha=False, vip=None
        )
        sleep(3)

        if operation == "override_earmark":
            earmark2= "smb"
            fs_util.set_subvolume_earmark(client=clients[0],
                                          vol_name=fs_name,
                                          subvol_name=subvolume_name,
                                          group_name=subvolume_group,
                                          earmark=earmark2)

        log.info(f"subvolume earmark is {subvolume_earmark}")

        sub_volume_path = fs_util.get_subvolume_info(client=clients[0],
                                                     vol_name=fs_name,
                                                     subvol_name=subvolume_name,
                                                     group_name=subvolume_group).get("path")

        try:
            create_nfs_export(clients[0],
                              fs_name,
                              nfs_name,
                              nfs_export,
                              sub_volume_path, '')
            log.info(f"nfs export {nfs_export} has been created for subvolume path {nfs_export}")

            Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export)
            log.info(f"nfs export {nfs_export} has been deleted for subvolume path {nfs_export}")
            return 0

        except Exception as e:
            # The export should fail earmark has already been set by smb
            if ("earmark has already been set by smb" in e.args[0] and
                    operation in ["override_earmark","wrong_earmark"]):
                log.info(f"expected, failed earmark has already been set by smb {e}")
                return 0
            else:
                log.error(f"Unexpected {e}")

    except Exception as e:
        log.error(f"unable to create nfs cluster {nfs_name}")
        return 1
    finally:
        log.info("Cleaning up in progress")
        fs_util.remove_subvolume(client=clients[0],
                                 vol_name=fs_name,
                                 subvol_name=subvolume_name,
                                 validate=True,
                                 group_name=subvolume_group)
        log.info(f"Removed the subvolume {subvolume_name} from group {subvolume_group}")
        Ceph(clients[0]).nfs.cluster.delete(nfs_name)
        sleep(30)
        check_nfs_daemons_removed(clients[0])