import json
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def nfs_test(nfs_req_params):
    """
    NFS post upgrade validation

    Args:
        nfs_req_params as dict_type: mandatory below params,
        nfs_req_params = {
            "existing_nfs_mount" : "/mnt/nfs",
            "nfs_client" : nfs_client[0],
            "nfs_server" : nfs_server,
            "nfs_name" : default_nfs_name,
            "fs_name" : default_fs,
            "fs_util" : fs_util
        }
        param data types:
        existing_nfs_mount,nfs_client,nfs_server,nfs_name,fs_name - str
        fsutil - fsutil testlib object
    Returns:
        None
    Raises:
        AssertionError
    """
    nfs_client = nfs_req_params["nfs_client"]
    existing_nfs_mount = nfs_req_params["existing_nfs_mount"]
    nfs_name = nfs_req_params["nfs_name"]
    nfs_server = nfs_req_params["nfs_server"]
    nfs_server_name = nfs_server[0].node.hostname
    nfs_server_name_1 = nfs_server[1].node.hostname
    fs_name = nfs_req_params["fs_name"]
    fs_util = nfs_req_params["fs_util"]

    log.info("Get pre-upgrade nfs server and exports")
    out, rc = nfs_client.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
    output = json.loads(out)
    if nfs_name not in output:
        assert False, f"nfs cluster {nfs_name} doesn't exist after upgrade"
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")
    output = json.loads(out)
    if len(output) == 0:
        assert False, f"nfs cluster {nfs_name} exports doesn't exist after upgrade"
    nfs_export_name = output[0]
    log.info("Verify existing mounts are accessible")
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ls {existing_nfs_mount}")

    log.info("Verify exports are active by mounting and running IO")
    nfs_mounting_dir = f"{existing_nfs_mount}_new"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name, nfs_export_name, nfs_mounting_dir
    )

    log.info("Create new exports on existing nfs server")
    new_export_name = "/export2"
    path = "/"
    nfs_client.exec_command(
        sudo=True,
        cmd=f"ceph nfs export create cephfs {nfs_name} "
        f"{new_export_name} {fs_name} path={path}",
    )

    log.info("Verify ceph nfs new export is created")
    out, rc = nfs_client.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")
    if new_export_name in out:
        log.info("ceph nfs new export created successfully")
    else:
        raise CommandFailed("Failed to create nfs export")

    time.sleep(2)
    log.info("Run IO on new exports in existing nfs server")
    nfs_mounting_dir = f"{existing_nfs_mount}_new1"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name, new_export_name, nfs_mounting_dir
    )

    log.info("Create new nfs cluster, new exports and run IO")

    new_nfs_name = f"{nfs_name}_new"
    out, rc = nfs_client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {new_nfs_name} {nfs_server_name_1}"
    )
    log.info("Verify ceph nfs cluster is created")
    if wait_for_process(client=nfs_client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")

    log.info("Create cephfs nfs export")
    new_export_name = "/export3"
    nfs_client.exec_command(
        sudo=True,
        cmd=f"ceph nfs export create cephfs {new_nfs_name} "
        f"{new_export_name} {fs_name} path={path}",
    )
    log.info("Verify ceph nfs export is created")
    out, rc = nfs_client.exec_command(
        sudo=True, cmd=f"ceph nfs export ls {new_nfs_name}"
    )
    if new_export_name in out:
        log.info("ceph nfs export created successfully")
    else:
        raise CommandFailed("Failed to create nfs export")

    time.sleep(2)
    log.info("Mount ceph nfs new exports and run IO")
    nfs_mounting_dir = f"{existing_nfs_mount}_new2"
    fs_util.nfs_mount_and_io(
        nfs_client, nfs_server_name_1, new_export_name, nfs_mounting_dir
    )
    log.info("NFS post upgrade validation succeeded")


def run(ceph_cluster, **kw):
    """
    1. NFS validation, CEPH-83575098:
          a)Verify existing nfs cluster is active and exports exists. Run IO on existing exports,
          create new exports with existing nfs-cluster and verify.
          b)Verify NFS workflow on new nfs cluster, add new exports, run IO on new nfs.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        default_nfs_name = "cephfs-nfs"
        default_fs = "cephfs"

        if fs_util.get_fs_info(clients[0], "cephfs_new"):
            default_fs = "cephfs_new"
            clients[0].exec_command(sudo=True, cmd="ceph fs volume create cephfs")

        log.info("Test1 CEPH-83575098 : Post upgrade NFS Validation")
        nfs_client = ceph_cluster.get_ceph_objects("client")
        nfs_server = ceph_cluster.get_ceph_objects("nfs")
        nfs_reqs = {
            "existing_nfs_mount": "/mnt/nfs",
            "nfs_client": nfs_client[0],
            "nfs_server": nfs_server,
            "nfs_name": default_nfs_name,
            "fs_name": default_fs,
            "fs_util": fs_util,
        }
        nfs_test(nfs_reqs)
        return 0

    except Exception as e:
        log.info(traceback.format_exc())
        log.error(e)
        return 1
