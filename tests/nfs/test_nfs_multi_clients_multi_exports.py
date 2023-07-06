import secrets
import string

from ceph.waiter import WaitUntil
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

FS_NAME = "cephfs"


class NfsClusterOpFailed(Exception):
    pass


def create_nfs_cluster(client, server, nfs_name):
    """
    Creates a nfs cluster
    """

    out, rc = client.exec_command(sudo=True, cmd="ceph fs ls | awk {' print $2'} ")

    nfs_server_name = server.hostname

    # Create ceph nfs cluster
    client.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
    out, _ = client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server_name}"
    )
    if out:
        return False

    # Verify ceph nfs cluster is created
    for w in WaitUntil(timeout=100, interval=5):
        if not client.exec_command(
            sudo=True, cmd=f"ceph orch ps | grep {nfs_name}", check_ec=False
        ):
            log.info("ceph nfs cluster created successfully")
            return True
    if w.expired:
        log.info("Failed to create nfs cluster")
        return False


def setup_nfs_clients(client, exports_per_client, nfs_name, nfs_server, fs_util):
    mounts = []
    for i in range(exports_per_client):
        subvols = []
        log.info(f"Creating export {str(i)} in {client.hostname}")
        nfs_export = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )

        nfs_mount = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        subvol_grp = "subvol_group_nfs_1"
        subvol = f"subvol_nfs_{i}"
        fs = f"cephfs"
        subvols.append(subvol)
        if i == 0:
            cmd = f"ceph fs subvolumegroup create {FS_NAME} {subvol_grp}"
            client.exec_command(sudo=True, cmd=cmd)
        # Create a subvolume and subvolume group first
        cmds = [
            f"ceph fs subvolume create {FS_NAME} {subvol}"
        ]
        for cmd in cmds:
            client.exec_command(sudo=True, cmd=cmd, long_running=True)
        # else:
        # Create a new subvolume
        cmd = f"ceph fs subvolume create {FS_NAME} {subvol} {subvol_grp}"
        client.exec_command(sudo=True, cmd=cmd, long_running=True)

        # Get the subvolume path
        subvol_path, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs subvolume getpath {FS_NAME} {subvol}"
        )

        # Perform nfs export
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create {FS_NAME} {nfs_name} "
            f"{nfs_export} {fs} path={subvol_path}",
        )

        rc = fs_util.cephfs_nfs_mount(
            client, nfs_server.hostname, nfs_export, nfs_mount
        )
        if not rc:
            raise NfsClusterOpFailed("cephfs nfs export mount failed")
        mounts.append(nfs_mount)
    return mounts


def run(ceph_cluster, **kw):
    """
    Verifies nfs operations with larger number of clients and exports
    """
    export_details = {}
    subvols = []
    try:
        config = kw.get("config")
        io_ops = config.get("ops")
        no_exports = config.get("exports")
        ingress = config.get("ingress")

        # clients = ceph_cluster.get_nodes(role="client")
        # clients = ceph_cluster.get_nodes("client")
        clients = ceph_cluster.get_ceph_objects("client")
        # exports_per_client = int(no_exports) // len(clients)
        exports_per_client = no_exports
        nfs_name = "cephfs-nfs"

        log.info(f"No: Clients: {len(clients)}")
        log.info(f"No: Exports: {no_exports}")
        log.info(f"No: Exports per client: {exports_per_client}")

        # Step 1 : Create the required number of clusters
        fs_util = FsUtils(ceph_cluster)
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        nfs_server = ceph_cluster.get_nodes("nfs")[0]
        clients = ceph_cluster.get_nodes("client")
        # Step 2 : Create NFS cluster
        create_nfs_cluster(client=clients[0], server=nfs_server, nfs_name=nfs_name)

        # Step 3: Mount and export to given clients
        export_details = {}
        for client in clients:
            log.info(f"In client {client.hostname}")
            mounts = setup_nfs_clients(
                client, exports_per_client, nfs_name, nfs_server, fs_util
            )
            export_details[client] = mounts

        # Step 4: Perform IOs
        for client, nfs_mounts in export_details.items():
            log.info(f"Performing IOs {client.hostname}")
            for nfs_mount in nfs_mounts:
                log.info(f"Performing IOs on {nfs_mount}")
                client.exec_command(
                    sudo=True,
                    cmd=f"for i in create read"
                        f"create symlink stat chmod ls-l delete cleanup  ; "
                        f"do python3 /home/cephuser/smallfile/smallfile_cli.py "
                        f"--operation $i --threads 8 --file-size 10240 "
                        f"--files 10 --top {nfs_mount} ; done",
                    long_running=True,
                )
        return 0

    except Exception as e:
        log.error(e)
        return 1
    finally:
        log.info("Cleaning up")
        for client, nfs_mounts in export_details:
            for nfs_mount in nfs_mounts:
                client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}/*")
                client.exec_command(sudo=True, cmd=f"umount {nfs_mount}")
            for subvol in subvols:
                subvol_grp = "subvol_group_nfs_1"
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume rm {FS_NAME} {subvol}",
                    check_ec=False,
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume rm {FS_NAME} {subvol} {subvol_grp}",
                    check_ec=False,
                )
