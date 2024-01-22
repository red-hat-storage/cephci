from datetime import datetime
from threading import Thread
from time import sleep

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import check_coredump_generated, get_ip_from_node, reboot_node
from utility.log import Log

log = Log(__name__)

ceph_cluster_obj = None
setup_start_time = None
GANESHA_CONF_PATH = "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"


class NfsCleanupFailed(Exception):
    pass


def setup_nfs_cluster(
    clients,
    nfs_server,
    port,
    version,
    nfs_name,
    nfs_mount,
    fs_name,
    export,
    fs,
    ha=False,
    vip=None,
    ceph_cluster=None,
):
    # Get ceph cluter object and setup start time
    global ceph_cluster_obj
    global setup_start_time
    ceph_cluster_obj = ceph_cluster
    setup_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    setup_start_time = datetime.strptime(setup_start_time, "%Y-%m-%d %H:%M:%S")

    # Step 1: Enable nfs
    Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)
    sleep(3)

    # Step 2: Create an NFS cluster
    Ceph(clients[0]).nfs.cluster.create(
        name=nfs_name, nfs_server=nfs_server, ha=ha, vip=vip
    )
    sleep(3)

    # Step 3: Perform Export on clients
    export_list = []
    i = 0
    for client in clients:
        export_name = f"{export}_{i}"
        Ceph(client).nfs.export.create(
            fs_name=fs_name, nfs_name=nfs_name, nfs_export=export_name, fs=fs
        )
        i += 1
        export_list.append(export_name)
        sleep(1)

    # If the mount version is v3, make necessary changes
    if version == 3:
        _enable_v3_mount(export_list, nfs_name)

    # Step 4: Perform nfs mount
    # If there are multiple nfs servers provided, only one is required for mounting
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
    if ha:
        nfs_server = vip.split("/")[0]  # Remove the port

    i = 0
    for client in clients:
        client.create_dirs(dir_path=nfs_mount, sudo=True)
        if Mount(client).nfs(
            mount=nfs_mount,
            version=version,
            port=port,
            server=nfs_server,
            export=f"{export}_{i}",
        ):
            raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
        i += 1
        sleep(1)
    log.info("Mount succeeded on all clients")

    # Step 5: Enable nfs coredump to nfs nodes
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    Enable_nfs_coredump(nfs_nodes)


def cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export):
    """
    Clean up the cluster post nfs operation
    Steps:
        1. rm -rf of the content inside the mount folder --> rm -rf /mnt/nfs/*
        2. Unmount the volume
        3. rm -rf of the mount point
        4. delete export
        5. delete cluster
    Args:
        clients (ceph): Client nodes
        nfs_mount (str): nfs mount path
        nfs_name (str): nfs cluster name
        nfs_export (str): nfs export path
    """
    if not isinstance(clients, list):
        clients = [clients]

    # Check nfs coredump
    if ceph_cluster_obj:
        nfs_nodes = ceph_cluster_obj.get_nodes("nfs")
        coredump_path = "/var/lib/systemd/coredump"
        for nfs_node in nfs_nodes:
            if check_coredump_generated(nfs_node, coredump_path, setup_start_time):
                raise NfsCleanupFailed(
                    "Coredump generated post execution of the current test case"
                )

    # Wait until the rm operation is complete
    timeout, interval = 600, 10
    for client in clients:
        # Clear the nfs_mount, at times rm operation can fail
        # as the dir is not empty, this being an expected behaviour,
        # the solution is to repeat the rm operation.
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf {nfs_mount}/*", long_running=True
                )
                break
            except Exception as e:
                log.warning(f"rm operation failed, repeating!. Error {e}")
        if w.expired:
            raise NfsCleanupFailed(
                "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
            )

        log.info("Unmounting nfs-ganesha mount on client:")
        sleep(3)
        if Unmount(client).unmount(nfs_mount):
            raise OperationFailedError(f"Failed to unmount nfs on {client.hostname}")
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}")
        sleep(3)

    # Delete all exports
    for i in range(len(clients)):
        Ceph(clients[0]).nfs.export.delete(nfs_name, f"{nfs_export}_{i}")
    Ceph(clients[0]).nfs.cluster.delete(nfs_name)
    sleep(30)


def _enable_v3_mount(exports, nfs_name):
    installer = ceph_cluster_obj.get_nodes("installer")[0]

    # Get Ganesha.conf file
    cmd = f"-- cat {GANESHA_CONF_PATH} > ganesha.conf"
    CephAdm(installer).shell(cmd=cmd)

    # Edit ganesha.conf file to include the version 3 and "mount_path_pseudo = true"
    cmds = [
        "sed -i 's/Protocols = 4;/Protocols = 3, 4;/' ganesha.conf",
        r"sed -i '/Protocols = 3, 4;/a \        mount_path_pseudo = true;' ganesha.conf",
    ]
    for cmd in cmds:
        installer.exec_command(cmd=cmd, sudo=True)

    # Mount the ganesha file inside shell
    cmd = (
        "--mount ganesha.conf:/var/lib/ceph/ganesha.conf -- "
        "ceph config-key set mgr/cephadm/services/nfs/ganesha.conf -i /var/lib/ceph/ganesha.conf"
    )
    CephAdm(installer).shell(cmd=cmd)

    # Get the ganesha conf to verify the set operation
    CephAdm(installer).ceph.config_key.get(key="mgr/cephadm/services/nfs/ganesha.conf")

    # Update all export file content to accommodate v3
    for export in exports:
        export_file = f"export_{export[-1]}.conf"
        cmd = f" -- ceph nfs export info {nfs_name} {export} > {export_file}"
        CephAdm(installer).shell(cmd=cmd)

        # Add v3 to the export file
        cmd = rf"sed -i '/\"protocols\": /a \    3,' {export_file}"
        installer.exec_command(cmd=cmd, sudo=True)

        # Mount the export file with changes
        cmd = (
            f" --mount {export_file}:/var/lib/ceph/{export_file} "
            f"-- ceph nfs export apply {nfs_name} -i /var/lib/ceph/{export_file}"
        )
        CephAdm(installer).shell(cmd=cmd)

        # Apply the export
        CephAdm(installer).ceph.nfs.export.apply(
            nfs_name=nfs_name, export_conf=f"/var/lib/ceph/{export_file}"
        )

    # Redeploy nfs
    CephAdm(installer).ceph.orch.redeploy(service=f"nfs.{nfs_name}")

    # Adding a sleep for the redeployment to complete
    sleep(10)


def perform_failover(nfs_nodes, failover_node, vip):
    # Trigger reboot on the failover node
    th = Thread(target=reboot_node, args=(failover_node,))
    th.start()

    # Validate any of the other nodes has got the VIP
    flag = False

    # Remove the port from vip
    if "/" in vip:
        vip = vip.split("/")[0]

    # Perform the check with a timeout of 60 seconds
    for w in WaitUntil(timeout=60, interval=5):
        for node in nfs_nodes:
            if node != failover_node:
                assigned_ips = get_ip_from_node(node)
                log.info(f"IP addrs assigned to node : {assigned_ips}")
                # If vip is assigned, set the flag and exit
                if vip in assigned_ips:
                    flag = True
                    log.info(f"Failover success, VIP reassigned to {node.hostname}")
        if flag:
            break
    if w.expired:
        raise OperationFailedError(
            "The failover process failed and vip is not assigned to the available nodes"
        )
    # Wait for the node to complete reboot
    th.join()


def Enable_nfs_coredump(nfs_nodes, conf_file="/etc/systemd/coredump.conf"):
    """nfs_coredump
    Args:
        nfs_nodes(obj): nfs server node
        conf_file: conf file path
    """
    if not isinstance(nfs_nodes, list):
        nfs_nodes = [nfs_nodes]

    for nfs_node in nfs_nodes:
        try:
            nfs_node.exec_command(
                sudo=True, cmd=f"echo Storage=external >> {conf_file}"
            )
            nfs_node.exec_command(
                sudo=True, cmd=f"echo DefaultLimitCORE=infinity >> {conf_file}"
            )
            nfs_node.exec_command(sudo=True, cmd="systemctl daemon-reexec")
        except Exception:
            raise OperationFailedError(f"failed enable coredump for {nfs_node}")


def get_nfs_pid_and_memory(nfs_nodes):
    """get nfs-ganesha pid and memory consumption(RSS)
    Args:
        nfs_nodes(obj): nfs server node
    Returns:
        nfs_server_info(dic): {"nfs server1": ["PID","RSS(MB)"], "nfs server2": ["PID","RSS(MB)"]}
    """
    nfs_server_info = {}
    if not isinstance(nfs_nodes, list):
        nfs_nodes = [nfs_nodes]

    for nfs_node in nfs_nodes:
        try:
            pid = nfs_node.exec_command(sudo=True, cmd="pgrep ganesha")[0].strip()
            rss = nfs_node.exec_command(sudo=True, cmd=f"ps -p {pid} -o rss=")[
                0
            ].strip()
            nfs_server_info[nfs_node.hostname] = [pid, rss]
        except Exception:
            raise OperationFailedError(
                f"failed get nfs process ID and rss for {nfs_node}"
            )
    return nfs_server_info
