import json
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Thread
from time import sleep

import yaml
from looseversion import LooseVersion

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import FuseMount, Mount, Unmount
from cli.utilities.utils import check_coredump_generated, get_ip_from_node, reboot_node
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

ceph_cluster_obj = None
setup_start_time = None


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
    active_standby=False,
):
    # Get ceph cluter object and setup start time
    global ceph_cluster_obj
    global setup_start_time
    ceph_cluster_obj = ceph_cluster
    installer_node = ceph_cluster.get_nodes("installer")[0]
    log.info("Logged into installer Node...")

    setup_start_time, err = installer_node.exec_command(
        sudo=True, cmd="date +'%Y-%m-%d %H:%M:%S'"
    )
    setup_start_time = setup_start_time.strip()
    setup_start_time = datetime.strptime(setup_start_time, "%Y-%m-%d %H:%M:%S")

    # Step 1: Enable nfs
    version_info = installer_node.exec_command(
        sudo=True, cmd="cephadm shell -- rpm -qa | grep nfs"
    )
    log.info("nfs info: %s", version_info)
    Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)
    sleep(3)

    nfs_nodes = ceph_cluster.get_nodes("nfs")

    # Step 2: Create an NFS cluster
    # Extract NFS version from version parameter (could be "3", "4", "4.2", etc.)
    nfs_version = None
    if version:
        # Check if version contains "3" (e.g., "3", "3.0", or list containing 3)
        if isinstance(version, (list, tuple)):
            nfs_version = 3 if 3 in version else None
        elif isinstance(version, (int, str)):
            # Convert to string and check if it starts with "3"
            version_str = str(version)
            if version_str.startswith("3") or version_str == "3":
                nfs_version = 3
                # for NFSv3, We need to run cephadm prepare-host on the nfs nodes
                # prepare the host and check if rpcbin service is running
                for nfs_node in nfs_nodes:
                    nfs_node.exec_command(sudo=True, cmd="cephadm prepare-host")

    create_kwargs = {"nfs_version": nfs_version}

    Ceph(clients[0]).nfs.cluster.create(
        name=nfs_name,
        nfs_server=nfs_server,
        ha=ha,
        vip=vip,
        active_standby=active_standby,
        nfs_nodes_obj=nfs_nodes,
        **create_kwargs,
    )
    sleep(3)

    # Step 3: Perform Export on clients
    export_list = []
    i = 0
    for client in clients:
        export_name = "{export}_{i}".format(export=export, i=i)
        Ceph(client).nfs.export.create(
            fs_name=fs_name, nfs_name=nfs_name, nfs_export=export_name, fs=fs
        )
        i += 1
        all_exports = Ceph(client).nfs.export.ls(nfs_name)
        if export_name not in all_exports:
            raise OperationFailedError(
                f"Export {export_name} not found in the list of exports {all_exports}"
            )
        export_list.append(export_name)
        sleep(1)

    # Get the mount versions specific to clients
    mount_versions = _get_client_specific_mount_versions(version, clients)

    # Step 4: Perform nfs mount
    # If there are multiple nfs servers provided, only one is required for mounting
    # Check if the mount version v3 is included in the list of versions and
    # if the mount version is v3, make necessary changes
    if 3 in mount_versions.keys():
        ports_to_open = ["portmapper", "mountd"]
        for nfs_node in nfs_nodes:
            open_mandatory_v3_ports(nfs_node, ports_to_open)
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
    if ha:
        nfs_server = vip.split("/")[0]  # Remove the port

    i = 0
    for version, clients in mount_versions.items():
        for client in clients:
            client.create_dirs(dir_path=nfs_mount, sudo=True)
            if mount_retry(client, nfs_mount, version, port, nfs_server, export_name):
                log.info("Mount succeeded on %s" % client.hostname)
            i += 1
            sleep(1)
    log.info("Mount succeeded on all clients")

    try:
        cmd_used = None
        services = [
            x["service_name"]
            for x in json.loads(Ceph(clients[0]).orch.ls(format="json"))
        ]
        # check nfs cluster and services are up
        if Ceph(clients[0]).nfs.cluster.ls():
            cmd_used = "ceph nfs cluster ls"
            log.info(
                "NFS cluster %s created successfully using %s"
                % (Ceph(clients[0]).nfs.cluster.ls()[0], cmd_used)
            )
        # verifying with orch cmd
        if [x for x in services if x.startswith("nfs")]:
            cmd_used = "ceph orch ls"
            log.info("NFS services are up and running from cmd %s" % cmd_used)

        if Ceph(clients[0]).execute("ps aux | grep nfs-ganesha")[0]:
            cmd_used = "ps aux | grep nfs-ganesha"
            log.info("NFS daemons are up and running verifying using %s" % cmd_used)
    except Exception as e:
        log.error(
            "Failed to verify nfs cluster and services %s cmd used: %s" % (e, cmd_used)
        )

    # Step 5: Enable nfs coredump to nfs nodes
    Enable_nfs_coredump(nfs_nodes)


def cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=None):
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
    nfs_log_parser(client=clients[0], nfs_node=nfs_nodes, nfs_name=nfs_name)

    for client in clients:
        # Clear the nfs_mount, at times rm operation can fail
        # as the dir is not empty, this being an expected behaviour,
        # the solution is to repeat the rm operation.
        if not mount_cleanup_retry(client, nfs_mount):
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
    check_nfs_daemons_removed(clients[0])

    # Delete the subvolume
    for i in range(len(clients)):
        cmd = "ceph fs subvolume ls cephfs --group_name ganeshagroup"
        out = client.exec_command(sudo=True, cmd=cmd)
        json_string, _ = out
        data = json.loads(json_string)
        # Extract names of subvolume
        for item in data:
            subvol = item["name"]
            cmd = f"ceph fs subvolume rm cephfs {subvol} --group_name ganeshagroup"
            client.exec_command(sudo=True, cmd=cmd)

    # Delete the subvolume group
    cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
    client.exec_command(sudo=True, cmd=cmd)


def setup_custom_nfs_cluster_multi_export_client(
    clients,
    nfs_server,
    port,
    version,
    nfs_name,
    fs_name,
    fs,
    nfs_mount,
    nfs_export,
    ha=False,
    vip=None,
    export_num=None,
    ceph_cluster=None,
    active_standby=None,
    **kwargs,
):
    # Get ceph cluter object and setup start time
    global ceph_cluster_obj
    global setup_start_time
    ceph_cluster_obj = ceph_cluster
    setup_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    setup_start_time = datetime.strptime(setup_start_time, "%Y-%m-%d %H:%M:%S")

    # Step 1: Enable nfs
    installer_node = ceph_cluster.get_nodes("installer")[0]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    version_info = installer_node.exec_command(
        sudo=True, cmd="cephadm shell -- rpm -qa | grep nfs"
    )
    log.info("nfs info: %s", version_info)
    Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)
    sleep(3)

    # Step 2: Create an NFS cluster
    # Extract NFS version from version parameter (could be "3", "4", "4.2", etc.)
    nfs_version = None
    if version:
        # Check if version contains "3" (e.g., "3", "3.0", or list containing 3)
        if isinstance(version, (list, tuple)):
            nfs_version = 3 if 3 in version else None
        elif isinstance(version, (int, str)):
            # Convert to string and check if it starts with "3"
            version_str = str(version)
            if version_str.startswith("3") or version_str == "3":
                nfs_version = 3

    create_kwargs = {"nfs_version": nfs_version}
    if "in-file" in kwargs:
        create_kwargs["in-file"] = kwargs["in-file"]

    Ceph(clients[0]).nfs.cluster.create(
        name=nfs_name,
        nfs_server=nfs_server,
        ha=ha,
        vip=vip,
        active_standby=active_standby,
        nfs_nodes_obj=nfs_nodes,
        **create_kwargs,
    )
    sleep(3)

    # Step 3: Perform Export on clients
    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, export_num
    )

    # Create the export and mount points for each client
    for client_num in range(len(clients)):
        for export_num in range(
            len(client_export_mount_dict[clients[client_num]]["export"])
        ):
            export_name = client_export_mount_dict[clients[client_num]]["export"][
                export_num
            ]
            mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                export_num
            ]
            Ceph(clients[client_num]).nfs.export.create(
                fs_name=fs_name,
                nfs_name=nfs_name,
                nfs_export=export_name,
                fs=fs,
                **({"enctag": kwargs["enctag"]} if "enctag" in kwargs else {}),
            )
            all_exports = Ceph(clients[0]).nfs.export.ls(nfs_name)
            if export_name not in all_exports:
                raise OperationFailedError(
                    f"Export {export_name} not found in the list of exports {all_exports}"
                )
            sleep(1)
            # Get the mount versions specific to clients
            mount_versions = _get_client_specific_mount_versions(version, clients)
            # Step 4: Perform nfs mount
            # If there are multiple nfs servers provided, only one is required for mounting
            if isinstance(nfs_server, list):
                nfs_server = nfs_server[0]
            if ha:
                nfs_server = vip.split("/")[0]  # Remove the port
            for version, clients in mount_versions.items():
                clients[client_num].create_dirs(dir_path=mount_name, sudo=True)
                if Mount(clients[client_num]).nfs(
                    mount=mount_name,
                    version=version,
                    port=port,
                    server=nfs_server,
                    export=export_name,
                ):
                    raise OperationFailedError(
                        "Failed to mount nfs on %s" % clients[client_num].hostname
                    )
                sleep(1)
        log.info("Mount succeeded on all clients")

    try:
        cmd_used = None
        services = [
            x["service_name"]
            for x in json.loads(Ceph(clients[0]).orch.ls(format="json"))
        ]
        # check nfs cluster and services are up
        if Ceph(clients[0]).nfs.cluster.ls():
            cmd_used = "ceph nfs cluster ls"
            log.info(
                "NFS cluster %s created successfully using %s"
                % (Ceph(clients[0]).nfs.cluster.ls()[0], cmd_used)
            )
        # verifying with orch cmd
        if [x for x in services if x.startswith("nfs")]:
            cmd_used = "ceph orch ls"
            log.info("NFS services are up and running from cmd %s" % cmd_used)

        if Ceph(clients[0]).execute("ps aux | grep nfs-ganesha")[0]:
            cmd_used = "ps aux | grep nfs-ganesha"
            log.info("NFS daemons are up and running verifying using %s" % cmd_used)
    except Exception as e:
        log.error(
            "Failed to verify nfs cluster and services %s cmd used: %s" % (e, cmd_used)
        )

    nfs_nodes = ceph_cluster.get_nodes("nfs")

    # Step 5: Enable nfs coredump to nfs nodes
    Enable_nfs_coredump(nfs_nodes)


def exports_mounts_perclient(clients, nfs_export, nfs_mount, export_num) -> dict:
    """
    Args:
        clients (list): List of client nodes
        nfs_export (str): NFS export path
        nfs_mount (str): NFS mount path
        export_num (int): Number of exports to create
    Returns:
        dict: Dictionary mapping each client to its corresponding exports and mounts
    """
    export_list = [f"{nfs_export}_{i}" for i in range(export_num)]
    mount_list = [f"{nfs_mount}_{i}" for i in range(export_num)]

    # Divide exports among clients
    num_clients = len(clients)
    exports_per_client = len(export_list) // num_clients
    remainder = len(export_list) % num_clients
    client_exports = []
    client_mounts = []
    start = 0

    for i in range(num_clients):
        end = start + exports_per_client + (1 if i < remainder else 0)
        client_exports.append(export_list[start:end])
        client_mounts.append(mount_list[start:end])
        start = end

    client_export_mount_dict = {}
    for i in range(len(clients)):
        client_export_mount_dict.update(
            {
                clients[i]: {
                    "export": client_exports[i],
                    "mount": client_mounts[i],
                }
            }
        )
    return client_export_mount_dict


def cleanup_custom_nfs_cluster_multi_export_client(
    clients, nfs_mount, nfs_name, nfs_export, export_num, nfs_nodes=None
):
    """
    Clean up the cluster post nfs operation
    """
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

    nfs_log_parser(client=clients[0], nfs_node=nfs_nodes, nfs_name=nfs_name)

    client_export_mount_dict = exports_mounts_perclient(
        clients, nfs_export, nfs_mount, export_num
    )

    for client_num in range(len(clients)):
        for export_num in range(
            len(client_export_mount_dict[clients[client_num]]["export"])
        ):
            export_name = client_export_mount_dict[clients[client_num]]["export"][
                export_num
            ]
            mount_name = client_export_mount_dict[clients[client_num]]["mount"][
                export_num
            ]
            client = list(client_export_mount_dict.keys())[client_num]
            # Clear the nfs_mount, at times rm operation can fail
            # as the dir is not empty, this being an expected behaviour,
            # the solution is to repeat the rm operation.
            if not mount_cleanup_retry(client, mount_name):
                raise NfsCleanupFailed(
                    "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
                )

            log.info("Unmounting nfs-ganesha mount on client:")
            sleep(3)
            if Unmount(client).unmount(mount_name):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            log.info("Removing nfs-ganesha mount dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {mount_name}")
            sleep(3)

            # Delete all exports
            Ceph(clients[0]).nfs.export.delete(nfs_name, export_name)

    Ceph(clients[0]).nfs.cluster.delete(nfs_name)
    sleep(30)
    check_nfs_daemons_removed(clients[0])

    # Delete the subvolume
    for i in range(len(clients)):
        cmd = "ceph fs subvolume ls cephfs --group_name ganeshagroup"
        out = client.exec_command(sudo=True, cmd=cmd)
        json_string, _ = out
        data = json.loads(json_string)
        # Extract names of subvolume
        for item in data:
            subvol = item["name"]
            cmd = f"ceph fs subvolume rm cephfs {subvol} --group_name ganeshagroup"
            client.exec_command(sudo=True, cmd=cmd)

    # Delete the subvolume group
    cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
    client.exec_command(sudo=True, cmd=cmd)


def _get_client_specific_mount_versions(versions, clients):
    # Identify the multi mount versions specific to clients
    version_dict = {}
    if not isinstance(versions, list):
        version_dict[versions] = clients
        return version_dict
    ctr = 0
    for entry in versions:
        ver = list(entry.keys())[0]
        count = list(entry.values())[0]
        version_dict[ver] = clients[ctr : ctr + int(count)]
        ctr = ctr + int(count)
    return version_dict


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
    for w in WaitUntil(timeout=120, interval=5):
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


def permission(client, nfs_name, nfs_export, old_permission, new_permission):
    # Change export permissions to RO
    out = Ceph(client).nfs.export.get(nfs_name, f"{nfs_export}_0")
    client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
    client.exec_command(
        sudo=True,
        cmd=f'sed -i \'s/"access_type": "{old_permission}"/"access_type": "{new_permission}"/\' export.conf',
    )
    Ceph(client).nfs.export.apply(nfs_name, "export.conf")

    # Wait till the NFS daemons are up
    sleep(10)


def enable_v3_locking(installer, nfs_name, nfs_node, nfs_server_name):
    # Enable the NLM support for v3 Locking
    # # --enable-nfsv3 flag was introduced after 8.1z4 (19.2.1-292); only add for newer
    ceph_version = get_ceph_version(installer, prefix_cephadm=True)
    log.info(f"ceph_version: {ceph_version}")
    spec_lines = ["    enable_nlm: true"]
    if LooseVersion(ceph_version) > LooseVersion("19.2.1-292"):
        log.info(
            f"Ceph version {ceph_version} is above 19.2.1-292, adding enable_nfsv3: true"
        )
        spec_lines.append("    enable_nfsv3: true")
    spec_block = "\n".join(spec_lines)
    content = f"""service_type: nfs
service_id: {nfs_name}
placement:
    hosts:
        - {nfs_server_name}
spec:
{spec_block}"""

    with open("ganesha.yaml", "w") as f:
        yaml.dump(content, f)
    log.info(content)

    # Adding the configurations into the ganesha.yaml file.
    cmd = f"echo '{content}' >> ganesha.yaml"
    CephAdm(installer).shell(cmd=cmd)

    # Mount the export file inside shell and apply changes
    cmd = (
        "--mount ganesha.yaml:/var/lib/ceph/ganesha.yaml -- "
        "ceph orch apply -i /var/lib/ceph/ganesha.yaml"
    )
    CephAdm(installer).shell(cmd=cmd)

    # Restart the NFS Ganesha service
    CephAdm(installer).ceph.orch.redeploy(service=f"nfs.{nfs_name}")

    # Wait till the NFS daemons are up
    sleep(10)

    verify_nfs_ganesha_service(node=installer, timeout=300)
    log.info("NFS Ganesha spec file applied successfully.")

    # Start the rpc-statd service on server
    cmd = "sudo systemctl start rpc-statd"
    nfs_node.exec_command(cmd=cmd)

    # Open the NLM port
    ports_to_open = ["nlockmgr"]
    open_mandatory_v3_ports(nfs_node, ports_to_open)


def getfattr(client, file_path, attribute_name=None):
    # Fetch the extended attribute for file or directory
    """
    Args:
    attribute_name (str): Specific attribute name to retrieve. If None, retrieves all attributes.
    file_path (str): Path to the file/dir whose extended attribute is to be retrieved.
    """
    cmd = f"getfattr -d {file_path}"
    if attribute_name:
        cmd += " -n user.{attribute_name}"
    out = client.exec_command(sudo=True, cmd=cmd)
    log.info(out)
    return out


def setfattr(client, file_path, attribute_name, attribute_value):
    """
    Sets the value of an extended attribute on a file using setfattr command.

    Args:
    - file_path (str): Path to the file/dir where the extended attribute is to be set.
    - attribute_name (str): Name of the extended attribute to set.
    - attribute_value (str): Value to set for the extended attribute.
    """
    cmd = f"setfattr -n user.{attribute_name} -v {attribute_value} {file_path}"
    out = client.exec_command(sudo=True, cmd=cmd)
    return out


def removeattr(client, file_path, attribute_name):
    """
    Remove the value of an extended attribute on a file.

    Args:
    - file_path (str): Path to the file/dir where the extended attribute needs tp be removed.
    - attribute_name (str): Name of the extended attribute to be removed.
    """
    cmd = f"setfattr -x user.{attribute_name} {file_path}"
    out = client.exec_command(sudo=True, cmd=cmd)
    return out


def check_nfs_daemons_removed(client):
    """
    Check if NFS daemons are removed.
    Wait until there are no NFS daemons listed by 'ceph orch ls'.
    """
    while True:
        try:
            cmd = "ceph orch ls | grep nfs"
            out = client.exec_command(sudo=True, cmd=cmd)

            if out:
                log.info("NFS daemons are still present. Waiting...")
                sleep(10)  # Wait before checking again
            else:
                log.info("All NFS daemons have been removed.")
                break
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            break


def create_nfs_via_file_and_verify(
    installer_node, nfs_objects, timeout, nfs_nodes=None
):
    """
    Create a temporary YAML file with NFS Ganesha configuration.
    Args:
        installer_node: The node where the NFS Ganesha configuration will be applied.
                      Can be a single node or list of nodes (first node will be used)
        nfs_objects: List of NFS Ganesha configuration objects.
    Returns:
        str: Path to the temporary YAML file.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")

    # Handle case where installer_node is a list
    if isinstance(installer_node, list):
        installer_node = installer_node[0]

    spec_file = installer_node.remote_file(
        sudo=True, file_name=temp_file.name, file_mode="wb"
    )
    spec = yaml.dump_all(nfs_objects, sort_keys=False, indent=2).encode("utf-8")
    spec_file.write(spec)
    spec_file.flush()

    try:
        pos_args = []
        CephAdm(installer_node, mount="/tmp/").ceph.orch.apply(
            input=temp_file.name, check_ec=True, pos_args=pos_args
        )
        verify_nfs_ganesha_service(node=installer_node, timeout=timeout)
        log.info("NFS Ganesha spec file applied successfully.")
        return True
    except Exception as err:
        log.error(f"Failed to apply NFS Ganesha spec file: {err}")


def delete_nfs_clusters_in_parallel(installer_node, timeout=300, clusters=None):
    """
    Delete NFS clusters in batch.
    Args:
        installer_node: The node where the NFS Ganesha configuration will be applied.
        nfs_objects: List of NFS Ganesha configuration objects.
    """
    if not clusters:
        clusters = CephAdm(installer_node).ceph.nfs.cluster.ls()
    else:
        clusters = clusters
    with ThreadPoolExecutor(max_workers=None) as executor:
        futures = [
            executor.submit(
                CephAdm(installer_node).ceph.nfs.cluster.delete,
                cluster,
            )
            for cluster in clusters
        ]
        for future in futures:
            future.result()
    log.info("All NFS clusters deletion initiated.")
    # Check if any NFS Ganesha service is still running
    for w in WaitUntil(timeout=timeout, interval=5):
        result = json.loads(
            CephAdm(installer_node).ceph.orch.ls(format="json", service_type="nfs")
        )
        if all(x["status"]["running"] == 0 for x in result) or not result:
            log.info("sleep(10)  # Allow some time for the service to stabilize")
            sleep(10)
            log.info(
                "\n"
                + "=" * 30
                + "\n"
                + "All NFS Ganesha services are down. Time taken : -- %s seconds \n"
                + "=" * 30,
                w._attempt * w.interval,
            )
            return True
        else:
            log.error(
                "\n \n NFS Ganesha services are still running after deletion trying again...... "
                "Time remaining : -- %s seconds \n",
                timeout - (w._attempt * w.interval),
            )
            log.debug("Current status: %s", result)
    if w.expired:
        raise OperationFailedError(
            "NFS Ganesha services are still running after deletion. Timeout expired. -- %s seconds"
            % timeout
        )


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def open_mandatory_v3_ports(nfs_node, ports_to_open):
    """
    Open the required ports for v3 mount (portmapper, mountd, nlockmgr) based on rpcinfo output.
    """
    # Initialize the service_ports_mapping dictionary to store the port lists
    service_ports_mapping = {"portmapper": None, "mountd": None, "nlockmgr": None}

    # Execute rpcinfo command to get the port information
    cmd = "sudo rpcinfo -p"
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd)
    log.debug(f"rpcinfo output: {out}")
    if not out:
        log.error(f"Failed to execute rpcinfo -p on {nfs_node}")
        return

    # Split the output into lines and iterate over them
    lines = out.splitlines()
    port_mapper_found = False  # Have the first portmapper port

    for line in lines:
        # Skip header and empty lines
        if "program vers proto port service" in line or not line.strip():
            continue

        # Split line into columns
        columns = line.split()
        port, service = columns[3], columns[4]

        # Check for relevant services
        if service == "portmapper" and not port_mapper_found:
            service_ports_mapping["portmapper"] = port
            port_mapper_found = True
        elif service == "mountd":
            service_ports_mapping["mountd"] = port
        elif service == "nlockmgr":
            service_ports_mapping["nlockmgr"] = port

    # Open firewall ports based on services in ports_to_open
    for service in ports_to_open:
        port_to_open = service_ports_mapping.get(service)

        if port_to_open:
            # Open the port using the firewall command
            nfs_node.exec_command(
                sudo=True,
                cmd=f"sudo firewall-cmd --zone=public --add-port={port_to_open}/tcp --permanent",
            )
            log.info(f"Opened {service} port: {port_to_open}")
        else:
            raise OperationFailedError(f"{service} port not found")

    # Reload the firewall to apply the changes
    nfs_node.exec_command(sudo=True, cmd="sudo firewall-cmd --reload")
    log.info("Firewall rules reloaded.")


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def mount_retry(client, mount_name, version, port, nfs_server, export_name):
    if Mount(client).nfs(
        mount=mount_name,
        version=version,
        port=port,
        server=nfs_server,
        export=export_name,
    ):
        raise OperationFailedError("Failed to mount nfs on %s" % {export_name.hostname})
    return True


@retry(OperationFailedError, tries=5, delay=10, backoff=2)
def mount_cleanup_retry(client, mount_name):
    _, err = client.exec_command(sudo=True, cmd=f"rm -rf {mount_name}/*", timeout=120)
    if err:
        raise OperationFailedError("Failed to clenaup the mount directory")
    return True


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def fuse_mount_retry(client, mount, **kwargs):
    """
    Retry mounting NFS using FUSE.
    Args:
        client: Client node where the mount is to be performed.
        mount: Mount point path.
        **kwargs: Additional arguments for the Mount method.
    """
    if FuseMount(client).mount(
        client_hostname=client.hostname, mount_point=mount, **kwargs
    ):
        raise OperationFailedError("Failed to fuse mount nfs on %s" % client.hostname)
    return True


def verify_nfs_ganesha_service(node, timeout):
    """
    Verify the status of NFS Ganesha service.
    Args:
        node: Installer Node.
    Returns:
        bool: True if the service is in the expected state, False otherwise.
    """
    interval = 5

    for w in WaitUntil(timeout=timeout, interval=interval):
        result = json.loads(
            CephAdm(node).ceph.orch.ls(format="json", service_type="nfs")
        )
        if all(x["status"]["running"] == x["status"]["size"] for x in result):
            log.info(
                "\n"
                + "=" * 30
                + "\n"
                + "NFS Ganesha service is up and running. Time taken : -- %s seconds \n"
                + "=" * 30,
                w._attempt * w.interval,
            )
            log.info("sleep(20)  # Allow some time for the service to stabilize")
            sleep(20)  # Allow some time for the service to stabilize
            return True
        else:
            log.info(
                "\n \n NFS Ganesha service is not running as expected, retrying...... "
                "Time remaining : -- %s seconds \n",
                timeout - (w._attempt * w.interval),
            )
            log.debug("Current status: %s", result)
    log.error("\n NFS Ganesha service is not running as expected.")
    if w.expired:
        raise OperationFailedError(
            "NFS daemons check failed Timeout expired. -- %s seconds" % timeout
        )


def create_multiple_nfs_instance_via_spec_file(
    spec, replication_number, installer, timeout=300
):
    """
    Create multiple NFS Ganesha service instances from a base spec file.

    This function takes a service specification dictionary (as used in Ceph orchestrator),
    clones it `replication_number` times, and increments the `service_id`, `port`, and
    `monitoring_port` fields for each replica. The generated spec list is then used to
    deploy the instances on the Ceph cluster via `create_nfs_via_file_and_verify`.

    Args:
        spec (dict): Base NFS Ganesha service spec containing:
                     - service_type (str): common name for nfs instances
                     - service_id (str): Base identifier for the service
                     - placement.host_pattern (str): Node(s) to host the service(s)
                     - spec.port (int): Base NFS server port
                     - spec.monitoring_port (int): Base prometheus exporter port
        replication_number (int): Number of NFS instances to create.
        installer (CephAdm or str): Installer node or handler used for deployment.
        timeout (int, optional): Timeout in seconds for instance creation and verification.
                                 Defaults to 300.

    Returns:
        int: 0 on success, 1 on failure.
    """
    try:
        new_objects = []
        for i in range(replication_number):
            service_id = f"{spec['service_id']}{i if i != 0 else ''}"
            port = spec["spec"]["port"] + i
            monitoring_port = spec["spec"]["monitoring_port"] + i

            placement = {}
            if "host_pattern" in spec.get("placement", {}):
                placement["host_pattern"] = spec["placement"]["host_pattern"]
            elif "label" in spec.get("placement", {}):
                placement["label"] = spec["placement"]["label"]

            new_object = {
                "service_type": spec["service_type"],
                "service_id": service_id,
                "placement": placement,
                "spec": {
                    "port": port,
                    "monitoring_port": monitoring_port,
                    "kmip_cert": spec["kmip_cert"][0],
                    "kmip_key": spec["kmip_key"][0],
                    "kmip_ca_cert": spec["kmip_ca_cert"][0],
                    "kmip_host_list": spec["kmip_host_list"],
                },
            }

            new_objects.append(new_object)

            log.debug(
                f"Prepared spec for NFS instance {i + 1}/{replication_number}: "
                f"service_id={service_id}, port={port}, monitoring_port={monitoring_port}"
            )

        log.info(
            f"Creating {replication_number} NFS Ganesha instance(s) "
            f"using base spec service_id='{spec['service_id']}'..."
        )
        log.debug(f"Full generated specs: {new_objects}")

        # Deploy the NFS service(s) via orchestrator
        if not create_nfs_via_file_and_verify(installer, new_objects, timeout):
            log.error("NFS Ganesha instance creation failed during verification.")
            return 1

        log.info(
            f"Successfully created {replication_number} NFS Ganesha instance(s) "
            f"with IDs: {[obj['service_id'] for obj in new_objects]}"
        )
        return new_objects

    except KeyError as e:
        log.error(f"Missing required key in spec: {e}")
        return 1
    except Exception as e:
        log.error(f"Unexpected error creating NFS Ganesha instances: {e}")
        return 1


def dynamic_cleanup_common_names(
    clients, mounts_common_name, clusters=None, mount_point="/mnt/", group_name=None
):
    """
    Dynamically clean up NFS resources by common name.

    Steps performed:
        1. Unmount and remove all directories matching the given mount name prefix on all clients.
        2. Delete all NFS exports in the specified clusters.
        3. Delete all CephFS subvolumes associated with the exports.
        4. Delete all NFS clusters.
        5. Check for NFS coredumps and raise if found.

    Args:
        clients (list): List of client nodes.
        mounts_common_name (str): Prefix for mount directories to clean up.
        clusters (list, optional): List of NFS cluster names to clean up. If None, will auto-discover.
        mount_point (str, optional): Base mount directory. Default is "/mnt/".
        group_name (str, optional): CephFS subvolume group name.
    Raises:
        NfsCleanupFailed: If coredump is found or cleanup times out.
        OperationFailedError: If unmount or resource deletion fails.
    """
    if not isinstance(clients, list):
        clients = [clients]
    # Check for NFS coredump on all NFS nodes before cleanup
    if ceph_cluster_obj:
        nfs_nodes = ceph_cluster_obj.get_nodes("nfs")
        coredump_path = "/var/lib/systemd/coredump"
        for nfs_node in nfs_nodes:
            if check_coredump_generated(nfs_node, coredump_path, setup_start_time):
                log.error(
                    f"Coredump found on {nfs_node.hostname} after test execution."
                )
                raise NfsCleanupFailed(
                    "Coredump generated post execution of the current test case"
                )

    # Step 1: Unmount and remove all matching mount directories on each client
    for client in clients:
        log.info(f"Starting mount cleanup on client {client.hostname}")
        for w in WaitUntil(timeout=600, interval=10):
            # Find all mounts matching the common name prefix
            mounts = [
                x
                for x in client.get_dir_list(mount_point)
                if x.startswith(mounts_common_name)
            ]
            if not mounts:
                log.info(
                    f"No mounts found with prefix '{mounts_common_name}' on {client.hostname}"
                )
                break
            for mount in mounts:
                log.info(f"Removing files from mount {mount} on {client.hostname}")
                client.remove_file(f"{mount_point}{mount}/*")
                log.info(f"Unmounting {mount_point}{mount} on {client.hostname}")
                if Unmount(client).unmount(mount_point + mount):
                    log.error(
                        f"Failed to unmount {mount_point}{mount} on {client.hostname}"
                    )
                    raise OperationFailedError(
                        f"Failed to unmount {mount_point}{mount} nfs on {client.hostname}"
                    )
                client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}{mount}")
                log.info(
                    f"Removed mount directory {mount_point}{mount} on {client.hostname}"
                )
            break
        if w.expired:
            log.error(f"Timeout while cleaning up mounts on {client.hostname}")
            raise NfsCleanupFailed(
                "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
            )

    # Step 2: Delete all exports in each cluster
    client = clients[0]
    if not clusters:
        clusters = Ceph(client).nfs.cluster.ls()
        log.info(f"Auto-discovered clusters for cleanup: {clusters}")
    if not isinstance(clusters, list):
        clusters = [clusters]

    # Step 3: Delete all CephFS subvolumes associated with the exports
    subvols = json.loads(
        Ceph(client).fs.sub_volume.ls(volume="cephfs", group_name=group_name)
    )
    log.info(Ceph(clients[0]).orch.ls())
    for cluster in clusters:
        exports = json.loads(Ceph(client).nfs.export.ls(cluster))
        log.info(f"Found {len(exports)} exports in cluster '{cluster}': {exports}")

        for export in exports:
            Ceph(client).nfs.export.delete(cluster, export)
            log.info(f"Deleted export '{export}' in cluster '{cluster}'")

        sub_vols_infos = []
        for subvol in subvols:
            subvol_info = Ceph(client).fs.sub_volume.info(
                volume="cephfs", subvolume=subvol["name"], group_name=group_name
            )
            sub_vols_infos.append(subvol_info)
            for export in exports:
                # Check if export is referenced in the subvolume's pool_namespace
                if re.findall(export, subvol_info["pool_namespace"])[1:]:
                    log.info(f"Export '{export}' found in subvolume '{subvol['name']}'")
                    Ceph(client).fs.sub_volume.rm(
                        volume="cephfs",
                        subvolume=subvol["name"],
                        group_name=group_name,
                        force=True,
                    )
                    log.info(
                        f"Deleted subvolume '{subvol['name']}' in group '{group_name}'"
                    )

        # Step 4: Remove the NFS cluster itself
        Ceph(clients[0]).nfs.cluster.delete(cluster)
        log.info(f"Deleted NFS cluster '{cluster}'")

    # Step 5: Wait for all NFS daemons to be removed
    log.info("Waiting for all NFS daemons to be removed from the cluster...")
    sleep(30)
    check_nfs_daemons_removed(client)
    log.info("Dynamic cleanup of NFS resources completed")


def get_ganesha_info_from_container(installer, nfs_service_name, nfs_host_node):
    """
    Retrieves Ganesha NFS daemon information from a containerized NFS service.
    This function extracts the process ID (PID) and related container information
    for a Ganesha NFS daemon running inside a container managed by Ceph orchestrator.
    Args:
        installer: CephAdm installer object used to execute ceph orchestrator commands
        nfs_service_name (str): Name of the NFS service to query (without 'nfs.' prefix)
        nfs_host_node: Host node object where the NFS container is running, used for
                       executing commands on the host system
    Returns:
        tuple: A tuple containing:
            - int or None: Host PID of the Ganesha process, or None if not found
            - dict or None: Dictionary containing container information with keys:
                - 'container_id': Container ID of the NFS service
                - 'hostname': Hostname where the container is running
                - 'container_pid': PID of the container process on the host
                - 'ganesha_pid_in_container': PID of Ganesha process inside container
                - 'host_ganesha_pid': Host PID of the Ganesha process
              Returns None if information cannot be retrieved
    Raises:
        Exception: Catches and logs any exceptions that occur during the process,
                   returning (None, None) on failure
    Note:
        This function uses podman commands to inspect containers and find process IDs.
        It requires sudo privileges on the host node to execute container inspection
        and process listing commands.

    """
    try:
        log.info(f"Getting Ganesha PID for service: {nfs_service_name}")

        # Get NFS service container information
        out = CephAdm(installer).ceph.orch.ps(
            service_name=f"nfs.{nfs_service_name}", format="json"
        )

        nfs_processes = json.loads(out)

        if not nfs_processes:
            log.error(f"No running processes found for NFS service {nfs_service_name}")
            return (None, None)

        nfs_process = nfs_processes[0]
        container_id = nfs_process.get("container_id", "")
        hostname = nfs_process.get("hostname", "")

        if not container_id:
            log.error("Container ID not found for NFS service")
            return (None, None)

        log.info(f"NFS container ID: {container_id} on host: {hostname}")

        # Method 1: Get PID from container inspection
        cmd = f"podman inspect {container_id} --format '{{{{.State.Pid}}}}'"
        out, err = nfs_host_node.exec_command(cmd=cmd, sudo=True)

        if not err and out.strip().isdigit():
            container_pid = int(out.strip())
            log.info(f"Container PID: {container_pid}")

            # Method 2: Find Ganesha process inside container
            cmd = f"podman exec {container_id} ps aux | grep '[g]anesha.nfsd' | awk '{{print $2}}'"
            out, err = nfs_host_node.exec_command(cmd=cmd, sudo=True)

            if not err and out.strip():
                ganesha_pid_in_container = out.strip()
                log.info(f"Ganesha PID inside container: {ganesha_pid_in_container}")

                # Get the actual host PID (container PID + offset usually)
                # For most container runtimes, we can find the host PID through /proc
                cmd = "ps -ef | grep '[g]anesha.nfsd' | grep -v grep | awk '{print $2}'"
                out, err = nfs_host_node.exec_command(cmd=cmd, sudo=True)

                if not err and out.strip():
                    host_ganesha_pid = out.strip().split("\n")[0]
                    log.info(f"Ganesha host PID: {host_ganesha_pid}")

                    return (
                        int(host_ganesha_pid),
                        {
                            "container_id": container_id,
                            "hostname": hostname,
                            "container_pid": container_pid,
                            "ganesha_pid_in_container": ganesha_pid_in_container,
                            "host_ganesha_pid": host_ganesha_pid,
                        },
                    )

        log.error("Could not determine Ganesha PID")
        return (None, None)

    except Exception as e:
        log.error(f"Failed to get Ganesha PID: {e}")
        return (None, None)


def nfs_log_parser(client, nfs_node, nfs_name, expect_list=None):
    """
    This method parses the nfs debug log for given list of strings and returns 0 on Success
    and 1 on failure
    Required args:
    client : for ceph cmds
    nfs_node : Node object where nfs_daemon is hosted
    nfs_name : Name of nfs cluster
    expect_list : List of strings to be parsed
    """
    results = {"expect": {}}
    if expect_list:
        cmd = f"ceph orch ps | grep {nfs_name}"
        out = list(client.exec_command(sudo=True, cmd=cmd))[0]
        nfs_daemon_name = out.split()[0]
        for search_str in expect_list:
            cmd = f"cephadm logs --name {nfs_daemon_name} > nfs_log"
            nfs_node.exec_command(sudo=True, cmd=cmd)
            try:
                cmd = f'grep "{search_str}" nfs_log'
                out = nfs_node.exec_command(sudo=True, cmd=cmd)
                if len(out) > 0:
                    log.info(
                        f"Found {search_str} in {nfs_daemon_name} log on {nfs_node.hostname}:\n {out}"
                    )
                    results["expect"].update({search_str: nfs_node})
            except BaseException as ex:
                log.info(ex)

        expect_not_found = []
        for exp_str in expect_list:
            if exp_str not in results["expect"]:
                expect_not_found.append(exp_str)
        if len(expect_not_found):
            log.error(
                f"Some of expected strings not found in debug logs for {nfs_daemon_name}:{expect_not_found}"
            )
            return 1
        return 0

    else:
        if type(nfs_node) is not list:
            nfs_node = [nfs_node]
        for node in nfs_node:
            try:
                log.info(
                    "\n\n" + "-" * 30 + "Fetching logs and Ganesha confs" + "-" * 30
                )
                log.info(Ceph(client).orch.ls())
                nfs_containers_deatail = [
                    x
                    for x in node.exec_command(
                        sudo=True, cmd=f"podman ps | grep {nfs_name}"
                    )[0].split("\n")
                    if x and "ceph/keepalived" not in x
                ]
                nfs_containers = [
                    x.split(" ")[0]
                    for x in node.exec_command(
                        sudo=True, cmd=f"podman ps | grep {nfs_name}"
                    )[0].split("\n")
                    if x and "ceph/keepalived" not in x
                ]
                container_logs = [
                    "\n".join(
                        node.exec_command(sudo=True, cmd=f"podman logs {container}")
                    )
                    for container in nfs_containers
                ]

                log.info("\n\nNFS containers logs on {0}\n".format(node.hostname))

                log.info(Ceph(client).version())
                log.info("\n Ceph Health :\n {0}".format(Ceph(client).status()))

                [fsid] = node.get_dir_list("/var/lib/ceph", sudo=True)
                nfs_conf_path = "/var/lib/ceph/{0}".format(fsid)
                nfs_full_names = [
                    x
                    for x in node.get_dir_list(nfs_conf_path, sudo=True)
                    if nfs_name in x
                ]
                nfs_instances_confs_path = [
                    os.path.join(nfs_conf_path + "/" + x + "/etc/ganesha/ganesha.conf")
                    for x in nfs_full_names
                ]
                for nfs_conf in nfs_instances_confs_path:
                    nfs_conf = node.exec_command(sudo=True, cmd=f"cat {nfs_conf}")[0]
                    log.info(
                        "\n Ganesha Conf for {0}: \n \n {1}".format(nfs_name, nfs_conf)
                    )

                for container, logs in zip(nfs_containers_deatail, container_logs):
                    log.info("\nLogs for container {0}:\n{1}\n".format(container, logs))
            except Exception:
                log.info(
                    "Since we are collecting logs, ignoring the exception will not fail the test"
                )


def get_ceph_version(client, prefix_cephadm=False):
    """
    Retrieve the Ceph version installed on a cluster using the provided client.

    Args:
        client : An instance of the client used for executing commands.

    Returns:
        str or None: The Ceph version if installed, or None if Ceph is not installed.

    Raises:
        ValueError: If the JSON output does not contain the expected version information.
    """
    if prefix_cephadm:
        cmd = "cephadm shell -- ceph version -f json"
    else:
        cmd = "ceph version -f json"
    out, rc = client.exec_command(
        sudo=True,
        cmd=cmd,
        check_ec=False,
    )
    log.info(out)
    ceph_version = json.loads(out)
    version_string = ceph_version.get("version", None)
    log.info(version_string)
    if not version_string:
        log.error("Ceph is not installed please install ceph")
        return None
    version_pattern = r"ceph version (\S+).*"
    match = re.search(version_pattern, version_string)
    re.search(version_pattern, version_string)
    if not match:
        raise RuntimeError("Failed to get ceph version from cluster")
    ceph_version_installed = match.group(1)
    return ceph_version_installed
