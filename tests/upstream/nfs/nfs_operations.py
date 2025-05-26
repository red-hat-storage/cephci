import json
from datetime import datetime
from threading import Thread
from time import sleep

import yaml

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
    installer_node = ceph_cluster.get_nodes("installer")[0]
    ganesha_conf = """NFS_CORE_PARAM {
    Enable_NLM = false;
    Enable_RQUOTA = false;
    Protocols = 4;
}

EXPORT_DEFAULTS {
    Access_Type = RW;
}
"""
    conf_template = """
EXPORT {
    Export_ID = %s;
    Path = "%s";
    Pseudo = "/export_%s";
    Protocols = 4;
    Transports = TCP;
    Access_Type = RW;
    Squash = None;
    FSAL {
        Name = "CEPH";
    }
}
"""

    # Step 3: Create export
    i = 0
    export_list = []
    for _ in clients:
        export_name = f"{export}_{i}"
        # Step 1: Check if the subvolume group is present.If not, create subvolume group
        cmd = "ceph fs subvolumegroup ls cephfs"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        subvol_name = export_name.replace("/", "")
        if "[]" in out[0]:
            cmd = "ceph fs subvolumegroup create cephfs ganeshagroup"
            installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("Subvolume group created successfully")

        # Step 2: Create subvolume
        cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated"
        installer_node.exec_command(sudo=True, cmd=cmd)

        # Get volume path
        cmd = (
            f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
        )
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        path = out[0].strip()
        ganesha_conf += conf_template % (100+i, path, i)
        i += 1
        export_list.append(export_name)
        sleep(1)
    # stop ganesha service
    pid = ""
    try:
        cmd = "pgrep ganesha"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        print("PID : ", pid)
    except Exception:
        pass

    if pid:
        cmd = f"kill -9 {pid}"
        installer_node.exec_command(sudo=True, cmd=cmd)
    cmds = ["mkdir -p /var/run/ganesha",
           "chmod 755 /var/run/ganesha",
           "chown root:root /var/run/ganesha"]
    for cmd in cmds:
        installer_node.exec_command(cmd=cmd, sudo=True)
    # Update ganesha.conf file
    cmd = f"echo \"{ganesha_conf}\" > /etc/ganesha/ganesha.conf"
    installer_node.exec_command(sudo=True, cmd=cmd)
    ganesha_conf_file = "/etc/ganesha/ganesha.conf"
    with installer_node.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="w") as _f:
        _f.write(ganesha_conf)

    # Restart Ganesha
    cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Check if ganesha service is up
    cmd = "pgrep ganesha"
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    pid = out[0].strip()
    if not pid:
        raise OperationFailedError("Failed to restart nfs service")


    # Get the mount versions specific to clients
    mount_versions = _get_client_specific_mount_versions(version, clients)

    # If there are multiple nfs servers provided, only one is required for mounting
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
    if ha:
        nfs_server = vip.split("/")[0]  # Remove the port

    i = 0
    for version, clients in mount_versions.items():
        for client in clients:
            client.create_dirs(dir_path=nfs_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_mount,
                version=version,
                port=port,
                server=installer_node.ip_address,
                export=f"export_{i}",
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
            i += 1
            sleep(1)
    log.info("Mount succeeded on all clients")

    # Step 5: Enable nfs coredump to nfs nodes
    nfs_nodes = ceph_cluster.get_nodes("installer")
    Enable_nfs_coredump(nfs_nodes)


def create_export(installer_node, nfs_export, squash="None"):
    conf_template = """
    EXPORT {
        Export_ID = %s;
        Path = "%s";
        Pseudo = "%s";
        Protocols = 4;
        Transports = TCP;
        Access_Type = RW;
        Squash = %s;
        FSAL {
            Name = "CEPH";
        }
    }"""
    # stop ganesha service
    pid = ""
    try:
        cmd = "pgrep ganesha"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        print("PID : ", pid)
    except Exception:
        print("Ganesha process not running")

    if pid:
        cmd = f"kill -9 {pid}"
        installer_node.exec_command(sudo=True, cmd=cmd)

    subvol_name = nfs_export.replace("/", "")
    # Create subvolume
    cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Get volume path
    cmd = (
        f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
    )
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    path = out[0].strip()

    cmd = "cat /etc/ganesha/ganesha.conf | grep -o -P '(?<=Export_ID = ).*(?=;)' | tail -1"
    out = installer_node.exec_command(cmd=cmd, sudo=True)
    _id = out[0].strip()
    id = str(int(_id) + 1)
    ganesha_conf = conf_template % (id, path, nfs_export,squash)

    ganesha_conf_file = "/etc/ganesha/ganesha.conf"
    with installer_node.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="a") as _f:
        _f.write(ganesha_conf)

    # Restart Ganesha
    cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Check if ganesha service is up
    cmd = "pgrep ganesha"
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    pid = out[0].strip()
    if not pid:
        raise OperationFailedError("Failed to restart nfs service")


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
    content = f"""service_type: nfs
service_id: {nfs_name}
placement:
    hosts:
        - {nfs_server_name}
spec:
    enable_nlm: true"""

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

    # Start the rpc-statd service on server
    cmd = "sudo systemctl start rpc-statd"
    nfs_node.exec_command(cmd=cmd)


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
                print("NFS daemons are still present. Waiting...")
                sleep(10)  # Wait before checking again
            else:
                print("All NFS daemons have been removed.")
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
