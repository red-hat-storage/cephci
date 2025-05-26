import json
import re
from threading import Thread
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.io.fio import Fio
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def get_ip_from_node(node):
    """
    Returns the list of ip addresses assigned to the given node
    Args:
        node (ceph): Ceph node
    """
    pattern = r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"
    cmd = "ip addr"
    out = node.exec_command(cmd=cmd, sudo=True)
    return re.findall(pattern=pattern, string=out[0])


def get_vip_node(nfs_nodes, vip):
    for node in nfs_nodes:
        assigned_ips = get_ip_from_node(node)
        if vip in assigned_ips:
            return node


def get_mem_usage(nfs_node):
    pid = nfs_node.exec_command(sudo=True, cmd="pgrep ganesha")[0].strip()
    rss = nfs_node.exec_command(sudo=True, cmd=f"ps -p {pid} -o rss=")[0].strip()
    log.info(f"PID {pid}")
    log.info(f"RSS VALUE {rss}")


def run(ceph_cluster, **kw):
    """
    Validate the scale cluster scenario with nfs ganesha
    """

    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    no_clients = int(config.get("num_of_clients", "2"))
    no_servers = int(config.get("num_of_servers", "4"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    clients = clients[:no_clients]  # Select only the required number of clients
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    fs = "cephfs"
    nfs_client_count = config.get("num_of_clients")
    threads = []

    for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
        ceph_cluster.node_list.append(windows_client_obj)
    windows_clients = ceph_cluster.get_nodes("windows_client")

    try:
        # Step 1: Enable nfs
        Ceph(clients[0]).mgr.module(action="enable", module="nfs", force=True)

        # Step 2: Create an NFS cluster
        nfs_servers = []
        for i in range(no_servers):
            nfs_servers.append(nfs_nodes[i].ip_address)
        Ceph(clients[0]).nfs.cluster.create(
            name=nfs_name, nfs_server=nfs_servers, ha=ha, vip=vip
        )

        nfs_export_count = config.get("num_of_exports")
        nfs_export_list = []

        # Check if ceph fs volume is present
        cmd = "ceph fs volume ls"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)
        if "[]" in out[0]:
            cmd = "ceph fs volume create cephfs"
            clients[0].exec_command(cmd=cmd, sudo=True)

        # Create a subvolume group
        cmd = "ceph fs subvolumegroup create cephfs ganeshagroup"
        clients[0].exec_command(cmd=cmd, sudo=True)

        for i in range(1, nfs_export_count + 1):
            # Create a subvol for each export
            cmd = f"ceph fs subvolume create cephfs ganesha{i} --group_name ganeshagroup --namespace-isolated"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Get volume path
            cmd = (
                f"ceph fs subvolume getpath cephfs ganesha{i} --group_name ganeshagroup"
            )
            out, _ = clients[0].exec_command(cmd=cmd, sudo=True)
            path = out.strip()

            # Create export
            nfs_export_name = f"/export_{i}"
            export_path = "/" if not path else path
            cmd = f"ceph nfs export create cephfs {nfs_name} {nfs_export_name} cephfs --path={path}"
            clients[0].exec_command(cmd=cmd, sudo=True)
            out = Ceph(clients[0]).nfs.export.create(
                fs_name=fs_name,
                nfs_name=nfs_name,
                nfs_export=nfs_export_name,
                fs=fs,
                installer=nfs_nodes[0]
            )

            log.info("ceph nfs export created successfully")

            out = Ceph(clients[0]).nfs.export.get(nfs_name, nfs_export_name)
            log.info(out)
            output = json.loads(out)
            export_get_path = output["path"]
            if export_get_path != export_path:
                log.error("Export path is not correct")
                return 1
            nfs_export_list.append(nfs_export_name)

        exports_per_client = config.get("exports_per_client")
        mounts_by_client = {client: [] for client in windows_clients}

        for i, command in enumerate(nfs_export_list):
            client_index = i % nfs_client_count
            client = clients[client_index]
            mounts_by_client[client].append(command)

        virtual_ip = None
        if vip:
            virtual_ip = vip.split("/")[0]

        # th_list = []
        # for client in windows_clients:
        #     cmd = r"curl --create-dirs -O --output-dir " \
        #           r"C:\Users\Manisha http://magna002.ceph.redhat.com/cephci-jenkins/fio/fio.exe"
        #     # out = client.exec_command(cmd=cmd)
        #     th = Thread(
        #         target=lambda: client.exec_command(cmd=cmd), args=()
        #     )
        #     th.start()
        #     th_list.append(th)
        # for th in th_list:
        #     th.join()
        # for client in windows_clients:
        #     cmd = r"dir C:\Users\Manisha"
        #     out = client.exec_command(cmd=cmd)
        #     log.info(out)

        var = 1
        for client in windows_clients:
            dir = "P"
            mount_dir_list = []
            # Do mounts
            for i in range(int(exports_per_client)):
                nfs_mounting_dir = chr(ord(dir) + i) + ":"

                # Perform umount in case the mount is already present
                try:
                    cmd = f"umount {nfs_mounting_dir}"
                    out = client.exec_command(cmd=cmd)
                    log.info(out)
                except Exception as e:
                    log.info(str(e))

                export = f"/export_{var}"
                cmd = f"mount {virtual_ip}:{export} {nfs_mounting_dir}"
                out = client.exec_command(cmd=cmd)
                log.info(out)
                if "is now successfully connected" not in out[0]:
                    raise OperationFailedError(f"Failed to mount. Error: {out[0]}")
                cmd = "mount"
                out = client.exec_command(cmd=cmd)
                log.info(out[0])
                mount_dir_list.append(nfs_mounting_dir)
                var += 1
            log.info(f"\nMount succeeded on {client.hostname} \n\n")

        # Get Mem usage
        vip_node = get_vip_node(nfs_nodes, virtual_ip)
        get_mem_usage(vip_node)

        log.info("Starting IO")
        ioengine = "libaio"
        workloads = ["randrw"]
        size = ["4G"]
        iodepth_value = ["4"]
        numjobs = "4"
        for client in windows_clients:
            for mount in mount_dir_list:
                th = Thread(
                    target=Fio(client).run,
                    args=(mount, ioengine, workloads, size, iodepth_value, numjobs),
                )
                threads.append(th)
                th.start()
                sleep(5)

        for th in threads:
            th.join()
        log.info("Completed running IO on all mounts")

        # Get RSS value
        log.info("RSS value post IO")
        get_mem_usage(vip_node)

    except Exception as e:
        log.error(f"Failed to perform scale ops : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        for windows_client in windows_clients:
            for mount in mount_dir_list:
                cmd = f"del /q /f {mount}\\*.*"
                windows_client.exec_command(cmd=cmd)
                cmd = f"umount {mount}"
                windows_client.exec_command(cmd=cmd)
        # RSS val
        log.info("RSS value after rm of mount points")
        get_mem_usage(vip_node)

        for i in range(1, nfs_export_count + 1):
            Ceph(client).nfs.export.delete(nfs_name, f"/export_{i}")

        # RSS val
        log.info("RSS value after deleting all exports")
        get_mem_usage(vip_node)

        # Delete all the subvolumes
        for i in range(1, nfs_export_count + 1):
            cmd = f"ceph fs subvolume rm cephfs ganesha{i} --group_name ganeshagroup"
            client.exec_command(sudo=True, cmd=cmd)

        # Delete the subvolumegroup
        cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
        client.exec_command(sudo=True, cmd=cmd)

    return 0
