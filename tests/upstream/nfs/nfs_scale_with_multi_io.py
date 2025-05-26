import json
import re
from threading import Thread
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.io.small_file import SmallFile
from cli.utilities.filesys import Mount, Unmount
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

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("num_of_clients", "2"))
    no_servers = int(config.get("num_of_servers", "4"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    fs = "cephfs"
    nfs_client_count = config.get("num_of_clients")

    execution_type = config.get("execution_type", "serial")
    threads = []
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
            Ceph(clients[0]).nfs.export.create(
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
        mounts_by_client = {client: [] for client in clients}

        for i, command in enumerate(nfs_export_list):
            client_index = i % nfs_client_count
            client = clients[client_index]
            mounts_by_client[client].append(command)

        virtual_ip = None
        if vip:
            virtual_ip = vip.split("/")[0]
        completed = 0

        # THIS CODE IS FOR SCENARIOS WHERE EXPORTS ARE ALREDY CREATED AND JUST MOUNT ALONE IS SUFFICIENT
        # var = 1
        # for client in clients:
        #     # Do mounts
        #     for _ in range(10):
        #         export = f"/export_{var}"
        #         nfs_mounting_dir = f"/mnt/nfs_small_file_{var}"
        #         if Mount(client).nfs(
        #             mount=nfs_mounting_dir,
        #             version=version,
        #             port=port,
        #             server=virtual_ip,
        #             export=export,
        #         ):
        #             raise OperationFailedError(
        #                 f"Failed to mount nfs on {client.hostname}"
        #             )
        #         var += 1
        #         # completed_exports.append(mount)
        #         # remaining = len(exports_to_mount) - len(completed_exports)
        #         # log.info(f"{remaining} mounts remaining for {client.hostname}")
        #         # completed += 1
        #         # log.info(f"Total mounts completed : '{completed}' out of '{nfs_export_count}'")
        #     log.info(f"\nMount succeeded on {client.hostname} \n\n")

        for client, mounts in mounts_by_client.items():
            log.info(f"Starting mount for  {client.hostname}")
            exports_to_mount = mounts[:exports_per_client]
            log.info(f"Exports to mount for {client.hostname} : {exports_to_mount}")
            completed_exports = []
            for mount in exports_to_mount:
                nfs_mounting_dir = f"/mnt/nfs1_{mount.replace('/', '')}"

                if Mount(client).nfs(
                    mount=nfs_mounting_dir,
                    version=version,
                    port=port,
                    server=virtual_ip,
                    export=mount,
                ):
                    raise OperationFailedError(
                        f"Failed to mount nfs on {client.hostname}"
                    )
                completed_exports.append(mount)
                remaining = len(exports_to_mount) - len(completed_exports)
                log.info(f"{remaining} mounts remaining for {client.hostname}")
                completed += 1
                log.info(
                    f"Total mounts completed : '{completed}' out of '{nfs_export_count}'"
                )
            log.info(f"\nMount succeeded on {client.hostname} \n\n")

        # Get Mem usage
        vip_node = get_vip_node(nfs_nodes, virtual_ip)
        get_mem_usage(vip_node)

        log.info("Starting IO")
        for client, mounts in mounts_by_client.items():
            exports_to_mount = mounts[:exports_per_client]
            for mount in exports_to_mount:
                nfs_mounting_dir = f"/mnt/nfs1_{mount.replace('/', '')}"
                if execution_type == "serial":
                    # trigger_io(client, nfs_mounting_dir)
                    log.info(f"IO run completed on {mount}")
                else:
                    th = Thread(
                        target=SmallFile(client).run,
                        args=(
                            nfs_mounting_dir,
                            ["create", "read", "delete"],
                            4,
                            4194,
                            1024,
                        ),
                    )
                    threads.append(th)
                    th.start()
                    sleep(1)

        # CODE TO HANDLE WHEN THE EXPORTS WERE ALREADY EXISTING
        # for client in clients:
        #     cmd = "ls /mnt"
        #     out, _ = client.exec_command(sudo=True, cmd=cmd)
        #     actual = out.split("\n")
        #     mounts_in_node = list(filter(None, actual))
        #
        #     for mount in mounts_in_node:
        #         if "nfs_small_file_" in mount:
        #             nfs_mounting_dir = f"/mnt/{mount.replace('/', '')}"
        #             if execution_type == "serial":
        #                 # trigger_io(client, nfs_mounting_dir)
        #                 log.info(f"IO run completed on {mount}")
        #             else:
        #                 th = Thread(
        #                     target=SmallFile(client).run,
        #                     args=(nfs_mounting_dir, ["create", "read", "delete"], 4, 4194, 1024),
        #                 )
        #                 threads.append(th)
        #                 th.start()
        #                 sleep(5)

        # Wait for all threads to complete
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
        for client, mounts in mounts_by_client.items():
            cmd = "ls /mnt"
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            actual = out.split("\n")
            actual = list(filter(None, actual))

            for m in actual:
                dir = f"/mnt/{m}"
                client.exec_command(sudo=True, cmd=f"rm -rf {dir}/*", long_running=True)
                try:
                    if Unmount(client).unmount(dir):
                        raise OperationFailedError(
                            f"Failed to unmount nfs on {client.hostname}"
                        )
                except Exception:
                    pass

                log.info("Removing nfs-ganesha mount dir on client:")
                client.exec_command(sudo=True, cmd=f"rm -rf {dir}", long_running=True)

        # RSS val
        log.info("RSS value after rm of mount points")
        get_mem_usage(vip_node)

        for i in range(1, nfs_export_count + 1):
            Ceph(client).nfs.export.delete(nfs_name, f"/export_{i}")

        # RSS val
        log.info("RSS value after deleting all exports")
        get_mem_usage(vip_node)

        sleep(60)

        log.info("RSS value after deleting all exports (post sleep of 60 sec)")
        get_mem_usage(vip_node)

        # Delete all the subvolumes
        for i in range(1, nfs_export_count + 1):
            cmd = f"ceph fs subvolume rm cephfs ganesha{i} --group_name ganeshagroup"
            client.exec_command(sudo=True, cmd=cmd)

        # Delete the subvolumegroup
        cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
        client.exec_command(sudo=True, cmd=cmd)

    return 0
