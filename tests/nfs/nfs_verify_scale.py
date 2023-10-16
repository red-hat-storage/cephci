import json
from threading import Thread

from nfs_operations import cleanup_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.io.spec_storage import SpecStorage
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def run_spec_storage_io(
    primary_client,
    benchmark,
    load,
    incr_load,
    num_runs,
    clients,
    nfs_mount,
    benchmark_defination,
):
    if SpecStorage(primary_client).run_spec_storage(
        benchmark, load, incr_load, num_runs, clients, nfs_mount, benchmark_defination
    ):
        raise OperationFailedError("SPECstorage run failed")
    log.info("SPECstorage run completed")


def run(ceph_cluster, **kw):
    """
    Validate the scale cluster scenario with nfs ganesha
    """

    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("num_of_clients", "2"))
    no_servers = int(config.get("num_of_servers", "1"))
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    nfs_client_count = config.get("num_of_clients")

    # IO params
    benchmark = config.get("benchmark", "SWBUILD")
    benchmark_defination = config.get("benchmark_defination")
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")
    execution_type = config.get("execution_type", "serial")
    threads = []
    try:
        # Step 1: Enable nfs
        Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)

        # Step 2: Create an NFS cluster
        nfs_servers = []
        for i in range(no_servers):
            nfs_servers.append(nfs_nodes[i].hostname)
        Ceph(clients[0]).nfs.cluster.create(name=nfs_name, nfs_server=nfs_servers)

        nfs_export_count = config.get("num_of_exports")
        nfs_export_list = []
        for i in range(1, nfs_export_count + 1):
            nfs_export_name = f"/export_{i}"
            export_path = "/"
            Ceph(clients[0]).nfs.export.create(
                fs_name=fs_name, nfs_name=nfs_name, nfs_export=nfs_export_name, fs=fs
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

        for client, mounts in mounts_by_client.items():
            exports_to_mount = mounts[:exports_per_client]
            for mount in exports_to_mount:
                nfs_mounting_dir = f"/mnt/nfs1_{mount.replace('/', '')}"
                if Mount(client).nfs(
                    mount=nfs_mounting_dir,
                    version=version,
                    port=port,
                    server=nfs_server_name,
                    export=nfs_export,
                ):
                    raise OperationFailedError(
                        f"Failed to mount nfs on {client.hostname}"
                    )
                log.info(f"Mount succeeded on {client.hostname}")

                # Perform IO
                if execution_type == "serial":
                    run_spec_storage_io(
                        client,
                        benchmark,
                        load,
                        incr_load,
                        num_runs,
                        clients,
                        nfs_mounting_dir,
                        benchmark_defination,
                    )
                    log.info(f"SPECstorage run completed on {mount}")
                else:
                    th = Thread(
                        target=run_spec_storage_io,
                        args=(
                            client,
                            benchmark,
                            load,
                            incr_load,
                            num_runs,
                            clients,
                            nfs_mounting_dir,
                            benchmark_defination,
                        ),
                    )
                    th.start()
                    threads.append(th)
                    log.info(f"Started specstorage on {mount}")

        # Wait for all threads to complete
        for th in threads:
            th.join()
        log.info("Completed running IO on all mounts")

    except Exception as e:
        log.error(f"Failed to perform scale ops : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        for client, mounts in mounts_by_client.items():
            exports_to_mount = mounts[:exports_per_client]
            for mount in exports_to_mount:
                nfs_mounting_dir = f"/mnt/nfs1_{mount.replace('/', '')}"
                client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/*")
                if Unmount(client).unmount(nfs_mounting_dir):
                    raise OperationFailedError(
                        f"Failed to unmount nfs on {client.hostname}"
                    )
                log.info("Removing nfs-ganesha mount dir on client:")
                client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_mounting_dir}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
