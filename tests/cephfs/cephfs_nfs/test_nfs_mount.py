import csv
import json
import os
import traceback
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574028 - Ensure the path of the nfs export is displayed properly.
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Verify path of cephfs nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>

    Clean-up:
    1. Remove cephfs nfs export
    """
    try:
        tc = "CEPH-83574028"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        csv_columns = ["Client", "Export_name", "Execution_time"]
        log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        csv_file = f"{log_dir}/{__name__}.csv"
        csvfile = open(csv_file, "w")
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        test_run_details = []
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        nfs_server_count = config.get("num_of_servers")
        if nfs_server_count > len(nfs_servers):
            log.error(
                f"NFS servers required to perform test is {nfs_server_count} but "
                f"conf file has only {len(nfs_servers)}"
            )
            return 1
        nfs_client_count = config.get("num_of_clients")
        if nfs_client_count > len(clients):
            log.error(
                f"NFS clients required to perform test is {nfs_client_count} but "
                f"conf file has only {len(clients)}"
            )
            return 1
        nfs_server_list = [
            nfs_servers[i].node.hostname for i in range(0, nfs_server_count)
        ]
        fs_util.create_nfs(
            client1,
            nfs_name,
            placement=f"{nfs_server_count} {' '.join([str(x) for x in nfs_server_list])}",
        )

        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_count = config.get("num_of_exports")
        nfs_export_list = []
        for i in range(1, nfs_export_count + 1):
            start_time = datetime.now()
            nfs_export_name = f"/export_{i}"
            export_path = "/"
            fs_name = "cephfs" if not erasure else "cephfs-ec"
            fs_details = fs_util.get_fs_info(client1, fs_name)

            if not fs_details:
                fs_util.create_fs(client1, fs_name)
            if "5.0" in rhbuild:
                client1.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                    f"{nfs_export_name} path={export_path}",
                )
            else:
                fs_util.create_nfs_export(
                    client1,
                    nfs_cluster_name=nfs_name,
                    binding=nfs_export_name,
                    fs_name=fs_name,
                    path=export_path,
                )

            log.info("ceph nfs export created successfully")
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
            )
            output = json.loads(out)
            export_get_path = output["path"]
            if export_get_path != export_path:
                log.error("Export path is not correct")
                return 1
            nfs_export_list.append(nfs_export_name)
        exports_per_client = config.get("exports_per_client")
        mounts_by_client = {client: [] for client in clients}
        for i, command in enumerate(nfs_export_list):
            client_index = i % nfs_client_count  # Cycle through clients
            client = clients[client_index]
            mounts_by_client[client].append(command)
        log.info(f"Clients and respective exports:  {mounts_by_client}")
        for client, mounts in mounts_by_client.items():
            exports_to_mount = mounts[:exports_per_client]
            for mount in exports_to_mount:
                start_time = datetime.now()
                nfs_mounting_dir = f"/mnt/nfs1_{mount.replace('/', '')}"
                fs_util.cephfs_nfs_mount(client, nfs_server, mount, nfs_mounting_dir)
                fio_filenames = fs_util.generate_all_combinations(
                    client,
                    config.get("fio_config").get("global_params").get("ioengine"),
                    nfs_mounting_dir,
                    config.get("fio_config")
                    .get("workload_params")
                    .get("random_rw")
                    .get("rw"),
                    config.get("fio_config").get("global_params").get("size", ["1G"]),
                    config.get("fio_config")
                    .get("workload_params")
                    .get("random_rw")
                    .get("iodepth", ["1"]),
                    config.get("fio_config")
                    .get("workload_params")
                    .get("random_rw")
                    .get("numjobs", ["4"]),
                )
                for file in fio_filenames:
                    client.exec_command(
                        sudo=True,
                        cmd=f"fio {file} --output-format=json "
                        f"--output={file}_{client.node.hostname}_{mount.replace('/', '')}.json",
                        long_running=True,
                        timeout="notimeout",
                    )
                    end_time = datetime.now()
                    exec_time = (end_time - start_time).total_seconds()
                    log.info(
                        f"Iteration Details \n"
                        f"Export name created : {mount}\n"
                        f"Time Taken for the Iteration : {exec_time}\n"
                    )
                    writer.writerow(
                        {
                            "Client": client.node.hostname,
                            "Export_name": mount,
                            "Execution_time": exec_time,
                        }
                    )
                    csvfile.flush()
                    os.fsync(csvfile.fileno())
                    test_run_details.append(
                        {
                            "Client": client.node.hostname,
                            "Export_name": mount,
                            "Execution_time": exec_time,
                        }
                    )
                    os.makedirs(f"{log_dir}/{client.node.hostname}", exist_ok=True)
                    client.download_file(
                        src=f"/root/{file}_{client.node.hostname}_{mount.replace('/', '')}.json",
                        dst=f"{log_dir}/{client.node.hostname}/{file}_{client.node.hostname}"
                        f"_{mount.replace('/', '')}.json",
                        sudo=True,
                    )
            log.info(f"Test completed successfully on client: {client.node.hostname}")
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        [
            print("%s          %s\n" % (item["Export_name"], item["Execution_time"]))
            for item in test_run_details
        ]
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        log.info(test_run_details)
        [
            print("%s          %s\n" % (item["Export_name"], item["Execution_time"]))
            for item in test_run_details
        ]
        status, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph -s",
        )
        df, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph df",
        )
        csvfile.write(status)
        csvfile.write(df)
        csvfile.flush()
        csvfile.close()
        for i in range(1, nfs_export_count + 1):
            nfs_export_name = f"/export_{i}"
            fs_util.remove_nfs_export(
                client1, nfs_cluster_name=nfs_name, binding=nfs_export_name
            )
        fs_util.remove_nfs_cluster(client1, nfs_cluster_name=nfs_name)
