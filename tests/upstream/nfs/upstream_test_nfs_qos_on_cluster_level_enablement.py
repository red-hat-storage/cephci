import json
import re
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def capture_copy_details(client, nfs_mount, file_name, size="100"):
    """
    Captures the output of the dd command executed remotely.
    :param nfs_mount: The NFS mount path where the file will be created.
    :param size: The size of the file to create (default is "100M").
    :return: A tuple containing (stdout, stderr) from the dd command.
    """

    write_speed, read_speed = "", ""
    cmd = "touch {0}/{1}".format(nfs_mount, file_name)
    client.exec_command(
        sudo=True,
        cmd=cmd,
    )

    write_cmd = "dd if=/dev/urandom of={0}/{1} bs={2}M count=1".format(
        nfs_mount, file_name, size
    )
    write_results = client.exec_command(sudo=True, cmd=write_cmd)[1]

    if (
        re.search(r"(\d+\.\d+ MB/s)$", write_results)
        or re.findall(r"(\d+ MB/s)$", write_results)[0]
    ):
        write_speed = (
            re.findall(r"(\d+\.\d+ MB/s)$", write_results)[0]
            if re.search(r"(\d+\.\d+ MB/s)$", write_results)
            else re.findall(r"(\d+ MB/s)$", write_results)[0]
        )
        log.info("File created successfully on {0}".format(client.hostname))
        log.info("write speed is {0}".format(write_speed))

    # dropping cache
    drop_cache_cmd = "echo 3 > /proc/sys/vm/drop_caches"
    client.exec_command(
        sudo=True,
        cmd=drop_cache_cmd,
    )
    log.info("Cache dropped successfully")

    read_cmd = "dd if={0}/{1} of=/dev/urandom".format(nfs_mount, file_name)
    read_results = client.exec_command(sudo=True, cmd=read_cmd)[1]

    if (
        re.search(r"(\d+\.\d+ MB/s)$", read_results)
        or re.findall(r"(\d+ MB/s)$", read_results)[0]
    ):
        read_speed = (
            re.findall(r"(\d+\.\d+ MB/s)$", read_results)[0]
            if re.search(r"(\d+\.\d+ MB/s)$", read_results)
            else re.findall(r"(\d+ MB/s)$", read_results)[0]
        )
        log.info("read speed is {0}".format(read_speed))

    rm_cmd = "rm -rf {0}/{1}".format(nfs_mount, file_name)
    client.exec_command(
        sudo=True,
        cmd=rm_cmd,
    )

    if write_speed and read_speed:
        log.info(
            'Read and write speed captured successfully on {0} "write_speed": {1}, "read_speed": {2}'.format(
                client.hostname, write_speed, read_speed
            ),
        )
        return {"write_speed": write_speed, "read_speed": read_speed}
    else:
        raise OperationFailedError(
            "Failed to capture read/write speed on {0}".format(client.hostname)
        )


def validate_qos_operation(operation_key, qos_type, cluster_name, qos_data):
    """Validate QoS operation result and handle logging."""
    expected_key = True if operation_key == "enable" else False
    qos_data = json.loads(qos_data)

    log.info("QoS data: {0}".format(qos_data))

    if qos_data.get("enable_bw_control") == expected_key:
        log.info(
            "QoS {0} for {1} cluster {2} is successful".format(
                operation_key, qos_type, cluster_name
            )
        )
    else:
        raise OperationFailedError(
            "QoS {0} for {1} cluster {2} failed".format(
                operation_key, qos_type, cluster_name
            )
        )


def enable_disable_qos_for_cluster(
    enable_flag,
    ceph_cluster_nfs_obj,
    cluster_name,
    qos_type=None,
    **qos_parameters,
):
    # Common validation
    if enable_flag and not qos_type:
        raise ValueError("qos_type is required when enabling QoS")

    operation_key = "enable" if enable_flag else "disable"

    try:
        if enable_flag:
            if qos_type == "PerShare":
                if "max_export_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_share(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_combined_bw=qos_parameters.get(
                            "max_export_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_share(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                        max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                    )
            elif qos_type == "PerClient":
                if "max_client_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_client_combined_bw=qos_parameters.get(
                            "max_client_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_client_read_bw=qos_parameters.get("max_client_read_bw"),
                        max_client_write_bw=qos_parameters.get("max_client_write_bw"),
                    )
            elif qos_type == "PerShare_PerClient":
                if "max_client_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_share_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_combined_bw=qos_parameters.get(
                            "max_export_combined_bw"
                        ),
                        max_client_combined_bw=qos_parameters.get(
                            "max_client_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_share_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                        max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                        max_client_read_bw=qos_parameters.get("max_client_read_bw"),
                        max_client_write_bw=qos_parameters.get("max_client_write_bw"),
                    )
        else:
            ceph_cluster_nfs_obj.qos.disable(cluster_id=cluster_name)

        qos_data = ceph_cluster_nfs_obj.qos.get(cluster_id=cluster_name, format="json")
        validate_qos_operation(
            operation_key=operation_key,
            qos_type=qos_type,
            cluster_name=cluster_name,
            qos_data=qos_data,
        )
        log.info(
            "QoS {0} {1} for cluster {2}".format(qos_data, operation_key, cluster_name)
        )

    except Exception as e:
        raise RuntimeError(
            "QoS {0} failed for {1} cluster {2}".format(
                operation_key, qos_type, cluster_name
            )
        ) from e


def run(ceph_cluster, **kw):
    """Verify QoS operations on NFS cluster"""
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    cluster_name = config["cluster_name"]
    operation = config.get("operation", None)
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    subvolume_group = "ganeshagroup"

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes found in cluster")

    nfs_node = nfs_nodes[0]
    qos_type = config.get("qos_type", None)
    no_clients = int(config.get("clients", "1"))

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    client = clients[0]
    ceph_nfs_client = Ceph(client).nfs
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            clients,
            nfs_node.hostname,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ceph_cluster=ceph_cluster,
        )

        # Process QoS operations
        enable_disable_qos_for_cluster(
            enable_flag=True,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
            **{
                k: config[k]
                for k in [
                    "max_export_write_bw",
                    "max_export_read_bw",
                    "max_client_write_bw",
                    "max_client_read_bw",
                    "max_export_combined_bw",
                    "max_client_combined_bw",
                ]
                if k in config
            },
        )

        if operation == "restart":
            data = json.loads(Ceph(client).orch.ls(format="json"))
            [service_name] = [
                x["service_name"] for x in data if x.get("service_id") == cluster_name
            ]

            Ceph(client).orch.restart(service_name)
            if cluster_name not in [x["service_name"] for x in data]:
                sleep(1)

            qos_data_after_restart = ceph_nfs_client.cluster.qos.get(
                cluster_id=cluster_name, format="json"
            )
            qos_data_after_restart = json.loads(qos_data_after_restart)
            if qos_data_after_restart["qos_type"] == qos_type:
                log.info(
                    "Qos data for {0} persists even after the nfs cluster restarted".format(
                        qos_type
                    )
                )
            else:
                raise OperationFailedError(
                    "Qos data for {0} did not persists after the nfs cluster restarted, after restart {1}".format(
                        qos_type, qos_data_after_restart
                    )
                )

        speed = capture_copy_details(client, nfs_mount, "sample.txt")
        log.info(
            "Transfer speed is {0} for QoS {1} enabled in cluster level".format(
                speed, qos_type
            )
        )

        write_speed = speed.get("write_speed")
        read_speed = speed.get("read_speed")

        max_export_write_bw = config.get("max_export_write_bw")
        max_export_read_bw = config.get("max_export_read_bw")
        max_client_write_bw = config.get("max_client_write_bw")
        max_client_read_bw = config.get("max_client_read_bw")
        max_export_combined_bw = config.get("max_export_combined_bw")
        if max_export_combined_bw:
            max_export_write_bw = max_export_combined_bw
            max_export_read_bw = max_export_combined_bw
        max_client_combined_bw = config.get("max_client_combined_bw")
        if max_client_combined_bw:
            max_client_write_bw = max_client_combined_bw
            max_client_read_bw = max_client_combined_bw

        if (
            (max_export_write_bw is not None and max_export_read_bw is not None)
            and float(max_export_write_bw.replace("MB", ""))
            >= float(write_speed.replace(" MB/s", ""))
            and float(max_export_read_bw.replace("MB", ""))
            >= float(read_speed.replace(" MB/s", ""))
        ) or (
            (max_client_write_bw is not None and max_client_read_bw is not None)
            and float(max_client_write_bw.replace("MB", ""))
            >= float(write_speed.replace(" MB/s", ""))
            and float(max_client_read_bw.replace("MB", ""))
            >= float(read_speed.replace(" MB/s", ""))
        ):
            log.info(
                "Test passed: QoS {0} enabled successfully in export level write speed is {1}"
                " , max_export_write_bw is {2} and read speed is {3}"
                " and max_export_read_bw is {4}".format(
                    qos_type,
                    write_speed,
                    max_export_write_bw,
                    read_speed,
                    max_export_read_bw,
                )
            )
        else:
            raise OperationFailedError(
                "Test failed: QoS {0} enabled successfully in export level write speed is {1}"
                " and read speed is {2} config is {3}".format(
                    qos_type, write_speed, read_speed, config
                )
            )

        enable_disable_qos_for_cluster(
            enable_flag=False,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
        )
        return 0
    except (ConfigError, OperationFailedError, RuntimeError) as e:
        log.error("Test failed: {0}".format(e))
        return 1
    finally:
        log.info("Cleanup in progress")
        log.debug("deleting NFS cluster {0}".format(cluster_name))
        cleanup_cluster(client, nfs_mount, nfs_name, nfs_export)
