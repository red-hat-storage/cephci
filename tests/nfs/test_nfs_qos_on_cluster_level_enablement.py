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
    cmd = "touch {0}/{1}".format(nfs_mount, file_name)
    client.exec_command(
        sudo=True,
        cmd=cmd,
    )

    cmd = "dd if=/dev/urandom of={0}/{1} bs={2}M count=1".format(
        nfs_mount, file_name, size
    )
    data = client.exec_command(sudo=True, cmd=cmd)

    if (
        re.search(r"(\d+\.\d+ MB/s)$", data[1])
        or re.findall(r"(\d+ MB/s)$", data[1])[0]
    ):
        speed = (
            re.findall(r"(\d+\.\d+ MB/s)$", data[1])[0]
            if re.search(r"(\d+\.\d+ MB/s)$", data[1])[0]
            else re.findall(r"(\d+ MB/s)$", data[1])[0]
        )
        log.info("File created successfully on {0}".format(client.hostname))
        return speed
    else:
        raise OperationFailedError(
            "Failed to run dd command on {0} : {1}".format(client.hostname, data)
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
                ceph_cluster_nfs_obj.qos.enable_per_share(
                    qos_type=qos_type,
                    cluster_id=cluster_name,
                    max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                    max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                )
            elif qos_type == "PerClient":
                ceph_cluster_nfs_obj.qos.enable_per_client(
                    qos_type=qos_type,
                    cluster_id=cluster_name,
                    max_client_read_bw=qos_parameters.get("max_client_read_bw"),
                    max_client_write_bw=qos_parameters.get("max_client_write_bw"),
                )
            elif qos_type == "PerShare_PerClient":
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
    nfs_nodes = ceph_cluster.get_nodes("nfs")
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
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
            qos_type=qos_type,
            **{
                k: config[k]
                for k in [
                    "max_export_write_bw",
                    "max_export_read_bw",
                    "max_client_write_bw",
                    "max_client_read_bw",
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

        if float(re.findall(r"\d+", config["max_export_write_bw"])[0]) >= float(
            re.findall(r"\d+\.\d+", speed)[0]
        ):
            log.info(
                "Test passed: QoS {0} enabled successfully in cluster level".format(
                    qos_type
                )
            )
        else:
            raise OperationFailedError(
                "Test failed: QoS {0} enabled successfully in cluster level transfer speed is {1} "
                "and max_export_write_bw is {2}".format(
                    qos_type, speed, config["max_export_write_bw"]
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
