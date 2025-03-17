import json
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def validate_qos_operation(
    operation_key: str, qos_type: str, cluster_name: str, qos_data: dict
) -> None:
    """Validate QoS operation result and handle logging."""
    expected_key = "qos_type" if operation_key == "enable" else None
    success = (
        (expected_key in qos_data)
        if operation_key == "enable"
        else (expected_key not in qos_data)
    )

    log_message = (
        f"QoS {qos_type} {operation_key}d for cluster {cluster_name}. Current state: {qos_data}"
        if success
        else f"QoS {qos_type} failed to {operation_key} for cluster {cluster_name}. State: {qos_data}"
    )

    log.info(log_message) if success else log.error(log_message)

    if not success:
        raise RuntimeError(log_message)


def enable_disable_qos_for_cluster(
    enable_flag: bool,
    ceph_cluster_nfs_obj,
    cluster_name: str,
    qos_type: str = None,
    **qos_parameters,
) -> None:
    # Common validation
    if enable_flag and not qos_type:
        raise ValueError("qos_type is required when enabling QoS")

    operation_key = "enable" if enable_flag else "disable"

    try:
        if enable_flag:
            ceph_cluster_nfs_obj.qos.enable(
                cluster_id=cluster_name, qos_type=qos_type, **qos_parameters
            )
        else:
            ceph_cluster_nfs_obj.qos.disable(cluster_id=cluster_name)

        qos_data = ceph_cluster_nfs_obj.qos.get(cluster_id=cluster_name)
        validate_qos_operation(
            operation_key=operation_key,
            qos_type=qos_type,
            cluster_name=cluster_name,
            qos_data=qos_data,
        )

    except Exception as e:
        raise RuntimeError(
            f"QoS {operation_key} failed for cluster {cluster_name}"
        ) from e


def run(ceph_cluster, **kw):
    """Verify QoS operations on NFS cluster"""
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    cluster_name = config["cluster_name"]
    operation = config.get("operation", None)

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes found in cluster")

    nfs_node = nfs_nodes[0]
    qos_type = config.get("qos_type", [])
    ceph_nfs_client = Ceph(clients[0]).nfs

    try:
        # Create NFS cluster
        ceph_nfs_client.cluster.create(
            name=cluster_name, nfs_server=nfs_node.hostname, ha=False
        )
        sleep(3)

        # Get cluster name reliably
        clusters = ceph_nfs_client.cluster.ls()
        if not clusters:
            raise OperationFailedError("No NFS clusters found")
        cluster_name_created = clusters[0]
        if cluster_name_created != cluster_name:
            raise OperationFailedError("NFS cluster was not created as user parameter")

        # Process QoS operations
        for qos in qos_type:
            # Enable QoS with parameters
            enable_disable_qos_for_cluster(
                enable_flag=True,
                ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
                cluster_name=cluster_name,
                qos_type=qos,
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
                # Get nfs service name
                data = json.loads(Ceph(nfs_node).orch.ls(format="json"))
                [service_name] = [
                    x["service_name"]
                    for x in data
                    if x.get("service_id") == cluster_name
                ]

                # restart the service
                Ceph(nfs_node).orch.restart(service_name)
                if cluster_name not in [x["service_name"] for x in data]:
                    sleep(1)

                # validate if QOS data persists after cluster restart
                qos_data_after_restart = ceph_nfs_client.cluster.qos.get(
                    cluster_id=cluster_name
                )
                if qos_data_after_restart["qos_type"] == qos:
                    log.info(
                        f"Qos data for {qos} persists even after the nfs cluster restarted"
                    )
                else:
                    raise OperationFailedError(
                        f"Qos data for {qos} did not persists after the nfs cluster restarted"
                    )

            # Disable QoS
            enable_disable_qos_for_cluster(
                enable_flag=False,
                ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
                cluster_name=cluster_name,
            )
        return 0
    except (ConfigError, OperationFailedError, RuntimeError) as e:
        log.error(f"Test failed: {e}")
        return 1
    finally:
        log.info("Cleanup in progress <Finally block>")
        log.debug(f"deleting NFS cluster {cluster_name}")
        ceph_nfs_client.cluster.delete(cluster_name)
