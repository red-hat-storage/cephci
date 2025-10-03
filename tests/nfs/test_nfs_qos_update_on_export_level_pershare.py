import json
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from tests.nfs.test_nfs_qos_on_cluster_level_enablement import (
    capture_copy_details,
    enable_disable_qos_for_cluster,
)
from utility.log import Log

log = Log(__name__)


def qos_update_pershare_for_export(
    enable_flag,
    ceph_export_nfs_obj,
    cluster_name,
    qos_type=None,
    nfs_name=str,
    export=str,
    **qos_parameters,
):
    if enable_flag and not qos_type:
        raise ValueError("qos_type is required when enabling QoS")

    operation_key = "enable" if enable_flag else "disable"

    try:
        if enable_flag:
            if qos_type == "PerShare":
                if "max_export_combined_bw" in qos_parameters:
                    ceph_export_nfs_obj.qos.enable_per_share(
                        nfs_name=nfs_name,
                        export=export,
                        max_export_combined_bw=qos_parameters.get(
                            "max_export_combined_bw"
                        ),
                    )
                else:
                    ceph_export_nfs_obj.qos.enable_per_share(
                        nfs_name=nfs_name,
                        export=export,
                        max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                        max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                    )
        else:
            ceph_export_nfs_obj.qos.disable(cluster_id=nfs_name, export=export)

        qos_data = ceph_export_nfs_obj.qos.get(nfs_name=nfs_name, export=export)
        log.info(
            "QoS {0} {1} for export {2} in cluster {3}".format(
                qos_data, operation_key, export, cluster_name
            )
        )
    except Exception as e:
        raise OperationFailedError(
            "Failed to {0} QoS for export {1} in cluster {2} : {3}".format(
                operation_key, export, cluster_name, str(e)
            )
        )


def run(ceph_cluster, **kw):
    """Verify QoS PerShare export-level behavior with updated values"""
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")

    cluster_name = config["cluster_name"]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    operation = config.get("operation", None)
    qos_type = config.get("qos_type", "PerShare")

    cluster_bw = config["cluster_bw"][0]
    export_bw = config["export_bw"][0]
    updated_export_bw = config.get("updated_export_bw", [])[0]

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes found in cluster")

    nfs_node = nfs_nodes[0]
    subvolume_group = "ganeshagroup"

    no_clients = int(config.get("clients", "1"))
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    client = clients[0]
    ceph_nfs_client = Ceph(client).nfs
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    try:
        # Setup NFS
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

        nfs_export = f"{nfs_export}_0"

        # Enable cluster-level QoS
        enable_disable_qos_for_cluster(
            enable_flag=True,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
            **{k: cluster_bw[k] for k in cluster_bw},
        )

        # Enable export-level QoS (initial values)
        qos_update_pershare_for_export(
            enable_flag=True,
            ceph_export_nfs_obj=ceph_nfs_client.export,
            cluster_name=cluster_name,
            qos_type=qos_type,
            nfs_name=nfs_name,
            export=nfs_export,
            **{k: export_bw[k] for k in export_bw},
        )
        # Then capture QoS
        for _ in range(5):
            qos_before_restart = json.loads(
                ceph_nfs_client.export.get(nfs_name=nfs_name, nfs_export=nfs_export)
            ).get("qos_block", {})
            if qos_before_restart:
                break
            sleep(2)
        log.info(f"Stored QoS before restart: {qos_before_restart}")

        # Restart NFS service if operation is restart
        if operation == "restart":
            log.info("Restart operation requested. Restarting the NFS cluster.")
            data = json.loads(Ceph(client).orch.ls(format="json"))
            [service_name] = [
                x["service_name"] for x in data if x.get("service_id") == cluster_name
            ]
            Ceph(client).orch.restart(service_name)
            if cluster_name not in [x["service_name"] for x in data]:
                sleep(5)

            # Validate QoS after restart
            export_data_after_restart = json.loads(
                ceph_nfs_client.export.get(nfs_name=nfs_name, nfs_export=nfs_export)
            )
            qos_after_restart = export_data_after_restart.get("qos_block", {})

            log.info(f"QoS before restart: {qos_before_restart}")
            log.info(f"QoS after restart: {qos_after_restart}")

            if qos_before_restart != qos_after_restart:
                raise OperationFailedError(
                    f"QoS mismatch after restart.\nBefore: {qos_before_restart}\nAfter: {qos_after_restart}"
                )

        # Run IO and capture speeds (initial QoS)
        initial_speed = capture_copy_details(client, nfs_mount, "sample.txt")
        log.info(f"Initial Transfer Speeds: {initial_speed}")

        # Update export-level QoS (updated values)
        qos_update_pershare_for_export(
            enable_flag=True,
            ceph_export_nfs_obj=ceph_nfs_client.export,
            cluster_name=cluster_name,
            qos_type=qos_type,
            nfs_name=nfs_name,
            export=nfs_export,
            **{k: updated_export_bw[k] for k in updated_export_bw},
        )

        # Run IO and capture speeds (after update)
        updated_speed = capture_copy_details(client, nfs_mount, "sample_updated.txt")
        log.info(f"Updated Transfer Speeds: {updated_speed}")

        # Comparison and validation
        initial_write = float(initial_speed["write_speed"].replace(" MB/s", ""))
        updated_write = float(updated_speed["write_speed"].replace(" MB/s", ""))
        initial_read = float(initial_speed["read_speed"].replace(" MB/s", ""))
        updated_read = float(updated_speed["read_speed"].replace(" MB/s", ""))

        if (updated_write != initial_write) or (updated_read != initial_read):
            log.info("Test passed: Updated QoS values are reflected in IO speeds.")
        else:
            raise OperationFailedError(
                "Test failed: QoS update did not reflect in IO performance."
            )

        # Disable QoS
        qos_update_pershare_for_export(
            enable_flag=False,
            nfs_name=nfs_name,
            export=nfs_export,
            ceph_export_nfs_obj=ceph_nfs_client.export,
            cluster_name=cluster_name,
        )
        enable_disable_qos_for_cluster(
            enable_flag=False,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
        )

        return 0

    except (ConfigError, OperationFailedError, RuntimeError) as e:
        log.error(f"Test failed: {e}")
        return 1

    finally:
        log.info("Cleanup in progress")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
