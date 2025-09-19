from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from tests.nfs.test_nfs_qos_on_cluster_level_enablement import (
    capture_copy_details,
    enable_disable_qos_for_cluster,
)
from utility.log import Log

log = Log(__name__)


def enable_disable_qos_for_export(
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
            if qos_type == "PerShare_PerClient":
                # Only combined bandwidth parameters allowed
                ceph_export_nfs_obj.qos.enable_per_share_per_client(
                    nfs_name=nfs_name,
                    export=export,
                    max_export_combined_bw=qos_parameters.get("max_export_combined_bw"),
                    max_client_combined_bw=qos_parameters.get("max_client_combined_bw"),
                )
            else:
                raise ConfigError(
                    f"Unsupported qos_type {qos_type} in export QoS enable"
                )
        else:
            ceph_export_nfs_obj.qos.disable(cluster_id=nfs_name, export=export)

        qos_data = ceph_export_nfs_obj.qos.get(nfs_name=nfs_name, export=export)
        log.info(
            f"QoS {qos_data} {operation_key}d for export {export} in cluster {cluster_name}"
        )
    except Exception as e:
        raise OperationFailedError(
            f"Failed to {operation_key} QoS for export {export} in cluster {cluster_name} : {str(e)}"
        )


def validate_combined_bw_keys(bw_config, label="export_bw"):
    required_keys = ["max_export_combined_bw", "max_client_combined_bw"]
    missing = [k for k in required_keys if k not in bw_config]
    if missing:
        raise ConfigError(f"Missing required QoS keys in {label}: {missing}")


def run(ceph_cluster, **kw):
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    cluster_name = config["cluster_name"]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    fs_name = config.get("cephfs_volume", "cephfs")
    nfs_name = cluster_name
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    qos_type = config.get("qos_type", None)
    no_clients = int(config.get("clients", "1"))

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes found in cluster")

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    client = clients[0]

    ceph_nfs_client = Ceph(client).nfs
    subvolume_group = "ganeshagroup"
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    cluster_bw = config.get("cluster_bw", None)
    export_bw = config.get("export_bw", None)
    updated_export_bw = config.get("updated_export_bw", None)

    if not cluster_bw or not export_bw or not updated_export_bw:
        raise ConfigError(
            "cluster_bw, export_bw, and updated_export_bw must be provided in config"
        )

    try:
        # Validate required QoS keys (combined bandwidth only)
        validate_combined_bw_keys(cluster_bw, "cluster_bw")
        validate_combined_bw_keys(export_bw, "export_bw")
        validate_combined_bw_keys(updated_export_bw, "updated_export_bw")

        # Setup NFS cluster
        nfs_node = nfs_nodes[0]
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

        # Enable QoS at cluster level first (required)
        enable_disable_qos_for_cluster(
            enable_flag=True,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
            **{
                k: cluster_bw[k]
                for k in ["max_export_combined_bw", "max_client_combined_bw"]
                if k in cluster_bw
            },
        )

        # Enable QoS at export level with initial values
        export_name = f"{nfs_export}_0"
        enable_disable_qos_for_export(
            enable_flag=True,
            ceph_export_nfs_obj=ceph_nfs_client.export,
            cluster_name=cluster_name,
            qos_type=qos_type,
            nfs_name=nfs_name,
            export=export_name,
            **{
                k: export_bw[k]
                for k in ["max_export_combined_bw", "max_client_combined_bw"]
                if k in export_bw
            },
        )

        # Run IO and validate initial QoS limits
        speed = capture_copy_details(client, nfs_mount, "sample.txt")
        log.info(
            f"Initial Transfer speed: {speed} for QoS {qos_type} enabled at export level"
        )

        write_speed = float(speed.get("write_speed").replace(" MB/s", ""))
        read_speed = float(speed.get("read_speed").replace(" MB/s", ""))

        max_export_bw = float(export_bw["max_export_combined_bw"].replace("MB", ""))
        max_client_bw = float(export_bw["max_client_combined_bw"].replace("MB", ""))

        if (
            write_speed <= max_export_bw
            and read_speed <= max_export_bw
            and write_speed <= max_client_bw
            and read_speed <= max_client_bw
        ):
            log.info(
                f"Initial QoS validation PASSED: write_speed {write_speed} MB/s, "
                f"read_speed {read_speed} MB/s within limits "
                f"export_bw: {max_export_bw} MB/s, client_bw: {max_client_bw} MB/s"
            )
        else:
            raise OperationFailedError(
                f"Initial QoS validation FAILED: write_speed {write_speed} MB/s, read_speed {read_speed} MB/s "
                f"exceeded limits export_bw: {max_export_bw} MB/s, client_bw: {max_client_bw} MB/s"
            )

        # Update QoS at export level with updated values
        enable_disable_qos_for_export(
            enable_flag=True,
            ceph_export_nfs_obj=ceph_nfs_client.export,
            cluster_name=cluster_name,
            qos_type=qos_type,
            nfs_name=nfs_name,
            export=export_name,
            **{
                k: updated_export_bw[k]
                for k in ["max_export_combined_bw", "max_client_combined_bw"]
                if k in updated_export_bw
            },
        )

        # Run IO and validate updated QoS limits
        speed_updated = capture_copy_details(client, nfs_mount, "sample.txt")
        log.info(
            f"Updated Transfer speed: {speed_updated} for QoS {qos_type} after export QoS update"
        )

        write_speed_updated = float(
            speed_updated.get("write_speed").replace(" MB/s", "")
        )
        read_speed_updated = float(speed_updated.get("read_speed").replace(" MB/s", ""))

        max_export_bw_updated = float(
            updated_export_bw["max_export_combined_bw"].replace("MB", "")
        )
        max_client_bw_updated = float(
            updated_export_bw["max_client_combined_bw"].replace("MB", "")
        )

        if (
            write_speed_updated <= max_export_bw_updated
            and read_speed_updated <= max_export_bw_updated
            and write_speed_updated <= max_client_bw_updated
            and read_speed_updated <= max_client_bw_updated
        ):

            log.info(
                f"Updated QoS validation PASSED: write_speed {write_speed_updated} MB/s, "
                f"read_speed {read_speed_updated} MB/s within updated limits "
                f"export_bw: {max_export_bw_updated} MB/s, client_bw: {max_client_bw_updated} MB/s"
            )
        else:
            raise OperationFailedError(
                f"Updated QoS validation FAILED: write_speed {write_speed_updated} MB/s, "
                f"read_speed {read_speed_updated} MB/s exceeded updated limits "
                f"export_bw: {max_export_bw_updated} MB/s, client_bw: {max_client_bw_updated} MB/s"
            )

        # Disable QoS at export and cluster level after test
        enable_disable_qos_for_export(
            enable_flag=False,
            nfs_name=nfs_name,
            export=export_name,
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
        log.debug(f"Deleting NFS cluster {cluster_name}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
