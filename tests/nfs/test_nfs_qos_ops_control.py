import json
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    cleanup_cluster,
    setup_nfs_cluster,
    verify_ops_control_settings,
    validate_ops_control,
    validate_ops_limit
)
from utility.log import Log

log = Log(__name__)


def enable_cluster_ops_control(client, cluster_name, qos_type, ops_config):
    """Enable ops control at cluster level"""
    cmd = f"ceph nfs cluster qos enable ops_control {cluster_name} {qos_type}"
    if qos_type.lower() == "pershare":
        if "max_export_iops" not in ops_config:
            raise ConfigError("max_export_iops required for PerShare ops control")
        cmd += f" {ops_config['max_export_iops']}"
    elif qos_type.lower() == "perclient":
        if "max_client_iops" not in ops_config:
            raise ConfigError("max_client_iops required for PerClient ops control")
        cmd += f" {ops_config['max_client_iops']}"
    elif qos_type.lower() == "pershare_perclient":
        if not all(k in ops_config for k in ["max_export_iops", "max_client_iops"]):
            raise ConfigError("Both max_export_iops and max_client_iops required")
        cmd += f" {ops_config['max_export_iops']} {ops_config['max_client_iops']}"
    else:
        raise ConfigError(f"Invalid ops control type: {qos_type}")
    client.exec_command(sudo=True, cmd=cmd)
    return cmd


def enable_export_ops_control(client, cluster_name, pseudo_path, qos_type, ops_config):
    """Enable ops control at export level"""
    cmd = f"ceph nfs export qos enable ops_control {cluster_name} {pseudo_path}"
    if qos_type.lower() == "pershare":
        if "max_export_iops" not in ops_config:
            raise ConfigError("max_export_iops required for PerShare ops control")
        cmd += f" {ops_config['max_export_iops']}"
    elif qos_type.lower() == "pershare_perclient":
        if not all(k in ops_config for k in ["max_export_iops", "max_client_iops"]):
            raise ConfigError("Both max_export_iops and max_client_iops required")
        cmd += f" {ops_config['max_export_iops']} {ops_config['max_client_iops']}"
    elif qos_type.lower() == "perclient":
        raise ConfigError("PerClient ops control not allowed at export level")
    client.exec_command(sudo=True, cmd=cmd)
    return cmd


def run(ceph_cluster, **kw):
    """Test NFS ops control at cluster and export levels"""
    config = kw.get("config")
    if not config:
        raise ConfigError("Config not found")

    try:
        # Get nodes
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        clients = ceph_cluster.get_nodes("client")
        if not nfs_nodes:
            raise OperationFailedError("No NFS nodes found in cluster")

        nfs_node = nfs_nodes[0]
        no_clients = int(config.get("clients", "1"))
        if no_clients > len(clients):
            raise ConfigError("The test requires more clients than available")

        clients = clients[:no_clients]
        client = clients[0]

        # Setup parameters
        cluster_name = config.get("cluster_name", "cephfs-nfs")
        fs_name = config.get("cephfs_volume", "cephfs")
        nfs_export = "/export_0"
        nfs_mount = "/mnt/nfs"
        fs = "cephfs"
        qos_type = config.get("qos_type")
        dd_params = config.get("dd_parameters")

        # Setup NFS cluster
        setup_nfs_cluster(
            clients=clients,
            nfs_server=nfs_node.hostname,
            port=config.get("port", "2049"),
            version=config.get("nfs_version", "4.2"),
            nfs_name=cluster_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )

        # Get baseline performance (no ops control)
        log.info("=" * 80)
        log.info("STEP 1: Measuring baseline performance (no ops control)...")
        baseline = validate_ops_control(
            client=client,
            nfs_mount=nfs_mount,
            file_name="baseline.txt",
            dd_params=dd_params
        )
        log.info("Baseline Results:")
        log.info(f"- Write time: {baseline['write_time']} seconds")
        log.info(f"- Read time: {baseline['read_time']} seconds")

        # Enable cluster level ops control
        log.info("=" * 80)
        log.info("STEP 2: Setting up cluster level ops control...")
        cluster_ops = config.get("cluster_ops", {})
        log.info(f"Cluster ops control configuration: {cluster_ops}")
        cluster_cmd = enable_cluster_ops_control(
            client, cluster_name, qos_type, cluster_ops
        )
        log.info(f"Enabled cluster ops control with command: {cluster_cmd}")

        # Verify cluster settings and test performance
        cluster_settings, _ = verify_ops_control_settings(client, cluster_name)
        log.info("Testing performance with cluster ops control...")
        cluster_test = validate_ops_control(
            client=client,
            nfs_mount=nfs_mount,
            file_name="cluster_ops.txt",
            dd_params=dd_params
        )

        # Validate cluster ops control limits
        if qos_type in ["pershare", "pershare_perclient"]:
            log.info("Validating cluster level ops control limits:")
            log.info(f"- Expected limit: {cluster_ops['max_export_iops']} IOPS")
            log.info(f"- Write time: {cluster_test['write_time']} seconds")
            matches, calc_limit = validate_ops_limit(
                cluster_test["write_time"],
                cluster_ops["max_export_iops"]
            )
            if not matches:
                raise OperationFailedError(
                    f"Cluster ops control validation failed: calculated={calc_limit}, "
                    f"expected={cluster_ops['max_export_iops']}"
                )
            log.info("Cluster level ops control validation successful")

        # Enable export level ops control if configured
        export_ops = config.get("export_ops")
        nfs_export_path = None
        if export_ops:
            log.info("=" * 80)
            log.info("STEP 3: Setting up export level ops control...")
            log.info(f"Export ops control configuration: {export_ops}")
            nfs_export_path = f"{nfs_export}_0"
            export_cmd = enable_export_ops_control(
                client, cluster_name, nfs_export_path,
                qos_type, export_ops
            )
            log.info(f"Enabled export ops control with command: {export_cmd}")
            # Verify settings and test performance
            cluster_settings, export_settings = verify_ops_control_settings(
                client, cluster_name, nfs_export_path
            )
            log.info("Testing performance with export ops control...")
            export_test = validate_ops_control(
                client=client,
                nfs_mount=nfs_mount,
                file_name="export_ops.txt",
                dd_params=dd_params
            )

            # Validate export ops control limits
            if qos_type in ["pershare", "pershare_perclient"]:
                log.info("Validating export level ops control limits:")
                log.info(f"- Expected limit: {export_ops['max_export_iops']} IOPS")
                log.info(f"- Write time: {export_test['write_time']} seconds")
                matches, calc_limit = validate_ops_limit(
                    export_test["write_time"],
                    export_ops["max_export_iops"]
                )
                if not matches:
                    raise OperationFailedError(
                        f"Export ops control validation failed: calculated={calc_limit}, "
                        f"expected={export_ops['max_export_iops']}"
                    )
                log.info("Export level ops control validation successful")

        # Handle restart validation
        if config.get("operation") == "restart":
            log.info("=" * 80)
            log.info("STEP 4: Testing ops control persistence after restart...")
            data = json.loads(Ceph(client).orch.ls(format="json"))
            service_name = next(
                (x["service_name"] for x in data if x.get("service_id") == cluster_name),
                None
            )
            if service_name:
                Ceph(client).orch.restart(service_name)
                sleep(10)

                # Verify settings persist after restart
                post_restart_settings, post_restart_export = verify_ops_control_settings(
                    client,
                    cluster_name,
                    nfs_export_path if export_ops else None
                )

                if post_restart_settings != cluster_settings:
                    raise OperationFailedError("Ops control settings changed after restart")

                # Verify ops control is still effective
                post_restart_test = validate_ops_control(
                    client=client,
                    nfs_mount=nfs_mount,
                    file_name="post_restart.txt",
                    dd_params=dd_params
                )
                log.info(f"Post-restart performance: {post_restart_test}")
        log.info("=" * 80)
        log.info("NFS ops control test completed successfully")
        return 0

    except Exception as e:
        log.error(f"NFS ops control test failed: {str(e)}")
        return 1

    finally:
        log.info("Cleanup in progress")
        cleanup_cluster(client, nfs_mount, cluster_name, nfs_export)
