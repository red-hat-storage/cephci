from nfs_delegation_operations import (
    CONF_KEY,
    LOG_TEMPLATE_BACKUP_PATH,
    backup_ganesha_template,
    enable_ganesha_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    redeploy_nfs_clusters,
    restore_ganesha_template,
    run_cephadm_shell,
    run_export_level_delegation_checks,
    set_cluster_delegation,
    skip_delegation_tests_unless_supported,
    verify_cluster_delegation_on_nfs_nodes,
)
from nfs_operations import (
    cleanup_cluster,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Validate cluster-level Delegations toggles and export-level delegations (rw/ro/none).

    Toggles delegation_sequence on the ganesha template, redeploys NFS, verifies container
    and host conf, optionally runs export create/update/negative checks when Delegations=true.
    """
    config = kw.get("config", {})
    if skip_delegation_tests_unless_supported(config):
        return 0
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")

    if not clients:
        raise ConfigError("NFS delegation test requires at least one client node")
    if not nfs_nodes:
        raise ConfigError("NFS delegation test requires at least one nfs node")
    if not installers:
        raise ConfigError("NFS delegation test requires an installer node")

    installer = installers[0]
    nfs_cmd_host = nfs_nodes[0]
    log.info(
        "Running cephadm shell / config-key / template steps on NFS node %s",
        nfs_cmd_host.hostname,
    )
    cephadm = CephAdm(installer).ceph
    redeploy_wait = int(config.get("redeploy_wait", 10))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    delegation_verify_timeout = int(
        config.get("delegation_verify_timeout", service_wait_timeout)
    )
    delegation_verify_poll = int(config.get("delegation_verify_poll_seconds", 5))
    delegation_sequence = [
        str(item).strip().lower()
        for item in config.get("delegation_sequence", ["true", "false"])
    ]
    if any(item not in ("true", "false") for item in delegation_sequence):
        raise ConfigError("delegation_sequence accepts only 'true'/'false' values")
    enable_debug_logging = bool(config.get("enable_ganesha_debug_logging", False))
    run_export_level = bool(config.get("run_export_level_checks", True))
    reset_to_default_on_exit = bool(config.get("reset_to_default_on_exit", False))

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_cluster_level_delegation")
    nfs_export = config.get("nfs_export", "/export_cluster_level_delegation")
    fs_name = config.get("fs_name", "cephfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    auto_create_nfs_cluster = bool(config.get("auto_create_nfs_cluster", True))

    nfs_clusters = cephadm.nfs.cluster.ls()
    created_cluster = False
    if nfs_name not in nfs_clusters:
        if not auto_create_nfs_cluster:
            raise ConfigError(
                f"NFS cluster {nfs_name!r} not found ({nfs_clusters}). "
                "Set auto_create_nfs_cluster=true or pre-deploy the cluster."
            )
        nfs_servers = [node.hostname for node in nfs_nodes]
        log.info(
            "Creating %s on nfs nodes: %s",
            nfs_name,
            ", ".join(nfs_servers),
        )
        setup_nfs_cluster(
            clients=[clients[0]],
            nfs_server=nfs_servers,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs_name,
            ceph_cluster=ceph_cluster,
            single_export=True,
        )
        verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
        nfs_clusters = cephadm.nfs.cluster.ls()
        if nfs_name not in nfs_clusters:
            raise OperationFailedError(
                f"Failed to create nfs cluster {nfs_name}. Current clusters: {nfs_clusters}"
            )
        created_cluster = True

    ensure_ceph_conf_and_admin_keyring_on_hosts(installer, nfs_nodes)

    backup_exists = False
    log_backup_exists = False
    export_checks_done = False
    try:
        backup_exists = backup_ganesha_template(nfs_cmd_host)
        if enable_debug_logging:
            log.info(
                "Enabling Ganesha FULL_DEBUG logging block before delegation validation"
            )
            log_backup_exists = enable_ganesha_debug_logging(nfs_cmd_host)
            redeploy_nfs_clusters(
                cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
            )

        for delegation in delegation_sequence:
            log.info("Applying cluster-level NFSv4 Delegations = %s", delegation)
            set_cluster_delegation(nfs_cmd_host, delegation)
            redeploy_nfs_clusters(
                cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
            )

            verify_cluster_delegation_on_nfs_nodes(
                cephadm,
                nfs_nodes,
                nfs_clusters,
                delegation,
                timeout_seconds=delegation_verify_timeout,
                poll_interval=delegation_verify_poll,
            )

            if run_export_level and delegation == "true" and not export_checks_done:
                log.info(
                    "Cluster-level Delegations=true verified. "
                    "Running export-level delegation checks"
                )
                run_export_level_delegation_checks(
                    nfs_cmd_host, fs_name, nfs_name, config
                )
                export_checks_done = True

        if run_export_level and not export_checks_done:
            raise ConfigError(
                "run_export_level_checks=true requires delegation_sequence to include 'true'"
            )

        return 0
    except Exception as err:
        log.error("NFS cluster-level Delegations validation failed: %s", err)
        return 1
    finally:
        try:
            if reset_to_default_on_exit:
                log.info(
                    "reset_to_default_on_exit=true: removing custom nfs template config-key"
                )
                run_cephadm_shell(
                    nfs_cmd_host, f"ceph config-key rm {CONF_KEY}", check_ec=False
                )
            else:
                if enable_debug_logging:
                    restore_ganesha_template(
                        nfs_cmd_host, log_backup_exists, LOG_TEMPLATE_BACKUP_PATH
                    )
                restore_ganesha_template(nfs_cmd_host, backup_exists)
            redeploy_nfs_clusters(
                cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
            )
            if created_cluster:
                cleanup_cluster(
                    clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes
                )
        except Exception as cleanup_err:
            log.warning(
                "Failed to restore previous nfs template state: %s", cleanup_err
            )
