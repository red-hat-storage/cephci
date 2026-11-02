"""
Deploy and verify gRPC-enabled cephadm NFS image configuration.

Run early in gRPC suites (after bootstrap, before NFS cluster create).

Operations:
    set_nfs_image — apply ``nfs_container_image`` via mgr config
    verify_grpc_bootstrap — post-NFS-deploy gRPC port + discovery smoke
    deploy_grpc_certs — copy ``/root/certs`` into cephadm NFS instance cert path
"""

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    ensure_subvolume_group,
    grpc_invoke_kwargs,
    install_grpcurl,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    setup_grpc_transport,
)
from tests.nfs.grpc_admin.grpc_deploy import (
    apply_nfs_container_image,
    copy_grpc_certs_to_nfs_instances,
    deploy_grpc_certs_if_configured,
    resolve_nfs_container_image,
    verify_grpc_in_nfs_pod,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    installers = ceph_cluster.get_nodes("installer")
    if not installers:
        raise OperationFailedError("No installer node available")
    installer = installers[0]

    if operation == "set_nfs_image":
        image = resolve_nfs_container_image(config)
        if not image:
            log.info("SKIP: no nfs_container_image in config")
            return -1
        apply_nfs_container_image(installer, image)
        log.info("TEST PASSED - set nfs_container_image=%s", image)
        return 0

    if operation == "deploy_grpc_certs":
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        if not nfs_nodes:
            raise OperationFailedError("No NFS nodes available for cert deploy")
        nfs_node = nfs_nodes[0]
        nfs_name = config.get("nfs_name", "cephfs-nfs")
        source = config.get("grpc_cert_source", "/root/certs")
        restart = config.get("restart_nfs_after_cert_copy", True)
        if not copy_grpc_certs_to_nfs_instances(
            nfs_node,
            nfs_name,
            cert_source=source,
            restart_nfs=restart,
            installer=installer,
            restart_sleep=int(config.get("cert_copy_restart_sleep", 15)),
        ):
            log.info(
                "No certs copied — create %s on %s before running this step",
                source,
                nfs_node.hostname,
            )
            return -1
        return 0

    if operation == "verify_grpc_bootstrap":
        clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = (
            prepare_cluster_nodes(ceph_cluster, config)
        )
        nfs_name = config.get("nfs_name", "cephfs-nfs")
        nfs_export = config.get("nfs_export", "/export")
        nfs_mount = config.get("nfs_mount", "/mnt/nfs")
        nfs_version = config.get("nfs_version", 4.1)
        nfs_port = config.get("port", 2049)
        fs_name = config.get("fs_name", "cephfs")
        fs = config.get("fs", "cephfs")

        image = resolve_nfs_container_image(config)
        if image:
            apply_nfs_container_image(installer, image)

        try:
            install_grpcurl(client)
            open_grpc_firewall(nfs_node)
            ensure_subvolume_group(client, fs_name, ceph_cluster=ceph_cluster)
            setup_nfs_cluster(
                clients=[client],
                nfs_server=nfs_server,
                port=nfs_port,
                version=nfs_version,
                nfs_name=nfs_name,
                nfs_mount=nfs_mount,
                fs_name=fs_name,
                export=nfs_export,
                fs=fs,
                ceph_cluster=ceph_cluster,
            )
            deploy_grpc_certs_if_configured(config, nfs_node, nfs_name, installer)
            target = resolve_grpc_target(config, nfs_ip)
            transport = setup_grpc_transport(config, [client], installer, nfs_name)
            kw_tls = grpc_invoke_kwargs(transport)
            if not verify_grpc_in_nfs_pod(nfs_node, client, nfs_ip, **kw_tls):
                raise OperationFailedError(
                    f"gRPC bootstrap verification failed on {target}"
                )
            log.info("TEST PASSED - gRPC bootstrap verification on %s", target)
            return 0
        finally:
            try:
                cleanup_cluster(
                    client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
                )
            except Exception as exc:
                log.warning("Cleanup error (non-fatal): %s", exc)

    raise ConfigError(f"Unknown deploy operation: {operation}")
