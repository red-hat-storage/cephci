"""
NFS-Ganesha gRPC API tests (management / stats plane).

Uses the ``operation`` config key to select a scenario. All operations invoke
gRPC interfaces only — D-Bus parity and NFS protocol regression are out of scope.
"""

import json
from shlex import quote
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    add_client,
    add_export,
    admin_rpc,
    check_blocked_operation,
    client_stats,
    disconnect_nfsv41_client,
    display_export,
    ensure_subvolume_group,
    export_stats,
    first_export_id,
    get_client_ids,
    get_grace_period,
    get_session_ids,
    grpc_invoke_kwargs,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    remove_client,
    remove_export,
    resolve_grpc_target,
    setup_grpc_transport,
    show_clients,
    show_exports,
    show_idmapper,
    start_grace_with_event,
    update_export,
    verify_expected_services,
    verify_grpc_port_listening,
)
from tests.nfs.grpc_admin.grpc_deploy import (
    deploy_grpc_certs_if_configured,
    find_nfs_instance_dirs,
)
from tests.nfs.nfs_operations import cleanup_cluster, nfs_log_parser, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

GRACE_EVENT_OPS = {
    "start_grace_event_0": 0,
    "start_grace_event_2": 2,
    "start_grace_event_4": 4,
    "start_grace_event_5": 5,
    "grace_event_0": 0,
    "grace_event_2": 2,
    "grace_event_4": 4,
    "grace_event_5": 5,
}


def _setup_additional_mounts(clients, nfs_server, nfs_export, nfs_mount, version, port):
    """Mount export on clients[1:] and return (client, mount_point) pairs."""
    mount_points = []
    for i, client in enumerate(clients[1:], start=1):
        mount_point = f"{nfs_mount}_{i}"
        client.create_dirs(dir_path=mount_point, sudo=True)
        if Mount(client).nfs(
            mount=mount_point,
            version=version,
            port=port,
            server=nfs_server,
            export=nfs_export,
        ):
            log.error("Failed to mount NFS on %s", client.hostname)
            continue
        client.exec_command(sudo=True, cmd=f"touch {mount_point}/grpc_admin_test_{i}")
        mount_points.append((client, mount_point))
        sleep(2)
    return mount_points


def _cleanup_additional_mounts(mount_points):
    for client, mount_point in mount_points:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf -- {quote(mount_point)}/*",
                check_ec=False,
                timeout=300,
            )
            Unmount(client).unmount(mount_point)
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf -- {quote(mount_point)}",
                check_ec=False,
                timeout=300,
            )
        except Exception as exc:
            log.warning("Cleanup error on %s: %s", client.hostname, exc)


def _write_export_config(nfs_node, path, content):
    """Write an EXPORT config snippet on the NFS node for Add/UpdateExport."""
    # Escape single quotes for shell-safe write via printf.
    escaped = content.replace("'", "'\"'\"'")
    nfs_node.exec_command(
        sudo=True,
        cmd=f"printf '%s' '{escaped}' > {path}",
    )


def _stage_export_config(nfs_node, nfs_name, filename, content):
    """
    Write EXPORT config into the cephadm NFS instance ``etc/ganesha`` dir.

    That directory is bind-mounted into the container, so ganesha can read
    ``/etc/ganesha/<filename>`` when AddExport/UpdateExport is called.

    Returns:
        str: Path as seen inside the ganesha container.
    """
    _, instance_dirs = find_nfs_instance_dirs(nfs_node, nfs_name)
    host_path = f"{instance_dirs[0]}/etc/ganesha/{filename}"
    _write_export_config(nfs_node, host_path, content)
    container_path = f"/etc/ganesha/{filename}"
    log.info(
        "Staged export config on host %s (container path %s)",
        host_path,
        container_path,
    )
    return container_path


def _build_ceph_export_block(
    export_id,
    pseudo,
    fs_name,
    user_id,
    secret,
    access_type="RW",
    path="/",
):
    """Return a Ganesha EXPORT {...} block for CEPH FSAL."""
    return (
        f"EXPORT {{\n"
        f"  Export_Id = {export_id};\n"
        f'  Path = "{path}";\n'
        f'  Pseudo = "{pseudo}";\n'
        f"  Access_Type = {access_type};\n"
        f"  Protocols = 4;\n"
        f"  Transports = TCP;\n"
        f"  FSAL {{\n"
        f"    Name = CEPH;\n"
        f'    User_Id = "{user_id}";\n'
        f'    Secret_Access_Key = "{secret}";\n'
        f'    Filesystem = "{fs_name}";\n'
        f"  }}\n"
        f"}}\n"
    )


# nfsAdmin RPCs exercised by dedicated operations (Shutdown intentionally omitted).
ADMIN_EMPTY_OPS = {
    "purge_gids": "PurgeGids",
    "purge_netgroups": "PurgeNetGroups",
    "init_fd_limit": "InitFdLimit",
    "purge_idmapper_cache": "PurgeIdmapperCache",
    "purge_idmapper_negative_cache": "PurgeIdmapperNegativeCache",
    "malloc_untrace": "MallocUntrace",
    "trim_call": "TrimCall",
    "reread_config": "ReReadConfig",
    "trim_status": "TrimStatus",
}


def run(ceph_cluster, **kw):
    """
    Run a gRPC API test scenario.

    Supported operations:
        verify_port, list_services,
        get_grace_period, start_grace_event_0|2|4|5,
        get_client_ids, get_session_ids,
        show_clients_empty, show_clients_active, show_clients_and_sessions,
        show_exports, display_export, add_export, remove_export, update_export,
        show_clients_mgr, add_client, remove_client, disconnect_nfsv41_client,
        reread_config, trim_status, trim_enable, trim_disable, trim_call,
        purge_gids, purge_netgroups, init_fd_limit,
        purge_idmapper_cache, purge_idmapper_negative_cache,
        malloc_trace, malloc_untrace, show_idmapper,
        export_stats_smoke, client_stats_smoke,
        shutdown (destructive — opt-in, not scheduled)
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    skip_rc = check_blocked_operation(operation, config)
    if skip_rc is not None:
        return skip_rc

    if operation == "shutdown" and not config.get("allow_shutdown", False):
        log.info("SKIP: shutdown is destructive — set allow_shutdown: true to enable")
        return -1

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    subvolume_group = config.get("subvolume_group", "ganeshagroup")
    setup_cluster = config.get("setup_nfs_cluster", True)

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    installers = ceph_cluster.get_nodes("installer")
    installer = installers[0] if installers else None
    target = resolve_grpc_target(config, nfs_ip)
    additional_mounts = []

    log.info("Running gRPC API operation: %s", operation)
    log.info("Target: %s", target)

    try:
        for c in clients:
            install_grpcurl(c)
        open_grpc_firewall(nfs_node, int(config.get("grpc_port", DEFAULT_GRPC_PORT)))
        ensure_subvolume_group(
            client, fs_name, subvolume_group, ceph_cluster=ceph_cluster
        )

        if setup_cluster:
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
                enable_rdma=config.get("enable_rdma", False),
                rdma_port=config.get("rdma_port"),
            )
            deploy_grpc_certs_if_configured(config, nfs_node, nfs_name, installer)

        transport = setup_grpc_transport(config, clients, installer, nfs_name)
        kw_tls = grpc_invoke_kwargs(transport)
        log.info(
            "NFS gRPC transport: %s",
            "mTLS" if transport["use_mtls"] else "plaintext",
        )

        export_path = f"{nfs_export}_0"

        if operation == "verify_port":
            if not verify_grpc_port_listening(nfs_node):
                raise OperationFailedError("gRPC port is not listening")

        elif operation == "list_services":
            ok, services = list_grpc_services(client, target, **kw_tls)
            require_all = bool(config.get("require_all_services", False))
            if not ok or not verify_expected_services(
                services, require_all=require_all
            ):
                raise OperationFailedError("Expected gRPC services not discovered")

        elif operation == "get_grace_period":
            ok, ingrace, raw = get_grace_period(client, target, **kw_tls)
            if not ok or not isinstance(ingrace, bool):
                raise OperationFailedError(f"GetGracePeriod invalid response: {raw}")
            log.info("Grace period state ingrace=%s", ingrace)

        elif operation in GRACE_EVENT_OPS:
            event_id = GRACE_EVENT_OPS[operation]
            ok, response = start_grace_with_event(
                client,
                target,
                event_id,
                node_id=int(config.get("node_id", 1)),
                ip_addr=config.get("grace_ip", nfs_ip),
                **kw_tls,
            )
            if not ok:
                raise OperationFailedError(
                    f"StartGraceWithEvent event {event_id} failed: {response}"
                )
            sleep(int(config.get("post_grace_sleep", 5)))

        elif operation == "show_clients_empty":
            ok, client_ids, _ = get_client_ids(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError("GetClientIds RPC failed")
            log.info("Client IDs with primary mount only: %s", client_ids)

        elif operation in ("show_clients_active", "show_clients_and_sessions"):
            if len(clients) > 1:
                additional_mounts = _setup_additional_mounts(
                    clients,
                    nfs_server,
                    export_path,
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )
            sleep(int(config.get("client_settle_sleep", 10)))

            ok, client_ids, _ = get_client_ids(client, target, **kw_tls)
            if not ok or not client_ids:
                raise OperationFailedError(
                    "GetClientIds failed or returned empty list with active mounts"
                )
            log.info("Active client IDs: %s", client_ids)

            if operation == "show_clients_and_sessions":
                ok, session_ids, _ = get_session_ids(client, target, **kw_tls)
                if not ok or not session_ids:
                    raise OperationFailedError(
                        "GetSessionIds failed or returned empty list with active mounts"
                    )
                log.info("Active session IDs: %s", session_ids)

        elif operation == "get_client_ids":
            ok, client_ids, _ = get_client_ids(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError("GetClientIds RPC failed")
            log.info("Client IDs: %s", client_ids)

        elif operation == "get_session_ids":
            ok, session_ids, _ = get_session_ids(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError("GetSessionIds RPC failed")
            log.info("Session IDs: %s", session_ids)

        # ---- ExportService -------------------------------------------------
        elif operation == "show_exports":
            ok, exports, raw = show_exports(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError(f"ShowExports failed: {raw}")
            if not exports and not config.get("allow_empty_exports", False):
                raise OperationFailedError("ShowExports returned no exports")
            log.info("ShowExports count=%s: %s", len(exports), exports)

        elif operation == "display_export":
            ok, exports, raw = show_exports(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError(f"ShowExports failed: {raw}")
            export_id = config.get("export_id", first_export_id(exports))
            if export_id is None:
                raise OperationFailedError("No export_id available for DisplayExport")
            ok, payload, raw = display_export(client, target, export_id, **kw_tls)
            if not ok:
                raise OperationFailedError(f"DisplayExport({export_id}) failed: {raw}")
            log.info("DisplayExport payload keys=%s", list(payload.keys()))

        elif operation == "add_export":
            filename = config.get("export_config_filename", "grpc_add_export.conf")
            expression = config.get("export_expression", "EXPORT")
            content = config.get("export_config_content")
            if not content:
                export_id = int(config.get("new_export_id", 77))
                pseudo = config.get("new_export_pseudo", "/grpc_added_export")
                user_id = config.get(
                    "fsal_user_id",
                    f"nfs.{nfs_name}.{fs_name}.{subvolume_group}.0",
                )
                secret = config.get("fsal_secret_access_key", "")
                content = _build_ceph_export_block(
                    export_id=export_id,
                    pseudo=pseudo,
                    fs_name=fs_name,
                    user_id=user_id,
                    secret=secret,
                    access_type=config.get("access_type", "RW"),
                )
            # Prefer config override path (already container-visible); else stage
            # into the bind-mounted etc/ganesha directory.
            if config.get("export_config_path"):
                file_path = config["export_config_path"]
                _write_export_config(nfs_node, file_path, content)
            else:
                file_path = _stage_export_config(nfs_node, nfs_name, filename, content)
            ok, raw = add_export(client, target, file_path, expression, **kw_tls)
            if not ok:
                raise OperationFailedError(f"AddExport failed: {raw}")
            ok, exports, _ = show_exports(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError("ShowExports after AddExport failed")
            log.info("Exports after AddExport: %s", exports)

        elif operation == "remove_export":
            # Prefer suite-provided export_id; else create a second ceph export and remove it.
            export_id = config.get("export_id")
            created_path = None
            if export_id is None:
                created_path = config.get("remove_export_path", f"{nfs_export}_grpc_rm")
                Ceph(client).nfs.export.create(
                    fs_name=fs_name,
                    nfs_name=nfs_name,
                    nfs_export=created_path,
                    fs=fs,
                )
                sleep(int(config.get("export_settle_sleep", 5)))
                ok, exports, raw = show_exports(client, target, **kw_tls)
                if not ok:
                    raise OperationFailedError(f"ShowExports failed: {raw}")
                # Match the export we just created — never fall back to an
                # arbitrary id (could remove the primary setup export).
                for entry in exports:
                    pseudo = entry.get("pseudoPath") or entry.get("pseudo_path") or ""
                    full = entry.get("fullPath") or entry.get("full_path") or ""
                    if created_path in pseudo or created_path in full:
                        export_id = first_export_id([entry])
                        break
            if export_id is None:
                raise OperationFailedError(
                    f"Could not resolve export_id for RemoveExport "
                    f"(created_path={created_path})"
                )
            ok, raw = remove_export(client, target, export_id, **kw_tls)
            if not ok:
                raise OperationFailedError(f"RemoveExport({export_id}) failed: {raw}")
            sleep(2)
            ok, exports, _ = show_exports(client, target, **kw_tls)
            remaining_ids = [e.get("exportId", e.get("export_id")) for e in exports]
            if int(export_id) in [int(i) for i in remaining_ids if i is not None]:
                raise OperationFailedError(
                    f"Export {export_id} still present after RemoveExport"
                )
            log.info(
                "RemoveExport(%s) succeeded; remaining=%s", export_id, remaining_ids
            )

        elif operation == "update_export":
            filename = config.get("export_config_filename", "grpc_update_export.conf")
            expression = config.get("export_expression", "EXPORT")
            content = config.get("export_config_content")
            export_id = config.get("export_id")
            if not content:
                ok, exports, raw = show_exports(client, target, **kw_tls)
                if not ok or not exports:
                    raise OperationFailedError(
                        f"ShowExports required for update_export: {raw}"
                    )
                export_id = (
                    export_id if export_id is not None else first_export_id(exports)
                )
                if export_id is None:
                    raise OperationFailedError("No export_id for UpdateExport")
                ok, payload, raw = display_export(client, target, export_id, **kw_tls)
                if not ok:
                    raise OperationFailedError(
                        f"DisplayExport({export_id}) failed before update: {raw}"
                    )
                pseudo = (
                    payload.get("pseudoPath")
                    or payload.get("pseudo_path")
                    or config.get("update_pseudo", export_path)
                )
                full_path = payload.get("fullPath") or payload.get("full_path") or "/"
                user_id = config.get(
                    "fsal_user_id",
                    f"nfs.{nfs_name}.{fs_name}.{subvolume_group}.0",
                )
                secret = config.get("fsal_secret_access_key", "")
                # Flip access type so the update is observable.
                new_access = config.get("update_access_type", "RO")
                content = _build_ceph_export_block(
                    export_id=int(export_id),
                    pseudo=pseudo,
                    fs_name=fs_name,
                    user_id=user_id,
                    secret=secret,
                    access_type=new_access,
                    path=full_path if full_path.startswith("/") else f"/{full_path}",
                )
            if config.get("export_config_path"):
                file_path = config["export_config_path"]
                _write_export_config(nfs_node, file_path, content)
            else:
                file_path = _stage_export_config(nfs_node, nfs_name, filename, content)
            ok, raw = update_export(client, target, file_path, expression, **kw_tls)
            if not ok:
                raise OperationFailedError(f"UpdateExport failed: {raw}")
            # Best-effort verify via DisplayExport
            ok, exports, _ = show_exports(client, target, **kw_tls)
            if export_id is None and exports:
                export_id = first_export_id(exports)
            if export_id is not None:
                ok, payload, raw = display_export(client, target, export_id, **kw_tls)
                if not ok:
                    raise OperationFailedError(
                        f"DisplayExport after UpdateExport failed: {raw}"
                    )
                log.info(
                    "UpdateExport verified DisplayExport perms=%s",
                    payload.get("exportPermissions")
                    or payload.get("export_permissions"),
                )

        # ---- ClientMgr -----------------------------------------------------
        elif operation == "show_clients_mgr":
            if len(clients) > 1:
                additional_mounts = _setup_additional_mounts(
                    clients,
                    nfs_server,
                    export_path,
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )
            sleep(int(config.get("client_settle_sleep", 10)))
            ok, mgr_clients, raw = show_clients(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError(f"ClientMgr.ShowClients failed: {raw}")
            log.info(
                "ClientMgr.ShowClients count=%s: %s", len(mgr_clients), mgr_clients
            )

        elif operation == "add_client":
            ipaddr = config.get("client_ip", clients[-1].ip_address)
            ok, raw = add_client(client, target, ipaddr, **kw_tls)
            if not ok:
                raise OperationFailedError(f"AddClient({ipaddr}) failed: {raw}")

        elif operation == "remove_client":
            ipaddr = config.get("client_ip", clients[-1].ip_address)
            # Ensure entry exists then remove.
            add_client(client, target, ipaddr, **kw_tls)
            ok, raw = remove_client(client, target, ipaddr, **kw_tls)
            if not ok:
                raise OperationFailedError(f"RemoveClient({ipaddr}) failed: {raw}")

        elif operation == "disconnect_nfsv41_client":
            if len(clients) > 1:
                additional_mounts = _setup_additional_mounts(
                    clients,
                    nfs_server,
                    export_path,
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )
            sleep(int(config.get("client_settle_sleep", 5)))
            ipaddr = config.get("client_ip", clients[-1].ip_address)
            ok, raw = disconnect_nfsv41_client(client, target, ipaddr, **kw_tls)
            if not ok:
                raise OperationFailedError(
                    f"DisconnectNfsv41Client({ipaddr}) failed: {raw}"
                )

        # ---- nfsAdmin / IdMapper -------------------------------------------
        elif operation in ADMIN_EMPTY_OPS:
            rpc_name = ADMIN_EMPTY_OPS[operation]
            ok, raw = admin_rpc(client, target, rpc_name, **kw_tls)
            if not ok:
                raise OperationFailedError(f"nfsAdmin.{rpc_name} failed: {raw}")

        elif operation == "trim_enable":
            data = json.dumps({"enable": True})
            ok, raw = admin_rpc(
                client, target, "TrimEnableDisable", data=data, **kw_tls
            )
            if not ok:
                raise OperationFailedError(
                    f"TrimEnableDisable(enable=true) failed: {raw}"
                )

        elif operation == "trim_disable":
            data = json.dumps({"enable": False})
            ok, raw = admin_rpc(
                client, target, "TrimEnableDisable", data=data, **kw_tls
            )
            if not ok:
                raise OperationFailedError(
                    f"TrimEnableDisable(enable=false) failed: {raw}"
                )

        elif operation == "malloc_trace":
            filename = config.get("malloc_trace_file", "/tmp/ganesha_malloc_trace.out")
            data = json.dumps({"filename": filename})
            ok, raw = admin_rpc(client, target, "MallocTrace", data=data, **kw_tls)
            if not ok:
                raise OperationFailedError(f"MallocTrace failed: {raw}")

        elif operation == "show_idmapper":
            ok, entries, raw = show_idmapper(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError(f"ShowIdMapper failed: {raw}")
            log.info("ShowIdMapper entries=%s", len(entries))

        elif operation == "shutdown":
            ok, raw = admin_rpc(client, target, "ShutdownGanesha", **kw_tls)
            if not ok:
                raise OperationFailedError(f"ShutdownGanesha failed: {raw}")

        # ---- Stats ---------------------------------------------------------
        elif operation == "export_stats_smoke":
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={nfs_mount}/grpc_stats_io bs=4k count=32 "
                "oflag=dsync 2>/dev/null",
                check_ec=False,
            )
            sleep(2)
            ok, exports, raw = show_exports(client, target, **kw_tls)
            if not ok:
                raise OperationFailedError(f"ShowExports failed: {raw}")
            export_id = config.get("export_id", first_export_id(exports))
            ok, raw = export_stats(client, target, "StatusStats", **kw_tls)
            if not ok:
                raise OperationFailedError(f"StatusStats failed: {raw}")
            if export_id is not None:
                ok, raw = export_stats(
                    client,
                    target,
                    config.get("export_stats_rpc", "GetNFSv41IO"),
                    export_id=export_id,
                    **kw_tls,
                )
                if not ok:
                    raise OperationFailedError(
                        f"ExportStats IO RPC failed for export {export_id}: {raw}"
                    )
            ok, raw = export_stats(client, target, "GetNFSIO", **kw_tls)
            if not ok:
                raise OperationFailedError(f"GetNFSIO failed: {raw}")
            log.info("ExportStats smoke passed")

        elif operation == "client_stats_smoke":
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={nfs_mount}/grpc_client_stats_io bs=4k "
                "count=32 oflag=dsync 2>/dev/null",
                check_ec=False,
            )
            sleep(2)
            ipaddr = config.get("client_ip", client.ip_address)
            ok, raw = client_stats(
                client,
                target,
                config.get("client_stats_rpc", "GetNFSv41IO"),
                ipaddr,
                **kw_tls,
            )
            if not ok:
                raise OperationFailedError(f"ClientStats failed for {ipaddr}: {raw}")
            log.info("ClientStats smoke passed for %s", ipaddr)

        else:
            raise ConfigError(f"Unknown operation: {operation}")

        log.info("TEST PASSED - gRPC API operation=%s target=%s", operation, target)
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("gRPC API test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("gRPC API test failed with unexpected error: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            _cleanup_additional_mounts(additional_mounts)
            nfs_log_parser(client=client, nfs_node=nfs_nodes, nfs_name=nfs_name)
            if setup_cluster and operation != "shutdown":
                cleanup_cluster(
                    client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
                )
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
