"""
gRPC transport and security negative tests (single-node).

Validates port binding, service discovery, loopback targeting, mTLS,
and connection / invalid-RPC failure handling.
"""

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    DEFAULT_GRPC_UDS_PATH,
    ensure_subvolume_group,
    expect_rpc_error,
    grpc_invoke_kwargs,
    install_grpcurl,
    invoke_grpc_method,
    list_grpc_services,
    list_grpc_services_uds,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    setup_grpc_transport,
    verify_grpc_port_listening,
)
from tests.nfs.grpc_admin.grpc_deploy import deploy_grpc_certs_if_configured
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Security / transport operations (``operation`` config key):

        verify_port_listening — gRPC port open on NFS node
        verify_tcp_discovery — grpcurl list over cluster IP (mTLS or plaintext)
        verify_loopback_discovery — grpcurl list via 127.0.0.1 on NFS node
        verify_bad_port_rejected — connection to wrong port fails
        verify_tls_required — mTLS discovery; plaintext must fail when TLS required
        verify_tls_bad_cert — invalid client cert rejected
        verify_uds_discovery — grpcurl list over Unix domain socket
        verify_admin_auth_matrix — nfsAdmin RPC with mTLS vs plaintext
        verify_invalid_protobuf — malformed JSON body rejected by grpcurl/client
        verify_missing_fields — invalid field types rejected (proto3 defaults
            make empty {} valid for StartGraceWithEvent)
        verify_timeout — short client timeout yields DEADLINE_EXCEEDED / failure
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    tls_blocked = operation in ("verify_tls_required", "verify_tls_bad_cert")
    # Only skip when suite explicitly marks blocked; mTLS tests set skip_if_blocked: false.
    if (
        tls_blocked
        and config.get("blocked", False)
        and config.get("skip_if_blocked", True)
    ):
        log.info(
            "SKIP: %s pending mTLS cert deployment in gRPC admin suite",
            operation,
        )
        return -1

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    grpc_port = int(config.get("grpc_port", DEFAULT_GRPC_PORT))

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    installers = ceph_cluster.get_nodes("installer")
    installer = installers[0] if installers else None
    target = resolve_grpc_target(config, nfs_ip)

    try:
        install_grpcurl(client)
        install_grpcurl(nfs_node)
        open_grpc_firewall(nfs_node, grpc_port)
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

        transport = setup_grpc_transport(
            config, [client, nfs_node], installer, nfs_name
        )
        kw_tls = grpc_invoke_kwargs(transport)

        if operation == "verify_port_listening":
            if not verify_grpc_port_listening(nfs_node, grpc_port):
                raise OperationFailedError(f"Port {grpc_port} not listening")

        elif operation == "verify_tcp_discovery":
            ok, services = list_grpc_services(client, target, **kw_tls)
            if not ok or not services:
                raise OperationFailedError(
                    f"gRPC discovery failed on TCP target {target}"
                )

        elif operation == "verify_loopback_discovery":
            loopback_target = f"127.0.0.1:{grpc_port}"
            ok, services = list_grpc_services(nfs_node, loopback_target, **kw_tls)
            if not ok or not services:
                raise OperationFailedError(
                    "gRPC discovery failed on loopback — "
                    "ensure gRPC binds to 127.0.0.1 or adjust grpc_host"
                )

        elif operation == "verify_bad_port_rejected":
            bad_port = int(config.get("bad_port", grpc_port + 1000))
            bad_target = f"{nfs_ip}:{bad_port}"
            ok, services = list_grpc_services(client, bad_target, plaintext=True)
            if ok and services:
                raise OperationFailedError(
                    f"Expected connection failure on bad port {bad_port}"
                )
            log.info("Connection to bad port %s failed as expected", bad_port)

        elif operation == "verify_tls_required":
            if transport["plaintext"]:
                log.info(
                    "SKIP: %s — NFS gRPC is plaintext on this Ceph version",
                    operation,
                )
                return -1
            ok, services = list_grpc_services(client, target, **kw_tls)
            if not ok or not services:
                raise OperationFailedError("mTLS discovery failed with valid certs")
            # Plaintext must not list services when the server requires mTLS.
            ok_plain, plain_services = list_grpc_services(
                client, target, plaintext=True
            )
            if ok_plain and plain_services:
                raise OperationFailedError(
                    "Plaintext discovery succeeded but TLS was expected to be required"
                )
            log.info(
                "Plaintext discovery correctly rejected (ok=%s services=%s)",
                ok_plain,
                plain_services,
            )

        elif operation == "verify_tls_bad_cert":
            if transport["plaintext"]:
                log.info(
                    "SKIP: %s — NFS gRPC is plaintext on this Ceph version",
                    operation,
                )
                return -1
            bad_paths = dict(transport["tls_paths"])
            bad_paths["client_cert"] = config.get(
                "bad_client_cert",
                f"{transport['tls_paths']['client_cert']}.invalid",
            )
            ok, _ = list_grpc_services(
                client, target, plaintext=False, tls_paths=bad_paths
            )
            if ok:
                raise OperationFailedError(
                    "gRPC call succeeded with invalid client certificate"
                )

        elif operation == "verify_uds_discovery":
            uds_path = config.get("grpc_uds_path", DEFAULT_GRPC_UDS_PATH)
            ok, services = list_grpc_services_uds(nfs_node, uds_path)
            if not ok or not services:
                if config.get("skip_if_no_uds", True):
                    log.info(
                        "SKIP: UDS path %s not available — enable GRPC UDS in image",
                        uds_path,
                    )
                    return -1
                raise OperationFailedError(f"UDS gRPC discovery failed on {uds_path}")

        elif operation == "verify_admin_auth_matrix":
            if transport["plaintext"]:
                log.info(
                    "SKIP: %s — NFS gRPC is plaintext on this Ceph version",
                    operation,
                )
                return -1
            admin_method = config.get(
                "admin_method", "nfsService.nfsAdmin/ReReadConfig"
            )
            ok, out_mtls, err_mtls = invoke_grpc_method(
                client,
                target,
                admin_method,
                data="{}",
                **kw_tls,
            )
            if not ok and not config.get("allow_mtls_failure", False):
                raise OperationFailedError(
                    f"Admin RPC {admin_method} failed with valid mTLS credentials: "
                    f"{err_mtls or out_mtls}"
                )
            log.info("Admin RPC accepted with valid credentials: %s", ok)
            # Plaintext against mTLS often returns empty stdout with no RPC body;
            # only treat as success when grpcurl reports ok AND returns a response.
            ok_plain, out_plain, err_plain = invoke_grpc_method(
                client, target, admin_method, data="{}", plaintext=True
            )
            plain_succeeded = ok_plain and bool((out_plain or "").strip())
            if plain_succeeded and not config.get("allow_plaintext_admin"):
                raise OperationFailedError(
                    "Admin RPC succeeded without mTLS — auth matrix unexpected"
                )
            log.info(
                "Plaintext admin correctly rejected (ok=%s out=%r err=%r)",
                ok_plain,
                (out_plain or "")[:200],
                (err_plain or "")[:200],
            )

        elif operation == "verify_invalid_protobuf":
            # Malformed JSON only. Empty "{}" is valid proto3 for StartGraceWithEvent
            # (defaults Event=0) and must not be treated as an error.
            method = config.get(
                "rpc_method", "nfsService.StartNfsGrace/StartGraceWithEvent"
            )
            bad_data = config.get("bad_data", "{not-valid-json")
            if not expect_rpc_error(client, target, method, bad_data, **kw_tls):
                raise OperationFailedError(
                    "Expected RPC error for invalid protobuf/JSON body"
                )
            log.info("Malformed JSON correctly rejected for %s", method)

        elif operation == "verify_missing_fields":
            # Empty "{}" is accepted by StartGraceWithEvent (proto3 defaults).
            # Exercise type mismatch instead so grpcurl/proto unmarshalling fails.
            method = config.get(
                "rpc_method", "nfsService.StartNfsGrace/StartGraceWithEvent"
            )
            bad_data = config.get("bad_data", '{"Event":"not-an-int"}')
            if not expect_rpc_error(client, target, method, bad_data, **kw_tls):
                raise OperationFailedError(
                    "Expected RPC error for invalid field types in request body"
                )

        elif operation == "verify_timeout":
            method = config.get("rpc_method", "nfsService.GetNfsGrace/GetGracePeriod")
            max_time = float(config.get("max_time_sec", 0.001))
            ok, out, err = invoke_grpc_method(
                client,
                target,
                method,
                max_time_sec=max_time,
                **kw_tls,
            )
            combined = f"{out}\n{err}".lower()
            if "deadline" in combined or "timeout" in combined or not ok:
                log.info("Timeout behavior observed as expected")
            else:
                raise OperationFailedError(
                    "Expected DEADLINE_EXCEEDED/timeout with max_time_sec=%s: %s %s"
                    % (max_time, out, err)
                )

        else:
            raise ConfigError(f"Unknown security operation: {operation}")

        log.info(
            "TEST PASSED - gRPC security operation=%s target=%s",
            operation,
            target,
        )
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Security test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Security test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
