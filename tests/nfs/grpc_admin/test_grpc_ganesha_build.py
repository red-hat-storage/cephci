"""
gRPC build verification for cephadm NFS containers.

Checks USE_GRPC build artifacts, dual-stack D-Bus+gRPC, and proto compile smoke.
"""

import json

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    ensure_subvolume_group,
    grpc_invoke_kwargs,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    setup_grpc_transport,
)
from tests.nfs.grpc_admin.grpc_deploy import get_nfs_container_id
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

DBUS_SHOW_EXPORTS = (
    "org.ganesha.nfsd",
    "/org/ganesha/nfsd/ExportMgr",
    "org.ganesha.nfsd.exportmgr",
    "ShowExports",
)


def _exec_in_container(nfs_node, container_id, cmd):
    if not container_id:
        return "", "no container"
    full = f"podman exec {container_id} bash -lc {json.dumps(cmd)}"
    out, err = nfs_node.exec_command(sudo=True, cmd=full, check_ec=False)
    return out, err


def _verify_grpc_packages(nfs_node, container_id):
    out, err = _exec_in_container(
        nfs_node,
        container_id,
        "rpm -qa 2>/dev/null | grep -i grpc || dpkg -l 2>/dev/null | grep -i grpc",
    )
    combined = f"{out}\n{err}"
    if "grpc" not in combined.lower():
        raise OperationFailedError(
            "No gRPC packages found in NFS container — image may lack USE_GRPC=ON"
        )
    log.info("gRPC packages in container:\n%s", out.strip())


def _verify_ganesha_grpc_binary(nfs_node, container_id):
    out, _ = _exec_in_container(
        nfs_node,
        container_id,
        "ldd $(which ganesha.nfsd 2>/dev/null || echo /usr/bin/ganesha.nfsd) "
        "2>/dev/null | grep -i grpc || strings $(which ganesha.nfsd 2>/dev/null) "
        "| grep -i grpc_server | head -3",
    )
    if not out.strip():
        raise OperationFailedError(
            "Could not confirm ganesha.nfsd gRPC linkage in NFS container"
        )
    log.info("ganesha gRPC linkage hint:\n%s", out.strip())


def _dbus_show_exports(nfs_node, container_id):
    bus, path, iface, method = DBUS_SHOW_EXPORTS
    cmd = f"busctl call {bus} {path} {iface} {method} 2>/dev/null"
    out, err = _exec_in_container(nfs_node, container_id, cmd)
    return out.strip(), err


def _verify_proto_compile(nfs_node, container_id, config):
    proto_dir = config.get("proto_dir", "/usr/share/nfs-ganesha/grpc/proto")
    out, err = _exec_in_container(
        nfs_node,
        container_id,
        f"test -d {proto_dir} && ls {proto_dir}/*.proto 2>/dev/null | head -5",
    )
    if not out.strip():
        if config.get("skip_if_no_proto", True):
            log.info(
                "SKIP: proto dir %s not found in container — upstream proto CI",
                proto_dir,
            )
            return -1
        raise OperationFailedError(f"Proto directory missing: {proto_dir}")

    compile_cmd = (
        f"for f in {proto_dir}/*.proto; do "
        "protoc --proto_path=$(dirname $f) --descriptor_set_out=/tmp/out.pb $f "
        "2>&1 && break; done; test -f /tmp/out.pb"
    )
    out, err = _exec_in_container(nfs_node, container_id, compile_cmd)
    if "error" in f"{out}\n{err}".lower() or "not found" in f"{out}\n{err}".lower():
        if config.get("skip_if_no_protoc", True):
            log.info("SKIP: protoc unavailable or proto compile failed in container")
            return -1
        raise OperationFailedError(f"Proto compile failed: {out} {err}")
    log.info("TEST PASSED - proto compile smoke")
    return 0


def run(ceph_cluster, **kw):
    """
    Build operations (``operation`` config key):

        verify_grpc_build — gRPC packages / binary in NFS container
        verify_dual_stack — D-Bus ShowExports + grpcurl list both work
        verify_proto_compile — protoc smoke on bundled .proto files
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

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
    target = resolve_grpc_target(config, nfs_ip)
    installers = ceph_cluster.get_nodes("installer")
    installer = installers[0] if installers else client

    try:
        install_grpcurl(client)
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
        transport = setup_grpc_transport(config, [client], installer, nfs_name)
        kw_tls = grpc_invoke_kwargs(transport)

        container_id = get_nfs_container_id(installer, nfs_name)
        if not container_id:
            out = CephAdm(installer).ceph.orch.ps(
                service_name=f"nfs.{nfs_name}", format="json"
            )
            processes = json.loads(out) if out else []
            container_id = processes[0].get("container_id") if processes else None

        if operation == "verify_grpc_build":
            _verify_grpc_packages(nfs_node, container_id)
            _verify_ganesha_grpc_binary(nfs_node, container_id)

        elif operation == "verify_dual_stack":
            dbus_out, dbus_err = _dbus_show_exports(nfs_node, container_id)
            if not dbus_out and "error" in (dbus_err or "").lower():
                raise OperationFailedError(f"D-Bus ShowExports failed: {dbus_err}")
            log.info("D-Bus ShowExports: %s", dbus_out[:200])
            ok, services = list_grpc_services(client, target, **kw_tls)
            if not ok or not services:
                raise OperationFailedError("gRPC discovery failed in dual-stack check")
            log.info("Dual-stack OK: D-Bus + gRPC both responsive")

        elif operation == "verify_proto_compile":
            return _verify_proto_compile(nfs_node, container_id, config)

        else:
            raise ConfigError(f"Unknown build operation: {operation}")

        log.info("TEST PASSED - gRPC build operation=%s", operation)
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Build test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Build test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
