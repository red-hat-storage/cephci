"""
BYOK NFS export with path-scoped cmount_path MDS caps.

Polarion: CEPH-83633334
Refs: IBMCEPH-22631, IBMCEPH-20981, tracker #81908, ceph PR #73696

Validates that a BYOK (KMIP) export created with --cmount_path (PEC) keeps
path-restricted MDS caps (not mds allow *) and that NFS mount / keyset succeed
on a build with the encryption auth-cap fix.
"""

import json
import random
import string
import traceback

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    collect_gklm_logs_on_failure,
    load_gklm_config,
    nfs_byok_test_setup,
    perform_io_operations_and_validate_fuse,
)
from tests.nfs.nfs_operations import cleanup_custom_nfs_cluster_multi_export_client
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

tc = "CEPH-83633334"


def _export_info(client, nfs_name, export_name):
    out = Ceph(client).nfs.export.get(nfs_name, export_name)
    return json.loads(out)


def _assert_path_scoped_caps(client, nfs_name, export_name):
    """Require cmount_path != / and MDS caps path-scoped (not allow *)."""
    info = _export_info(client, nfs_name, export_name)
    fsal = info.get("fsal") or {}
    cmount = fsal.get("cmount_path") or info.get("cmount_path")
    user_id = fsal.get("user_id")
    path = info.get("path")
    if not cmount or cmount == "/":
        raise OperationFailedError(
            f"Export {export_name} cmount_path={cmount!r}; expected subvol path (PEC)"
        )
    if path and cmount != path:
        log.warning(
            "cmount_path %r differs from export path %r (continuing)", cmount, path
        )
    if not user_id:
        raise OperationFailedError(
            f"Export {export_name} missing fsal.user_id; cannot verify MDS caps"
        )

    out, _ = client.exec_command(
        sudo=True, cmd=f"ceph auth get client.{user_id}", check_ec=False
    )
    caps_text = str(out)
    log.info("Auth for client.%s:\n%s", user_id, caps_text)
    if "mds" not in caps_text.lower():
        raise OperationFailedError(
            f"client.{user_id} auth output missing mds caps: {caps_text!r}"
        )
    # Path-scoped: must mention path= ; must not be unrestricted allow *
    if "path=" not in caps_text:
        raise OperationFailedError(
            f"client.{user_id} MDS caps are not path-scoped (no path=): {caps_text}"
        )
    # Reject pure unrestricted mds allow * without path (customer workaround)
    for line in caps_text.splitlines():
        stripped = line.strip()
        if "mds" in stripped and "path=" not in stripped and "allow *" in stripped:
            raise OperationFailedError(
                f"client.{user_id} has unrestricted mds allow * (not the cmount_path "
                f"regression path): {stripped}"
            )
    log.info(
        "Verified path-scoped MDS caps for client.%s cmount_path=%s", user_id, cmount
    )
    return info


def run(ceph_cluster, **kw):
    """
    BYOK + cmount_path path-scoped caps mount/keyset (CEPH-83633334).

    Steps:
      1. GKLM + BYOK NFS instance setup
      2. Create encrypted export with cmount_path (PEC)
      3. Assert export cmount_path and MDS caps remain path-scoped
      4. Mount must succeed; optional IO + FUSE encryption check
    """
    config = kw.get("config", {})
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    cloud_type = str(config.get("cloud-type", "")).lower()
    if cloud_type == "aws":
        log.info("Skipping BYOK cmount_path caps test: not supported on AWS")
        return 0

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installer = ceph_cluster.get_nodes(role="installer")
    clients = ceph_cluster.get_nodes("client")

    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok_cmount")
    nfs_export = config.get("nfs_export", "/export_byok_cmount")
    nfs_name = config.get("nfs_instance_name", "nfs_byok_cmount")
    export_num = int(config.get("total_export_num", 1))
    no_clients = int(config.get("clients", 1))
    fs_name = config.get("fs_name", "cephfs")
    port = config.get("nfs_port", "2049")
    version = config.get("nfs_version", "4.2")
    run_io = config.get("run_io_validate_fuse", True)

    if no_clients > len(clients):
        raise ConfigError(
            f"Test requires {no_clients} clients but only {len(clients)} available"
        )
    clients = clients[:no_clients]
    nfs_node = nfs_nodes[0]

    gklm_params = load_gklm_config(custom_data, config, cephci_data)
    # Unique GKLM client/cert names per run — fixed names collide on shared GKLM
    # (CTGKM3542E already exists) and abort the TC before cmount_path is exercised.
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(4)
    )
    gklm_client_name = config.get(
        "gklm_client_name", "automation_cmount_{}".format(rand_str)
    )
    gklm_cert_alias = config.get(
        "gklm_cert_alias", "cert_cmount_{}".format(rand_str)
    )
    client_export_mount_dict = None
    gklm_rest_client = None
    test_failed = True

    byok_setup_params = {
        "gklm_ip": gklm_params["gklm_ip"],
        "gklm_user": gklm_params["gklm_user"],
        "gklm_password": gklm_params["gklm_password"],
        "gklm_node_user": gklm_params["gklm_node_username"],
        "gklm_node_password": gklm_params["gklm_node_password"],
        "gklm_hostname": gklm_params["gklm_hostname"],
        "gklm_client_name": gklm_client_name,
        "gklm_cert_alias": gklm_cert_alias,
        "nfs_nodes": nfs_nodes,
        "installer": installer,
        "nfs_name": nfs_name,
    }
    if gklm_params.get("gklm_rest_prefix"):
        byok_setup_params["gklm_rest_prefix"] = gklm_params["gklm_rest_prefix"]

    try:
        log.info("[%s] Step 1: BYOK / GKLM NFS setup", tc)
        status, enctag, gklm_rest_client, _cert = nfs_byok_test_setup(byok_setup_params)
        if status != 0 or not enctag:
            raise OperationFailedError("nfs_byok_test_setup failed")

        Ceph(clients[0]).fs.sub_volume_group.create(
            volume=fs_name, group="ganeshagroup"
        )

        log.info(
            "[%s] Step 2: Create BYOK export with cmount_path (PEC) and mount", tc
        )
        client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            export_num,
            fs_name,
            nfs_name,
            fs_name,
            port,
            version=version,
            enctag=enctag,
            cmount_path=True,
            nfs_server=nfs_node.hostname,
        )

        log.info("[%s] Step 3: Assert path-scoped cmount_path + MDS caps", tc)
        for client in clients:
            for export_name in client_export_mount_dict[client]["export"]:
                _assert_path_scoped_caps(client, nfs_name, export_name)

        if run_io:
            log.info("[%s] Step 4: IO + FUSE encryption validation", tc)
            perform_io_operations_and_validate_fuse(
                client_export_mount_dict,
                clients,
                file_count=int(config.get("file_count", 5)),
                dd_command_size_in_M=int(config.get("dd_command_size_in_M", 5)),
                nfs_name=nfs_name,
            )

        test_failed = False
        log.info("[%s] PASS: BYOK cmount_path path-scoped caps mount OK", tc)
        return 0

    except Exception as exc:
        log.error("[%s] FAIL: %s", tc, exc)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("[%s] Cleanup", tc)
        if test_failed and gklm_params:
            try:
                collect_gklm_logs_on_failure(gklm_params)
            except Exception as cleanup_exc:
                log.warning("GKLM failure log collect: %s", cleanup_exc)
        # Unmount FUSE paths left by perform_io_operations_and_validate_fuse so
        # later suite TCs do not see leftover /mnt/nfs_*_fuse in mount(8) dumps.
        if client_export_mount_dict is not None:
            for client in clients:
                for mount in [
                    m + "_fuse"
                    for m in client_export_mount_dict.get(client, {}).get("mount", [])
                ]:
                    try:
                        client.exec_command(
                            sudo=True, cmd="rm -rf {}/*".format(mount), check_ec=False
                        )
                        Unmount(client).unmount(mount)
                    except Exception as cleanup_exc:
                        log.warning("FUSE cleanup %s: %s", mount, cleanup_exc)
            try:
                cleanup_custom_nfs_cluster_multi_export_client(
                    clients, nfs_mount, nfs_name, nfs_export, export_num
                )
            except Exception as cleanup_exc:
                log.warning("NFS cleanup: %s", cleanup_exc)
        if gklm_rest_client is not None:
            try:
                clean_up_gklm(
                    gklm_rest_client=gklm_rest_client,
                    gkml_client_name=gklm_client_name,
                    gklm_cert_alias=gklm_cert_alias,
                )
            except Exception as cleanup_exc:
                log.warning("GKLM cleanup: %s", cleanup_exc)
