"""
Regional FS parity — BYOK-01
Live key/cert rotation under I/O via GKLM (SIGHUP expected).
"""

import traceback

from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    collect_gklm_logs_on_failure,
    load_gklm_config,
    nfs_byok_test_setup,
    perform_io_operations_and_validate_fuse,
)
from tests.nfs.byok.test_byok_single_multi_restart import validate_sighup
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    gklm_params = None
    gklm_rest_client = None
    try:
        config = kw.get("config", {})
        custom_data = kw.get("test_data", {})
        cephci_data = get_cephci_config()
        gklm_params = load_gklm_config(custom_data, config, cephci_data)

        nfs_nodes = ceph_cluster.get_nodes("nfs")
        installer = ceph_cluster.get_nodes(role="installer")[0]
        clients = ceph_cluster.get_nodes("client")[: int(config.get("clients", 1))]
        nfs_name = config.get("nfs_instance_name", "nfs_byok_rotate")
        nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok_rotate")
        nfs_export = config.get("nfs_export", "/export_byok_rotate")
        fs_name = config.get("fs_name", "cephfs")
        port = str(config.get("nfs_port", "2049"))
        export_num = int(config.get("total_export_num", 2))

        byok_setup_params = {
            "nfs_nodes": nfs_nodes,
            "installer": installer,
            "nfs_name": nfs_name,
            **gklm_params,
        }
        rc, enctag, gklm_rest_client, _cert = nfs_byok_test_setup(byok_setup_params)
        if rc != 0:
            return 1

        client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
            clients,
            nfs_export,
            nfs_mount,
            export_num,
            fs_name,
            nfs_name,
            fs_name,
            port,
            version=config.get("nfs_version", "4.2"),
            enctag=enctag,
        )

        perform_io_operations_and_validate_fuse(
            client_export_mount_dict,
            clients,
            file_count=int(config.get("file_count", 10)),
            dd_command_size_in_M=int(config.get("dd_command_size_in_M", 5)),
            nfs_name=nfs_name,
        )

        old_alias = gklm_params["gklm_cert_alias"]
        new_alias = f"{old_alias}_rotated"
        ca_cert = gklm_rest_client.certificates.get_system_certificate(
            cert_name=gklm_params["gklm_hostname"]
        )
        ret = validate_sighup(
            installer,
            nfs_nodes[0],
            nfs_name,
            gklm_rest_client,
            gklm_params["gklm_client_name"],
            gklm_params["gklm_hostname"],
            ca_cert,
            gklm_params["gklm_user"],
            old_alias,
            new_alias,
        )
        if ret == 1:
            return 1

        perform_io_operations_and_validate_fuse(
            client_export_mount_dict,
            clients,
            file_count=int(config.get("file_count", 5)),
            dd_command_size_in_M=int(config.get("dd_command_size_in_M", 5)),
            nfs_name=nfs_name,
        )
        log.info("BYOK-01 live key rotation under I/O passed")
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("BYOK-01 failed: %s", exc)
        if gklm_params:
            collect_gklm_logs_on_failure(gklm_params)
        return 1
    finally:
        try:
            if gklm_rest_client and gklm_params:
                clean_up_gklm(
                    gklm_rest_client,
                    gklm_params.get("gklm_client_name"),
                    gklm_params.get("gklm_cert_alias"),
                )
        except Exception:  # noqa: BLE001
            pass
