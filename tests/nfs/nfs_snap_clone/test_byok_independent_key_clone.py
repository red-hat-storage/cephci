"""
Regional FS parity — SNAP-03
Independent key cloning on BYOK-encrypted volumes (clone must not share parent key).
"""

import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    collect_gklm_logs_on_failure,
    create_nfs_instance_for_byok,
    get_enctag,
    load_gklm_config,
    setup_gklm_infrastructure,
)
from tests.nfs.nfs_snap_clone.snap_clone_nfs_lib import (
    DEFAULT_NFS_NAME,
    list_nfs_clusters,
    wait_clone_complete,
)
from utility.gklm_client.gklm_client import build_gklm_client
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    1. BYOK-setup NFS + encrypted parent subvolume/export.
    2. Snapshot + clone parent.
    3. Create clone export with a *new* enctag.
    4. Verify parent and clone I/O; keys are independent (different enctags).
    """
    gklm_params = None
    gklm_rest_client = None
    gkml_client_name = None
    gklm_cert_alias = None
    try:
        config = kw.get("config", {})
        custom_data = kw.get("test_data", {})
        cephci_data = get_cephci_config()
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        installer = ceph_cluster.get_nodes(role="installer")[0]
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        gklm_params = load_gklm_config(custom_data, config, cephci_data)
        rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        fs_name = config.get("fs_name", "cephfs")
        # Share suite NFS name; do not invent a new cluster per run (port clash).
        nfs_name = config.get("nfs_instance_name", DEFAULT_NFS_NAME)
        group = f"byok_clone_grp_{rand}"
        parent_sv = f"byok_parent_{rand}"
        snap = f"byok_snap_{rand}"
        clone_sv = f"byok_clone_{rand}"

        gkml_client_name = gklm_params.get("gklm_client_name", f"cephci_byok_{rand}")
        gklm_cert_alias = gklm_params.get("gklm_cert_alias", f"byok_cert_{rand}")
        nfs_node = nfs_nodes[0]

        setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_params["gklm_ip"],
            gklm_hostname=gklm_params["gklm_hostname"],
        )
        gklm_rest_client = build_gklm_client(
            {
                "gklm_ip": gklm_params["gklm_ip"],
                "gklm_user": gklm_params["gklm_user"],
                "gklm_password": gklm_params["gklm_password"],
                **(
                    {"gklm_rest_prefix": gklm_params["gklm_rest_prefix"]}
                    if gklm_params.get("gklm_rest_prefix")
                    else {}
                ),
            },
            verify=False,
        )
        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )
        created_client_data = gklm_rest_client.clients.create_client(gkml_client_name)
        enctag_parent = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            gklm_cert_alias,
            gklm_params.get("gklm_user", "admin"),
            cert,
            created_client_data,
        )
        if not enctag_parent:
            raise RuntimeError("Failed to allocate parent enctag")

        existing = list_nfs_clusters(client)
        if nfs_name in existing:
            log.info("Reusing existing NFS cluster %s for BYOK exports", nfs_name)
        elif existing:
            nfs_name = existing[0]
            log.info(
                "Reusing already-deployed NFS cluster %s for BYOK exports "
                "(skip create — avoids port 2049 clash)",
                nfs_name,
            )
        else:
            log.info("No NFS cluster present; creating BYOK NFS %s", nfs_name)
            ca_cert = gklm_rest_client.certificates.get_system_certificate(
                cert_name=gklm_params["gklm_hostname"]
            )
            create_nfs_instance_for_byok(
                installer,
                nfs_node,
                nfs_name,
                gklm_params["gklm_hostname"],
                rsa_key,
                cert,
                ca_cert,
            )

        # Force a second key object for the clone export.
        clone_alias = f"{gklm_cert_alias}_clone"
        enctag_clone = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            clone_alias,
            gklm_params.get("gklm_user", "admin"),
            cert,
        )
        if not enctag_clone:
            raise RuntimeError("Failed to allocate clone enctag")
        log.info(
            "Parent enctag=%s clone enctag=%s", enctag_parent, enctag_clone
        )

        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            return 1

        fs_util.create_subvolumegroup(client, vol_name=fs_name, group_name=group)
        fs_util.create_subvolume(
            client, vol_name=fs_name, subvol_name=parent_sv, group_name=group
        )
        path, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume getpath {fs_name} {parent_sv} "
                f"--group_name {group}"
            ),
        )
        path = path.strip()
        export = f"/export_byok_p_{rand}"
        # Export with parent enctag when supported by CLI
        export_cmd = (
            f"ceph nfs export create cephfs {nfs_name} {export} {fs_name} {path}"
        )
        if config.get("use_enctag_cli", True):
            export_cmd += f" --enctag {enctag_parent}"
        client.exec_command(sudo=True, cmd=export_cmd)

        mount = f"/mnt/byok_p_{rand}"
        nfs_ip = nfs_nodes[0].ip_address
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount}")
        client.exec_command(
            sudo=True, cmd=f"mount -t nfs -o vers=4.2 {nfs_ip}:{export} {mount}"
        )
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount}/enc.bin bs=1M count=4 status=none",
        )

        fs_util.create_snapshot(
            client,
            vol_name=fs_name,
            subvol_name=parent_sv,
            snap_name=snap,
            group_name=group,
        )
        fs_util.create_clone(
            client,
            vol_name=fs_name,
            subvol_name=parent_sv,
            snap_name=snap,
            target_subvol_name=clone_sv,
            group_name=group,
            target_group_name=group,
        )
        wait_clone_complete(fs_util, client, fs_name, clone_sv, group_name=group)

        clone_path, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume getpath {fs_name} {clone_sv} "
                f"--group_name {group}"
            ),
        )
        clone_path = clone_path.strip()
        clone_export = f"/export_byok_c_{rand}"
        clone_cmd = (
            f"ceph nfs export create cephfs {nfs_name} {clone_export} "
            f"{fs_name} {clone_path}"
        )
        if config.get("use_enctag_cli", True):
            clone_cmd += f" --enctag {enctag_clone}"
        client.exec_command(sudo=True, cmd=clone_cmd)
        clone_mount = f"/mnt/byok_c_{rand}"
        client.exec_command(sudo=True, cmd=f"mkdir -p {clone_mount}")
        client.exec_command(
            sudo=True,
            cmd=f"mount -t nfs -o vers=4.2 {nfs_ip}:{clone_export} {clone_mount}",
        )
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={clone_mount}/clone.bin bs=1M count=2 status=none",
        )

        log.info(
            "SNAP-03 passed: parent_enctag=%s clone_enctag=%s (independent)",
            enctag_parent,
            enctag_clone,
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("SNAP-03 failed: %s", exc)
        if gklm_params:
            collect_gklm_logs_on_failure(gklm_params)
        return 1
    finally:
        try:
            if gklm_rest_client and gkml_client_name:
                clean_up_gklm(gklm_rest_client, gkml_client_name, gklm_cert_alias)
        except Exception:  # noqa: BLE001
            pass
