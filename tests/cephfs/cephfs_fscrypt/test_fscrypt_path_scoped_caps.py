"""
CephFS fscrypt encrypt under path-scoped MDS caps (allow all path=).

Polarion: CEPH-83633335
Refs: IBMCEPH-22631, tracker #81908, ceph PR #73696

Upstream repro: client with mds 'allow all path=<subvol>' must be able to set
fscrypt policy after the encryption auth-cap fix (without widening to allow *).
"""

import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.fscrypt_utils import FscryptUtils
from utility.log import Log

log = Log(__name__)

tc = "CEPH-83633335"


def _rand(n=6):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def run(ceph_cluster, **kw):
    """
    Fscrypt encrypt with mds 'allow all path=<subvol>' (CEPH-83633335).

    Config:
      expect_encrypt_fail (bool): if True, encrypt must fail (unfixed product).
                                  Default False — expect success on fixed builds.
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        fscrypt_util = FscryptUtils(ceph_cluster)
        config = kw.get("config") or {}
        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            log.error("No client nodes available")
            return 1

        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client = clients[0]
        expect_fail = bool(config.get("expect_encrypt_fail", False))

        erasure = (
            FsUtilsv1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        default_fs = config.get("fs_name", "cephfs")
        fs_name = default_fs if not erasure else "cephfs-ec"

        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health not OK before test")
            return 1

        sv_group = config.get("subvolume_group", "fscrypt_pathcaps_grp")
        sv_name = config.get("subvolume_name", f"sv_pathcaps_{_rand()}")
        entity = f"fscrypt_pathcaps_{_rand()}"
        mnt_pt = f"/mnt/fuse_{entity}"
        fuse_mounted = False
        keyring_path = f"/etc/ceph/ceph.client.{entity}.keyring"

        log.info("[%s] Create FS / subvolume group / subvolume", tc)
        if not fs_util.get_fs_info(client, fs_name):
            fs_util.create_fs(client, fs_name)
        fs_util.create_subvolumegroup(client, vol_name=fs_name, group_name=sv_group)
        fs_util.create_subvolume(
            client,
            vol_name=fs_name,
            subvol_name=sv_name,
            group_name=sv_group,
            size="536870912",
        )
        out, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume getpath {fs_name} {sv_name} "
                f"--group_name {sv_group}"
            ),
        )
        the_path = str(out).strip().splitlines()[-1].strip()
        if not the_path.startswith("/"):
            log.error("Unexpected subvolume path: %r", the_path)
            return 1
        log.info("[%s] Subvolume path P=%s", tc, the_path)

        log.info(
            "[%s] Create client.%s with mds 'allow all path=%s'", tc, entity, the_path
        )
        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph auth get-or-create client.{entity} "
                f"mon 'allow r' "
                f"mds 'allow all path={the_path}' "
                f"osd 'allow rw tag cephfs data={fs_name}' "
                f"-o {keyring_path}"
            ),
        )
        client.exec_command(sudo=True, cmd=f"chmod 644 {keyring_path}")
        caps_out, _ = client.exec_command(
            sudo=True, cmd=f"ceph auth get client.{entity}"
        )
        log.info("Auth caps:\n%s", caps_out)
        if f"path={the_path}" not in str(caps_out) and "path=" not in str(caps_out):
            log.error("Path-scoped caps not applied")
            return 1

        log.info("[%s] FUSE mount with path-scoped client at P", tc)
        client.exec_command(sudo=True, cmd=f"mkdir -p {mnt_pt}")
        fuse_cmd = (
            f"ceph-fuse -n client.{entity} {mnt_pt} "
            f"--client_fs {fs_name} --client_mountpoint {the_path}"
        )
        client.exec_command(sudo=True, cmd=fuse_cmd)
        fuse_mounted = True

        enc_dir = f"{mnt_pt}/enc_{_rand()}"
        client.exec_command(sudo=True, cmd=f"mkdir -p {enc_dir}")

        log.info("[%s] fscrypt setup on mount", tc)
        if fscrypt_util.setup(client, mnt_pt):
            log.error("fscrypt setup failed")
            return 1

        log.info("[%s] fscrypt encrypt on %s (expect_fail=%s)", tc, enc_dir, expect_fail)
        encrypt_rc = 0
        encrypt_err = ""
        try:
            result = fscrypt_util.encrypt(
                client,
                encrypt_path=enc_dir,
                mnt_pt=mnt_pt,
                protector_source="raw_key",
            )
            # encrypt() returns encrypt_params dict on success, 1 on failure
            if isinstance(result, dict):
                encrypt_rc = 0
            elif result == 1:
                encrypt_rc = 1
                encrypt_err = "fscrypt_util.encrypt returned failure"
            else:
                encrypt_rc = 1
                encrypt_err = f"unexpected encrypt result: {result!r}"
        except (CommandFailed, Exception) as exc:
            encrypt_rc = 1
            encrypt_err = str(exc)
            log.info("encrypt raised: %s", encrypt_err)

        denied = any(
            s in encrypt_err.lower()
            for s in ("permission denied", "operation not permitted", "eperm", "-13")
        )

        if expect_fail:
            if encrypt_rc != 0 or denied:
                log.info(
                    "[%s] PASS (expected fail): encrypt denied under path-scoped caps",
                    tc,
                )
                return 0
            log.error(
                "[%s] FAIL: expected encrypt to fail on unfixed build, but it succeeded",
                tc,
            )
            return 1

        if encrypt_rc != 0:
            log.error(
                "[%s] FAIL: encrypt failed under path-scoped caps "
                "(product fix #73696 required?): %s",
                tc,
                encrypt_err or encrypt_rc,
            )
            return 1

        log.info("[%s] PASS: encrypt succeeded with allow all path= caps", tc)
        return 0

    except Exception as exc:
        log.error("[%s] FAIL: %s", tc, exc)
        log.error(traceback.format_exc())
        return 1

    finally:
        try:
            client = ceph_cluster.get_ceph_objects("client")[0]
        except Exception:
            return
        try:
            if "fuse_mounted" in locals() and fuse_mounted:
                client.exec_command(
                    sudo=True, cmd=f"fusermount -u {mnt_pt}", check_ec=False
                )
                client.exec_command(sudo=True, cmd=f"rm -rf {mnt_pt}", check_ec=False)
        except Exception as cleanup_exc:
            log.warning("umount cleanup: %s", cleanup_exc)
        try:
            if "entity" in locals():
                client.exec_command(
                    sudo=True, cmd=f"ceph auth del client.{entity}", check_ec=False
                )
                client.exec_command(
                    sudo=True, cmd=f"rm -f {keyring_path}", check_ec=False
                )
        except Exception as cleanup_exc:
            log.warning("auth cleanup: %s", cleanup_exc)
        try:
            if "sv_name" in locals() and "sv_group" in locals() and "fs_name" in locals():
                fs_util = FsUtilsv1(ceph_cluster, test_data=kw.get("test_data"))
                fs_util.remove_subvolume(
                    client,
                    vol_name=fs_name,
                    subvol_name=sv_name,
                    group_name=sv_group,
                    validate=False,
                )
                fs_util.remove_subvolumegroup(
                    client,
                    vol_name=fs_name,
                    group_name=sv_group,
                    force=True,
                    validate=False,
                )
        except Exception as cleanup_exc:
            log.warning("subvolume cleanup: %s", cleanup_exc)
