"""
Dedicated CephX upgrade setup: cephfs + cephfs2, unique subvolumes, kernel+fuse
mounts on Client1/Client2/Client3. Client3 mounts use fstab=True for reboot path.
Persists mounts under CephX in cephfs_upgrade_config.json.
"""

import shlex
import traceback

from cli.exceptions import ConfigError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    DEFAULT_UPGRADE_CONFIG_PATH,
    CephfsMountRecipe,
    _probe_io,
    client_by_entity,
    ensure_ceph_fs,
    mount_at,
    write_cephx_upgrade_config,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

# First volume is "cephfs" so NFS export.create (hardcoded FS name) can share it.
FS_NAMES = ("cephfs", "cephfs2")
SVG_NAME = "cephx_svg"

# Same IO deps as FsUtils.prepare_clients, but without ceph-fuse (already
# pinned by test_client; unversioned yum would float it vs ceph-common).
_IO_PKGS = (
    "python3",
    "python3-pip",
    "fio",
    "fuse",
    "attr",
    "gcc",
    "python3-devel",
    "git",
    "lua",
    "acl",
)


def _prepare_cephx_io_tools(clients, build):
    """Install fio/smallfile/iozone without touching ceph-fuse."""
    pkgs = list(_IO_PKGS)
    if str(build).endswith("7") or str(build).startswith("3"):
        pkgs.extend(
            [
                "@development",
                "rh-python36",
                "rh-python36-numpy rh-python36-scipy",
                "rh-test-python-tools rh-python36-python-six",
                "libffi libffi-devel",
            ]
        )
    yum = "yum install -y --nogpgcheck " + " ".join(pkgs)
    for client in clients:
        client.node.exec_command(sudo=True, cmd=yum, long_running=True)
        client.node.exec_command(
            sudo=True, cmd="pip3 install xattr numpy scipy", long_running=True
        )
        out, _ = client.node.exec_command(sudo=True, cmd="ls /home/cephuser")
        if "smallfile" not in out:
            client.node.exec_command(
                cmd="git clone https://github.com/distributed-system-analysis/smallfile.git"
            )
        if "iozone" not in out:
            for cmd in (
                "cd /home/cephuser;wget https://www.iozone.org/src/current/iozone3_506.tar;",
                "cd /home/cephuser;tar xvf iozone3_506.tar",
                "sudo yum install make -y --nogpgcheck",
                "cd /home/cephuser/iozone3_506/src/current/;make;make linux",
            ):
                client.node.exec_command(cmd=cmd)


def run(ceph_cluster, **kw):
    """
    Create cephfs + cephfs2 (if missing), unique SVG/SV per mount, mount
    kernel+fuse on client.1/2/3, write CephX section to upgrade JSON.

    Client3 mounts are created with fstab=True so reboot (post key rotate)
    remounts via /etc/fstab. Client1/2 do not get fstab entries.
    """
    try:
        config = kw.get("config") or {}
        config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        clients = ceph_cluster.get_ceph_objects("client")
        if len(clients) < 3:
            raise ConfigError(f"Need at least 3 clients; found {len(clients)}")
        admin = clients[0]

        # IO tools for run_ios()/smallfile; do not call FsUtils.prepare_clients
        # (its unversioned ceph-fuse install floats fuse off the baseline pin).
        build = config.get("build") or config.get("rhbuild", "default")
        _prepare_cephx_io_tools(clients, build)

        for idx in range(1, len(clients) + 1):
            short = str(idx)
            log.info("Setting CephFS caps on client.%s via update_ceph_client", short)
            fs_util.update_ceph_client(
                admin,
                short,
                "allow *",
                "allow *",
                "allow *, allow * path=/",
            )

        for fs in FS_NAMES:
            ensure_ceph_fs(fs_util, admin, fs)

        # (entity, vol, kind, fstab)
        plan = [
            ("client.1", FS_NAMES[0], "kernel", False),
            ("client.1", FS_NAMES[1], "fuse", False),
            ("client.2", FS_NAMES[1], "kernel", False),
            ("client.2", FS_NAMES[0], "fuse", False),
            ("client.3", FS_NAMES[0], "kernel", True),
            ("client.3", FS_NAMES[1], "fuse", True),
        ]

        recipes = []
        mon_ips = ",".join(fs_util.get_mon_node_ips())
        for vol_name in FS_NAMES:
            fs_util.create_subvolumegroup(admin, vol_name, SVG_NAME, validate=True)

        for client_id, vol_name, kind, use_fstab in plan:
            client = client_by_entity(clients, client_id)
            entity_short = client_id.split(".", 1)[1]
            sv_name = f"sv_{entity_short}_{kind}_{vol_name}"
            fs_util.create_subvolume(
                admin, vol_name, sv_name, group_name=SVG_NAME, validate=True
            )
            subvol_path, _ = admin.exec_command(
                sudo=True,
                cmd=(
                    f"ceph fs subvolume getpath {shlex.quote(vol_name)} "
                    f"{shlex.quote(sv_name)} {shlex.quote(SVG_NAME)}"
                ),
                timeout=300,
            )
            subvol_path = (subvol_path or "").strip()
            mnt_pt = f"/mnt/cephx_{entity_short}_{kind}_{vol_name}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {shlex.quote(mnt_pt)}")

            if kind == "kernel":
                recipe = CephfsMountRecipe(
                    mnt_pt=mnt_pt,
                    client_hostname=client.node.hostname,
                    mount_kind="kernel",
                    fs_name=vol_name,
                    vol_name=vol_name,
                    subvol_name=sv_name,
                    group_name=SVG_NAME,
                    client_entity=client_id,
                    kernel_sub_dir=subvol_path,
                    kernel_extra_params=f",fs={vol_name}",
                    fstab=use_fstab,
                )
            else:
                recipe = CephfsMountRecipe(
                    mnt_pt=mnt_pt,
                    client_hostname=client.node.hostname,
                    mount_kind="fuse",
                    fs_name=vol_name,
                    vol_name=vol_name,
                    subvol_name=sv_name,
                    group_name=SVG_NAME,
                    client_entity=client_id,
                    fuse_extra_params=f"-r {subvol_path} --client_fs {vol_name}",
                    fstab=use_fstab,
                )
            mount_at(fs_util, client, recipe, mnt_pt, mon_ips, fstab=use_fstab)
            recipes.append(recipe)

        payload = {
            "fs_names": list(FS_NAMES),
            "svg": SVG_NAME,
            "mounts": [r.to_dict() for r in recipes],
        }
        for c in clients[:3]:
            write_cephx_upgrade_config(c, payload, path=config_path)
        log.info("Wrote CephX mounts (%s) to %s", len(recipes), config_path)

        for r in recipes:
            client = client_by_entity(clients, r.client_entity)
            _probe_io(client, r.mnt_pt, tag=f"setup_{r.mount_kind}_{r.vol_name}")
        log.info("CephX setup PASSED: 2 FS, C1+C2 live mounts, C3 fstab mounts")
        return 0
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        return 1
