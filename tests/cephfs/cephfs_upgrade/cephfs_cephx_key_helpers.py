"""
Shared helpers for CephFS CephX upgrade suites (pacific/quincy CephFS-only and
reef/squid/tentacle combined NFS+CephX tracks).

Imported by setup / holdout / rotate / NFS modules. Not a suite entry point.

Hand-off file matches upgrade_pre_req convention:
``/home/cephuser/cephfs_upgrade_config.json`` (section ``CephX``; NFS uses
``NfsCephX`` via nfs_setup).

Rotate/admin keyring bits ported from feat/reef_upgrade_suite_cephx
(Jenkins clean-keyring / mon-identity lessons). No CephFS mirror helpers here.
"""

import json
import os
import re
import shlex
import time
from dataclasses import dataclass
from typing import List, Optional

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

DEFAULT_KEY_TYPE = "aes256k"
# Same path as upgrade_pre_req / post_upgrade_validation.
DEFAULT_UPGRADE_CONFIG_PATH = "/home/cephuser/cephfs_upgrade_config.json"
CEPHX_CONFIG_KEY = "CephX"

ADMIN_BACKUP_ENTITY = "client.admin-backup"
ADMIN_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
CEPHADM_ADMIN_KEYRING_TMPL = "/var/lib/ceph/{fsid}/config/ceph.client.admin.keyring"
KEYRING_PATTERN = re.compile(r"^\s*key\s*=\s*(\S+)\s*$", re.MULTILINE)
PROBE_BASENAME = "cephx_post_upgrade_probe"
PROBE_CONTENT = "cephx-post-upgrade-probe"


@dataclass
class CephfsMountRecipe:
    """One kernel or fuse CephFS mount for rotate / remount / probe."""

    mnt_pt: str
    client_hostname: str
    mount_kind: str  # "kernel" or "fuse"
    fs_name: str
    vol_name: str
    subvol_name: str
    group_name: str
    client_entity: str = ""  # e.g. client.1; default client.<hostname>
    fuse_extra_params: str = ""
    kernel_sub_dir: str = "/"
    kernel_extra_params: str = ""
    fstab: bool = False  # True when setup wrote /etc/fstab (Client3 reboot path)

    def entity(self) -> str:
        if self.client_entity:
            return self.client_entity
        return f"client.{self.client_hostname}"

    def to_dict(self) -> dict:
        return {
            "mnt_pt": self.mnt_pt,
            "client_hostname": self.client_hostname,
            "client_entity": self.client_entity,
            "mount_kind": self.mount_kind,
            "fs_name": self.fs_name,
            "vol_name": self.vol_name,
            "subvol_name": self.subvol_name,
            "group_name": self.group_name,
            "fuse_extra_params": self.fuse_extra_params,
            "kernel_sub_dir": self.kernel_sub_dir,
            "kernel_extra_params": self.kernel_extra_params,
            "fstab": self.fstab,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CephfsMountRecipe":
        return cls(
            mnt_pt=data["mnt_pt"],
            client_hostname=data["client_hostname"],
            mount_kind=data["mount_kind"],
            fs_name=data["fs_name"],
            vol_name=data["vol_name"],
            subvol_name=data["subvol_name"],
            group_name=data["group_name"],
            client_entity=data.get("client_entity", ""),
            fuse_extra_params=data.get("fuse_extra_params", ""),
            kernel_sub_dir=data.get("kernel_sub_dir", "/"),
            kernel_extra_params=data.get("kernel_extra_params", ""),
            fstab=bool(data.get("fstab", False)),
        )


def _remote_exit_status(host) -> int:
    """
    Return last command exit status for a Ceph object or Node.

    cephci stores ``exit_status`` on the underlying ``Node``
    (``client.node.exit_status``), not on the Ceph wrapper.
    """
    node = getattr(host, "node", host)
    return int(getattr(node, "exit_status", 1))


def write_cephx_upgrade_config(client, payload: dict, path: str = None) -> None:
    """
    Write CephX suite hand-off JSON on a client (upgrade_pre_req style).

    ``payload`` is stored under key ``CephX`` in ``cephfs_upgrade_config.json``.
    Existing top-level keys (e.g. from upgrade_pre_req) are preserved.
    """
    path = path or DEFAULT_UPGRADE_CONFIG_PATH
    data = {}
    out, _ = client.exec_command(
        sudo=True, cmd=f"cat {shlex.quote(path)}", check_ec=False
    )
    if _remote_exit_status(client) == 0 and (out or "").strip():
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            log.warning("Existing %s not valid JSON; rewriting with CephX only", path)
            data = {}
    if not isinstance(data, dict):
        data = {}
    data[CEPHX_CONFIG_KEY] = payload
    blob = json.dumps(data, indent=4) + "\n"
    remote = client.remote_file(sudo=True, file_name=path, file_mode="w")
    remote.write(blob)
    remote.flush()
    remote.close()
    log.info("Wrote CephX config to %s on %s", path, client.node.hostname)


def _read_upgrade_json(client, path: str) -> dict:
    """Return parsed upgrade config JSON (empty dict if missing/invalid)."""
    out, _ = client.exec_command(
        sudo=True, cmd=f"cat {shlex.quote(path)}", check_ec=False
    )
    if _remote_exit_status(client) != 0 or not (out or "").strip():
        return {}
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def load_cephx_section(client, path: str = None) -> dict:
    """Return the ``CephX`` dict from the upgrade config JSON."""
    path = path or DEFAULT_UPGRADE_CONFIG_PATH
    return dict(_read_upgrade_json(client, path).get(CEPHX_CONFIG_KEY) or {})


def update_cephx_section(client, updates: dict, path: str = None) -> None:
    """Merge ``updates`` into the existing ``CephX`` section (preserves mounts)."""
    path = path or DEFAULT_UPGRADE_CONFIG_PATH
    data = _read_upgrade_json(client, path)
    cephx = dict(data.get(CEPHX_CONFIG_KEY) or {})
    cephx.update(updates)
    data[CEPHX_CONFIG_KEY] = cephx
    blob = json.dumps(data, indent=4) + "\n"
    remote = client.remote_file(sudo=True, file_name=path, file_mode="w")
    remote.write(blob)
    remote.flush()
    remote.close()
    log.info("Updated CephX section keys %s on %s", list(updates), client.node.hostname)


def load_cephx_mount_recipes(client, path: str = None) -> List[CephfsMountRecipe]:
    """Load mount recipes from ``CephX.mounts`` in the upgrade config JSON."""
    path = path or DEFAULT_UPGRADE_CONFIG_PATH
    mounts = load_cephx_section(client, path=path).get("mounts") or []
    if not mounts:
        raise OperationFailedError(f"No CephX.mounts in {path}")
    return [CephfsMountRecipe.from_dict(m) for m in mounts]


def load_cephx_new_mounts(client, path: str = None) -> list:
    """Load ``CephX.new_mounts`` list written by the new-mounts suite step."""
    path = path or DEFAULT_UPGRADE_CONFIG_PATH
    new_mounts = load_cephx_section(client, path=path).get("new_mounts") or []
    if not new_mounts:
        raise OperationFailedError(f"No CephX.new_mounts in {path}")
    return new_mounts


def client_by_entity(clients, entity: str):
    """Map ``client.N`` to the Nth client object (1-based)."""
    try:
        num = int(entity.split(".", 1)[1])
    except (IndexError, ValueError) as exc:
        raise OperationFailedError(f"Bad client id {entity!r}") from exc
    if num < 1 or num > len(clients):
        raise OperationFailedError(
            f"Need client index {num}; only {len(clients)} client(s) present"
        )
    return clients[num - 1]


def ensure_ceph_fs(fs_util, client, vol_name: str) -> None:
    """Idempotent: create FS volume + MDS placement + max_mds=2 if missing."""
    out, _ = client.exec_command(sudo=True, cmd="ceph fs ls --format json", timeout=300)
    names = [i["name"] for i in json.loads(out or "[]")]
    if vol_name in names:
        log.info("FS %s already exists", vol_name)
        return
    log.info("Creating FS volume %s", vol_name)
    fs_util.create_fs(client, vol_name)
    client.exec_command(
        sudo=True,
        cmd=f"ceph orch apply mds {vol_name} --placement='label:mds'",
        timeout=300,
    )
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs set {vol_name} max_mds 2",
        check_ec=False,
        timeout=300,
    )


def recipes_for_entity(recipes: List[CephfsMountRecipe], entity: str):
    """Filter mount recipes belonging to ``entity``."""
    matched = [r for r in recipes if r.entity() == entity]
    if not matched:
        raise OperationFailedError(f"No CephX mounts for {entity}")
    return matched


def admin_keyring_nodes(ceph_cluster, clients=None) -> list:
    """
    Nodes that must get a rewritten admin keyring after ``client.admin`` rotate.

    Installer (cephadm shell bind-mount) + every client-role node (local
    ``ceph`` CLI for remount / auth get uses ``/etc/ceph/ceph.client.admin.keyring``).
    """
    nodes = list(ceph_cluster.get_nodes("installer"))
    seen = {n.hostname for n in nodes}
    if clients is None:
        clients = ceph_cluster.get_ceph_objects("client")
    for client in clients:
        node = getattr(client, "node", client)
        if node.hostname not in seen:
            nodes.append(node)
            seen.add(node.hostname)
    return nodes


def _cmd_detail(out, err) -> str:
    """Merge stdout/stderr for failure messages."""
    parts = [p for p in ((out or "").strip(), (err or "").strip()) if p]
    return " | ".join(parts) if parts else "(no output)"


def _extract_client_admin_keyring(text: str) -> Optional[str]:
    """
    Return a clean ``[client.admin]`` keyring block from command output.

    ``cephadm shell`` / ``auth rotate`` often prefix noise (e.g. Inferring fsid).
    """
    if not text:
        return None
    marker = "[client.admin]"
    idx = text.find(marker)
    if idx < 0:
        return None
    lines = text[idx:].splitlines()
    kept = [lines[0]]
    for line in lines[1:]:
        if line.startswith("[") and not line.startswith(marker):
            break
        kept.append(line)
    blob = "\n".join(kept).strip()
    if not KEYRING_PATTERN.search(blob):
        return None
    return blob


def _cephadm_shell(installer, subcmd: str) -> str:
    """Run ``cephadm shell -- <subcmd>`` on the installer."""
    out, err = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- {subcmd}",
        check_ec=False,
        timeout=300,
    )
    rc = _remote_exit_status(installer)
    if rc != 0:
        raise OperationFailedError(
            f"cephadm shell failed (rc={rc}): " f"{_cmd_detail(out, err)}"
        )
    return (out or err or "").strip()


def _cephadm_ls_daemons(installer) -> list:
    """Return parsed ``cephadm ls`` JSON list (plain ls; no --format json)."""
    out, err = installer.exec_command(
        sudo=True,
        cmd="cephadm ls",
        check_ec=False,
        timeout=300,
    )
    rc = _remote_exit_status(installer)
    if rc != 0:
        raise OperationFailedError(
            f"cephadm ls failed (rc={rc}): " f"{_cmd_detail(out, err)}"
        )
    try:
        daemons = json.loads(out or "[]")
    except json.JSONDecodeError as exc:
        raise OperationFailedError(f"cephadm ls JSON parse failed: {exc}") from exc
    if not isinstance(daemons, list):
        raise OperationFailedError("cephadm ls did not return a JSON list")
    return daemons


def _cluster_fsid(installer) -> str:
    """Return cluster fsid from ``cephadm ls``."""
    for daemon in _cephadm_ls_daemons(installer):
        fsid = daemon.get("fsid") or ""
        if fsid:
            return fsid
    raise OperationFailedError("No fsid in cephadm ls")


def _cephadm_admin_keyring_path(installer) -> str:
    """Host path cephadm shell bind-mounts as the admin keyring."""
    return CEPHADM_ADMIN_KEYRING_TMPL.format(fsid=_cluster_fsid(installer))


def _local_mon_daemon_name(installer) -> str:
    """Return a local ``mon.*`` daemon name from ``cephadm ls``."""
    for daemon in _cephadm_ls_daemons(installer):
        name = daemon.get("name") or ""
        if name.startswith("mon."):
            return name
    raise OperationFailedError(
        "No local mon.* on installer; needed after client.admin rotate "
        "while host admin keyring is stale"
    )


def _cephadm_shell_as_mon(installer, subcmd: str) -> str:
    """Run ``cephadm shell --name mon.* -- <subcmd>`` (stale admin-safe)."""
    mon_name = _local_mon_daemon_name(installer)
    out, err = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell --name {mon_name} -- {subcmd}",
        check_ec=False,
        timeout=300,
    )
    rc = _remote_exit_status(installer)
    if rc != 0:
        raise OperationFailedError(
            f"cephadm shell --name {mon_name} failed "
            f"(rc={rc}): {_cmd_detail(out, err)}"
        )
    return (out or err or "").strip()


def _auth_rotate_available(installer) -> bool:
    """Return True if ``ceph auth --help`` lists rotate."""
    help_text = _cephadm_shell(installer, "ceph auth --help")
    return "rotate" in help_text


def _mask_key(key: str) -> str:
    """Half-masked key for logs."""
    if not key or len(key) < 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def _get_entity_key(installer, entity: str) -> str:
    """Parse secret from ``ceph auth get <entity>`` via cephadm."""
    raw = _cephadm_shell(installer, f"ceph auth get {entity}")
    match = KEYRING_PATTERN.search(raw)
    if not match:
        raise OperationFailedError(f"Could not parse key for {entity}")
    return match.group(1)


def _write_remote_text(host, path: str, content: str, mode: str = "644") -> None:
    """Write text to a remote path; create with ``mode`` and chmod (fail on error)."""
    text = content if content.endswith("\n") else content + "\n"
    qpath = shlex.quote(path)
    host.exec_command(
        sudo=True, cmd=f"install -m {shlex.quote(mode)} /dev/null {qpath}"
    )
    remote = host.remote_file(sudo=True, file_name=path, file_mode="w")
    remote.write(text)
    remote.flush()
    remote.close()
    host.exec_command(sudo=True, cmd=f"chmod {shlex.quote(mode)} {qpath}")


def _rotate_client_admin(installer, key_nodes, key_type: str) -> None:
    """
    Rotate ``client.admin`` and rewrite admin keyrings on ``key_nodes``.

    Prefers cleaned rotate stdout; falls back to mon-identity get. Rewrites
    host ``/etc/ceph/...`` and installer cephadm config keyring path.
    ``client.admin-backup`` is always removed in ``finally``.
    """
    log.info("=== rotate client.admin (with backup entity) ===")
    backup_created = False
    try:
        raw_before = _cephadm_shell_as_mon(installer, "ceph auth get client.admin")
        match_before = KEYRING_PATTERN.search(raw_before)
        if not match_before:
            raise OperationFailedError(
                "Could not parse client.admin key before rotate (mon-identity get)"
            )
        before = match_before.group(1)
        log.info("client.admin baseline key %s", _mask_key(before))

        _cephadm_shell(
            installer,
            f'ceph auth get-or-create {ADMIN_BACKUP_ENTITY} mon "allow *"',
        )
        backup_created = True
        _cephadm_shell(installer, f"ceph auth get {ADMIN_BACKUP_ENTITY}")
        rotate_out = _cephadm_shell(
            installer, f"ceph auth rotate client.admin --key-type {key_type}"
        )

        keyring_blob = _extract_client_admin_keyring(rotate_out or "")
        if keyring_blob:
            log.info("Using cleaned client.admin key block from auth rotate stdout")
            match = KEYRING_PATTERN.search(keyring_blob)
        else:
            log.info(
                "auth rotate stdout had no clean [client.admin] block; "
                "fetching via mon identity"
            )
            keyring_blob = _cephadm_shell_as_mon(
                installer, "ceph auth get client.admin"
            )
            keyring_blob = _extract_client_admin_keyring(keyring_blob) or keyring_blob
            match = KEYRING_PATTERN.search(keyring_blob or "")
        if not match:
            raise OperationFailedError("Could not parse client.admin key after rotate")
        after = match.group(1)
        if before == after:
            raise OperationFailedError("client.admin key unchanged after auth rotate")
        log.info("client.admin rotated key %s", _mask_key(after))

        for node in key_nodes:
            log.info("Refreshing %s on %s", ADMIN_KEYRING_PATH, node.hostname)
            _write_remote_text(node, ADMIN_KEYRING_PATH, keyring_blob, mode="600")

        cephadm_keyring = _cephadm_admin_keyring_path(installer)
        log.info(
            "Refreshing cephadm-shell admin key file %s on %s",
            cephadm_keyring,
            installer.hostname,
        )
        installer.exec_command(
            sudo=True,
            cmd=f"mkdir -p {shlex.quote(os.path.dirname(cephadm_keyring))}",
            check_ec=False,
        )
        _write_remote_text(installer, cephadm_keyring, keyring_blob, mode="600")

        _cephadm_shell(installer, "ceph auth get client.admin")
        log.info("client.admin rotated; admin credentials updated")
    finally:
        if backup_created:
            try:
                _cephadm_shell_as_mon(installer, f"ceph auth rm {ADMIN_BACKUP_ENTITY}")
            except Exception as exc:
                log.warning("%s cleanup failed: %s", ADMIN_BACKUP_ENTITY, exc)


def _deploy_client_credentials(installer, client, entity: str) -> None:
    """Deploy rotated keyring + secret for ``entity`` onto ``client``."""
    # client.1 -> secret file name "1" (matches FsUtils new_client_hostname)
    short = entity.replace("client.", "", 1)
    keyring_blob = _cephadm_shell(installer, f"ceph auth get {entity}")
    secret = _cephadm_shell(installer, f"ceph auth get-key {entity}")
    _write_remote_text(
        client, f"/etc/ceph/ceph.{entity}.keyring", keyring_blob, mode="644"
    )
    _write_remote_text(client, f"/etc/ceph/{short}.secret", secret, mode="600")


def _umount_cephfs(client, mnt_pt: str) -> None:
    """Umount a CephFS path; lazy ``-l`` fallback."""
    qm = shlex.quote(mnt_pt)
    client.exec_command(sudo=True, cmd=f"umount {qm}", timeout=120, check_ec=False)
    umount_rc = _remote_exit_status(client)
    if umount_rc != 0:
        log.info(
            "Doing lazy umount (-l) for %s (plain umount rc=%s)",
            mnt_pt,
            umount_rc,
        )
        client.exec_command(
            sudo=True, cmd=f"umount -l {qm}", timeout=120, check_ec=False
        )


def mount_at(
    fs_util, client, r: CephfsMountRecipe, mnt_pt: str, mon_ips: str, fstab=False
):
    """Mount recipe ``r`` at ``mnt_pt`` (kernel or fuse)."""
    short = r.entity().split(".", 1)[1]
    if r.mount_kind == "kernel":
        fs_util.kernel_mount(
            [client],
            mnt_pt,
            mon_ips,
            sub_dir=r.kernel_sub_dir,
            new_client_hostname=short,
            extra_params=r.kernel_extra_params,
            fstab=fstab,
        )
    else:
        fs_util.fuse_mount(
            [client],
            mnt_pt,
            new_client_hostname=short,
            extra_params=r.fuse_extra_params,
            fstab=fstab,
        )


def _remount_recipe(fs_util: FsUtils, client, recipe: CephfsMountRecipe) -> None:
    """Remount one kernel/fuse recipe via FsUtils."""
    mon_ips = ",".join(fs_util.get_mon_node_ips())
    mount_at(fs_util, client, recipe, recipe.mnt_pt, mon_ips)


def _probe_io(client, mnt_pt: str, tag: str = None) -> None:
    """printf/cat smoke check on path (optional tagged filename/content)."""
    if tag:
        path = f"{mnt_pt.rstrip('/')}/{tag}.txt"
        content = tag
    else:
        path = f"{mnt_pt.rstrip('/')}/{PROBE_BASENAME}"
        content = PROBE_CONTENT
    qpath = shlex.quote(path)
    client.exec_command(
        sudo=True,
        cmd=f"printf '%s\\n' {shlex.quote(content)} > {qpath}",
    )
    client.exec_command(sudo=True, cmd=f"cat {qpath} > /dev/null")


def _cleanup_probes(client, mnt_pt: str, tag: str = None) -> None:
    """Best-effort rm of probe file."""
    if tag:
        path = f"{mnt_pt.rstrip('/')}/{tag}.txt"
    else:
        path = f"{mnt_pt.rstrip('/')}/{PROBE_BASENAME}"
    client.exec_command(sudo=True, cmd=f"rm -f {shlex.quote(path)}", check_ec=False)


def assert_paths_mounted(
    client,
    mnt_pts: List[str],
    retries: int = 12,
    interval: int = 15,
) -> None:
    """
    Fail if any path is not present in ``findmnt -n`` output.

    After a reboot, ``_netdev`` fstab mounts are brought up by
    ``remote-fs.target`` which waits for ``network-online.target``.  SSH
    becomes available well before that target completes, so a single
    ``findmnt`` call issued immediately after reconnect will see missing
    mounts even when the fstab entries are perfectly correct.  Poll up to
    ``retries * interval`` seconds (default 3 minutes) before failing.
    """
    deadline = time.time() + retries * interval
    missing: List[str] = list(mnt_pts)
    while True:
        # --list: without it, findmnt prints a tree (├─/mnt/...) and exact
        # path match fails even when mounts are up (false FAIL after reboot).
        out, _ = client.exec_command(
            sudo=True,
            cmd="findmnt -n -o TARGET --list",
            check_ec=False,
            timeout=60,
        )
        mounted = {line.strip().rstrip("/") for line in (out or "").splitlines()}
        missing = [p for p in mnt_pts if p.rstrip("/") not in mounted]
        if not missing:
            break
        if time.time() >= deadline:
            raise OperationFailedError(
                f"Mount(s) not present after reboot on {client.node.hostname}: {missing}"
            )
        log.info(
            "Waiting for mounts on %s: %s still missing; retrying in %ss",
            client.node.hostname,
            missing,
            interval,
        )
        time.sleep(interval)
    log.info("All expected mounts present on %s: %s", client.node.hostname, mnt_pts)
