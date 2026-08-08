"""
CephFS Subvolume Quarantine utility module.

Wraps MGR quarantine CLI, subvolume path discovery, client caps (rw / rwq),
fuse mount helpers, and common pass/fail assertions for cephci workflows.
"""

from __future__ import annotations

import json
import os
import traceback
from typing import List, Optional, Tuple

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class SubvolQuarantineUtils:
    """Helpers for subvolume quarantine cephci tests."""

    def __init__(self, ceph_cluster):
        self.ceph_cluster = ceph_cluster
        self.fs_util = FsUtils(ceph_cluster)

    def feature_available(self, client) -> bool:
        """Return True if quarantine subcommand exists in this build.

        Bare ``ceph fs subvolume quarantine`` returns EINVAL with usage hints
        listing enable/disable; do not rely on exit code alone.
        """
        out, err = client.exec_command(
            sudo=True,
            cmd="ceph fs subvolume quarantine",
            check_ec=False,
        )
        text = f"{out}\n{err}".lower()
        return "quarantine enable" in text and "quarantine disable" in text

    def _quarantine_cmd(
        self,
        client,
        operation: str,
        vol_name: str,
        subvol_name: str,
        group_name: Optional[str] = None,
        expect_success: bool = True,
    ) -> dict:
        cmd = f"ceph fs subvolume quarantine {operation} {vol_name} {subvol_name}"
        if group_name:
            cmd += f" --group_name {group_name}"
        cmd += " --format json"
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        rc = client.node.exit_status
        parsed = None
        try:
            parsed = json.loads(out) if out else None
        except json.JSONDecodeError:
            parsed = None

        if rc and not expect_success:
            if isinstance(parsed, dict):
                parsed.setdefault("return_code", rc)
                parsed.setdefault("raw", out or err)
                return parsed
            return {"return_code": rc, "raw": out or err, "err": err}
        if rc:
            raise CommandFailed(
                f"quarantine {operation} failed: rc={rc} out={out} err={err}"
            )
        if isinstance(parsed, dict):
            return parsed
        log.warning("quarantine response is not JSON: %s", out)
        return {"return_code": 0, "raw": out}

    def quarantine_enable(
        self, client, vol_name: str, subvol_name: str, group_name: Optional[str] = None
    ) -> dict:
        result = self._quarantine_cmd(
            client, "enable", vol_name, subvol_name, group_name=group_name
        )
        if result.get("status") not in (None, "successful"):
            raise CommandFailed(f"quarantine enable unexpected status: {result}")
        return result

    def quarantine_disable(
        self, client, vol_name: str, subvol_name: str, group_name: Optional[str] = None
    ) -> dict:
        result = self._quarantine_cmd(
            client, "disable", vol_name, subvol_name, group_name=group_name
        )
        if result.get("status") not in (None, "successful"):
            raise CommandFailed(f"quarantine disable unexpected status: {result}")
        return result

    def get_subvolume_paths(
        self, client, vol_name: str, subvol_name: str, group_name: Optional[str] = None
    ) -> Tuple[str, str]:
        """Return (subvolume_root_path, data_path)."""
        cmd = f"ceph fs subvolume getpath {vol_name} {subvol_name}"
        if group_name:
            cmd += f" --group_name {group_name}"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        data_path = out.strip()
        root_path = os.path.dirname(data_path)
        return root_path, data_path

    def list_subvolume_entries(
        self, client, vol_name: str, group_name: Optional[str] = None
    ) -> List[dict]:
        cmd = f"ceph fs subvolume ls {vol_name} --format json"
        if group_name:
            cmd += f" --group_name {group_name}"
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        if client.node.exit_status:
            log.error("subvolume ls failed: out=%s err=%s", out, err)
            return []
        try:
            entries = json.loads(out)
        except json.JSONDecodeError:
            log.error("subvolume ls returned non-JSON: %s", out)
            return []
        if not isinstance(entries, list):
            log.error("subvolume ls unexpected type: %s", type(entries))
            return []
        return entries

    def list_subvolume_names(
        self, client, vol_name: str, group_name: Optional[str] = None
    ) -> List[str]:
        return [
            e["name"]
            for e in self.list_subvolume_entries(client, vol_name, group_name)
            if "name" in e
        ]

    def assert_subvolume_ls_complete(
        self, client, vol_name: str, expected_names: List[str]
    ) -> int:
        """
        Return 0 if all expected names appear in ls.

        Dev contract: ls remains enabled while quarantined and returns names only
        (no quarantine indication on entries).
        """
        entries = self.list_subvolume_entries(client, vol_name)
        found = {e["name"] for e in entries if "name" in e}
        missing = [n for n in expected_names if n not in found]
        if missing:
            log.error(
                "subvolume ls incomplete for vol %s; missing: %s; found: %s",
                vol_name,
                missing,
                sorted(found),
            )
            return 1
        for entry in entries:
            bad_keys = [k for k in entry if "quarantine" in k.lower()]
            if bad_keys:
                log.error(
                    "subvolume ls must not include quarantine indication; "
                    "entry=%s bad_keys=%s",
                    entry,
                    bad_keys,
                )
                return 1
        log.info("subvolume ls OK (names only, includes expected): %s", expected_names)
        return 0

    def get_subvolume_info_json(
        self, client, vol_name: str, subvol_name: str, group_name: Optional[str] = None
    ) -> Optional[dict]:
        """Return parsed subvolume info JSON, or None on failure."""
        cmd = f"ceph fs subvolume info {vol_name} {subvol_name} -f json"
        if group_name:
            cmd += f" --group_name {group_name}"
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        if client.node.exit_status:
            log.error(
                "subvolume info failed for %s: rc=%s out=%s err=%s",
                subvol_name,
                client.node.exit_status,
                out,
                err,
            )
            return None
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            log.error("subvolume info non-JSON: %s", out)
            return None

    def assert_info_not_quarantined(
        self,
        client,
        vol_name: str,
        subvol_name: str,
        group_name: Optional[str] = None,
    ) -> int:
        """
        Return 0 if info succeeds and quarantine flag is absent/false.

        Non-quarantined SVs should still return full info (e.g. path present).
        """
        info = self.get_subvolume_info_json(
            client, vol_name, subvol_name, group_name=group_name
        )
        if info is None:
            log.error("expected subvolume info to succeed before quarantine")
            return 1
        flag = None
        for key in ("quarantine", "quarantined", "is_quarantined"):
            if key in info:
                flag = bool(info[key])
                break
        if flag:
            log.error("subvolume unexpectedly quarantined before enable: %s", info)
            return 1
        if "path" not in info:
            log.error(
                "expected full subvolume info when not quarantined (missing path): %s",
                list(info.keys()),
            )
            return 1
        log.info("pre-quarantine info OK (full info, not quarantined)")
        return 0

    def assert_info_quarantined(
        self,
        client,
        vol_name: str,
        subvol_name: str,
        group_name: Optional[str] = None,
    ) -> int:
        """
        Dev contract: info succeeds for quarantined SVs with *minimal* payload:
        name, group, quarantine status — not size/path/etc.
        """
        info = self.get_subvolume_info_json(
            client, vol_name, subvol_name, group_name=group_name
        )
        if info is None:
            log.error(
                "FAILED: subvolume info must succeed while quarantined with "
                "minimal quarantine status (got error instead)"
            )
            return 1

        flag = None
        matched_key = None
        for key in ("quarantine", "quarantined", "is_quarantined"):
            if key in info:
                flag = bool(info[key])
                matched_key = key
                break
        if flag is None:
            log.error(
                "FAILED: quarantined info missing quarantine status field; keys=%s",
                list(info.keys()),
            )
            return 1
        if not flag:
            log.error(
                "FAILED: info.%s is false after enable: %s",
                matched_key,
                info,
            )
            return 1

        if "name" in info and info["name"] != subvol_name:
            log.error(
                "FAILED: info.name mismatch: expected %s got %s",
                subvol_name,
                info.get("name"),
            )
            return 1

        if group_name is not None and "group" in info and info["group"] != group_name:
            log.error(
                "FAILED: info.group mismatch: expected %s got %s",
                group_name,
                info.get("group"),
            )
            return 1

        # Full-detail fields must not appear on quarantined minimal info
        forbidden = (
            "path",
            "bytes_used",
            "bytes_quota",
            "bytes_pcent",
            "data_pool",
            "mon_addrs",
            "features",
        )
        present_forbidden = [k for k in forbidden if k in info]
        if present_forbidden:
            log.error(
                "FAILED: quarantined info must be minimal (no size/path/etc); "
                "unexpected keys=%s full=%s",
                present_forbidden,
                info,
            )
            return 1

        log.info(
            "quarantined info OK (minimal; status via '%s'): %s",
            matched_key,
            info,
        )
        return 0

    def get_quarantine_from_info(
        self, client, vol_name: str, subvol_name: str, group_name: Optional[str] = None
    ) -> Optional[bool]:
        info = self.get_subvolume_info_json(
            client, vol_name, subvol_name, group_name=group_name
        )
        if not info:
            return None
        for key in ("quarantine", "quarantined", "is_quarantined"):
            if key in info:
                return bool(info[key])
        log.info("subvolume info has no quarantine flag; keys=%s", list(info.keys()))
        return None

    def get_active_mds_name(self, client, vol_name: str) -> str:
        active = self.fs_util.get_active_mdss(client, vol_name)
        if not active:
            raise CommandFailed(f"no active MDS found for {vol_name}")
        return active[0]

    def mds_quarantine(
        self,
        client,
        mds_name: str,
        operation: str,
        subvol_root: str,
        expect_success: bool = True,
    ) -> dict:
        """MDS admin-socket path: ceph tell mds.<name> quarantine enable|disable <root>."""
        cmd = f"ceph tell mds.{mds_name} quarantine {operation} {subvol_root}"
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        rc = client.node.exit_status
        if rc and not expect_success:
            return {"return_code": rc, "raw": out or err}
        if rc:
            raise CommandFailed(
                f"mds quarantine {operation} failed: rc={rc} out={out} err={err}"
            )
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            log.warning("mds quarantine response is not JSON: %s", out)
            return {"return_code": 0, "raw": out}

    def assert_content_equals(
        self, client, mount_point: str, file_name: str, expected: str
    ) -> int:
        try:
            out, _ = client.exec_command(
                sudo=True, cmd=f"cat {mount_point}/{file_name}", check_ec=True
            )
        except CommandFailed as exc:
            log.error("read failed while expecting content match: %s", exc)
            return 1
        actual = out.strip()
        if expected not in actual and actual != expected:
            log.error(
                "content mismatch for %s: expected '%s', got '%s'",
                file_name,
                expected,
                actual,
            )
            return 1
        return 0

    def create_rw_client(
        self,
        client,
        vol_name: str,
        subvol_root: str,
        client_name: str,
    ) -> None:
        self.fs_util.create_ceph_client(
            client,
            client_name,
            mon_caps="allow r",
            osd_caps=f"allow rw tag cephfs data={vol_name}",
            mds_caps=f"allow rw fsname={vol_name} path={subvol_root}",
        )

    def create_r_client(
        self,
        client,
        vol_name: str,
        subvol_root: str,
        client_name: str,
    ) -> None:
        """Read-only MDS caps (no q) — must not open a quarantined SV."""
        self.fs_util.create_ceph_client(
            client,
            client_name,
            mon_caps="allow r",
            osd_caps=f"allow rw tag cephfs data={vol_name}",
            mds_caps=f"allow r fsname={vol_name} path={subvol_root}",
        )

    def create_rwq_client(
        self,
        client,
        vol_name: str,
        subvol_root: str,
        client_name: str,
    ) -> None:
        self.fs_util.create_ceph_client(
            client,
            client_name,
            mon_caps="allow r",
            osd_caps=f"allow rw tag cephfs data={vol_name}",
            mds_caps=f"allow rwq fsname={vol_name} path={subvol_root}",
        )

    def create_rwQ_client(
        self,
        client,
        vol_name: str,
        client_name: str,
    ) -> None:
        """Recovery client authorized for all quarantined paths (capital Q)."""
        self.fs_util.create_ceph_client(
            client,
            client_name,
            mon_caps="allow r",
            osd_caps=f"allow rw tag cephfs data={vol_name}",
            mds_caps=f"allow rwQ fsname={vol_name}",
        )

    def create_star_client(self, client, client_name: str) -> None:
        """Client with allow * — must NOT grant quarantine access."""
        self.fs_util.create_ceph_client(
            client,
            client_name,
            mon_caps="allow *",
            osd_caps="allow *",
            mds_caps="allow *",
        )

    def assert_mgr_op_succeeds(self, client, cmd: str, label: str) -> int:
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        rc = client.node.exit_status
        if rc != 0:
            log.error(
                "expected %s to succeed but failed: rc=%s out=%s err=%s cmd=%s",
                label,
                rc,
                out,
                err,
                cmd,
            )
            return 1
        log.info("%s succeeded as expected", label)
        return 0

    def delete_client(self, client, client_name: str) -> None:
        client.exec_command(
            sudo=True,
            cmd=f"ceph auth del client.{client_name}",
            check_ec=False,
        )

    def mount_fuse(
        self,
        client,
        mount_point: str,
        data_path: str,
        client_name: str,
        vol_name: str = "cephfs",
    ) -> None:
        # fuse_mount iterates fuse_clients; pass a single-client list
        self.fs_util.fuse_mount(
            [client],
            mount_point,
            new_client_hostname=client_name,
            extra_params=f" -r {data_path} --client_fs {vol_name}",
        )

    def assert_fuse_mount_fails(
        self,
        client,
        mount_point: str,
        data_path: str,
        client_name: str,
        vol_name: str = "cephfs",
    ) -> int:
        """
        Acceptance 2: normal client mount after quarantine must fail (EACCES / Permission denied).
        """
        try:
            self.mount_fuse(client, mount_point, data_path, client_name, vol_name)
            log.error(
                "fuse mount succeeded for client.%s but expected Permission denied",
                client_name,
            )
            self.umount_fuse(client, mount_point)
            return 1
        except CommandFailed as exc:
            err = str(exc).lower()
            if (
                "permission denied" in err
                or "eacces" in err
                or "(13)" in err
                or "failed with error" in err
            ):
                log.info(
                    "fuse mount blocked as expected for client.%s: %s",
                    client_name,
                    exc,
                )
                return 0
            log.error("unexpected fuse mount failure: %s", exc)
            return 1

    def umount_fuse(self, client, mount_point: str) -> None:
        client.exec_command(
            sudo=True,
            cmd=f"fusermount -u {mount_point}",
            check_ec=False,
        )

    def assert_read_blocked(self, client, mount_point: str, file_name: str) -> int:
        try:
            client.exec_command(
                sudo=True, cmd=f"cat {mount_point}/{file_name}", check_ec=True
            )
            log.error("read succeeded but expected EACCES on %s", file_name)
            return 1
        except CommandFailed as exc:
            err = str(exc).lower()
            if "permission denied" in err or "eacces" in err:
                log.info("read blocked as expected: %s", exc)
                return 0
            log.error("unexpected read error: %s", exc)
            return 1

    def assert_write_blocked(self, client, mount_point: str, file_name: str) -> int:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"bash -c 'echo blocked-test > {mount_point}/{file_name}'",
                check_ec=True,
            )
            log.error("write succeeded but expected EACCES on %s", file_name)
            return 1
        except CommandFailed as exc:
            err = str(exc).lower()
            if "permission denied" in err or "eacces" in err:
                log.info("write blocked as expected: %s", exc)
                return 0
            log.error("unexpected write error: %s", exc)
            return 1

    def assert_read_ok(self, client, mount_point: str, file_name: str) -> int:
        try:
            client.exec_command(
                sudo=True, cmd=f"cat {mount_point}/{file_name}", check_ec=True
            )
            return 0
        except CommandFailed as exc:
            log.error("read failed unexpectedly: %s", exc)
            return 1

    def assert_write_ok(self, client, mount_point: str, file_name: str) -> int:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"bash -c 'echo write-ok > {mount_point}/{file_name}'",
                check_ec=True,
            )
            return 0
        except CommandFailed as exc:
            log.error("write failed unexpectedly: %s", exc)
            return 1

    def write_baseline_file(
        self, client, mount_point: str, file_name: str, content: str
    ) -> int:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"bash -c 'echo {content} > {mount_point}/{file_name}'",
                check_ec=True,
            )
            return 0
        except CommandFailed as exc:
            log.error("baseline write failed: %s", exc)
            return 1

    def setup_subvolume(
        self,
        client,
        vol_name: str,
        subvol_name: str,
        group_name: Optional[str] = None,
        mode: str = "777",
    ) -> int:
        kwargs = {"mode": mode}
        if group_name:
            kwargs["group_name"] = group_name
        try:
            self.fs_util.create_subvolume(client, vol_name, subvol_name, **kwargs)
            return 0
        except CommandFailed as exc:
            log.error("create_subvolume failed: %s", exc)
            return 1

    def cleanup_subvolume(
        self,
        client,
        vol_name: str,
        subvol_name: str,
        group_name: Optional[str] = None,
    ) -> None:
        try:
            self.quarantine_disable(
                client, vol_name, subvol_name, group_name=group_name
            )
        except CommandFailed:
            pass
        self.fs_util.remove_subvolume(
            client,
            vol_name,
            subvol_name,
            group_name=group_name,
            force=True,
            check_ec=False,
            validate=False,
        )

    def assert_mgr_op_fails(self, client, cmd: str, label: str) -> int:
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        rc = client.node.exit_status
        if rc == 0:
            log.error(
                "expected %s to fail but command succeeded: %s (out=%s)",
                label,
                cmd,
                out,
            )
            return 1
        log.info("%s blocked as expected (rc=%s err=%s)", label, rc, err)
        return 0

    def log_exception(self) -> None:
        log.info(traceback.format_exc())
