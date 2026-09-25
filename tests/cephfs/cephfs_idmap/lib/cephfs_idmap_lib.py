"""
Shared helpers for CephFS idmapped mount functional tests.

Standalone validation of kernel CephFS idmap support (no ODF/Kubernetes).
Target: Tentacle on RHEL 10.2 clients.
"""

import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

FS_NAME = "cephfs"
IDMAP_BASE_UID = 100000
IDMAP_BASE_UID_B = 200000
IDMAP_RANGE = 65536
IDMAP_LAST_VIEW_UID = IDMAP_RANGE - 1
IDMAP_LAST_DISK_UID = IDMAP_BASE_UID + IDMAP_LAST_VIEW_UID
IDMAP_OUT_OF_RANGE_VIEW_UID = 70000
IDMAP_OUT_OF_RANGE_DISK_UID = IDMAP_BASE_UID + IDMAP_OUT_OF_RANGE_VIEW_UID
DEFAULT_IDMAP_SPEC = f"b:{IDMAP_BASE_UID}:0:{IDMAP_RANGE}"
ALT_IDMAP_SPEC = f"b:{IDMAP_BASE_UID_B}:0:{IDMAP_RANGE}"
IDMAP_SYSFS_PARAM = "/sys/module/ceph/parameters/enable_unsafe_idmap"
DMESG_FAIL_PATTERNS = ("mount_setattr",)
# Matches test_client.py steps in tier-2_cephfs_test-idmap.yaml (node8, node9).
CONFIGURED_CLIENT_COUNT = 2


def get_configured_clients(ceph_cluster, test_data=None, config=None):
    """
    Return only client nodes prepared by the suite bootstrap.

    Cluster conf may define more client-role nodes than test_client.py configures.
    """
    all_clients = ceph_cluster.get_ceph_objects("client")
    count = CONFIGURED_CLIENT_COUNT

    if test_data and test_data.get("idmap_suite", {}).get("configured_client_count"):
        count = test_data["idmap_suite"]["configured_client_count"]
    if config and config.get("configured_client_count"):
        count = config["configured_client_count"]

    if len(all_clients) < count:
        raise CommandFailed(
            f"Expected at least {count} configured client node(s), "
            f"found {len(all_clients)} with client role"
        )

    clients = all_clients[:count]
    log.info(
        "Using %d configured client(s): %s",
        len(clients),
        ", ".join(client.node.hostname for client in clients),
    )
    if len(all_clients) > count:
        skipped = all_clients[count:]
        log.info(
            "Skipping %d unconfigured client node(s): %s",
            len(skipped),
            ", ".join(client.node.hostname for client in skipped),
        )
    return clients


class IdmapTestHelper:
    """Utilities for idmapped CephFS kernel mount tests."""

    def __init__(self, ceph_cluster, test_data=None):
        self.ceph_cluster = ceph_cluster
        self.test_data = test_data or {}
        self.fs_util = FsUtils(ceph_cluster, test_data=self.test_data)
        self.common_utils = CephFSCommonUtils(ceph_cluster)
        self.mon_node_ips = self.fs_util.get_mon_node_ips()

    @staticmethod
    def random_suffix(length=8):
        return "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(length)
        )

    def prepare_clients(self, clients, build):
        for client in clients:
            self.ensure_ceph_kernel_module(client)
        self.fs_util.prepare_clients(clients, build)
        self.fs_util.auth_list(clients)
        self._ensure_filesystem(clients[0])

    def _ensure_filesystem(self, client):
        if not self.fs_util.get_fs_info(client, FS_NAME):
            self.fs_util.create_fs(client, FS_NAME)

    def mount_paths(self, suffix=None):
        suffix = suffix or self.random_suffix()
        plain = f"/mnt/cephfs_idmap_plain_{suffix}"
        idmap = f"/mnt/cephfs_idmap_view_{suffix}"
        return plain, idmap, suffix

    def record_environment_versions(self, client):
        cmds = {
            "kernel": "uname -r",
            "ceph": "ceph version",
            "mount": "mount --version | head -1",
            "ceph_common": "rpm -q ceph-common 2>/dev/null || dpkg -l ceph-common 2>/dev/null | tail -1",
            "util_linux": "rpm -q util-linux 2>/dev/null || dpkg -l util-linux 2>/dev/null | tail -1",
        }
        versions = {}
        for key, cmd in cmds.items():
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            versions[key] = (out or "").strip()
            log.info("Environment %s: %s", key, versions[key])
        return versions

    def get_enable_unsafe_idmap(self, client):
        """Return enable_unsafe_idmap sysfs value, or None if unavailable."""
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"cat {IDMAP_SYSFS_PARAM} 2>/dev/null",
            check_ec=False,
        )
        value = (out or "").strip()
        return value if value else None

    def set_enable_unsafe_idmap(self, client, enabled):
        """Set enable_unsafe_idmap via sysfs (0 or 1)."""
        value = "1" if enabled else "0"
        self.exec_cmd(
            client,
            f"test -f {IDMAP_SYSFS_PARAM} && echo {value} > {IDMAP_SYSFS_PARAM}",
        )
        log.info("Set %s=%s", IDMAP_SYSFS_PARAM, value)

    def _apply_enable_unsafe_idmap(self, client):
        """
        Enable idmap creates against MDS without CEPHFS_FEATURE_HAS_OWNER_UIDGID.

        The parameter can be set at runtime via sysfs even when modprobe cannot
        reload the module (e.g. module already loaded from an earlier step).
        """
        if self.get_enable_unsafe_idmap(client) is not None:
            self.set_enable_unsafe_idmap(client, True)

    def ensure_ceph_kernel_module(self, client):
        """
        Load the CephFS kernel client module on the client node.

        The module may be installed but not listed in /proc/filesystems until loaded.
        enable_unsafe_idmap allows idmap creates when MDS lacks owner uid/gid support.
        """
        client.exec_command(
            sudo=True,
            cmd=(
                "dnf install -y kernel-modules-extra 2>/dev/null || "
                "yum install -y kernel-modules-extra"
            ),
            check_ec=False,
        )
        client.exec_command(sudo=True, cmd="modprobe -r ceph", check_ec=False)
        _out, err, exit_code, _duration = client.exec_command(
            sudo=True,
            cmd="modprobe ceph enable_unsafe_idmap=1",
            check_ec=False,
            verbose=True,
        )
        if exit_code != 0:
            modinfo_out, _ = client.exec_command(
                sudo=True, cmd="modinfo ceph 2>/dev/null", check_ec=False
            )
            if not modinfo_out:
                raise CommandFailed(
                    "CephFS kernel module is not installed on client; "
                    f"modprobe ceph failed: {err}"
                )
            log.warning(
                "modprobe ceph enable_unsafe_idmap=1 returned %s; loading plain module",
                exit_code,
            )
            client.exec_command(
                sudo=True, cmd="modprobe ceph", check_ec=False, verbose=True
            )

        self._apply_enable_unsafe_idmap(client)
        unsafe_out, _ = client.exec_command(
            sudo=True,
            cmd="cat /sys/module/ceph/parameters/enable_unsafe_idmap 2>/dev/null",
            check_ec=False,
        )
        unsafe_val = (unsafe_out or "").strip()
        log.info("ceph enable_unsafe_idmap=%s", unsafe_val or "unset")
        if unsafe_val not in ("1", "Y", "y"):
            log.warning(
                "enable_unsafe_idmap is not active; idmap create may fail with "
                "EOVERFLOW/-EIO on MDS without CEPHFS_FEATURE_HAS_OWNER_UIDGID"
            )

    def check_mds_high_uid_support(self, client, plain_mount):
        """
        Verify MDS can store UIDs in the idmap range (e.g. 100000).

        Without CEPHFS_FEATURE_HAS_OWNER_UIDGID, chown/touch above 16-bit uid fails.
        """
        probe = f"{plain_mount}/mds-uid-probe"
        self.exec_cmd(client, f"touch {probe}")
        self.exec_cmd(client, f"chown {IDMAP_BASE_UID}:{IDMAP_BASE_UID} {probe}")
        self.assert_stat_uid_gid(client, probe, IDMAP_BASE_UID, IDMAP_BASE_UID)
        self.exec_cmd(client, f"rm -f {probe}")

    def check_cephfs_kernel_module(self, client):
        self.ensure_ceph_kernel_module(client)
        out, _ = client.exec_command(
            sudo=True, cmd="grep -w ceph /proc/filesystems", check_ec=False
        )
        if "ceph" not in (out or ""):
            modinfo_out, _ = client.exec_command(
                sudo=True, cmd="modinfo ceph 2>/dev/null", check_ec=False
            )
            if not modinfo_out:
                raise CommandFailed(
                    "CephFS kernel client not available in /proc/filesystems "
                    "and modinfo ceph returned no data"
                )
            log.info(
                "ceph kernel module available (modinfo OK); "
                "/proc/filesystems will populate on first mount"
            )

    def check_idmap_sysfs_present(self, client):
        """
        Confirm the CephFS idmap backport is present.

        Do not gate on kernel version (RHEL 9 ships 5.14.x with backport).
        """
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"test -e {IDMAP_SYSFS_PARAM} && echo present || echo missing",
            check_ec=False,
        )
        if "present" not in (out or ""):
            raise CommandFailed(
                "CephFS idmap backport not present: "
                f"{IDMAP_SYSFS_PARAM} does not exist"
            )

    def check_util_linux_idmap_support(self, client):
        out, _ = client.exec_command(
            sudo=True,
            cmd="mount --help 2>&1",
            check_ec=False,
        )
        help_text = out or ""
        if "map-users" not in help_text and "X-mount.idmap" not in help_text:
            raise CommandFailed(
                "util-linux lacks idmap support (expected map-users or X-mount.idmap)"
            )

    def idmap_functional_probe(self, client, plain_mount):
        """Bind-mount idmap probe on an existing plain CephFS mount."""
        probe_dst = f"{plain_mount}/.idmap_probe_{self.random_suffix()}"
        self.prepare_idmap_mount_root(client, plain_mount)
        self.idmap_bind_mount(client, plain_mount, probe_dst, DEFAULT_IDMAP_SPEC)
        self.exec_cmd(client, f"touch {probe_dst}/probe-ok")
        self.exec_cmd(client, f"umount {probe_dst}")
        client.exec_command(sudo=True, cmd=f"rmdir {probe_dst}", check_ec=False)
        self.assert_dmesg_clean(client)

    def check_kernel_idmap_support(self, client, plain_mount=None):
        """
        Feature-based idmap gate (backport-aware).

        Probes sysfs parameter presence and util-linux support; optionally runs
        a functional idmap bind-mount on plain_mount.
        """
        self.check_idmap_sysfs_present(client)
        self.check_util_linux_idmap_support(client)
        if plain_mount:
            self.idmap_functional_probe(client, plain_mount)

    def mds_has_owner_uidgid(self, client, plain_mount=None):
        """Return True when MDS advertises or supports owner UID/GID storage."""
        out, _ = client.exec_command(
            sudo=True, cmd="ceph fs dump 2>/dev/null", check_ec=False
        )
        if "owner_uidgid" in (out or "").lower():
            return True
        if plain_mount:
            try:
                self.check_mds_high_uid_support(client, plain_mount)
                return True
            except CommandFailed:
                return False
        return False

    def check_cluster_health(self, client, wait_time=300):
        if self.common_utils.wait_for_healthy_ceph(client, wait_time):
            raise CommandFailed("Cluster health is not OK")
        out, _ = client.exec_command(sudo=True, cmd="ceph fs status")
        if "active" not in (out or "").lower():
            raise CommandFailed(f"No active MDS reported by ceph fs status:\n{out}")

    def kernel_mount_plain(self, client, mount_point, fs_name=FS_NAME):
        self.fs_util.kernel_mount(
            [client],
            mount_point,
            ",".join(self.mon_node_ips),
            extra_params=f",fs={fs_name}",
        )

    def _parse_idmap_spec(self, map_spec):
        """
        Parse X-mount.idmap spec into (disk_base, view_base, count).

        X-mount.idmap uses on-disk:view order per Ceph #62217, e.g.
        b:100000:0:65536 maps on-disk UID 100000 to view UID 0.

        mount --map-users uses the opposite order (view:disk:count).
        """
        parts = map_spec.split(":")
        if parts[0] == "b" and len(parts) == 4:
            disk_base, view_base, count = parts[1], parts[2], parts[3]
        elif len(parts) == 3:
            disk_base, view_base, count = parts
        else:
            raise CommandFailed(f"Unsupported idmap spec: {map_spec}")
        return disk_base, view_base, count

    def prepare_idmap_mount_root(
        self, client, plain_mount, map_spec=DEFAULT_IDMAP_SPEC
    ):
        """
        Chown the plain mount root into the idmap on-disk UID/GID range.

        With mapping b:100000:0:65536, an on-disk root of 0:0 appears as
        overflow uid 65534 in the idmapped view and creates fail with EOVERFLOW.
        See Ceph tracker #62217 step 2 (chown mount root before idmap bind).
        """
        disk_base, _view_base, _count = self._parse_idmap_spec(map_spec)
        self.exec_cmd(client, f"chown {disk_base}:{disk_base} {plain_mount}")
        log.info(
            "Prepared idmap mount root %s as %s:%s", plain_mount, disk_base, disk_base
        )

    def verify_idmap_mount(self, client, idmap_mount):
        out, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"findmnt -no OPTIONS {idmap_mount} 2>/dev/null; "
                f"grep -F ' {idmap_mount} ' /proc/self/mountinfo | tail -1"
            ),
            check_ec=False,
        )
        info = (out or "").lower()
        if "idmap" not in info:
            raise CommandFailed(
                f"Bind mount at {idmap_mount} does not appear idmapped: {out!r}"
            )

    def idmap_bind_mount(
        self, client, plain_mount, idmap_mount, map_spec=DEFAULT_IDMAP_SPEC
    ):
        client.exec_command(sudo=True, cmd=f"mkdir -p {plain_mount} {idmap_mount}")
        disk_base, view_base, count = self._parse_idmap_spec(map_spec)
        bind_attempts = [
            f'mount --bind -o X-mount.idmap="{map_spec}" {plain_mount} {idmap_mount}',
            (
                f"mount --bind --map-users {view_base}:{disk_base}:{count} "
                f"--map-groups {view_base}:{disk_base}:{count} "
                f"{plain_mount} {idmap_mount}"
            ),
        ]
        last_err = None
        for bind_cmd in bind_attempts:
            try:
                self.exec_cmd(client, bind_cmd)
                self.verify_idmap_mount(client, idmap_mount)
                return
            except CommandFailed as exc:
                last_err = exc
                client.exec_command(
                    sudo=True, cmd=f"umount {idmap_mount}", check_ec=False
                )
        raise CommandFailed(f"Failed to create idmapped bind mount: {last_err}")

    def setup_plain_and_idmap_mounts(
        self,
        client,
        map_spec=DEFAULT_IDMAP_SPEC,
        suffix=None,
        enable_unsafe_idmap=True,
    ):
        plain, idmap, suffix = self.mount_paths(suffix)
        if enable_unsafe_idmap:
            self._apply_enable_unsafe_idmap(client)
        self.kernel_mount_plain(client, plain)
        self.prepare_idmap_mount_root(client, plain, map_spec=map_spec)
        self.idmap_bind_mount(client, plain, idmap, map_spec=map_spec)
        return plain, idmap, suffix

    def umount_plain(self, client, plain_mount):
        client.exec_command(sudo=True, cmd=f"umount {plain_mount}", check_ec=False)
        client.exec_command(sudo=True, cmd=f"rm -rf {plain_mount}", check_ec=False)

    def umount_idmap_stack(self, client, idmap_mount, plain_mount):
        for mount in (idmap_mount, plain_mount):
            client.exec_command(sudo=True, cmd=f"umount {mount}", check_ec=False)
        client.exec_command(
            sudo=True,
            cmd=f"rm -rf {idmap_mount} {plain_mount}",
            check_ec=False,
        )

    def get_stat_uid_gid(self, client, path):
        out, _ = client.exec_command(
            sudo=True, cmd=f"stat -c '%u %g' {path}", check_ec=False
        )
        parts = (out or "").strip().split()
        if len(parts) != 2:
            raise CommandFailed(f"Unable to parse stat output for {path}: {out!r}")
        return int(parts[0]), int(parts[1])

    def assert_stat_uid_gid(self, client, path, expected_uid, expected_gid):
        uid, gid = self.get_stat_uid_gid(client, path)
        if uid != expected_uid or gid != expected_gid:
            raise CommandFailed(
                f"Ownership mismatch for {path}: expected {expected_uid}:{expected_gid}, "
                f"got {uid}:{gid}"
            )

    def assert_dmesg_clean(self, client, patterns=None):
        patterns = patterns or DMESG_FAIL_PATTERNS
        out, _ = client.exec_command(sudo=True, cmd="dmesg | tail -200", check_ec=False)
        for pattern in patterns:
            if pattern in (out or ""):
                raise CommandFailed(
                    f"dmesg contains unexpected pattern '{pattern}' after idmap operation"
                )

    def exec_cmd(self, client, cmd, expect_fail=False):
        out, err, exit_code, _duration = client.exec_command(
            sudo=True, cmd=cmd, check_ec=False, verbose=True
        )
        if expect_fail and exit_code == 0:
            raise CommandFailed(f"Command expected to fail but succeeded: {cmd}")
        if not expect_fail and exit_code != 0:
            raise CommandFailed(
                f"Command failed (exit {exit_code}): {cmd}\nstdout: {out}\nstderr: {err}"
            )
        return out, err, exit_code

    @staticmethod
    def acl_perm_satisfies(actual, required):
        """Return True when actual getfacl perms grant every bit in required."""
        norm = (actual or "").ljust(3, "-")[:3]
        for bit in required:
            if bit in "rwx" and norm["rwx".index(bit)] != bit:
                return False
        return True

    def unshare_run(
        self,
        client,
        shell_cmd,
        inner_uid=None,
        outer_uid=None,
        inner_gid=None,
        outer_gid=None,
        map_root=False,
        run_as_inner=False,
        use_outer_cred=False,
        expect_fail=False,
    ):
        """
        Run a command inside a user namespace.

        util-linux 2.39+ uses --map-users inner:outer:count for host mapping.
        When run_as_inner is set, the process must run as inner_uid inside the new
        namespace (required when invoking via sudo as root).

        By default use --map-user/--map-group so the VFS keeps host root credentials;
        real mapped host UIDs hit EOVERFLOW on idmapped CephFS I/O. Pass
        use_outer_cred=True for plain-mount I/O that must use the true outer UID
        (e.g. high-UID creates or ACL enforcement on disk UIDs).
        """
        parts = ["unshare", "--user"]
        gid_inner = inner_gid if inner_gid is not None else inner_uid
        gid_outer = outer_gid if outer_gid is not None else outer_uid

        if map_root:
            parts.append("--map-root-user")
        elif inner_uid is not None and outer_uid is not None:
            parts.append(f"--map-users {inner_uid}:{outer_uid}:1")
            parts.append(f"--map-groups {gid_inner}:{gid_outer}:1")
            if run_as_inner:
                if use_outer_cred:
                    parts.append(f"--setuid {inner_uid}")
                    parts.append(f"--setgid {gid_inner}")
                else:
                    parts.append(f"--map-user {inner_uid}")
                    parts.append(f"--map-group {gid_inner}")

        parts.append(f"sh -c {repr(shell_cmd)}")
        return self.exec_cmd(client, " ".join(parts), expect_fail=expect_fail)

    def install_recursive_tools(self, client):
        client.exec_command(
            sudo=True,
            cmd="dnf install -y rsync tar 2>/dev/null || yum install -y rsync tar",
            check_ec=False,
        )

    def install_acl_tools(self, client):
        client.exec_command(
            sudo=True,
            cmd="dnf install -y acl 2>/dev/null || yum install -y acl",
            check_ec=False,
        )

    def getfacl_user_permissions(self, client, path):
        """Return {uid: permission} from numeric getfacl output."""
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"getfacl -c -n {path} 2>/dev/null",
            check_ec=False,
        )
        entries = {}
        for line in (out or "").splitlines():
            if not line.startswith("user:"):
                continue
            parts = line.split(":")
            if len(parts) >= 3 and parts[1].isdigit():
                entries[int(parts[1])] = parts[2]
        return entries

    def cleanup_xfstests_artifacts(self, client):
        """Remove leftover xfstests users/dirs from a prior TC-S13 run."""
        mounts_out, _ = client.exec_command(
            sudo=True,
            cmd="findmnt -t ceph -o TARGET -n 2>/dev/null || true",
            check_ec=False,
        )
        ceph_mounts = sorted(
            [m.strip() for m in (mounts_out or "").splitlines() if m.strip()],
            key=len,
            reverse=True,
        )
        for mount in ceph_mounts:
            client.exec_command(sudo=True, cmd=f"umount -f {mount}", check_ec=False)
        client.exec_command(sudo=True, cmd="rm -rf /root/xfstests-dev", check_ec=False)
        for user in ("fsgqa", "fsgqa2"):
            client.exec_command(sudo=True, cmd=f"userdel -r {user}", check_ec=False)

    def umount_plain_mounts(self, client, *mount_points):
        for mount in mount_points:
            client.exec_command(sudo=True, cmd=f"umount {mount}", check_ec=False)
            client.exec_command(sudo=True, cmd=f"rm -rf {mount}", check_ec=False)

    def configure_idmap_xfstests_local_config(self, client, mon_node_ips, mount_info):
        """
        Write xfstests local.config for CephFS idmapped group tests.

        Call after ``XfsTestSetup.mount_fs()`` so test/scratch subpaths exist
        on CephFS (``mount error 2`` otherwise). TEST_DEV and SCRATCH_DEV must
        use distinct mon+subpath strings so findmnt does not match both mounts.
        """
        admin_key, _ = client.exec_command(
            sudo=True, cmd="ceph auth get-key client.admin"
        )
        admin_key = (admin_key or "").strip()
        fs_name = mount_info["fs_name"]
        test_dir = mount_info["test_mount"]
        scratch_mnt = mount_info["scratch_mount"]
        test_sub = mount_info["test_dev"]
        scratch_sub = mount_info["scratch_dev"]

        # Subpaths must live on CephFS; mount_fs() must already be in place.
        self.exec_cmd(client, f"mkdir -p {test_dir}/{test_sub}")
        self.exec_cmd(client, f"mkdir -p {scratch_mnt}/{scratch_sub}")

        test_dev = f"{mon_node_ips[0]}:/{test_sub}"
        scratch_dev = f"{mon_node_ips[1 % len(mon_node_ips)]}:/{scratch_sub}"

        config = (
            f"export TEST_DEV={test_dev}\n"
            f"export SCRATCH_DEV={scratch_dev}\n"
            f"export TEST_DIR={test_dir}\n"
            f"export SCRATCH_MNT={scratch_mnt}\n"
            f"export FSTYP=ceph\n"
            f'\nCOMMON_OPTIONS="name=admin,secret={admin_key},fs={fs_name},noshare"\n'
            f'TEST_FS_MOUNT_OPTS="-o ${{COMMON_OPTIONS}}"\n'
            f'MOUNT_OPTIONS="-o ${{COMMON_OPTIONS}}"\n'
            f"export TEST_FS_MOUNT_OPTS\n"
            f"export MOUNT_OPTIONS\n"
        )
        client.exec_command(
            sudo=True,
            cmd="cat > /root/xfstests-dev/local.config <<'EOF'\n" f"{config}" "EOF",
        )
        local_config, _ = client.exec_command(
            sudo=True, cmd="cat /root/xfstests-dev/local.config", check_ec=False
        )
        log.info("xfstests local.config:\n%s", local_config)

    def capture_failure_artifacts(self, client):
        for cmd in (
            "dmesg | tail -100",
            "ceph health detail",
            "ceph fs status",
            "cat /sys/module/ceph/parameters/enable_unsafe_idmap 2>/dev/null || true",
            "findmnt -t ceph 2>/dev/null || true",
        ):
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            log.error("%s:\n%s", cmd, out)


def init_idmap_test(ceph_cluster, kw):
    """Initialize helpers used by idmap test modules."""
    config = kw.get("config") or {}
    test_data = kw.get("test_data")
    build = config.get("build", config.get("rhbuild"))
    clients = get_configured_clients(ceph_cluster, test_data=test_data, config=config)
    helper = IdmapTestHelper(ceph_cluster, test_data=test_data)
    return config, test_data, build, clients, helper


def run_idmap_test(ceph_cluster, kw, test_fn):
    """Run a test function with standard error handling."""
    config = kw.get("config") or {}
    test_data = kw.get("test_data")
    clients = get_configured_clients(ceph_cluster, test_data=test_data, config=config)
    client = clients[0] if clients else None
    try:
        return test_fn(ceph_cluster, kw)
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        if client:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(client)
        return 1
