"""
Reusable NFS v4 ACL helper class.

Wraps nfs4_setfacl / nfs4_getfacl operations and provides convenience
methods for ACL manipulation, user/group management, and verification
on remote NFS client nodes.

Usage
-----
    from tests.nfs.lib.nfs_acl import NfsAcl

    acl = NfsAcl(client, mount_point="/mnt/nfs")
    acl.install_acl_tools()
    acl.set_acl("f1", "A::1001:rw")
    entries = acl.get_acl("f1")
"""

from time import sleep

from utility.log import Log

log = Log(__name__)

_ACL_TOOLS_PKG = "nfs4-acl-tools"


class NfsAcl:
    """Helper class for NFSv4 ACL operations on a remote client node."""

    def __init__(self, client, mount_point):
        self.client = client
        self.mount = mount_point

    # -- package management ---------------------------------------------------

    def install_acl_tools(self):
        """Install nfs4-acl-tools on the client node."""
        log.info("Installing %s on %s", _ACL_TOOLS_PKG, self.client.hostname)
        self.client.exec_command(
            sudo=True, cmd=f"yum install -y {_ACL_TOOLS_PKG}", long_running=True
        )

    # -- path helpers ---------------------------------------------------------

    def _full_path(self, relative_path):
        return f"{self.mount}/{relative_path}"

    # -- file / directory creation --------------------------------------------

    def create_file(self, name):
        """Create (or recreate) a regular file under the mount point."""
        path = self._full_path(name)
        log.info("Creating file %s", path)
        self.client.exec_command(sudo=True, cmd=f"rm -f {path} && touch {path}")

    def create_dir(self, name):
        """Create a directory under the mount point (with -p)."""
        path = self._full_path(name)
        log.info("Creating directory %s", path)
        self.client.exec_command(sudo=True, cmd=f"mkdir -p {path}")

    def write_file(self, name, content="hello"):
        """Write *content* into a file (creates it if absent)."""
        path = self._full_path(name)
        log.info("Writing to %s", path)
        self.client.exec_command(sudo=True, cmd=f"echo '{content}' > {path}")

    def remove(self, name, recursive=False):
        """Remove a file or directory."""
        flag = "-rf" if recursive else "-f"
        path = self._full_path(name)
        log.info("Removing %s", path)
        self.client.exec_command(sudo=True, cmd=f"rm {flag} {path}")

    def create_symlink(self, target, link_name):
        """Create a symbolic link *link_name* -> *target* (both relative to mount)."""
        target_path = f"../{target}" if not target.startswith("/") else target
        link_path = self._full_path(link_name)
        log.info("Creating symlink %s -> %s", link_path, target_path)
        self.client.exec_command(sudo=True, cmd=f"ln -sfn {target_path} {link_path}")

    def create_hardlink(self, src, dest):
        """Create a hard link *dest* pointing to *src*."""
        src_path = self._full_path(src)
        dest_path = self._full_path(dest)
        log.info("Creating hard link %s -> %s", dest_path, src_path)
        self.client.exec_command(sudo=True, cmd=f"ln {src_path} {dest_path}")

    def rename(self, old_name, new_name):
        """Rename / move a file."""
        old_path = self._full_path(old_name)
        new_path = self._full_path(new_name)
        log.info("Renaming %s -> %s", old_path, new_path)
        self.client.exec_command(sudo=True, cmd=f"mv {old_path} {new_path}")

    def inode(self, name):
        """Return the inode number of *name*."""
        path = self._full_path(name)
        out, _ = self.client.exec_command(
            sudo=True, cmd=f"ls -li {path} | awk '{{print $1}}'"
        )
        return out.strip()

    # -- ACL operations -------------------------------------------------------

    def get_acl(self, name):
        """Run nfs4_getfacl and return the output lines as a list."""
        path = self._full_path(name)
        log.info("Getting ACL for %s", path)
        out, _ = self.client.exec_command(sudo=True, cmd=f"nfs4_getfacl {path}")
        lines = [l.strip() for l in out.strip().splitlines() if l.strip()]
        log.info("ACL entries for %s: %s", path, lines)
        return lines

    def set_acl(self, name, ace_spec):
        """Full-replace ACL using ``nfs4_setfacl -s``."""
        path = self._full_path(name)
        log.info("Setting ACL on %s: %s", path, ace_spec)
        self.client.exec_command(sudo=True, cmd=f"nfs4_setfacl -s '{ace_spec}' {path}")

    def add_acl(self, name, ace_spec):
        """Incrementally add an ACE using ``nfs4_setfacl -a``."""
        path = self._full_path(name)
        log.info("Adding ACE to %s: %s", path, ace_spec)
        self.client.exec_command(sudo=True, cmd=f"nfs4_setfacl -a '{ace_spec}' {path}")

    def set_acl_from_file(self, name, spec_file_path):
        """Apply ACL from a spec file using ``nfs4_setfacl -S``."""
        path = self._full_path(name)
        log.info("Applying ACL from spec file %s to %s", spec_file_path, path)
        self.client.exec_command(
            sudo=True, cmd=f"nfs4_setfacl -S {spec_file_path} {path}"
        )

    def set_acl_from_file_expect_fail(self, name, spec_file_path):
        """Apply ACL from a spec file, expecting failure. Returns (rc, out, err)."""
        path = self._full_path(name)
        log.info(
            "Applying ACL from spec file %s to %s (expecting failure)",
            spec_file_path,
            path,
        )
        out, err = self.client.exec_command(
            sudo=True,
            cmd=f"nfs4_setfacl -S {spec_file_path} {path}",
            check_ec=False,
        )
        return out, err

    def set_acl_recursive(self, name, ace_spec, follow_symlinks=None):
        """
        Recursively set ACL using ``nfs4_setfacl -R``.

        follow_symlinks: None  -> default behaviour
                         True  -> -L (follow symlinks)
                         False -> -P (skip symlinks)
        """
        path = self._full_path(name)
        flags = "-R"
        if follow_symlinks is True:
            flags += " -L"
        elif follow_symlinks is False:
            flags += " -P"
        log.info("Recursive set ACL on %s (flags=%s): %s", path, flags, ace_spec)
        self.client.exec_command(
            sudo=True, cmd=f"nfs4_setfacl {flags} -s '{ace_spec}' {path}"
        )

    def save_acl_to_file(self, name, dest_file):
        """Dump current ACL to a file via ``nfs4_getfacl name > dest``."""
        path = self._full_path(name)
        log.info("Saving ACL of %s to %s", path, dest_file)
        self.client.exec_command(sudo=True, cmd=f"nfs4_getfacl {path} > {dest_file}")

    # -- chmod helpers --------------------------------------------------------

    def chmod(self, name, mode):
        """Run chmod on a path."""
        path = self._full_path(name)
        log.info("chmod %s %s", mode, path)
        self.client.exec_command(sudo=True, cmd=f"chmod {mode} {path}")

    def get_mode(self, name):
        """Return the octal mode string (e.g. '0644') for a path."""
        path = self._full_path(name)
        out, _ = self.client.exec_command(sudo=True, cmd=f"stat -c '%a' {path}")
        return out.strip()

    # -- user / group management ----------------------------------------------

    @staticmethod
    def create_user(client, username, uid):
        """Create a local user with the given UID; no-op if exists."""
        log.info("Creating user %s (uid=%s) on %s", username, uid, client.hostname)
        client.exec_command(
            sudo=True,
            cmd=f"id -u {username} &>/dev/null || useradd -u {uid} {username}",
        )

    @staticmethod
    def create_group(client, groupname, gid):
        """Create a local group with the given GID; no-op if exists."""
        log.info("Creating group %s (gid=%s) on %s", groupname, gid, client.hostname)
        client.exec_command(
            sudo=True,
            cmd=f"getent group {groupname} &>/dev/null || groupadd -g {gid} {groupname}",
        )

    @staticmethod
    def add_user_to_group(client, username, groupname):
        """Add an existing user to a group."""
        log.info(
            "Adding user %s to group %s on %s", username, groupname, client.hostname
        )
        client.exec_command(sudo=True, cmd=f"usermod -aG {groupname} {username}")

    @staticmethod
    def delete_user(client, username):
        """Delete a user (ignore errors if missing)."""
        log.info("Deleting user %s on %s", username, client.hostname)
        client.exec_command(sudo=True, cmd=f"userdel -rf {username}", check_ec=False)

    @staticmethod
    def delete_group(client, groupname):
        """Delete a group (ignore errors if missing)."""
        log.info("Deleting group %s on %s", groupname, client.hostname)
        client.exec_command(sudo=True, cmd=f"groupdel {groupname}", check_ec=False)

    # -- command execution as a specific user ---------------------------------

    def run_as_user(self, username, cmd, check_ec=True):
        """Execute *cmd* as *username* via ``su - <user> -c '...'``."""
        log.info("Running as %s: %s", username, cmd)
        out, err = self.client.exec_command(
            sudo=True,
            cmd=f'su - {username} -c "{cmd}"',
            check_ec=check_ec,
        )
        return out, err

    # -- verification helpers -------------------------------------------------

    def _find_matching_ace(self, acl, expected_ace):
        """Return the first actual ACL entry that contains *expected_ace*, or None."""
        for entry in acl:
            if expected_ace in entry:
                return entry
        return None

    def verify_acl_contains(self, name, expected_ace):
        """Assert that the ACL of *name* contains *expected_ace*."""
        acl = self.get_acl(name)
        matched = self._find_matching_ace(acl, expected_ace)
        if matched:
            log.info(
                "Verified ACL of %s contains '%s' (matched entry: '%s')",
                name,
                expected_ace,
                matched,
            )
        else:
            log.error(
                "ACL of %s does NOT contain '%s'. Current ACL: %s",
                name,
                expected_ace,
                acl,
            )
        return matched is not None

    def verify_acl_not_contains(self, name, unexpected_ace):
        """Assert that the ACL of *name* does NOT contain *unexpected_ace*."""
        acl = self.get_acl(name)
        matched = self._find_matching_ace(acl, unexpected_ace)
        if not matched:
            log.info("Verified ACL of %s does not contain '%s'", name, unexpected_ace)
        else:
            log.error(
                "ACL of %s unexpectedly contains '%s' (matched entry: '%s'). "
                "Current ACL: %s",
                name,
                unexpected_ace,
                matched,
                acl,
            )
        return matched is None

    def verify_acl_exact(self, name, expected_aces):
        """
        Verify the ACL matches the *expected_aces* list exactly
        (order-insensitive comparison).
        """
        acl = self.get_acl(name)
        if set(acl) == set(expected_aces):
            log.info("ACL of %s matches expected entries exactly", name)
            return True
        log.error(
            "ACL mismatch for %s. Expected: %s, Got: %s", name, expected_aces, acl
        )
        return False

    def verify_ace_count(self, name, expected_count):
        """Verify total number of ACE entries."""
        acl = self.get_acl(name)
        actual = len(acl)
        if actual == expected_count:
            log.info("ACE count for %s is %d as expected", name, expected_count)
            return True
        log.error(
            "ACE count mismatch for %s. Expected: %d, Got: %d",
            name,
            expected_count,
            actual,
        )
        return False

    def verify_access(self, username, file_path, operation="read", expect_success=True):
        """
        Verify a user can or cannot perform an operation on a file.

        operation: "read" | "write" | "append" | "delete"
        expect_success: True if the operation should succeed, False if it should fail.
        Returns True if result matches expectation.
        """
        full_path = self._full_path(file_path)
        if operation == "read":
            cmd = f"cat {full_path}"
        elif operation == "write":
            cmd = f"echo test_data > {full_path}"
        elif operation == "append":
            cmd = f"echo test_data >> {full_path}"
        elif operation == "delete":
            cmd = f"rm {full_path}"
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        out, err = self.run_as_user(username, cmd, check_ec=False)
        # If err contains "Permission denied" or similar, the operation failed
        operation_succeeded = not (
            err
            and (
                "denied" in err.lower()
                or "permission" in err.lower()
                or "not permitted" in err.lower()
                or "cannot" in err.lower()
                or "no such" in err.lower()
            )
        )
        if expect_success and operation_succeeded:
            log.info(
                "User %s successfully performed '%s' on %s (as expected)",
                username,
                operation,
                file_path,
            )
            return True
        elif not expect_success and not operation_succeeded:
            log.info(
                "User %s was denied '%s' on %s (as expected)",
                username,
                operation,
                file_path,
            )
            return True
        else:
            log.error(
                "Unexpected result for user %s operation '%s' on %s. "
                "Expected success=%s. stdout='%s' stderr='%s'",
                username,
                operation,
                file_path,
                expect_success,
                out,
                err,
            )
            return False

    # -- NFS service helpers --------------------------------------------------

    @staticmethod
    def restart_nfs_service(client, nfs_name):
        """Restart the NFS-Ganesha service via ceph orch."""
        log.info("Restarting NFS service %s", nfs_name)
        client.exec_command(
            sudo=True,
            cmd=f"ceph orch restart nfs.{nfs_name}",
        )
        sleep(15)
        log.info("NFS service %s restarted, waited 15s for stabilisation", nfs_name)

    @staticmethod
    def remount_export(
        client,
        mount_point,
        nfs_server,
        export,
        version="4.1",
        port="2049",
        mount_type="nfs",
    ):
        """
        Unmount and remount an NFS export.

        mount_type: "nfs" (standard NFS), "kernel" (kernel NFS), "fuse" (ceph-fuse)
        """
        log.info(
            "Remounting %s on %s (mount_type=%s)", export, client.hostname, mount_type
        )
        client.exec_command(sudo=True, cmd=f"umount -f {mount_point}", check_ec=False)
        sleep(3)

        _, err = client.exec_command(
            sudo=True,
            cmd=f"mountpoint -q {mount_point}",
            check_ec=False,
        )

        if err:
            log.warning(
                "%s is still mounted after umount -f, retrying with lazy umount",
                mount_point,
            )
            client.exec_command(
                sudo=True,
                cmd=f"umount -l {mount_point}",
                check_ec=False,
            )
            sleep(3)
        log.info("Confirmed %s is unmounted on %s", mount_point, client.hostname)

        if mount_type == "fuse":
            from cli.utilities.filesys import FuseMount

            FuseMount(client).mount(
                client_hostname=client.hostname, mount_point=mount_point
            )
        else:
            client.exec_command(
                sudo=True,
                cmd=(
                    f"mount -t nfs -o vers={version},port={port} "
                    f"{nfs_server}:{export} {mount_point}"
                ),
            )
        client.exec_command(sudo=True, cmd=f"chown cephuser:cephuser {mount_point}")
        log.info("Remount of %s completed on %s", export, client.hostname)

    # -- logging helpers ------------------------------------------------------

    @staticmethod
    def log_test_start(test_name):
        """Print a prominent banner marking the start of a sub-test."""
        banner = "\n" + "#" * 70 + "\n" + f"###  TEST START : {test_name}\n" + "#" * 70
        log.info(banner)

    @staticmethod
    def log_test_end(test_name, passed):
        """Print a prominent banner marking the end of a sub-test."""
        status = "PASSED" if passed else "FAILED"
        banner = (
            "\n"
            + "-" * 70
            + "\n"
            + f"###  TEST END   : {test_name} -> {status}\n"
            + "-" * 70
        )
        if passed:
            log.info(banner)
        else:
            log.error(banner)

    # -- bulk ACL generation --------------------------------------------------

    def generate_acl_file(self, dest_path, uid_start, uid_end, permission="r"):
        """
        Generate an ACL spec file with one ACE per UID in [uid_start, uid_end].
        """
        log.info(
            "Generating ACL spec file %s for UIDs %d-%d",
            dest_path,
            uid_start,
            uid_end,
        )
        cmd = (
            f"seq {uid_start} {uid_end} | "
            f'awk \'{{print "A::"$1":{permission}"}}\' > {dest_path}'
        )
        self.client.exec_command(sudo=True, cmd=cmd)

    def timed_getfacl(self, name):
        """Run ``time nfs4_getfacl`` and return (acl_output, real_time_seconds)."""
        path = self._full_path(name)
        log.info("Timed nfs4_getfacl on %s", path)
        out, err = self.client.exec_command(
            sudo=True,
            cmd=f"{{ time nfs4_getfacl {path} ; }} 2>&1",
        )
        time_sec = None
        for line in out.splitlines():
            if line.startswith("real"):
                parts = line.split()
                if len(parts) >= 2:
                    t = parts[1]
                    if "m" in t:
                        mins, secs = t.split("m")
                        secs = secs.rstrip("s")
                        time_sec = float(mins) * 60 + float(secs)
                    else:
                        time_sec = float(t.rstrip("s"))
        log.info("nfs4_getfacl completed in %s seconds", time_sec)
        return out, time_sec

    # -- cleanup helper -------------------------------------------------------

    def cleanup_users_groups(self, users=None, groups=None):
        """Remove test users and groups created during ACL tests."""
        for user in users or []:
            self.delete_user(self.client, user)
        for group in groups or []:
            self.delete_group(self.client, group)

    def cleanup_test_files(self, *names):
        """Remove test files/dirs under the mount point."""
        for name in names:
            self.client.exec_command(
                sudo=True,
                cmd=f"rm -rf {self._full_path(name)}",
                check_ec=False,
            )
