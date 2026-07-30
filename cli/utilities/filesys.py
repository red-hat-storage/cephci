from cli import Cli
from cli.ceph.auth.auth import Auth
from cli.exceptions import OperationFailedError


class MountFailedError(Exception):
    pass


class Mount(Cli):
    """This module provides CLI support for mount operations"""

    def __init__(self, nodes):
        super(Mount, self).__init__(nodes)
        self.base_cmd = "mount"

    def _ensure_nfs_utils(self):
        """Check if nfs-utils is installed on the client and install it if missing."""
        out = self.execute(cmd="rpm -qa nfs-utils", sudo=True)
        if isinstance(out, tuple):
            out = out[0]
        if not out.strip():
            self.execute(cmd="dnf install -y nfs-utils", sudo=True, timeout=600)

    def nfs(self, mount, version, port, server, export, **kwargs):
        """Perform an NFS mount on the client node.

        Installs nfs-utils if missing, creates the mount directory,
        and runs ``mount -t nfs`` with the assembled option string.

        The resulting command looks like::

            mount -t nfs -o vers=<version>[,port=<port>][,proto=<proto>]
                [,xprtsec=<xprtsec>][,sec=<sec>] <server>:<export> <mount>

        When ``sec`` is set (RPCSEC_GSS), ``port`` is omitted by default because
        many RHEL ``nfs-utils`` builds reject ``port=`` together with ``sec=krb5*``.

        Args:
            mount (str): Local mount-point path.
            version (str): NFS version string (e.g. "3", "4.0",
                "4.1", "4.2").
            port (str): NFS port to connect to.  When using RDMA
                this should be the RDMA listener port.
            server (str): NFS server hostname or IP address.
            export (str): NFS export pseudo-path.

        Keyword Args:
            proto (str): Transport protocol option appended to
                ``-o``.  Pass ``"rdma"`` for NFS-over-RDMA mounts.
            xprtsec (str): Transport-security option (e.g. ``"tls"``
                for RPC-with-TLS).
            sec (str): RPCSEC_GSS flavor (e.g. ``"krb5"``, ``"krb5i"``,
                ``"krb5p"``). Implies Kerberos authentication on the mount.
            fstype (str): Mount filesystem type (``"nfs"`` or ``"nfs4"``).
                Defaults to ``"nfs4"`` when ``sec`` is set and version is 4.x.
            use_nfsvers (bool): Use ``nfsvers=`` instead of ``vers=`` in ``-o``.
                Defaults to True when ``sec`` is set.
            include_port_with_sec (bool): If True, append ``port=`` even when
                ``sec`` is set (default False).

        Raises:
            MountFailedError: If the mount point does not appear in
                the ``mount`` table after the command completes.
        """
        self._ensure_nfs_utils()

        # Check if mount dir is present, else create
        out = self.execute(cmd=f"ls {mount}", sudo=True)
        if not out[0]:
            self.execute(cmd=f"mkdir {mount}", sudo=True)

        sec = kwargs.get("sec")
        fstype = kwargs.get("fstype")
        if fstype is None:
            fstype = "nfs4" if sec and str(version).startswith("4") else "nfs"
        use_nfsvers = kwargs.get("use_nfsvers", bool(sec))
        include_port_with_sec = kwargs.get("include_port_with_sec", False)

        opts = []
        if sec and fstype == "nfs4":
            # RHEL Kerberos mounts: mount.nfs4 -o sec=krb5 (no vers/nfsvers in -o).
            opts.append(f"sec={sec}")
        else:
            ver_opt = "nfsvers" if use_nfsvers else "vers"
            opts.append(f"{ver_opt}={version}")
            if sec:
                opts.append(f"sec={sec}")
        if port and (not sec or include_port_with_sec):
            opts.append(f"port={port}")
        proto = kwargs.get("proto")
        if proto:
            opts.append(f"proto={proto}")
        xprtsec = kwargs.get("xprtsec")
        if xprtsec:
            opts.append(f"xprtsec={xprtsec}")

        cmd = f"{self.base_cmd} -t {fstype} -o {','.join(opts)}"
        cmd += f" {server}:{export} {mount}"

        self.execute(sudo=True, long_running=True, cmd=cmd)

        out = self.execute(sudo=True, cmd="mount")
        if isinstance(out, tuple):
            out = out[0]

        if not mount.rstrip("/") in out:
            raise MountFailedError(f"Nfs mount failed: {out}")


class Unmount(Cli):
    def __init__(self, nodes):
        super(Unmount, self).__init__(nodes)
        self.base_cmd = "umount"

    def unmount(self, mount, lazy=True):
        """
        Perform unmount of volume
        Args:
            mount (str): path to mount point
            lazy (bool): Perform a lazy unmount or not
        Returns:

        """
        cmd = f"{self.base_cmd} {mount}"
        if lazy:
            cmd += " -l"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out


class FuseMount(Cli):
    """Supports CephFS mounts using ceph-fuse."""

    def __init__(self, nodes):
        super().__init__(nodes)
        self.base_cmd = "ceph-fuse"
        self.auth_tool = Auth(nodes, base_cmd="ceph")

    def mount(self, mount_point, client_hostname, extra_params=None):
        """
        Mount CephFS using FUSE.

        Args:
            mount_point (str): Local path to mount CephFS.
            client_hostname (str): Ceph client ID (host shortname).
            extra_params (str): Optional ceph-fuse arguments.

        Returns:
            str: Mount command's STDOUT on success.

        Raises:
            FuseMountError: On authentication or mount failure.
        """
        # Ensure keyring exists
        try:
            self.auth_tool.get_or_create_client_keyring(client_hostname)
        except Exception as e:
            raise OperationFailedError(
                f"Failed to prepare auth for client.{client_hostname}: {e}"
            )

        # Ensure mount directory exists
        out = self.execute(cmd=f"ls {mount_point}", sudo=True)
        if not out[0]:
            self.execute(cmd=f"mkdir -p {mount_point}", sudo=True)

        # Run ceph-fuse command
        cmd = f"{self.base_cmd} -n client.{client_hostname} {mount_point}"
        if extra_params:
            cmd += f" {extra_params}"

        out = self.execute(sudo=True, long_running=True, cmd=cmd)
        stdout = out[0].strip() if isinstance(out, tuple) else out
        return stdout
