from cli import Cli


class Lvm(Cli):
    """This module provides CLI interface to manage the ceph-volume plugin."""

    def __init__(self, nodes, base_cmd):
        super(Lvm, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} lvm"

    def prepare(self, data, bluestore=True, dmcrypt=False, **kw):
        """
        Adds metadata to logical volumes
        Args:
            data(str) : Block/raw device
            bluestore(boolean) : BlueStore is the default backend.
            Ceph permits changing the backend, which can be done by using the --bluestore flag
            dmcrypt(boolean) : For enabling encryption, the --dmcrypt flag is required
        """
        cmd = f"{self.base_cmd} prepare --data {data}"
        if bluestore:
            cmd += " --bluestore"
        if dmcrypt:
            cmd += " --dmcrypt"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return "\n".join(map(lambda x: x.strip(), out))
        return out

    def activate(self, osd_id, osd_fsid, bluestore=True, no_systemd=True, **kw):
        """
        Activate newly prepared OSD
        Args:
            bluestore(boolean) : BlueStore is the default backend.
            Ceph permits changing the backend, which can be done by using the --bluestore flag
            osd_id(str): OSD id (an integer unique to each OSD)
            osd_fsid(str): OSD FSID (unique identifier of an OSD)
            no_systemd(boolean): Skip creating and enabling systemd
            units and starting OSD services
        """
        cmd = f"{self.base_cmd} activate {osd_id} {osd_fsid}"
        if bluestore:
            cmd += " --bluestore"
        if no_systemd:
            cmd += " --no-systemd"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return "\n".join(map(lambda x: x.strip(), out))
        return out

    def deactivate(self, osd_id, osd_fsid, bluestore=True, **kw):
        """
        Deactivate prepared OSD
        Args:
            bluestore(boolean) : BlueStore is the default backend.
            Ceph permits changing the backend, which can be done by using the --bluestore flag
            osd_id(str): OSD id (an integer unique to each OSD)
            osd_fsid(str): OSD FSID (unique identifier of an OSD)
        """
        cmd = f"{self.base_cmd} deactivate {osd_id} {osd_fsid}"
        if bluestore:
            cmd += " --bluestore"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return "\n".join(map(lambda x: x.strip(), out))
        return out

    def create(self, data, bluestore=True, **kw):
        """
        The create subcommand calls the prepare subcommand,
        and then calls the activate subcommand.
        Args:
            data(str) : Block/raw device
            bluestore(boolean) : BlueStore is the default backend.
            Ceph permits changing the backend, which can be done by using the --bluestore flag
        Returns:
            boolean
        """
        cmd = f"{self.base_cmd} create --data {data}"
        if bluestore:
            cmd += " --bluestore"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return "\n".join(map(lambda x: x.strip(), out))
        return out

    def list(self, data, format=True, **kw):
        """
        List prepared OSD
        Args:
            data(str) : Block/raw device
            format(boolean): Format in json or pretty
        """
        cmd = f"{self.base_cmd} list {data}"
        if format:
            cmd += " --format json"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def zap(self, data, destroy=True, **kw):
        """
        List prepared OSD
        Args:
            data(str) : Block/raw device
            format(boolean): Format in json or pretty
        """
        cmd = f"{self.base_cmd} zap {data}"
        if destroy:
            cmd += " --destroy"
        out = self.execute(cmd=cmd, sudo=True, **kw)
        if isinstance(out, tuple):
            return "\n".join(map(lambda x: x.strip(), out))
        return out
