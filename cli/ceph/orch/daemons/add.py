from cli import Cli


class Add(Cli):
    def __init__(self, nodes, base_cmd):
        super(Add, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} add"

    def osd(self, hostname, device):
        """
        Add OSD device on the node
        Args:
            hostname (str) : hostname for which OSD to be created
            osd_device : Disk path
        """
        cmd = f"{self.base_cmd} osd {hostname}:{device}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
