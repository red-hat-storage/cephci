"""NVME Initiator."""

from .nvme_cli import NVMeCLI


class Initiator(NVMeCLI):
    def __init__(self, node):
        super().__init__(node)
        self.node = node
        self.configure()

    def nqn(self):
        out, _ = self.node.exec_command(cmd="nvme show-hostnqn")
        return out.strip()

    def distro_version(self):
        out, _ = self.node.exec_command(
            sudo=True, cmd="cat /etc/os-release | grep VERSION_ID"
        )
        rhel_version = out.split("=")[1].strip().strip('"')
        return rhel_version
