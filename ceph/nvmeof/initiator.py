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
