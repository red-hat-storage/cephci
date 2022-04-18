class CephCLI:
    def __init__(self, nodes) -> None:
        self.ceph_client = None

        # Identifying Client node
        for node in nodes:
            if node.role == "client":
                self.ceph_client = node
                break

    def exec_cmd(self, **kw):
        """
        Executes command in client node
        Args:
            node: client object to exec cmd
            commands: list of commands to be executed
        Returns:
            out: output after execution of command
            err: error after execution of command
        """
        node = kw.pop("node", self.ceph_client)
        out, err = node.exec_command(**kw)
        return out.read().decode(), err.read().decode()
