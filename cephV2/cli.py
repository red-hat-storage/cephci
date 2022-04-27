class CephCLI:
    @staticmethod
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

        node_role = kw["node_role"]
        node = kw["node"].get_ceph_object(node_role)

        return node.exec_command(
            cmd=kw["cmd"], long_running=kw.get("long_running", False)
        )
