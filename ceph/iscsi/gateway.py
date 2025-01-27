"""iSCSI GWCLI Library Module.

This module consists of,
    - All gwcli operations.
    - Gateway node access.
    - gwcli execution methods.
    - iscsi container related methods.
"""

from ceph.iscsi.utils import get_iscsi_container
from cli.utilities.containers import Container


class GWCLI:
    path = "/"

    def __init__(self, node):
        self.node = node
        self.target = self.IQNTarget(self)
        self.disks = self.Disks(self)
        self.container = Container(self.node)
        self._iscsi_container = get_iscsi_container(self.node)
        self.container_id = self.iscsi_container["Names"][0]

    @property
    def iscsi_container(self):
        return self._iscsi_container

    @iscsi_container.setter
    def iscsi_container(self, value):
        self._iscsi_container = value
        if not value:
            self._iscsi_container = get_iscsi_container(self.node)

    @staticmethod
    def format_command_options(**kwargs):
        """Construct the iscsi path to define target and its entities.

        Args:
            kwargs: command options in Dict

        Returns:
            Str
        """
        cmd_opts = str()
        for k, v in kwargs.items():
            cmd_opts += f" {k}={v}"
        return cmd_opts

    def exec_gw_cli(self, action, path, **kwargs):
        """Execute iSCSI GWCLI command

        Args:
            action: operation to be performed on specific entity
            path: iSCSI command path to manage entities
            kwargs: command options
        """
        cmd = f"gwcli --debug {path} {action} {self.format_command_options(**kwargs)}"
        return self.container.exec(
            container=self.container_id,
            interactive=True,
            cmds=cmd,
        )

    def list(self, **kwargs):
        return self.exec_gw_cli("ls", self.path, **kwargs)

    def export(self):
        return self.exec_gw_cli("export", self.path)

    class Disks:
        path = "/disks"

        def __init__(self, parent):
            self.parent = parent

        def attach(self, **kwargs):
            return self.parent.exec_gw_cli("attach", self.path, **kwargs)

        def create(self, **kwargs):
            return self.parent.exec_gw_cli("create", self.path, **kwargs)

        def delete(self, image_id):
            return self.parent.exec_gw_cli(
                "delete", self.path, **{"image_id": image_id}
            )

        def detach(self, image_id):
            return self.parent.exec_gw_cli(
                "detach", self.path, **{"image_id": image_id}
            )

        def list(self, path=""):
            return self.parent.exec_gw_cli("list", self.path, **{"path": path})

    class IQNTarget:
        path = "/iscsi-targets"

        def __init__(self, parent):
            self.parent = parent
            self.gateways = self.Gateways(parent)
            self.hosts = self.Hosts(parent)
            self.disks = self.Disks(parent)

        def create(self, iqn):
            return self.parent.exec_gw_cli("create", self.path, **{"target_iqn": iqn})

        def delete(self, iqn):
            return self.parent.exec_gw_cli("delete", self.path, **{"target_iqn": iqn})

        def discovery_auth(self, **kwargs):
            return self.parent.exec_gw_cli("discovery_auth", self.path, **kwargs)

        class Disks:
            path = "/iscsi-targets/{IQN}/disks"

            def __init__(self, parent):
                self.parent = parent

            def add(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "add",
                    self.path.format(IQN=iqn),
                    **kwargs,
                )

            def delete(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "delete", self.path.format(IQN=iqn), **kwargs
                )

        class Gateways:
            path = "/iscsi-targets/{IQN}/gateways"

            def __init__(self, parent):
                self.parent = parent

            def create(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "create", self.path.format(IQN=iqn), **kwargs
                )

            def delete(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "delete", self.path.format(IQN=iqn), **kwargs
                )

        class Hosts:
            path = "/iscsi-targets/{IQN}/hosts"

            def __init__(self, parent):
                self.parent = parent
                self.client = self.Client(parent)

            def create(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "create", self.path.format(IQN=iqn), **kwargs
                )

            def delete(self, iqn, **kwargs):
                return self.parent.exec_gw_cli(
                    "delete", self.path.format(IQN=iqn), **kwargs
                )

            class Client:
                path = "/iscsi-targets/{IQN}/hosts/{Client_IQN}"

                def __init__(self, parent):
                    self.parent = parent

                def disk(self, iqn, client_iqn, action="add", **kwargs):
                    iscsi_cmd_path = self.path.format(IQN=iqn, Client_IQN=client_iqn)
                    return self.parent.exec_gw_cli(
                        f"disk {action}", iscsi_cmd_path, **kwargs
                    )


class Iscsi_Gateway:

    def __init__(self, node):
        """
        iSCSI Gateway node

        Args:
            node: CephNode object
        """
        self.node = node
        self.gwcli = GWCLI(self.node)
