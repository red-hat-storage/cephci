from json import loads

from ceph.nvmegw_cli import NVMeGWCLI
from cli.utilities.utils import exec_command_on_container, get_running_containers
from utility.systemctl import SystemCtl


class NVMeGateway(NVMeGWCLI):
    def __init__(self, node, mtls=False):
        """NVMe Gateway Class"""
        self.node = node
        super(NVMeGateway, self).__init__(node, mtls=mtls)
        self._mtls = mtls
        self._ana_group = self.fetch_gateway()
        self._ana_group_id = self.ana_group["load_balancing_group"]
        self._daemon_name = self.ana_group["name"].split(".", 1)[1]
        self.systemctl = SystemCtl(node)

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value
        self.setter("mtls", value)

    @property
    def ana_group_id(self):
        return self._ana_group_id

    @ana_group_id.setter
    def ana_group_id(self, value):
        self._ana_group_id = value

    @property
    def ana_group(self):
        return self._ana_group

    @ana_group.setter
    def ana_group(self, value):
        self._ana_group = value

    @property
    def daemon_name(self):
        return self._daemon_name

    @daemon_name.setter
    def daemon_name(self, value):
        self._daemon_name = value

    @property
    def system_unit_id(self):
        return self.systemctl.get_service_unit("*@nvmeof*")

    @property
    def hostname(self):
        return self.node.hostname

    def get_io_stats(self, subsystem, namespaces):
        pass

    def get_nvme_container(self):
        """Fetch NVMeof GW container id."""
        args = {"filter": "label=name=ceph-nvmeof", "quite": True}
        return get_running_containers(self.node, **args)

    def get_ana_states(self, subsystem, ana_groups):
        """Fetch ANA states from NVMeoF GW container.

        Args:
            subsystem: Subsytem name
            ana_groups: ANA Group IDs
        Returns:
            lists of optimized and inaccessible ana_group ids.
        """
        cmd = f"/usr/libexec/spdk/scripts/rpc.py  nvmf_subsystem_get_listeners {subsystem}"
        out, _ = exec_command_on_container(self.node, self.get_nvme_container(), cmd)
        out = loads(out)[0]["ana_states"]
        optimized = []
        inaccessible = []
        for ana_group in out:
            ana_group_id = ana_group["ana_group"]
            ana_group_state = ana_group["ana_state"]
            if ana_group_id in ana_groups:
                if ana_group_state == "optimized":
                    optimized.append(ana_group_id)
                elif ana_group_id == "inaccessible":
                    inaccessible.append(ana_group_id)

        return optimized, inaccessible
