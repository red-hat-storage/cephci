"""Cephadm ansible module to run playbooks

playbooks supported,
- cephadm-preflight.yaml
- cephadm-purge-cluster.yaml
- cephadm-clients.yaml
"""
from ceph.ceph_admin.common import config_dict_to_string
from utility.log import Log

LOG = Log(__name__)

ANSIBLE_RPM = "ansible-2.9-for-rhel-8-x86_64-rpms"


class CephadmAnsibleError(Exception):
    pass


class CephadmAnsible:
    """Module to access cephadm ansible playbooks"""

    def __init__(self, cluster):
        """Initialize cephadm-ansible repository.

        Args:
            cluster: Ceph cluster object
        """
        self.cluster = cluster
        self.admin = self.cluster.get_ceph_object("installer")
        self.rpm = "cephadm-ansible"
        self.exec_path = f"/usr/share/{self.rpm}"
        self.inventory = f"{self.exec_path}/hosts"
        self.base_cmd = (
            f"cd {self.exec_path}; ansible-playbook -vvvv -i {self.inventory}"
        )
        self.install_cephadm_ansible()
        self._generate_inventory()

    def install_cephadm_ansible(self):
        """Enable ansible rpm repos and install cephadm-ansible."""
        if float(self.cluster._Ceph__rhcs_version) >= 6.0:
            self.admin.exec_command(
                cmd="yum install ansible-core -y --nogpgcheck",
                sudo=True,
            )
        else:
            self.admin.exec_command(
                cmd=f"subscription-manager repos --enable={ANSIBLE_RPM}",
                sudo=True,
            )
        self.admin.exec_command(
            cmd=f"yum install {self.rpm} -y --nogpgcheck",
            sudo=True,
            long_running=True,
        )
        self.admin.exec_command(cmd=f"rpm -qa | grep {self.rpm}")

    def _generate_inventory(self):
        """Create cephadm-ansible inventory."""
        groups = ["_admin", "client"]
        admins = []
        clients = []
        others = []

        for host in self.cluster.get_nodes():
            _roles = host.role.role_list
            hostname = host.shortname
            if not [i for i in groups if i in _roles]:
                others.append(hostname)
            else:
                if "_admin" in _roles:
                    admins.append(hostname)
                if "client" in _roles:
                    clients.append(hostname)

        if not admins:
            raise CephadmAnsibleError("Admin(_admin) nodes not found...")

        def entries(lst, group_name=None):
            lst.insert(0, f"\n[{group_name}]" if group_name else "")
            return "\n".join(lst)

        inventory = entries(others)
        inventory += entries(admins, "admin")
        inventory += entries(clients, "clients")

        inv_file = self.admin.remote_file(
            sudo=True, file_name=self.inventory, file_mode="w"
        )
        inv_file.write(inventory)
        inv_file.flush()

    def execute_playbook(self, playbook, extra_vars=None, extra_args=None):
        """Method to execute cephadm-ansible playbooks.

        Args:
            playbook: cephadm-ansible playbook file name
            extra_vars: extra ansible CLI variables (ex., -e 'key=value')
            extra_args: extra ansible CLI arguments (ex., --limit osds)
        """
        LOG.info(f"Running playbook {playbook}.....")
        cmd = f"{self.base_cmd} {playbook}"
        if extra_vars:
            for k, v in extra_vars.items():
                cmd += f" -e '{k}={v}'"
        if extra_args:
            cmd += config_dict_to_string(extra_args)

        rc = self.admin.exec_command(
            cmd=cmd,
            long_running=True,
        )

        if rc != 0:
            raise CephadmAnsibleError("Playbook {playbook} failed....")
