from cli.cephadm.ansible import Ansible
from cli.utilities.packages import Package, SubscriptionManager
from cli.utilities.utils import get_builds_by_rhbuild, get_custom_repo_url

RHEL8_ANSIBLE_REPO = "ansible-2.9-for-rhel-8-x86_64-rpms"

RHCS5_REPOS = {
    "rhel-8": "rhceph-5-tools-for-rhel-8-x86_64-rpms",
    "rhel-9": "rhceph-5-tools-for-rhel-9-x86_64-rpms",
}
RHCS6_REPOS = {"rhel-9": "rhceph-6-tools-for-rhel-9-x86_64-rpms"}

CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
CEPHADM_INVENTORY_PATH = f"{CEPHADM_ANSIBLE_PATH}/hosts"
CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"

ETC_HOSTS = "/etc/hosts"
CEPH_PUB_KEY = "/etc/ceph/ceph.pub"

SSH = "~/.ssh"
SSH_CONFIG = f"{SSH}/config"
SSH_ID_RSA_PUB = f"{SSH}/id_rsa.pub"
SSH_KNOWN_HOSTS = f"{SSH}/known_hosts"

SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
SSH_KEYSCAN = "ssh-keyscan {}"

CHMOD_CONFIG = f"chmod 600 {SSH_CONFIG}"
SSHPASS = "sshpass -p {}"


class ConfigureCephadmAnsibleInventoryError(Exception):
    pass


class EnableToolsRepositoriesError(Exception):
    pass


class ConfigureCephToolsRepositoriesError(Exception):
    pass


class ConfigureCephadmAnsibleNodeError(Exception):
    pass


class SetUpSSHKeys:
    """Set up SSH keys"""

    @staticmethod
    def run(installer, nodes):
        hosts, config = (
            "",
            "Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n",
        )
        for node in nodes:
            config += f"\nHost {node.ip_address}"
            config += f"\n\tHostname {node.hostname}"
            config += "\n\tUser root"

            hosts += f"\n{node.ip_address}"
            hosts += f"\t{node.hostname}"
            hosts += f"\t{node.shortname}"

            node.exec_command(cmd=f"rm -rf {SSH}", check_ec=False)
            node.exec_command(cmd=SSH_KEYGEN)

            node.exec_command(sudo=True, cmd=f"rm -rf {SSH}", check_ec=False)
            node.exec_command(sudo=True, cmd=SSH_KEYGEN)

        installer.exec_command(sudo=True, cmd=f"echo -e '{hosts}' >> {ETC_HOSTS}")
        installer.exec_command(
            cmd=f"touch {SSH_CONFIG} && echo -e '{config}' > {SSH_CONFIG}"
        )
        installer.exec_command(cmd=CHMOD_CONFIG)
        Package(installer).install("sshpass")

        for node in nodes:
            installer.exec_command(
                cmd="{} >> {}".format(
                    SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS
                ),
            )
            installer.exec_command(
                sudo=True,
                cmd="{} >> {}".format(
                    SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS
                ),
            )

            installer.exec_command(
                cmd="{} {}".format(
                    SSHPASS.format(node.password),
                    SSH_COPYID.format(SSH_ID_RSA_PUB, node.username, node.hostname),
                ),
            )
            installer.exec_command(
                cmd="{} {}".format(
                    SSHPASS.format(node.root_passwd),
                    SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
                ),
            )
            installer.exec_command(
                sudo=True,
                cmd="{} {}".format(
                    SSHPASS.format(node.root_passwd),
                    SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
                ),
            )


class CopyCephSshKeyToHost:
    """Copy ceph.pub to hosts"""

    @staticmethod
    def run(installer, nodes, user="root", key=CEPH_PUB_KEY):
        if not isinstance(nodes, list):
            nodes = [nodes]

        for node in nodes:
            installer.exec_command(
                sudo=True, cmd=SSH_COPYID.format(key, user, node.hostname)
            )


class ExecutePreflightPlaybook:
    """Execute cephadm-preflight.yaml playbook"""

    @staticmethod
    def run(installer, base_url, cloud_type, build_type):
        base_url = get_custom_repo_url(base_url, cloud_type)
        extra_vars = {"ceph_origin": "rhcs"}
        if build_type not in ["ga", "ga-async"]:
            extra_vars = {
                "ceph_origin": "custom",
                "gpgcheck": "no",
                "custom_repo_url": base_url,
            }

        Ansible(installer).run_playbook(
            playbook=CEPHADM_PREFLIGHT_PLAYBOOK,
            extra_vars=extra_vars,
        )


class ConfigureCephadmAnsibleNode:
    """Install cephadm-ansible and configure node"""

    @staticmethod
    def run(installer, nodes, build_type, base_url, rhbuild, cloud_type):
        base_url = get_custom_repo_url(base_url, cloud_type)

        ConfigureCephToolsRepositories().run(installer, build_type, base_url, rhbuild)

        _, rhel = get_builds_by_rhbuild(rhbuild)
        if rhel == "rhel-8" and SubscriptionManager(installer).repos.enable(
            RHEL8_ANSIBLE_REPO
        ):
            raise ConfigureCephadmAnsibleNodeError(
                f"Failed to enable ansible repo '{RHEL8_ANSIBLE_REPO}' on node '{installer}'"
            )

        Package(installer).install("cephadm-ansible", nogpgcheck=True)
        ConfigureCephadmAnsibleInventory().run(nodes)


class ConfigureCephadmAnsibleInventory:
    """Configures cephadm ansible inventory."""

    @staticmethod
    def run(nodes):
        nodes = nodes if isinstance(nodes, list) else [nodes]

        installer = None
        _admins, _clients, _installer, _others = (
            "[admin]",
            "[client]",
            "[installer]",
            "",
        )
        for node in nodes:
            if node.role == "installer":
                installer = node
                _installer += f"\n{node.shortname}"

            _roles = node.role.role_list
            if "_admin" in _roles:
                _admins += f"\n{node.shortname}"
            if "client" in _roles:
                _clients += f"\n{node.shortname}"
            if "_admin" not in _roles and "client" not in _roles:
                _others += f"\n{node.shortname}"

        if _admins == "[admin]":
            raise ConfigureCephadmAnsibleInventoryError(
                "Admin(_admin) nodes not found..."
            )

        cmd = f"echo -e '{_others}\n\n{_clients}\n\n{_admins}\n\n{_installer}\n' > {CEPHADM_INVENTORY_PATH}"
        installer.exec_command(sudo=True, cmd=cmd)


class ConfigureCephToolsRepositories:
    """Configures Ceph and Cephadm related repositories."""

    @staticmethod
    def run(nodes, build_type, base_url, rhbuild):
        """Install and configure cephadm package.

        Args:
            nodes (CephNode | List ): CephNode or list of CephNode object
            build_type (str): Build type
            base_url (str): Custom repo build url
            rhbuild (str): RHCS build version with RHEL
        """
        if build_type in ["latest", "tier-0", "tier-1", "tier-2", "rc", "upstream"]:
            Package(nodes).add_repo(base_url)

        elif build_type in ["ga", "ga-async"]:
            rhcs, rhel = get_builds_by_rhbuild(rhbuild)
            repos = RHCS5_REPOS
            if int(float(rhcs)) == 6:
                repos = RHCS6_REPOS

            repos = repos.get(rhel)
            if not repos:
                raise EnableToolsRepositoriesError(
                    f"'{rhel}' not supported for RHCS {rhcs}"
                )

            if not SubscriptionManager(nodes).repos.enable(repos):
                raise EnableToolsRepositoriesError(
                    f"Failed to enable tools repo '{repos}' on node '{nodes}'"
                )

        else:
            raise ConfigureCephToolsRepositoriesError("Required repos are not provided")

        return True
