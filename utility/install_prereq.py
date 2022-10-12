from ceph.utils import setup_repos
from cli.utilities.packages import Package, SubscriptionManager
from cli.utilities.utils import get_custom_repo_url, os_major_version
from utility.log import Log
from utility.utils import fetch_build_artifacts

RHEL8_EPEL_RELESE_RPM = (
    "https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm"
)
RHCEPH5_TOOLS_REPO = "rhceph-5-tools-for-rhel-{}-x86_64-rpms"
RHEL8_ANSIBLE_REPO = "ansible-2.9-for-rhel-8-x86_64-rpms"

CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
CEPHADM_INVENTORY_PATH = f"{CEPHADM_ANSIBLE_PATH}/hosts"

log = Log(__name__)


class ConfigureCephadmAnsibleInventoryError(Exception):
    pass


class EnableToolsRepositoriesError(Exception):
    pass


class ConfigureCephadmAnsibleInventory:
    """Configures cephadm ansible inventory."""

    @staticmethod
    def run(nodes):
        nodes = nodes if isinstance(nodes, list) else [nodes]

        _admins, _clients, _others, _installer = "[admin]", "[client]", "", None
        for node in nodes:
            if node.role == "installer":
                _installer = node

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

        cmd = f"echo -e '{_others}\n\n{_clients}\n\n{_admins}\n' > {CEPHADM_INVENTORY_PATH}"
        _installer.exec_command(sudo=True, cmd=cmd)
        log.info(f"Generated inventory file at '{CEPHADM_INVENTORY_PATH}'.")


class EnableToolsRepositories:
    """Enables tools repositories."""

    @staticmethod
    def run(nodes):
        nodes = nodes if isinstance(nodes, list) else [nodes]
        for node in nodes:
            stdout, _ = os_major_version(node)
            repos = [RHCEPH5_TOOLS_REPO.format(stdout.strip())]
            if stdout.strip() == "8":
                repos.append(RHEL8_ANSIBLE_REPO)

            if SubscriptionManager(node).repos.enable(repos):
                raise EnableToolsRepositoriesError("Failed to enable tools repo.")

        log.info("Enabled tools repos on cluster nodes successfully.")


class ConfigureCephadmRepositories:
    """Configures Ceph and Cephadm related repositories."""

    @staticmethod
    def run(nodes, args, config):
        """Install and configure cephadm package.

        Args:
            args (dict): key/value pairs passed from the test case
            config (dict): key/value pairs passed from the cluster config
        """
        nodes = nodes if isinstance(nodes, list) else [nodes]
        pkg = Package(nodes)

        custom_repo = args.get("custom_repo")
        rhcs_version = args.get("rhcs-version")
        rhcs_release = args.get("release")

        build_type = config.get("build_type")
        rhbuild = config.get("rhbuild")
        base_url = config.get("base_url")
        cloud_type = config.get("cloud-type", "openstack")

        if rhcs_release and rhcs_version:
            _platform = "-".join(rhbuild.split("-")[1:])
            _details = fetch_build_artifacts(rhcs_release, rhcs_version, _platform)
            config["base_url"] = _details[0]

        if build_type == "upstream":
            pkg.add_repo(config.get("base_url"))
            pkg.install(RHEL8_EPEL_RELESE_RPM)
            pkg.clean()

            # Enabling cdn tool repo to workaround, due to unavailability of
            # ceph x86_64 RPM pkgs in upstream builds
            EnableToolsRepositories.run(nodes)
        elif build_type == "released" or custom_repo.lower() == "cdn":
            EnableToolsRepositories.run(nodes)
        elif custom_repo:
            base_url = get_custom_repo_url(repo=custom_repo)
            pkg.add_repo(base_url, nogpgcheck=True)
            pkg.clean()

            log.info(f"Added repo {base_url} on cluster nodes successfully.")
        else:
            repos = ["Tools"]
            for node in nodes:
                setup_repos(node, base_url=base_url, repos=repos, cloud_type=cloud_type)

        log.info("Configured cephadm repositories successfully")
        return True
