from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, ConfigError

# Ceph ansible configs
CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
CEPHADM_INVENTORY_PATH = f"{CEPHADM_ANSIBLE_PATH}/hosts"
CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"
CEPHADM_CLIENTS_PLAYBOOK = "cephadm-clients.yml"
CEPHADM_INVENTORY_CLIENT_GROUP = "client"

# Ceph configs
CEPH_CONF_PATH = "/etc/ceph/ceph.conf"
CEPH_CLIENT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"


def configure_cephadm_ansible_inventory(nodes):
    """Generate cephadm ansible inventory file

    Args:
        nodes (List): Ceph node object list
    """
    # Check for node list
    nodes = nodes if isinstance(nodes, list) else [nodes]

    # Set installer node
    installer = None

    # Set host groups
    _admins, _clients, _installer, _others = (
        "[admin]",
        "[client]",
        "[installer]",
        "",
    )

    # Interate over nodes to group hosts
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

    # Check for admin node
    if _admins == "[admin]":
        raise ConfigError("Admin(_admin) nodes not found...")

    # Write to ansible hosts file
    cmd = f"echo -e '{_others}\n\n{_clients}\n\n{_admins}\n\n{_installer}\n' > {CEPHADM_INVENTORY_PATH}"
    installer.exec_command(sudo=True, cmd=cmd)


def exec_cephadm_preflight(node, build_type, repo=None):
    """Executes cephadm preflight playbook

    Args:
        node (CephInstallerNode): Ceph installer node object
        build_type (str): Build type RH or IBM
        repo (str): Ceph tools repo URL
    """
    # Set ceph origin with RN or IBM
    extra_vars = {"ceph_origin": build_type}

    # Set custom repository
    if repo:
        repo = repo if repo.endswith("repo") else f"{repo}/compose/Tools/x86_64/os"
        extra_vars = {
            "ceph_origin": "custom",
            "gpgcheck": "no",
            "custom_repo_url": repo,
        }

    # Execute preflight playbook
    Ansible(node).run_playbook(
        playbook=CEPHADM_PREFLIGHT_PLAYBOOK, extra_vars=extra_vars
    )


def exec_cephadm_clients(
    installer,
    keyring=CEPH_CLIENT_KEYRING_PATH,
    client_group=CEPHADM_INVENTORY_CLIENT_GROUP,
    conf=CEPH_CONF_PATH,
):
    """Executes cephadm clients playbook

    Args:
        installer (CephInstallerNode): Ceph installer node object
        keyring (str): Ceph keyring file
        client_group (str): Client group from inventory file
        conf (str): Ceph configuration file
    """
    # Get cluster FSID
    fsid = CephAdm(installer).ceph.fsid()
    if not fsid:
        raise CephadmOpsExecutionError("Failed to get cluster FSID")

    # Set extra vars
    extra_vars = {
        "fsid": fsid,
        "client_group": client_group,
        "keyring": keyring,
        "conf": conf,
    }

    # Execute preflight playbook
    Ansible(installer).run_playbook(
        playbook=CEPHADM_CLIENTS_PLAYBOOK, extra_vars=extra_vars
    )
