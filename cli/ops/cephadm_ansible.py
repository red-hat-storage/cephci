import os

from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, ConfigError, ResourceNotFoundError
from cli.utilities.configs import get_registry_details
from cli.utilities.utils import copy_ceph_sshkey_to_host, get_node_by_id, get_node_ip
from utility.log import Log

log = Log(__name__)

# CephAdm ansible wrapper playbook path
CEPHADM_ANSIBLE_WRAPPER_PLAYBOOKS = "tests/cephadm/ansible_wrapper"

# Ceph ansible configs
CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
CEPHADM_INVENTORY_PATH = f"{CEPHADM_ANSIBLE_PATH}/hosts"
CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"
CEPHADM_CLIENTS_PLAYBOOK = "cephadm-clients.yml"
CEPHADM_INVENTORY_CLIENT_GROUP = "client"

# Ceph configs
CEPH_CONF_PATH = "/etc/ceph/ceph.conf"
CEPH_CLIENT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"


def autoload_registry_details(ibm_build=False):
    """Get registry details

    Args:
        ibm_build (bool): Tag for IBM build
    """
    # Get registry details
    registry_details = get_registry_details(ibm_build)

    # Update module arguments
    args = {}
    args["registry_url"] = registry_details.get("registry-url")
    args["registry_username"] = registry_details.get("registry-username")
    args["registry_password"] = registry_details.get("registry-password")

    return args


def put_cephadm_ansible_playbook(
    node, playbook, cephadm_ansible_path=CEPHADM_ANSIBLE_PATH
):
    """Put playbook to cephadm ansible location.
    Args:
        playbook (str): Playbook need to be copied to cephadm ansible path
        cephadm_ansible_path (str): Path to where the playbook has to be copied
    """
    # Create path with cephadm ansible playbook
    dst = "/".join((cephadm_ansible_path, os.path.basename(playbook)))

    # Upload playbook to cephadm ansible path
    node.upload_file(sudo=True, src=playbook, dst=dst)

    return dst


def exec_cephadm_ansible_playbook(installer, module, playbook, **args):
    """Execute cephadm ansible playbook

    Args:
        installer (CephInstallerNode): Ceph installer node object
        module (str): Cephadm ansible module
        playbook (str): Cephadm ansible playbook
        args (dict):
            <key> : <val>
    """
    # Get playbook path
    playbook = "/".join((CEPHADM_ANSIBLE_WRAPPER_PLAYBOOKS, module, playbook))

    # Copy playbook to installer node
    playbook = put_cephadm_ansible_playbook(installer, playbook)

    # Execute playbook
    Ansible(installer).run_playbook(playbook=playbook, extra_vars=args)

    return True


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


def exec_cephadm_bootstrap(installer, nodes, playbook, **args):
    """Executes cephadm_bootstrap playbook

    Args:
        installer (CephInstallerNode): Ceph installer node object
        nodes (list): List of CephNodeObject
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                docker (bool): Use docker flag
                image (str): Ceph image URL
                mon-ip (str): Monitor node id
                dashboard (bool): Setup dashboard flag
                monitoring (bool): Enable monitoring flag
                firewalld (bool): Enable firewalld flag
    """
    # Check for mon ip
    if args.get("mon_ip"):
        if not nodes:
            raise ResourceNotFoundError("Nodes are required to get mon id")

        args["mon_ip"] = get_node_ip(nodes, args.pop("mon_ip"))

    return exec_cephadm_ansible_playbook(
        installer, "cephadm_bootstrap", playbook, **args
    )


def exec_ceph_orch_host(installer, nodes, playbook, **args):
    """Executes cephadm_bootstrap playbook

    Args:
        installer (CephInstallerNode): Ceph installer node object
        nodes (list): List of CephNodeObject
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                name (str): Node id
                address (str): Node id
    """
    # Check for name and address
    if args.get("name") or args.get("address"):
        if not nodes:
            raise ResourceNotFoundError(
                "Nodes are required to get node name and address"
            )

        host = get_node_by_id(nodes, args.pop("name"))
        args["name"] = host.hostname
        args["address"] = get_node_ip(nodes, args.pop("address"))

        copy_ceph_sshkey_to_host(installer, host)

    return exec_cephadm_ansible_playbook(installer, "ceph_orch_host", playbook, **args)


def exec_ceph_orch_apply(installer, playbook, **args):
    """Executes playbook with ceph_orch_apply module

    Args:
        installer (CephInstallerNode): Ceph installer node object
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                labels (str): Node labels
    """
    return exec_cephadm_ansible_playbook(installer, "ceph_orch_apply", playbook, **args)


def exec_ceph_config(installer, playbook, **args):
    """Executes playbook with ceph_config module

    Args:
        installer (CephInstallerNode): Ceph installer node object
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                labels (str): Node labels
    """
    return exec_cephadm_ansible_playbook(installer, "ceph_config", playbook, **args)


def exec_ceph_orch_daemon(installer, playbook, **args):
    """Executes playbook with ceph_orch_daemon module

    Args:
        installer (CephInstallerNode): Ceph installer node object
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                labels (str): Node labels
    """
    # Execute cephadm ansible playbook
    return exec_cephadm_ansible_playbook(
        installer, "ceph_orch_daemon", playbook, **args
    )


def exec_cephadm_registry_login(installer, playbook, **args):
    """Executes playbook with cephadm_registry_login module

    Args:
        installer (CephInstallerNode): Ceph installer node object
        playbook (str): Cephadm ansible playbook
        args (dict):
            Supported Keys:
                labels (str): Node labels
    """
    # Execute cephadm ansible playbook
    return exec_cephadm_ansible_playbook(
        installer, "cephadm_registry_login", playbook, **args
    )
