import json
import tempfile

from cli.exceptions import OperationFailedError, ResourceNotFoundError
from cli.ops.cephadm_ansible import (
    configure_cephadm_ansible_inventory,
    exec_cephadm_clients,
    exec_cephadm_preflight,
)
from cli.utilities.packages import Package, Repos

from .configs import get_registry_details

ETC_HOSTS = "/etc/hosts"

SSH = "~/.ssh"
SSH_CONFIG = f"{SSH}/config"
SSH_ID_RSA_PUB = f"{SSH}/id_rsa.pub"
SSH_KNOWN_HOSTS = f"{SSH}/known_hosts"

SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
SSH_KEYSCAN = "ssh-keyscan {}"
SSHPASS = "sshpass -p {}"
CHMOD_CONFIG = f"chmod 600 {SSH_CONFIG}"


def generate_registry_json_config(node, ibm_build=False):
    """Create json with registry credential details

    Args:
        node (CephInstallerNode): Ceph installer node
        ibm_build (bool): IBM build flag
    """
    # Create temporory file path
    temp_file = tempfile.NamedTemporaryFile(suffix=".json")

    # Get credential details
    registry = get_registry_details(ibm_build)

    # Create temporary file and dump data
    with node.remote_file(sudo=True, file_name=temp_file.name, file_mode="w") as _f:
        _f.write(json.dumps(registry, indent=4))
        _f.flush()

    return temp_file.name


def generate_mgr_ssl_certificate(node, key, crt):
    """Construct dashboard key and certificate files for bootstrapping cluster

    Args:
        node (CephInstallerNode): Ceph installer node
        key (str): Path to generate ssl key
        crt (str): Path to generate ssl certificate
    """
    # Install openssl
    Package(node).install("openssl")

    # Generate key and cert using openssl in /home/cephuser
    out, _ = node.exec_command(
        sudo=True,
        cmd=f'openssl req -new -nodes -x509 \
            -subj "/O=IT/CN=ceph-mgr-dashboard" -days 3650 \
            -keyout {key} \
            -out {crt} -extensions v3_ca',
    )

    if out:
        raise OperationFailedError("Failed to generate dashboard certificates")

    return True


def setup_ssh_keys(installer, nodes):
    """Setup ssh keys

    Args:
        installer (CephInstallerNode): Cluster installer node object
        nodes (list|tuple): List of CephNode objects
    """
    # Set hosts and config
    hosts, config = (
        "",
        "Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n",
    )

    # Generate ssh configs
    for node in nodes:
        # Update config woth IP Addr, Hostname and User
        config += f"\nHost {node.ip_address}"
        config += f"\n\tHostname {node.hostname}"
        config += "\n\tUser root"

        # Update host details
        hosts += f"\n{node.ip_address}"
        hosts += f"\t{node.hostname}"
        hosts += f"\t{node.shortname}"

        # Remove existing ssh directory from non-root user
        node.exec_command(cmd=f"rm -rf {SSH}", check_ec=False)
        node.exec_command(cmd=SSH_KEYGEN)

        # Remove existing ssh directory from root user
        node.exec_command(sudo=True, cmd=f"rm -rf {SSH}", check_ec=False)
        node.exec_command(sudo=True, cmd=SSH_KEYGEN)

    # Create hosts file on installer node
    installer.exec_command(sudo=True, cmd=f"echo -e '{hosts}' >> {ETC_HOSTS}")

    # Create ssh config on installer node
    installer.exec_command(
        cmd=f"touch {SSH_CONFIG} && echo -e '{config}' > {SSH_CONFIG}"
    )

    # Set permissions to ssh config
    installer.exec_command(cmd=CHMOD_CONFIG)

    # Install sshpass
    Package(installer).install("sshpass")

    # Setup passwordless ssh
    for node in nodes:
        # Add hosts to known_hosts for non-root user
        installer.exec_command(
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
        )

        # Add hosts to known_hosts for root user
        installer.exec_command(
            sudo=True,
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
        )

        # Copy ssh key to non-root user
        installer.exec_command(
            cmd="{} {}".format(
                SSHPASS.format(node.password),
                SSH_COPYID.format(SSH_ID_RSA_PUB, node.username, node.hostname),
            ),
        )

        # Copy ssh key to root user
        installer.exec_command(
            cmd="{} {}".format(
                SSHPASS.format(node.root_passwd),
                SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
            ),
        )

        # Copy ssh key to host root user
        installer.exec_command(
            sudo=True,
            cmd="{} {}".format(
                SSHPASS.format(node.root_passwd),
                SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
            ),
        )


def setup_ibm_licence(node, accept_eula={"ACCEPT_EULA": "Y"}):
    """Accepts the eula for IBM Storage Ceph license

    Args:
        node(CephInstallerNode): Ceph installer node object
    """
    # Install IBM licence
    Package(node).install("ibm-storage-ceph-license", env_vars=accept_eula)

    # Accept IBM licence
    cmd = "cat" if accept_eula else "touch"
    cmd += " /usr/share/ibm-storage-ceph-license/accept"

    return node.exec_command(sudo=True, cmd=cmd, check_ec=True)


def enable_ceph_tools_repo(node, ceph_version, platform):
    """Enable ceph tools repository

    Args:
        node (CephInstallerNode): Ceph installer node object
        ceph_version (str): Ceph major.minor version
        platform (str): OS version
    """
    # Set ceph tools repo
    cdn_repo = {}
    if ceph_version.startswith("7"):
        cdn_repo = {
            "rhel-9": "rhceph-7-tools-for-rhel-9-x86_64-rpms",
        }
    elif ceph_version.startswith("6"):
        cdn_repo = {
            "rhel-9": "rhceph-6-tools-for-rhel-9-x86_64-rpms",
        }
    elif ceph_version.startswith("5"):
        cdn_repo = {
            "rhel-8": "rhceph-5-tools-for-rhel-8-x86_64-rpms",
            "rhel-9": "rhceph-5-tools-for-rhel-9-x86_64-rpms",
        }
    else:
        raise Exception(f"Unsupported version {platform}")

    # Install cnd ceph repo
    Repos(node).enable(cdn_repo.get(platform))

    return True


def add_ceph_repo(node, repo):
    """Configure downstream repositories

    Args:
        node (CephInstallerNode): Ceph installer node object
        repo (str): Repo URL
    """
    # Update repo based on repo url
    repo = repo if repo.endswith("repo") else f"{repo}/compose/Tools/x86_64/os"

    # yum-config-manager add repo
    out = Package(node).add_repo(repo)
    if "Adding repo from" not in out:
        raise OperationFailedError(
            f"Failed to enable repo '{repo}' due to error -\n{out}"
        )

    return True


def install_cephadm_ansible(
    installer, ceph_version, platform, tools_repo, build_type, ibm_build
):
    """Configure CephAdm ansible

    Args:
        installer (CephInstallerNode): Ceph installer node object
        ceph_version (str): Ceph major.minor version
        platform (str): OS version
        tools_repo (str): Ceph tools repository repo url
        build_type (str): Build type
        ibm_build (bool): IBM build flag
    """
    # Check for build type
    if build_type in ("live", "cdn", "released"):
        enable_ceph_tools_repo(installer, ceph_version, platform)

    # Check if tools repo is provided
    elif tools_repo:
        add_ceph_repo(installer, tools_repo)

    # Raise exception for insufficient resources
    else:
        raise ResourceNotFoundError(
            "Ceph tools repo is required for installing CephAdm"
        )

    # Check for build type
    nogpgcheck = False if build_type in ("live", "cdn", "released") else True

    # Install license for IBM builds
    if ibm_build:
        setup_ibm_licence(installer)

    # Install cephadm ansible package
    Package(installer).install("cephadm-ansible", nogpgcheck=nogpgcheck)


def install_cephadm(node, ceph_version, platform, tools_repo, build_type, ibm_build):
    """Configure CephAdm utility

    Args:
        node (CephInstallerNode): Ceph installer node object
        ceph_version (str): Ceph major.minor version
        platform (str): OS version
        tools_repo (str): Ceph tools repository repo url
        build_type (str): Build type
        ibm_build (bool): IBM build flag
    """
    # Check for build type
    if build_type in ("live", "cdn", "released"):
        enable_ceph_tools_repo(node, ceph_version, platform)

    # Check if tools repo is provided
    elif tools_repo:
        add_ceph_repo(node, tools_repo)

    # Raise exception for insufficient resources
    else:
        raise ResourceNotFoundError(
            "Ceph tools repo is required for installing CephAdm"
        )

    # Check for build type
    nogpgcheck = False if build_type in ("live", "cdn", "released") else True

    # Install license for IBM builds
    if ibm_build:
        setup_ibm_licence(node)

    # Install cephadm package
    Package(node).install("cephadm", nogpgcheck=nogpgcheck)

    return True


def setup_installer_node(
    installer, nodes, rhbuild, tools_repo, build_type, ibm_build, ansible_preflight
):
    """Configure installer node

    Args:
        installer (CephInstallerNode): Ceph installer node object
        nodes (List): Ceph node object list
        rhbuild (str): Ceph build details
        tools_repo (str): Ceph tools repository repo url
        build_type (str): Build type
        ibm_build (bool): IBM build flag
        ansible_preflight (bool): Cephadm ansible preflight playbook
    """
    # Get cephversion and platform
    ceph_version, platform = rhbuild.split("-", 1)

    # Setup ssh keys
    setup_ssh_keys(installer, nodes)

    # Check for ansible preflight
    if not ansible_preflight:
        # Install cephadm package
        install_cephadm(
            installer, ceph_version, platform, tools_repo, build_type, ibm_build
        )

        return

    # Install cephadm ansible preflight
    install_cephadm_ansible(
        installer, ceph_version, platform, tools_repo, build_type, ibm_build
    )

    # Configure cephadm ansible inventory hosts
    configure_cephadm_ansible_inventory(nodes)

    # Execute cephadm ansible preflight playbook
    exec_cephadm_preflight(installer, build_type, tools_repo)


def setup_client_node(installer, ansible_clients):
    """Setup client node

    Args:
        installer (CephInstallerNode): Ceph installer node object
        ansible_clients (bool): Cephadm ansible playbook
    """
    # Check for client playbook
    if ansible_clients:
        # Configure cephadm ansible install clients
        exec_cephadm_clients(installer)
