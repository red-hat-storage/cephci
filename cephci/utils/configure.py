from cli.cephadm.ansible import Ansible
from cli.exceptions import AnsiblePlaybookExecutionError
from cli.utilities.packages import Package
from utility.log import Log

log = Log(__name__)

CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"
ETC_HOSTS = "/etc/hosts"

SSH = "~/.ssh"
SSH_CONFIG = f"{SSH}/config"
SSH_ID_RSA_PUB = f"{SSH}/id_rsa.pub"
SSH_KNOWN_HOSTS = f"{SSH}/known_hosts"

SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
SSH_KEYSCAN = "ssh-keyscan {}"

CHMOD_CONFIG = f"chmod 600 {SSH_CONFIG}"
SSHPASS = "sshpass -p {}"


def setup_ssh_keys(installer, nodes):
    """Set up SSH keys

    Args:
        installer (Node): Cluster installer node object
        nodes (List): List of Node objects
    """
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
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
        )
        installer.exec_command(
            sudo=True,
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
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


def accept_eula(node):
    """
    Accepts the eula for IBM Storage Ceph License
    Args:
        node(Node): Node object
    """
    cmd = "cat" if accept_eula else "touch"
    cmd += " /usr/share/ibm-storage-ceph-license/accept"

    Package(node).install("ibm-storage-ceph-license", env_vars=accept_eula)
    return node.exec_command(sudo=True, cmd=cmd, check_ec=True)


def exec_cephadm_preflight(installer, build_type, ceph_repo=None):
    """Executes cephadm preflight playbook"""
    try:
        if build_type == "ibm":
            accept_eula(installer)

        extra_vars = {"ceph_origin": build_type}
        if ceph_repo:
            extra_vars = {
                "ceph_origin": "custom",
                "gpgcheck": "no",
                "custom_repo_url": ceph_repo,
            }
        Ansible(installer).run_playbook(
            playbook=CEPHADM_PREFLIGHT_PLAYBOOK,
            extra_vars=extra_vars,
        )
    except Exception as e:
        raise AnsiblePlaybookExecutionError(
            f"Failed to execute cephadm preflight playbook:\n {e}"
        )
