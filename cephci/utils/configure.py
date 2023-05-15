from cli.utilities.packages import Package

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
