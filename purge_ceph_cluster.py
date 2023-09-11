"""Purge Ceph Cluster.

Usage:
    purge_ceph_cluster.py [--installer <IP>] [--username <USERNAME>] [--password <PASSWORD>]

Options:
    -h --help       Show this help message and exit.
    --installer <IP>     Installer node IP.
    --username <USERNAME>   SSH username.
    --password <PASSWORD>   SSH password.

Assumptions :
    This will assume all the ceph nodes will have same root username password as provided for installer node
"""

import json

import paramiko
from docopt import docopt


def execute_ssh_command(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    return stdout.read().decode("utf-8")


def get_fsid(ssh_client):
    fsid_command = "cephadm shell -- ceph fsid -f json"
    fsid_output = execute_ssh_command(ssh_client, fsid_command)
    fsid_json = json.loads(fsid_output)
    return fsid_json["fsid"]


def get_node_ips(ssh_client):
    node_ips_command = "cephadm shell -- ceph orch host ls -f json"
    node_ips_output = execute_ssh_command(ssh_client, node_ips_command)
    node_ips_json = json.loads(node_ips_output)
    return [node["addr"] for node in node_ips_json]


def main():
    args = docopt(__doc__)

    installer_node_ip = args["--installer"]
    username = args["--username"]
    password = args["--password"]

    ssh_installer = paramiko.SSHClient()
    ssh_installer.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_installer.connect(installer_node_ip, username=username, password=password)

    fsid = get_fsid(ssh_installer)

    node_ips = get_node_ips(ssh_installer)
    node_ips.remove(installer_node_ip)
    print("Cluster Node IPs list:", node_ips)
    for node_ip in node_ips:
        ssh_node = paramiko.SSHClient()
        ssh_node.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_node.connect(node_ip, username=username, password=password)

        command = f"cephadm rm-cluster --force --zap-osds --fsid {fsid}"
        output = execute_ssh_command(ssh_node, command)
        print(f"Command output for {node_ip}:", output)

        print("Removing the cephadm logs")
        command = "rm -rf /var/log/ceph/*"
        output = execute_ssh_command(ssh_node, command)
        print(f"Command output for {node_ip}:", output)

        print("Removing the cephadm")
        command = "dnf remove -y cephadm"
        output = execute_ssh_command(ssh_node, command)
        print(f"Command output for {node_ip}:", output)

        ssh_node.close()

    command_on_installer = f"cephadm rm-cluster --force --zap-osds --fsid {fsid}"
    output_on_installer = execute_ssh_command(ssh_installer, command_on_installer)
    print(
        f"Command output on installer node ({installer_node_ip}):", output_on_installer
    )
    print("Removing the cephadm on Installer Node")
    command = "dnf remove -y cephadm"
    output = execute_ssh_command(ssh_installer, command)
    print(f"Command output for {node_ip}:", output)
    ssh_installer.close()


if __name__ == "__main__":
    main()
