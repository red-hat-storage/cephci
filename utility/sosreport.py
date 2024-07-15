"""Standard script to collect all the logs from ceph cluster through and place it in directory provided

Through installer node get all other nodes in the cluster, generate sosreport for all the nodes obtained.
then upload all the collected logs to magna/directory(run_dir) provided

  Typical usage example:

  python sosreport.py --ip x.x.x.x --username abc --password abcd --directory /tmp/cephci-run-ysfyu
  python sosreport.py -h
"""

import os
import re
import sys

import paramiko
from docopt import docopt

doc = """
Standard script to collect all the logs from ceph cluster

    Usage:
        sosreport.py --ip <str> --username <str> --password <str> --directory <str>
        sosreport.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        --ip <str>         IP address of Installer node.
        --username <str>   Username to be used to access the system other than root
        --password <str>   password of given username
        --directory <str>  directory/folder
"""


def generate_sosreport_in_node(
    nodeip: str, uname: str, pword: str, directory: str, results: list
) -> None:
    """Generate sosreport in the given node and copy report to directory provided

    Args:
       nodeip              host Ip address
       uname               Username for accessing host
       pword               password for accessing host through given user
       directory           directory to store all the logs
       results             host IP address for which this operation are failed

    Returns:
        None
    """
    print(f"Connecting {nodeip} to generate sosreport")
    try:
        ssh_d = paramiko.SSHClient()
        ssh_d.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_d.connect(nodeip, username=uname, password=pword)
        ssh_d.exec_command("sudo yum -y install sos")
        stdin, stdout, stderr = ssh_d.exec_command(
            "sudo sosreport -a --all-logs -e ceph --batch"
        )
        rc = stdout.channel.recv_exit_status()
        sosreport = re.search(r"sosreport-.*.tar.xz", stdout.read().decode())
        if rc and not sosreport:
            print(f"Failed to generate sosreport {nodeip}")
            results.append(nodeip)
            return
        source_file = f"/var/tmp/{sosreport.group()}"
        ssh_d.exec_command(f"sudo chown {uname} {source_file}")
        directory_path = os.path.join(directory, "sosreports")
        dir_exist = os.path.exists(directory_path)
        if not dir_exist:
            os.makedirs(directory_path)
        ftp_client = ssh_d.open_sftp()
        ftp_client.get(f"{source_file}", f"{directory_path}/{sosreport.group()}")
        ftp_client.close()
        print(
            f"Successfully generated sosreport for node {nodeip} :{sosreport.group()}"
        )
        ssh_d.exec_command(f"sudo rm -rf {source_file}")
        ssh_d.close()
    except Exception:
        results.append(nodeip)


def run(installer_ip: str, uname: str, pword: str, directory: str) -> int:
    """Standard script to collect all the logs from ceph cluster

    Through installer node get all other nodes in the cluster, generate sosreport for all the nodes obtained.
    then upload all the collected logs to given directory

    Args:
       installer_ip   installer IP address
       uname          username to be used to access the system other than root
       pword          password for installer node
       directory      directory/folder name ex:

    Returns:
        0 on success or 1 for failures

    Raises:
        AssertionError: An error occurred if given IP is not of Installer node
    """
    results = []

    ssh_install = paramiko.SSHClient()
    ssh_install.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_install.connect(installer_ip, username=uname, password=pword)
    stdin, stdout, stderr = ssh_install.exec_command("hostname")
    if "installer" not in stdout.read().decode():
        raise AssertionError("Please provide installer node details")
    stdin, stdout, stderr = ssh_install.exec_command(
        "cut -f 1 /etc/hosts | cut -d ' ' -f 3"
    )
    nodes = stdout.read().decode().split("\n")

    print(f"Host that are obtained from given host: {nodes}")
    for nodeip in nodes:
        if nodeip:
            generate_sosreport_in_node(nodeip, uname, pword, directory, results)
    print(f"\n\nFailed to collect logs from nodes :{results}")
    return 1 if results else 0


if __name__ == "__main__":
    arguments = docopt(doc)
    installer_ip = arguments["--ip"]
    username = arguments["--username"]
    password = arguments["--password"]
    directory = arguments["--directory"]
    rc = run(installer_ip, username, password, directory)
    sys.exit(rc)
