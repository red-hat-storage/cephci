"""Standard script to collect all the logs from ceph cluster through installer node

Through installer node get all other nodes in the cluster, generate sosreport for all the nodes obtained.
Run shell script on installer node, then upload all the collected logs to magna

  Typical usage example:

  python collect_logs.py --ip x.x.x.x --username abc --password abcd
  python collect_logs.py -h
"""
import json
import os
import re
import sys

from docopt import docopt

from ceph.ceph import SSHConnectionManager
from ceph.parallel import parallel
from utility.utils import generate_unique_id

doc = """
Standard script to collect all the logs from ceph cluster through installer node

    Usage:
        collect_logs.py --ip <str> --username <str> --password <str> [--directory <str>]
        collect_logs.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        --ip <str>         IP address of Installer node.
        --username <str>   Username to be used to access the system other than root
        --password <str>   password of given username
        --directory <str>  directory/folder name
"""


def upload_logs(
    log_dir: str, ssh_install, nodeip: str, results: list, directory: str
) -> None:
    """Uploading all the collected logs to magna from installer node

    Args:
       log_dir             directory to store all the logs
       ssh_install         ssh object of installer node
       nodeip              host Ip address od installer node
       results             host Ip address which are failed

    Returns:
        None
    """
    try:
        file_share = "http://magna002.ceph.redhat.com/cephci-jenkins"
        print("uploading logs to Magna")
        ssh_install.exec_command("sudo mkdir -p tmp")
        cmd = "sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ tmp"
        ssh_install.exec_command(cmd)
        stdin, stdout, stderr = ssh_install.exec_command(
            f"[ -d tmp/cephci-jenkins/{directory} ]; echo $?"
        )
        if not directory or json.loads(stdout):
            print("Either directory is not provided or given diretory does not exist")
            ssh_install.exec_command("mkdir -p tmp/cephci-jenkins/ceph_logs")
            ssh_install.exec_command(f"mv {log_dir} tmp/cephci-jenkins/ceph_logs/")
            print(
                f"Logs Successfully uploaded to Magna, location:{file_share}/ceph_logs/{log_dir}"
            )
        else:
            print(f"Given directory {directory} exist")
            ssh_install.exec_command(f"mv {log_dir} ceph_logs")
            ssh_install.exec_command(f"mv ceph_logs tmp/cephci-jenkins/{directory}/")
            print(
                f"Logs Successfully uploaded to Magna, location:{file_share}/{directory}/ceph_logs"
            )
    except Exception:
        results.append(nodeip)


def collect_logs(
    nodeip: str, ssh_install, uname: str, log_dir: str, results: list
) -> None:
    """Collect log from installer node based on ceph version

    Args:
       nodeip              host Ip address od installer node
       ssh_install         ssh object of installer node
       uname               Username of installer node
       log_dir             directory to store all the logs
       results             host Ip address which are failed

    Returns:
        None
    """
    try:
        stdin, stdout, stderr = ssh_install.exec_command("sudo ceph --version")
        if not stderr:
            file_name = "cephansible.sh"
        else:
            file_name = "cephadm.sh"
        file = os.path.join(os.path.dirname(__file__), f"{file_name}")
        sftp = ssh_install.open_sftp()
        sftp.put(f"{file}", f"/home/{uname}/{file}")
        ssh_install.exec_command(
            f"sudo sh {file} > ceph_status.txt; tar -cf ceph_status.tar.gz ceph_status.txt"
        )
        ssh_install.exec_command(
            f"chmod 755 ceph_status.tar.gz; sudo mv ceph_status.tar.gz {log_dir}/"
        )
        print("Command output successfully stored")
    except Exception:
        results.append(nodeip)


def generate_sosreport_in_node(
    ssh_install, nodeip: str, uname: str, pword: str, log_dir: str, results: list
) -> None:
    """Generate sosreport in the given node and copy report to installer

    Args:
       ssh_install         ssh object of installer node
       nodeip              host Ip address
       uname               Username for accessing host
       pword               password for accessing host through given user
       log_dir             directory to store all the logs
       results             host Ip address which are failed

    Returns:
        None
    """
    print(f"Connecting {nodeip} to generate sosreport")
    try:
        ssh_d = SSHConnectionManager(nodeip, uname, pword).get_client()
        ssh_d.exec_command("sudo yum -y install sos")
        stdin, stdout, stderr = ssh_d.exec_command(
            "sudo sosreport -a --all-logs --batch"
        )
        sosreport = re.search(r"/var/tmp/sosreport-.*.tar.xz", stdout)
        print(f"Successfully generated sosreport in node {nodeip} :{sosreport.group()}")
        ssh_d.exec_command(f"sudo chmod 755 {sosreport.group()}")
        ssh_install.exec_command(f"scp {nodeip}:{sosreport.group()} {log_dir}")
        ssh_d.exec_command(f"sudo rm -rf {sosreport.group()}")
        ssh_d.close()
        print(f"Successfully moved report from {nodeip} to installer")
    except Exception:
        results.append(nodeip)


def run(args: dict) -> int:
    """Standard script to collect all the logs from ceph cluster through installer node

    Through installer node get all other nodes in the cluster, generate sosreport for all the nodes obtained.
    Run shell script on installer node, then upload all the collected logs to magna

    Args:
       ip         installer IP address
       username   username to be used to access the system other than root
       password   password for installer node

    Returns:
        0 on success or 1 for failures

    Raises:
        AssertionError: An error occurred if given IP is not of Installer node
    """
    results = []
    run_id = generate_unique_id(length=6)
    ip = args["--ip"]
    uname = args["--username"]
    pword = args["--password"]
    directory = args["--directory"]
    log_dir = f"ceph_logs_{run_id}"

    ssh_install = SSHConnectionManager(ip, uname, pword).get_client()
    stdin, stdout, stderr = ssh_install.exec_command("hostname")
    if "installer" not in stdout:
        raise AssertionError("Please provide installer node details")

    ssh_install.exec_command(f"sudo mkdir -p {log_dir}")
    ssh_install.exec_command(f"sudo chown -R {uname}:{uname} {log_dir}")
    stdin, stdout, stderr = ssh_install.exec_command(
        "cut -f 1 /etc/hosts | cut -d ' ' -f 3"
    )
    nodes = stdout.splitlines()
    print(f"Host that are obtained from given host: {nodes}")
    collect_logs(ip, ssh_install, uname, log_dir, results)
    with parallel() as p:
        for nodeip in nodes:
            if nodeip:
                p.spawn(
                    generate_sosreport_in_node,
                    ssh_install,
                    nodeip,
                    uname,
                    pword,
                    log_dir,
                    results,
                )
    upload_logs(log_dir, ssh_install, ip, results, directory)
    print(f"Failed to collect logs from nodes :{results}")
    return 1 if results else 0


if __name__ == "__main__":
    arguments = docopt(doc)
    rc = run(arguments)
    sys.exit(rc)
