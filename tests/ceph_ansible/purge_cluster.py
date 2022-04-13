""" Purges the Ceph the cluster"""

import datetime
import re
from time import sleep

from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """

    :param kw:
       - ceph_nodes: ceph node list representing a cluster
       - config: (optional)
         - ansible-dir: path to ansible working directory, default is /usr/share/ceph-ansible
         - inventory: ansible inventory file, default is hosts
         - playbook-command: ansible playbook command string,
            default is purge-cluster.yml --extra-vars 'ireallymeanit=yes'


    :return: 0 on sucess, non-zero for failures
    """
    log.info("Running test")
    ceph_nodes = kw.get("ceph_nodes")
    iscsi_clients_node = None
    installer_node = None
    ansible_dir = "/usr/share/ceph-ansible"
    inventory = "hosts"
    playbook_command = "purge-cluster.yml --extra-vars 'ireallymeanit=yes'"
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    if config:
        ansible_dir = config.get("ansible-dir", ansible_dir)
        inventory = config.get("inventory", inventory)
        playbook_command = config.get("playbook-command", playbook_command)

    for ceph in ceph_nodes:
        if ceph.role == "installer":
            installer_node = ceph
            break
    for ceph in ceph_nodes:
        if ceph.role == "iscsi-clients":
            iscsi_clients_node = ceph
            break

    log.info("Purge Ceph cluster")

    if iscsi_clients_node:
        iscsi_clients_node.exec_command(
            sudo=True, cmd="yum remove -y iscsi-initiator-utils"
        )
        iscsi_clients_node.exec_command(
            sudo=True, cmd="yum remove -y device-mapper-multipath"
        )

    playbook_regex = re.search("(purge-.*?\\.yml)(.*)", playbook_command)
    playbook = playbook_regex.group(1)
    playbook_options = playbook_regex.group(2)

    # Set ansible deprecation warning to false
    installer_node.exec_command(cmd="export ANSIBLE_DEPRECATION_WARNINGS=False")

    # Based on RHCS version choosing how purge playbook needs to be executed
    if not build.startswith("4"):
        installer_node.exec_command(
            sudo=True,
            cmd="cd {ansible_dir};cp {ansible_dir}/infrastructure-playbooks/{playbook} .".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
        )
    else:
        playbook = "infrastructure-playbooks/{playbook}".format(playbook=playbook)

    cmd = "cd {ansible_dir} ; ansible-playbook -vvvv -i {inventory} {playbook} {playbook_options}".format(
        ansible_dir=ansible_dir,
        playbook=playbook.strip(),
        playbook_options=playbook_options.strip(),
        inventory=inventory.strip(),
    )

    # executing the command to purge cluster
    err = installer_node.exec_command(cmd=cmd, long_running=True)

    # remove ceph-ansible after successful purge
    if err == 0:
        log.info("ansible-playbook purge cluster successful")
        installer_node.exec_command(
            sudo=True, cmd="rm -rf {ansible_dir}".format(ansible_dir=ansible_dir)
        )
        if installer_node.pkg_type == "deb":
            installer_node.exec_command(sudo=True, cmd="apt-get remove -y ceph-ansible")
            installer_node.exec_command(
                sudo=True, cmd="apt-get install -y ceph-ansible"
            )
        else:
            installer_node.exec_command(sudo=True, cmd="yum remove -y ceph-ansible")
            installer_node.exec_command(sudo=True, cmd="yum install -y ceph-ansible")
        with parallel() as p:
            for cnode in ceph_nodes:
                if cnode.role != "installer":
                    p.spawn(reboot_node, cnode)
        return 0

    log.info("ansible-playbook failed to purge cluster")
    return 1


def reboot_node(ceph_node, timeout=300):
    """Reboot the given node."""
    # Allow things to settle before performing a reboot
    sleep(10)
    ceph_node.exec_command(sudo=True, cmd="reboot", check_ec=False)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    while end_time > datetime.datetime.now():
        try:
            sleep(20)
            ceph_node.reconnect()
            # Connecting early could lead to connection reset
            sleep(10)
            return
        except BaseException as be:  # noqa
            log.debug(f"Waiting for node {ceph_node.vmshortname} to reboot.")

    raise RuntimeError(f"Failed to reconnect {ceph_node.ip_address}")
