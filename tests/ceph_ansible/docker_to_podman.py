"""Migrates from docker to podman"""
from utility.log import Log

log = Log(__name__)


def run(**kw):

    log.info("Running exec test")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    build = config.get("rhbuild")
    installer_node = None
    ansible_dir = "/usr/share/ceph-ansible"
    playbook = "docker-to-podman.yml"
    for cnode in ceph_nodes:
        if cnode.role == "installer":
            installer_node = cnode
    if not build.startswith("4"):
        installer_node.exec_command(
            sudo=True,
            cmd="cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/{playbook} .".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
        )
        err = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv {playbook}"
            " -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    else:
        err = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv"
            " infrastructure-playbooks/{playbook} -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    if err == 0:
        log.info("ansible-playbook docker-podman successful")
        return 0

    log.info("ansible-playbook docker-podman failed")
    return 1
