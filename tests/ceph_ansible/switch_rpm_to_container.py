"""switches non-containerized ceph daemon to containerized ceph daemon"""
from utility.log import Log

log = Log(__name__)


def run(**kw):

    log.info("Running exec test")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    build = config.get("rhbuild")
    installer_node = None
    ansible_dir = "/usr/share/ceph-ansible"
    playbook = "switch-from-non-containerized-to-containerized-ceph-daemons.yml"
    for cnode in ceph_nodes:
        if cnode.role == "installer":
            installer_node = cnode
    if build.startswith("3"):
        installer_node.exec_command(
            sudo=True,
            cmd="cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/{playbook} .".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
        )
        rc = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv {playbook}"
            " -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    else:
        rc = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv"
            " infrastructure-playbooks/{playbook} -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    if rc == 0:
        log.info(
            "ansible-playbook switch-from-non-containerized-to-containerized-ceph-daemons.yml successful"
        )
        return 0

    log.info(
        "ansible-playbook switch-from-non-containerized-to-containerized-ceph-daemons.yml failed"
    )
    return 1
