"""Filestore to bluestore migrations"""
from utility.log import Log

log = Log(__name__)


def run(**kw):

    log.info("Running Filestore to bluestore migrations test")
    ceph_nodes = kw.get("ceph_nodes")
    ansible_dir = "/usr/share/ceph-ansible"
    playbook = "filestore-to-bluestore.yml"
    replace_objectstore = "sed -i 's/osd_objectstore: filestore/osd_objectstore: bluestore/g' group_vars/all.yml"
    installer_node = kw["ceph_cluster"].get_nodes(role="installer")[0]
    for cnode in ceph_nodes:
        if cnode.role == "osd":
            rc = installer_node.exec_command(
                cmd="cd {ansible_dir};ansible-playbook -vvvv"
                " infrastructure-playbooks/{playbook} -e ireallymeanit=yes"
                " -i hosts --limit {osd_daemon_to_migrate}".format(
                    ansible_dir=ansible_dir,
                    playbook=playbook,
                    osd_daemon_to_migrate=cnode.hostname,
                ),
                long_running=True,
            )
    installer_node.exec_command(
        sudo=True,
        cmd="cd {ansible_dir} ; {replace_objectstore}".format(
            ansible_dir=ansible_dir, replace_objectstore=replace_objectstore
        ),
    )

    if rc == 0:
        log.info("ansible-playbook filestore-to-bluestore.yml successful")
        return 0

    log.info("ansible-playbook filestore-to-bluestore.yml failed")
    return 1
