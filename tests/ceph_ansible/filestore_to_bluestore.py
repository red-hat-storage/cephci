'''Filestore to bluestore migrations'''

import logging
logger = logging.getLogger(__name__)
log = logger


def run(**kw):

    log.info("Running Filestore to bluestore migrations test")
    ceph_nodes = kw.get('ceph_nodes')
    ansible_dir = '/usr/share/ceph-ansible'
    playbook = 'filestore-to-bluestore.yml'
    for cnode in ceph_nodes:
        if cnode.role == 'installer':
            installer_node = cnode
        if cnode.role == 'osd':
            out, err = installer_node.exec_command(cmd='cd {ansible_dir};ansible-playbook -vvvv'
                                                       ' infrastructure-playbooks/{playbook} -e ireallymeanit=yes'
                                                       ' -i hosts --limit {osd_daemon_to_migrate}'.format(
                                                           ansible_dir=ansible_dir, playbook=playbook,
                                                           osd_daemon_to_migrate=cnode.hostname), long_running=True)
    if err == 0:
        log.info("ansible-playbook filestore-to-bluestore.yml successful")
        return 0

    log.info("ansible-playbook filestore-to-bluestore.yml failed")
    return 1
