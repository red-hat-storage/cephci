'''Shrinks the Ceph monitors from the cluster'''

import logging
import re

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Remove monitor from cluster using shrink-mon.yml

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster

    Returns:
        int: non-zero on failure, zero on pass
    """
    log.info("Shrinking monitor")
    config = kw.get('config')
    build = config.get('build', config.get('rhbuild'))
    mon_to_kill_list = config.get('mon-to-kill')
    mon_to_kill = None
    playbook = "shrink-mon.yml"
    # list available monitors
    mon_short_name_list = [ceph_node.shortname for ceph_node in ceph_cluster.get_nodes('mon')]
    # check if the given monitor to kill exists in the cluster
    for _mon_to_kill in mon_to_kill_list:
        matcher = re.compile(_mon_to_kill)
        matched_short_names = list(filter(matcher.match, mon_short_name_list))
        if len(matched_short_names) > 0:
            shrinked_nodes = [ceph_node for ceph_node in ceph_cluster if ceph_node.shortname in matched_short_names]
            for ceph_node in shrinked_nodes:
                ceph_node.remove_ceph_object(ceph_node.get_ceph_objects('mon')[0])
        else:
            raise RuntimeError('No match for {node_name}'.format(node_name=_mon_to_kill))
        mon_to_kill = ','.join([mon_to_kill, ','.join(matched_short_names)]) if mon_to_kill else ','.join(
            matched_short_names)

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ceph_installer.node.obtain_root_permissions('/var/log')
    ansible_dir = ceph_installer.ansible_dir
    # Getting inventory
    out1, rc = ceph_installer.exec_command(sudo=True, cmd='cd {ansible_dir};cat hosts'.format(ansible_dir=ansible_dir))
    outbuf = out1.read().decode()
    inventory = outbuf.split("\n")
    log.info("\nInventory before shrink-mon playbook\n")
    log.info(outbuf)
    # Set ansible deprecation warning to false
    ceph_installer.exec_command(cmd='export ANSIBLE_DEPRECATION_WARNINGS=False')

    # Based on RHCS version choosing how shrink-mon playbook needs to be executed
    if build.startswith('4'):
        playbook = "infrastructure-playbooks/{playbook}".format(playbook=playbook)
    else:
        ceph_installer.exec_command(sudo=True,
                                    cmd='cd {ansible_dir};cp -R {ansible_dir}/infrastructure-playbooks/{playbook} .'
                                    .format(ansible_dir=ansible_dir, playbook=playbook))

    cmd = 'cd {ansible_dir} ;ansible-playbook -vvvv -e \
          ireallymeanit=yes {playbook} -e mon_to_kill={mon_to_kill} -i hosts'.format(
          ansible_dir=ansible_dir, playbook=playbook, mon_to_kill=mon_to_kill)

    out, err = ceph_installer.exec_command(cmd=cmd, long_running=True)

    if err != 0:
        log.error("Failed during ansible playbook run\n")
        return err

    # To remove the shrinked mon from inventory
    mons_index = inventory.index("[mons]")
    for node in inventory[mons_index:]:
        if re.match("%s" % matched_short_names[0], node):
            remove_index = inventory.index(node)
            break
    inventory.pop(remove_index)
    modified_inventory = "\n".join(inventory)
    log.info("\nInventory after shrink-mon playbook\n")
    log.info(modified_inventory)
    hosts_file = ceph_installer.write_file(
        sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='w')
    hosts_file.write(modified_inventory)
    hosts_file.flush()

    return ceph_cluster.check_health(build)
