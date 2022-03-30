"""
Shrink daemons using ceph-ansible shrink playbooks in nautilus
"""

from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):

    ansible_dir = "/usr/share/ceph-ansible"
    ceph_installer = ceph_cluster.get_ceph_object("installer")
    config = kw.get("config")
    build = config.get("build", config.get("rhbuild"))
    daemon_to_kill = config.get("daemon-to-kill")
    daemon = config.get("daemon")
    instance = config.get("instance")
    playbook = f"shrink-{daemon}.yml"
    short_names = []

    # For all daemons node name is required but for osds the osd.id is required to shrink daemon
    if daemon != "osd":
        for node in daemon_to_kill:
            short_name = get_node_by_id(ceph_cluster, node).shortname
            short_names.append(short_name)
            node_name = ",".join(short_names)
        log.info(f"Executing {playbook} playbook to shrink {node_name} daemons")
    else:
        daemons_to_kill = ",".join(daemon_to_kill)
        log.info(f"Executing {playbook} playbook to shrink {daemons_to_kill} daemons")

    check_inventory = f"sudo cat {ansible_dir}/hosts"

    # Display inventory before shrinking
    outbuf, _ = ceph_installer.exec_command(cmd=check_inventory)
    log.info(f"Inventory {outbuf}")

    # Based of RHCS version, use the playbook path
    if build.startswith("4"):
        playbook = f"infrastructure-playbooks/{playbook}"
    else:
        ceph_installer.exec_command(
            sudo=True,
            cmd=f"cd {ansible_dir};cp -R {ansible_dir}/infrastructure-playbooks/{playbook} .",
        )

    cmd = f"cd {ansible_dir}; ansible-playbook -vvvv -e ireallymeanit=yes {playbook}"

    # adding extra vars to the shrink playbook
    if daemon == "osd":
        cmd += f" -e {daemon}_to_kill={daemons_to_kill} -i hosts"
    elif daemon == "rgw":
        cmd += f" -e {daemon}_to_kill={node_name}.rgw{instance} -i hosts"
    else:
        cmd += f" -e {daemon}_to_kill={node_name} -i hosts"

    # Executing shrink playbook depending on provided daemon
    err = ceph_installer.exec_command(cmd=cmd, long_running=True)

    # If playbook execution fails log error
    if err != 0:
        log.error(f"Failed during ansible playbook execution: {playbook}\n")
        return err
    return 0
