import json
import re

from utility.log import Log

log = Log(__name__)

BOOT_DISK_CMD = "findmnt -v -n -T / -o SOURCE"


def run(ceph_cluster, **kw):
    """
    Rolls updates over existing ceph-ansible deployment
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster object
        **kw(dict):
            config sample:
                config:
                   demon:
                     - mon
    Returns:
        int: non-zero on failure, zero on pass
    """
    ceph_installer = ceph_cluster.get_ceph_object("installer")
    ansible_dir = ceph_installer.ansible_dir
    config = kw.get("config")

    # Get all OSD nodes to get boot disk
    osd_nodes = ceph_cluster.get_nodes("osd")
    log.info("Get all OSD nodes : {}".format(osd_nodes))

    # Read inventory file
    log.info("Get inventory file content")
    hosts_file = ceph_installer.read_inventory_file()
    log.info("Previous Inventory file : \n %s" % hosts_file)

    # Add boot disk for devices
    for line in hosts_file:
        host = ""
        devices = ""
        osd_node = ""
        boot_disk = ""

        # Searching for OSD node with devices
        if not re.search(r"devices", line):
            continue
        host = re.search(r"^(.*)\s+monitor.*devices=\"(.*)\"", line)
        devices = json.loads(host.group(2).replace("'", '"'))
        osd_node = next(filter(lambda x: x.hostname == host.group(1), osd_nodes))
        out, err = osd_node.exec_command(cmd=BOOT_DISK_CMD)
        boot_disk = re.sub(r"\d", "", out.strip())

        if boot_disk not in devices:
            devices.insert(int(), boot_disk)

        # update modified line in hosts_file
        mod_line = re.sub(r"devices=.*\"", 'devices="{}"'.format(str(devices)), line)
        hosts_file[hosts_file.index(line)] = mod_line

    hosts_file = "".join(hosts_file)
    log.info("Modified Inventory File : \n %s" % hosts_file)
    ceph_installer.write_inventory_file(hosts_file, file_name="new_hosts")
    ceph_installer.setup_ansible_site_yml(ceph_cluster.containerized)

    # Run Ansible with limit=OSD
    log.info("Run Ansible playbook with OSD limit")
    rc = ceph_installer.exec_command(
        cmd="cd {};"
        " ANSIBLE_STDOUT_CALLBACK=debug;"
        " ansible-playbook -vvvv -i new_hosts site.yml --limit {daemon}".format(
            ansible_dir, daemon=config.get("demon") + "s"
        ),
        long_running=True,
    )

    # Validate failure
    if rc != 0:
        log.info("Failed during deployment as expected")
        return 0
    return rc
