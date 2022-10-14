import yaml

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Add rbd_mirror daemon to provided cluster.
    Adds rbdmirror configs to rbdmirrors.yaml and initites playbook
    to add rbd mirror daemons.
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    ceph_installer = ceph_cluster.get_ceph_object("installer")
    config = kw.get("config")

    other_cluster_name = [
        cluster_name
        for cluster_name in kw.get("ceph_cluster_dict").keys()
        if cluster_name != ceph_cluster.name
    ][0]
    other_cluster = kw.get("ceph_cluster_dict")[other_cluster_name]

    cmd = "hostname -I | awk '{print $1}'"
    mon_ip = (
        other_cluster.get_ceph_object("mon").exec_command(cmd=cmd)[0].replace("\n", "")
    )
    config["ansi_config"]["ceph_rbd_mirror_remote_mon_hosts"] = mon_ip

    write_configs_to_rbdmirrors_yaml(ceph_installer, config)

    hosts = ["\n[rbdmirrors]"]
    for rbd_mirror in ceph_cluster.get_ceph_objects("rbd-mirror"):
        mirror_node = rbd_mirror.node
        hosts.append(mirror_node.shortname)
    hosts_config = "\n".join(hosts) + "\n"
    ceph_installer.append_inventory_file(hosts_config)

    file_name = "site.yml"

    if ceph_cluster.containerized:
        file_name = "site-container.yml"

    cmd = (
        "cd /usr/share/ceph-ansible; ANSIBLE_STDOUT_CALLBACK=debug;"
        f"ansible-playbook -vvvv -i hosts {file_name} --limit rbdmirrors"
    )
    rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    return rc


def write_configs_to_rbdmirrors_yaml(ceph_installer, config):
    """Write contents to rbdmirrors.yaml file."""

    rbdmirrors_yaml = yaml.dump(config.get("ansi_config"), default_flow_style=False)

    log.debug(f"rbdmirrors file content: {rbdmirrors_yaml}")
    gvars_file = ceph_installer.remote_file(
        sudo=True,
        file_name="/usr/share/ceph-ansible/group_vars/rbdmirrors.yml",
        file_mode="w",
    )
    gvars_file.write(rbdmirrors_yaml)
    gvars_file.flush()
