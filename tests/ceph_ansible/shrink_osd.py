from ceph.ceph import NodeVolume
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Remove osd from cluster using shrink-osd.yml

    Args:
        ceph_cluster(ceph.ceph.Ceph):
        **kw:
            config sample:
                config:
                  osd-to-kill:
                    - 2
    Returns:
        int: non-zero on failure, zero on pass
    """
    log.info("Shrinking osd")
    config = kw.get("config")
    osd_scenario = ceph_cluster.ansible_config.get("osd_scenario")
    build = config.get("build", config.get("rhbuild"))
    osd_to_kill_list = config.get("osd-to-kill")
    osd_to_kill = ",".join((str(osd) for osd in osd_to_kill_list))
    cluster_name = config.get("cluster")

    for osd_id in osd_to_kill_list:
        osd_host = ceph_cluster.get_osd_metadata(osd_id).get("hostname")
        for ceph_node in ceph_cluster:
            osd_volumes = ceph_node.get_allocated_volumes()
            if ceph_node.shortname == str(osd_host):
                osd_volumes.pop().status = NodeVolume.FREE
                if len(osd_volumes) < 1:
                    ceph_node.remove_ceph_object(ceph_node.get_ceph_objects("osd")[0])

    ceph_installer = ceph_cluster.get_ceph_object("installer")
    ceph_installer.node.obtain_root_permissions("/var/log")
    ansible_dir = ceph_installer.ansible_dir

    cmd = f"export ANSIBLE_DEPRECATION_WARNINGS=False; cd {ansible_dir}; ansible-playbook -e ireallymeanit=yes "
    if build.startswith("2"):
        ceph_installer.exec_command(
            sudo=True,
            cmd=f"cp -R {ansible_dir}/infrastructure-playbooks/shrink-osd.yml {ansible_dir}/shrink-osd.yml",
        )
        cmd += f"shrink-osd.yml -e osd_to_kill={osd_to_kill} -i hosts"
    elif build.startswith("3"):
        if osd_scenario == "lvm":
            ceph_installer.exec_command(
                sudo=True,
                cmd=f"cp -R {ansible_dir}/infrastructure-playbooks/shrink-osd.yml {ansible_dir}/shrink-osd.yml",
            )
            cmd += f"shrink-osd.yml -e osd_to_kill={osd_to_kill} -i hosts"
        else:
            ceph_installer.exec_command(
                sudo=True,
                cmd=f"cp -R {ansible_dir}/infrastructure-playbooks/shrink-osd-ceph-disk.yml "
                f"{ansible_dir}/shrink-osd-ceph-disk.yml",
            )
            cmd += f"shrink-osd-ceph-disk.yml -e osd_to_kill={osd_to_kill} -i hosts"
    else:
        cmd += f"infrastructure-playbooks/shrink-osd.yml -e osd_to_kill={osd_to_kill} -i hosts"

    rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    if rc != 0:
        log.error("Failed during ansible playbook run")
        return rc

    return ceph_cluster.check_health(build, cluster_name=cluster_name)
