"""adds ceph daemons on existing cluster"""
import datetime
import logging
import re

from ceph.ceph import NodeVolume

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    """
    ￼    Rolls updates over existing ceph-ansible deployment
    ￼    Args:
    ￼        ceph_cluster (ceph.ceph.Ceph): ceph cluster object
    ￼        **kw(dict):
    ￼            config sample:
    ￼                config:
    ￼                  add:
    ￼                      - node:
    ￼                          node-name: .*node15.*
    ￼                          daemon:
    ￼                             - mon
    ￼    Returns:
    ￼        int: non-zero on failure, zero on pass
    """
    log.info("Running test")
    log.info("Running ceph ansible test")
    config = kw.get("config")
    filestore = config.get("filestore")
    hotfix_repo = config.get("hotfix_repo")
    test_data = kw.get("test_data")
    cluster_name = config.get("cluster")

    ubuntu_repo = config.get("ubuntu_repo", None)
    base_url = config.get("base_url", None)
    installer_url = config.get("installer_url", None)
    mixed_lvm_configs = config.get("is_mixed_lvm_configs", None)
    device_to_add = config.get("device", None)

    ceph_cluster.use_cdn = config.get("use_cdn")
    build = config.get("build", config.get("rhbuild"))
    build_starts = build[0]
    if config.get("add"):
        for added_node in config.get("add"):
            added_node = added_node.get("node")
            node_name = added_node.get("node-name")
            daemon_list = added_node.get("daemon")
            osds_required = [daemon for daemon in daemon_list if daemon == "osd"]
            short_name_list = [
                ceph_node.shortname for ceph_node in ceph_cluster.get_nodes()
            ]
            matcher = re.compile(node_name)
            matched_short_names = list(filter(matcher.match, short_name_list))
            if len(matched_short_names) > 1:
                raise RuntimeError(
                    "Multiple nodes are matching node-name {node_name}: \n{matched_short_names}".format(
                        node_name=node_name, matched_short_names=matched_short_names
                    )
                )
            if len(matched_short_names) == 0:
                raise RuntimeError(
                    "No match for {node_name}".format(node_name=node_name)
                )
            for ceph_node in ceph_cluster:
                if ceph_node.shortname == matched_short_names[0]:
                    matched_ceph_node = ceph_node
                    break
            free_volumes = matched_ceph_node.get_free_volumes()
            if len(osds_required) > len(free_volumes):
                raise RuntimeError(
                    "Insufficient volumes on the {node_name} node. Required: {required} - Found: {found}".format(
                        node_name=matched_ceph_node.shortname,
                        required=len(osds_required),
                        found=len(free_volumes),
                    )
                )
            log.debug("osds_required: {}".format(osds_required))
            log.debug(
                "matched_ceph_node.shortname: {}".format(matched_ceph_node.shortname)
            )
            for osd in osds_required:
                free_volumes.pop().status = NodeVolume.ALLOCATED
            for daemon in daemon_list:
                if len(matched_ceph_node.get_ceph_objects(daemon)) == 0:
                    matched_ceph_node.create_ceph_object(daemon)

    test_data["install_version"] = build

    ceph_installer = ceph_cluster.get_ceph_object("installer")
    ansible_dir = ceph_installer.ansible_dir

    ceph_cluster.setup_ceph_firewall()

    ceph_cluster.setup_packages(
        base_url, hotfix_repo, installer_url, ubuntu_repo, build
    )

    ceph_installer.install_ceph_ansible(build)

    hosts_file = ceph_cluster.generate_ansible_inventory(
        device_to_add, mixed_lvm_configs, filestore
    )
    ceph_installer.write_inventory_file(hosts_file)

    if config.get("docker-insecure-registry"):
        ceph_cluster.setup_insecure_registry()

    # use the provided sample file as main site.yml
    ceph_installer.setup_ansible_site_yml(build, ceph_cluster.containerized)

    # add iscsi setting if it is necessary
    if test_data.get("luns_setting", None) and test_data.get("initiator_setting", None):
        ceph_installer.add_iscsi_settings(test_data)

    log.info("Ceph versions " + ceph_installer.get_installed_ceph_versions())

    # Run site-x.yml by default
    if ceph_cluster.ansible_config.get("containerized_deployment"):
        yaml = "site-container.yml"
    else:
        yaml = "site.yml"

    # Append "--limit" if daemon to be added is not osd
    yaml_dict = {
        "3": {"mon": "%s" % yaml, "osd": "add-osd.yml"},
        "4": {
            "mon": "infrastructure-playbooks/add-mon.yml",
        },
    }
    yaml_file = yaml_dict[build_starts].get(daemon, "%s --limit %ss" % (yaml, daemon))

    if build.startswith("3"):
        if daemon == "osd":
            ceph_installer.exec_command(
                sudo=True,
                cmd="cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/{yaml_file} .".format(
                    ansible_dir=ansible_dir, yaml_file=yaml_file
                ),
            )

    cmd = "cd {} ; ANSIBLE_STDOUT_CALLBACK=debug;ansible-playbook -vvvv -i hosts {}".format(
        ansible_dir, yaml_file
    )
    out, rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    if rc != 0:
        log.error("Failed during deployment")
        return rc

    # check if all osd's are up and in
    timeout = 300
    if config.get("timeout"):
        timeout = datetime.timedelta(seconds=config.get("timeout"))
    # add test_data for later use by upgrade test etc
    num_osds = ceph_cluster.ceph_demon_stat["osd"]
    num_mons = ceph_cluster.ceph_demon_stat["mon"]
    test_data["ceph-ansible"] = {
        "num-osds": num_osds,
        "num-mons": num_mons,
        "rhbuild": build,
    }

    if (
        ceph_cluster.check_health(build, timeout=timeout, cluster_name=cluster_name)
        != 0
    ):
        return 1
    return rc
