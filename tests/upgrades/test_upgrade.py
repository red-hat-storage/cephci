import logging
import os

import yaml

from tests.ceph_ansible import switch_rpm_to_container, test_ansible_upgrade
from tests.ceph_installer import test_ansible, test_cephadm
from tests.cephadm import test_cephadm_upgrade

log = logging.getLogger(__name__)


def load_file(file_name):
    """Retrieve yaml data content from file."""
    file_path = os.path.abspath(file_name)
    with open(file_path, "r") as conf_:
        content = yaml.safe_load(conf_)
    return content


def get_ansible_conf(config, version, is_repo_present, is_ceph_conf_present):
    """

    Fetch ansible conf for the version specified
    Args:
        config: configuration specified in suite file
        version: version for which config needs to be fetched
        is_repo_present: True if repo and image information are specified by user in CLI
        is_ceph_conf_present: True if ceph_origin and related confs are specified by user in suite config

    Returns:
        ansible configuration for install or upgrade
    """
    platform = config["rhbuild"].split("-", 1)[1]
    config["ansi_config"] = config["suite_setup"]
    if not is_ceph_conf_present:
        config["ansi_config"]["ceph_origin"] = "distro"
        config["ansi_config"]["ceph_repository"] = "rhcs"
        config["ansi_config"]["ceph_rhcs_version"] = version.split(".")[0]

    release_info = load_file("release.yaml")

    if not is_repo_present:
        config["base_url"] = release_info["releases"][version]["composes"][platform]
        container_image = release_info["releases"][version]["image"]["ceph"]
    else:
        container_image = config["container_image"]

    config["ansi_config"]["ceph_docker_registry"] = container_image.split("/", 1)[0]
    config["ansi_config"]["ceph_docker_image"] = container_image.split("/", 1)[1].split(
        ":"
    )[0]
    config["ansi_config"]["ceph_docker_image_tag"] = container_image.split("/", 1)[
        1
    ].split(":")[1]
    config["ansi_config"]["node_exporter_container_image"] = release_info["releases"][
        version
    ]["image"]["nodeexporter"]
    config["ansi_config"]["grafana_container_image"] = release_info["releases"][
        version
    ]["image"]["grafana"]
    config["ansi_config"]["prometheus_container_image"] = release_info["releases"][
        version
    ]["image"]["prometheus"]
    config["ansi_config"]["alertmanager_container_image"] = release_info["releases"][
        version
    ]["image"]["alertmanager"]
    config["rhbuild"] = "-".join([version, platform])
    return config


def get_cephadm_upgrade_config(config, version):
    """

    Fetch configuration for cephadm upgrade
    Args:
        config: configuration specified in suite file
        version: version to which upgrade is to be done

    Returns:
        config required for cephadm upgrade
    """
    config["command"] = "start"
    config["service"] = "upgrade"
    config["base_cmd_args"] = {"verbose": True}
    config["benchmark"] = {
        "type": "rados",
        "pool_per_client": True,
        "pg_num": 128,
        "duration": 10,
    }
    config["verify_cluster_health"] = True
    release_info = load_file("release.yaml")
    config["container_image"] = release_info["releases"][version]["image"]["ceph"]
    return config


def run(ceph_cluster, **kw):
    """
    Runs ceph-ansible and cephadm deployment and upgrade according to the path specified
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    log.info("Running test")
    log.info("Running ceph upgrade test")
    config = kw.get("config")
    paths = config.get("paths").split(" ")
    install_version = paths[0]
    upgrade_versions = paths[1:]
    ceph_cluster_dict = kw.get("ceph_cluster_dict")

    for cluster_name, cluster in ceph_cluster_dict.items():

        if install_version.startswith("5."):
            config["steps"] = config["suite_setup"]["steps"]
            rc = test_cephadm.run(
                ceph_cluster=ceph_cluster_dict[cluster_name],
                ceph_nodes=ceph_cluster_dict[cluster_name],
                config=config,
                test_data=kw.get("test_data"),
                ceph_cluster_dict=ceph_cluster_dict,
                clients=kw.get("clients"),
            )
            if rc != 0:
                return rc
        else:
            is_ceph_conf_present = (
                True if config["suite_setup"].get("ceph_origin") else False
            )
            is_repo_present = True if config.get("container_image") else False
            config = get_ansible_conf(
                config, install_version, is_ceph_conf_present, is_repo_present
            )
            rc = test_ansible.run(
                ceph_cluster=ceph_cluster_dict[cluster_name],
                ceph_nodes=ceph_cluster_dict[cluster_name],
                config=config,
                test_data=kw.get("test_data"),
                ceph_cluster_dict=ceph_cluster_dict,
                clients=kw.get("clients"),
            )
            if rc != 0:
                return rc

        for version in upgrade_versions:
            index = upgrade_versions.index(version)
            prev_version = upgrade_versions[index - 1] if index > 0 else install_version
            if version.startswith("5.") and prev_version.startswith("5."):
                config = get_cephadm_upgrade_config(config, version)
                rc = test_cephadm_upgrade.run(
                    ceph_cluster=ceph_cluster_dict[cluster_name],
                    ceph_nodes=ceph_cluster_dict[cluster_name],
                    config=config,
                    test_data=kw.get("test_data"),
                    ceph_cluster_dict=ceph_cluster_dict,
                    clients=kw.get("clients"),
                )
                if rc != 0:
                    return rc
            elif version.startswith("5.") and prev_version.startswith("4."):
                config = get_ansible_conf(config, version, False, False)
                if not ceph_cluster.containerized:
                    rc = switch_rpm_to_container.run(
                        ceph_cluster=ceph_cluster_dict[cluster_name],
                        ceph_nodes=ceph_cluster_dict[cluster_name],
                        config=config,
                        test_data=kw.get("test_data"),
                        ceph_cluster_dict=ceph_cluster_dict,
                        clients=kw.get("clients"),
                    )
                    if rc != 0:
                        return rc
                    config["ansi_config"]["containerized_deployment"] = True
                rc = test_ansible_upgrade.run(
                    ceph_cluster=ceph_cluster_dict[cluster_name],
                    ceph_nodes=ceph_cluster_dict[cluster_name],
                    config=config,
                    test_data=kw.get("test_data"),
                    ceph_cluster_dict=ceph_cluster_dict,
                    clients=kw.get("clients"),
                )
                if rc != 0:
                    return rc
            else:
                config = get_ansible_conf(config, version, False, False)
                rc = test_ansible_upgrade.run(
                    ceph_cluster=ceph_cluster_dict[cluster_name],
                    ceph_nodes=ceph_cluster_dict[cluster_name],
                    config=config,
                    test_data=kw.get("test_data"),
                    ceph_cluster_dict=ceph_cluster_dict,
                    clients=kw.get("clients"),
                )
                if rc != 0:
                    return rc
    return 0
