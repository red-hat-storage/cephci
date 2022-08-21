import datetime
from copy import deepcopy

from ceph.utils import get_public_network, set_container_info
from utility.utils import Log, fetch_build_artifacts, generate_self_signed_cert_on_rgw

LOG = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Runs ceph-ansible deployment
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object

    For cdn container installation GAed container parameters needs to override as below,
        config:
          use_cdn: True
          ansi_config:
            ceph_origin: repository
            ceph_repository_type: cdn

    For internal repository
        config:
          use_internal:
            rhcs-version: 4.1
            release: z1-async1
          ansi_config:
            ceph_origin: distro
    """
    LOG.info("Running ceph ansible deployment")
    ceph_nodes = kw.get("ceph_nodes")

    # Avoid conflicts arising from shallow reference modifications
    config = deepcopy(kw.get("config"))

    filestore = config.get("filestore", False)
    k_and_m = config.get("ec-pool-k-m")
    hotfix_repo = config.get("hotfix_repo")
    test_data = kw.get("test_data")
    cloud_type = config.get("cloud-type", "openstack")

    # In case of Interop, we would like to avoid updating the packages... mainly, the
    # ansible package.
    exclude_ansible = config.get("skip_enabling_rhel_rpms", False)

    ubuntu_repo = config.get("ubuntu_repo", None)
    base_url = config.get("base_url", None)
    installer_url = config.get("installer_url", None)
    mixed_lvm_configs = config.get("is_mixed_lvm_configs", None)
    device_to_add = config.get("device", None)
    config["ansi_config"]["public_network"] = get_public_network(ceph_nodes)
    ceph_cluster.ansible_config = config["ansi_config"]
    ceph_cluster.custom_config = test_data.get("custom-config")
    ceph_cluster.custom_config_file = test_data.get("custom-config-file")
    cluster_name = config.get("ansi_config").get("cluster")
    use_cdn = (
        config.get("build_type") == "released"
        or config.get("use_cdn")
        or config.get("ansi_config").get("ceph_repository_type") == "cdn"
    )
    containerized = config.get("ansi_config").get("containerized_deployment")

    # Support for released versions available in internal repositories
    use_internal = config.get("use_internal")
    if use_internal:
        _rhcs_version = use_internal["rhcs-version"]
        _rhcs_release = use_internal["release"]
        _platform = "-".join(config.get("rhbuild").split("-")[1:])
        config["build"] = f"{_rhcs_version}-{_platform}"
        (
            config["base_url"],
            config["ceph_docker_registry"],
            config["ceph_docker_image"],
            config["ceph_docker_image_tag"],
        ) = fetch_build_artifacts(_rhcs_release, _rhcs_version, _platform)
        base_url = config["base_url"]

    if all(
        key in ceph_cluster.ansible_config
        for key in ("rgw_multisite", "rgw_zonesecondary")
    ):
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        primary_node = config.get("primary_node", "ceph-rgw1")
        primary_rgw_node = (
            ceph_cluster_dict.get(primary_node).get_ceph_object("rgw").node
        )
        config["ansi_config"]["rgw_pullhost"] = primary_rgw_node.ip_address

    config["ansi_config"].update(
        set_container_info(ceph_cluster, config, use_cdn, containerized)
    )

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # create ssl certificate
    if (
        config.get("ansi_config").get("radosgw_frontend_ssl_certificate")
        == "/etc/ceph/server.pem"
    ):
        LOG.info("install self signed certificate on rgw node")
        rgw_node = ceph_nodes.get_ceph_object("rgw").node
        generate_self_signed_cert_on_rgw(rgw_node)

    if config.get("skip_setup") is True:
        LOG.info("Skipping setup of ceph cluster")
        return 0

    test_data["install_version"] = build

    ceph_installer = ceph_cluster.get_ceph_object("installer")
    ansible_dir = ceph_installer.ansible_dir

    ceph_cluster.setup_ceph_firewall()

    ceph_cluster.setup_ssh_keys()

    ceph_cluster.setup_packages(
        base_url,
        hotfix_repo,
        installer_url,
        ubuntu_repo,
        build,
        cloud_type,
        exclude_ansible,
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

    ceph_cluster.distribute_all_yml()

    # add iscsi setting if it is necessary
    if test_data.get("luns_setting", None) and test_data.get("initiator_setting", None):
        ceph_installer.add_iscsi_settings(test_data)

    LOG.info("Ceph ansible version " + ceph_installer.get_installed_ceph_versions())

    # ansible playbookk based on container or bare-metal deployment
    file_name = "site.yml"

    if ceph_cluster.containerized:
        file_name = "site-container.yml"

    rc = ceph_installer.exec_command(
        cmd="cd {ansible_dir} ; ANSIBLE_STDOUT_CALLBACK=debug;ansible-playbook -vvvv -i hosts {file_name}".format(
            ansible_dir=ansible_dir, file_name=file_name
        ),
        long_running=True,
    )

    # manually handle client creation in a containerized deployment (temporary)
    if ceph_cluster.containerized:
        for node in ceph_cluster.get_ceph_objects("client"):
            LOG.info("Manually installing client node")
            node.exec_command(sudo=True, cmd="yum install -y ceph-common")

    if rc != 0:
        LOG.error("Failed during deployment")
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

    # create rbd pool used by tests/workunits
    ceph_cluster.create_rbd_pool(k_and_m, cluster_name)

    if (
        ceph_cluster.check_health(build, timeout=timeout, cluster_name=cluster_name)
        != 0
    ):
        return 1
    return rc
