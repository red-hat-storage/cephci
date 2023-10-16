from cli.exceptions import ConfigError
from cli.ops.cephadm import bootstrap, set_container_image_config
from cli.utilities.configure import (
    add_ceph_repo,
    enable_ceph_tools_repo,
    generate_bootstrap_config,
    install_cephadm,
    setup_ssh_keys,
)
from cli.utilities.utils import get_node_ip
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs):
    """Verify cephadm bootstrap"""
    # Get config specs
    config = kwargs.get("config")
    custom_config = kwargs.get("test_data", {}).get("custom-config")

    # Check mandatory parameter bootstrap
    if not config.get("bootstrap"):
        raise ConfigError("Mandatory config 'bootstrap' not provided")

    # Get cluster config
    installer = ceph_cluster.get_nodes(role="installer")[0]
    nodes = ceph_cluster.get_nodes()

    # Get build details
    ceph_version, platform = config.get("rhbuild").split("-", 1)

    # Setup ssh keys
    setup_ssh_keys(installer, nodes)

    # Get build details
    build_type = config.get("build_type")
    ibm_build = config.get("ibm_build", False)

    # Enable repos required
    if build_type in ("live", "cdn", "released"):
        enable_ceph_tools_repo(installer, ceph_version, platform)

    else:
        add_ceph_repo(installer, config.get("base_url"))

    # Install cephadm package
    install_cephadm(installer, build_type, ibm_build)

    # Get bootstrap config
    bootstrap_config = config.get("bootstrap")

    # Check for mon IP
    if bootstrap_config.get("mon-ip"):
        bootstrap_config["mon-ip"] = get_node_ip(nodes, bootstrap_config.pop("mon-ip"))

    # Check for container imag
    if config.get("container_image"):
        bootstrap_config["image"] = config.pop("container_image")

    # Check for config
    if bootstrap_config.get("config"):
        bootstrap_config["config"] = generate_bootstrap_config(
            installer, bootstrap_config.pop("config")
        )

    # Bootstrap cluster
    bootstrap(node=installer, **bootstrap_config)

    # Set container configs
    if custom_config:
        set_container_image_config(installer, custom_config)

    return 0
