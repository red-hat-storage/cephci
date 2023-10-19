from cli.exceptions import ConfigError
from cli.ops.cephadm import bootstrap, set_container_image_config
from cli.utilities.configure import install_cephadm, setup_ssh_keys


def run(ceph_cluster, **kwargs):
    """Verify cephadm bootstrap"""
    # Get config specs
    config, custom_config = (
        kwargs.get("config"),
        kwargs.get("test_data", {}).get("custom-config", {}),
    )

    # Check mandatory parameter bootstrap
    if not config.get("bootstrap"):
        raise ConfigError("Mandatory config 'bootstrap' not provided")

    # Get cluster config
    installer, nodes = (
        ceph_cluster.get_nodes(role="installer")[0],
        ceph_cluster.get_nodes(),
    )

    # Get build details
    rhbuild, build_type, ibm_build, tools_repo, image = (
        config.get("rhbuild"),
        config.get("build_type"),
        config.get("ibm_build", False),
        config.get("base_url"),
        config.get("container_image"),
    )

    # Get cephversion and platform
    ceph_version, platform = rhbuild.split("-", 1)

    # Setup ssh keys
    setup_ssh_keys(installer, nodes)

    # Install cephadm package
    install_cephadm(installer, build_type, ibm_build)

    # Get bootstrap config
    bootstrap_config = config.get("bootstrap")

    # Bootstrap cluster
    bootstrap(
        installer=installer,
        nodes=nodes,
        ceph_version=ceph_version,
        platform=platform,
        build_type=build_type,
        ibm_build=ibm_build,
        tools_repo=tools_repo,
        image=image,
        **bootstrap_config
    )

    # Set container configs
    set_container_image_config(installer, custom_config)

    return 0
