"""
Test suite that verifies NVMeoF deployment with RBD snapshot-based mirroring
between two Ceph clusters.

This test:
1. Deploys two Ceph clusters (primary and secondary)
2. Configures clients on both clusters
3. Establishes RBD snapshot-based mirroring between clusters
4. Creates mirrored images on both sites with snapshot schedules
5. Deploys NVMe service on both clusters
6. Configures NVMe entities (subsystems, listeners, hosts, namespaces) for all mirrored images
"""

from copy import deepcopy

from ceph.ceph import Ceph
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import pool_cleanup
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from utility.log import Log

LOG = Log(__name__)


def _ensure_rbd_pool_cleanup(rbd, client, ceph_version):
    """Bind pool_cleanup onto the Rbd instance from initial_mirror_config for NVMe teardown.

    teardown() expects rbd_obj.clean_up(pools=...). cli.rbd.rbd.Rbd has no clean_up; we attach
    one without constructing a separate Rbd object.
    """
    if rbd is None or client is None:
        return rbd
    if hasattr(rbd, "clean_up"):
        return rbd

    def clean_up(**kw):
        pools = kw.get("pools") or []
        if not pools:
            return
        pool_cleanup(client, list(pools), ceph_version=int(ceph_version))

    rbd.clean_up = clean_up
    return rbd


def deploy_nvme_service_on_cluster(ceph_cluster, config, rbd_obj):
    """Deploy NVMe service on a cluster and configure gateway entities.

    Args:
        ceph_cluster: Ceph cluster object
        config: Test configuration
        rbd_obj: RBD helper with clean_up(pools=...) for teardown

    Returns:
        NVMeService instance
    """
    LOG.info(f"Deploying NVMe service on cluster {ceph_cluster.name}")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    LOG.info(f"NVMe service deployed successfully on cluster {ceph_cluster.name}")
    return nvme_service


def _build_rep_pool_config_for_nvme_mirror(config):
    """Build rep_pool_config for ceph.rbd.initial_config.initial_mirror_config."""
    poolname = config.get("poolname", "rbd")
    image_cfg = config.get("image_config", {})
    size = image_cfg.get("size", "2G")
    count = max(0, int(image_cfg.get("count", 2)))
    secondary_count = max(0, int(image_cfg.get("secondary_count", 0)))
    extra_primary = int(config.get("image_count") or 0)
    primary_total = count + extra_primary
    if primary_total < 1:
        primary_total = 1

    snap_level = image_cfg.get("snapshot_level", "image")
    snap_interval = image_cfg.get("snapshot_interval", "1h")

    def _image_opts(is_secondary=False):
        opts = {
            "size": size,
            "snap_schedule_levels": [snap_level],
            "snap_schedule_intervals": [snap_interval],
        }
        if is_secondary:
            opts["is_secondary"] = True
        return opts

    pool_body = {
        "mode": config.get("mode", "image"),
        "mirrormode": config.get("mirrormode", "snapshot"),
        "peer_mode": config.get("peer_mode", "bootstrap"),
        "rbd_client": config.get("rbd_client", "client.admin"),
        "way": config.get("way", "two-way"),
    }
    for i in range(primary_total):
        pool_body[f"mirror_image_{random_string(len=5)}_{i}"] = _image_opts(
            is_secondary=False
        )
    for i in range(secondary_count):
        pool_body[f"mirror_image_sec_{random_string(len=5)}_{i}"] = _image_opts(
            is_secondary=True
        )

    return {poolname: pool_body}


def _list_mirror_image_names(pool_cfg):
    """Image keys under a pool entry (values are dicts with size)."""
    return [
        k
        for k, v in getdict(pool_cfg).items()
        if k != "test_config" and isinstance(v, dict) and v.get("size")
    ]


def configure_nvme_namespaces_for_mirrored_images(
    nvme_service, rbd_obj, poolname, image_list, subsystem_config
):
    """Configure NVMe namespaces for all mirrored images.

    Args:
        nvme_service: NVMeService instance
        rbd_obj: RBD object
        poolname: Pool name
        image_list: List of image names
        subsystem_config: Subsystem configuration
    """
    gateway = nvme_service.gateways[0]
    nqn = subsystem_config.get("nqn") or subsystem_config.get("subnqn")
    if not nqn:
        raise ValueError("Subsystem NQN not provided in subsystem_config")

    LOG.info(f"Adding namespaces for {len(image_list)} mirrored images")
    for imagename in image_list:
        namespace_args = {
            "args": {
                "subsystem": nqn,
                "rbd-pool": poolname,
                "rbd-image": imagename,
                "rbd-create-image": False,  # Image already exists
            }
        }
        gateway.namespace.add(**namespace_args)
        LOG.info(f"Added namespace for image {imagename}")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Main test execution function.

    Args:
        ceph_cluster: Ceph cluster object (will be called for each cluster in clusters section)
        **kwargs: Test configuration

    Returns:
        0 on success, 1 on failure
    """
    LOG.info("Starting NVMeoF RBD Mirror test")
    config = kwargs["config"]
    mirror_obj = None
    mirror_kwargs = None
    rbd_primary = None
    rbd_secondary = None
    poolname = config.get("poolname", "rbd")

    # For multi-cluster tests, we only want to run the test logic once
    # Run only when processing the first cluster (ceph-rbd1) to avoid duplicate execution
    current_cluster_name = ceph_cluster.name if hasattr(ceph_cluster, "name") else None

    # Only execute test logic when processing ceph-rbd1 (primary cluster)
    # This prevents the test from running twice when both clusters are specified
    if current_cluster_name and current_cluster_name != "ceph-rbd1":
        LOG.info(
            f"Skipping test execution for cluster {current_cluster_name}, will run on ceph-rbd1"
        )
        return 0

    try:
        # Get cluster configurations - determine primary and secondary automatically
        ceph_cluster_dict = kwargs.get("ceph_cluster_dict", {})

        # Primary cluster is the one being processed (ceph-rbd1)
        # Primary cluster is ceph-rbd1, secondary is ceph-rbd2
        primary_cluster = ceph_cluster_dict.get("ceph-rbd1")
        secondary_cluster = ceph_cluster_dict.get("ceph-rbd2")

        if not primary_cluster or not secondary_cluster:
            raise ValueError(
                "Clusters ceph-rbd1 and/or ceph-rbd2 not found in ceph_cluster_dict"
            )

        mirror_kwargs = deepcopy(kwargs)
        mirror_kwargs["ceph_cluster"] = primary_cluster
        mirror_kwargs["ceph_cluster_dict"] = ceph_cluster_dict
        mirror_kwargs["config"] = deepcopy(config)
        mirror_kwargs["config"]["do_not_run_io"] = True
        mirror_kwargs["config"]["rep_pool_config"] = (
            _build_rep_pool_config_for_nvme_mirror(mirror_kwargs["config"])
        )
        _rh = mirror_kwargs["config"].get("rhbuild", "5")
        ceph_version = int(str(_rh)[0])

        LOG.info("Configuring RBD snapshot-based mirroring via ceph.rbd.initial_config")
        mirror_obj = initial_mirror_config(**mirror_kwargs)
        mirror_obj.pop("output", None)
        client_primary = client_secondary = None
        primary_cluster = secondary_cluster = None
        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd_primary = val.get("rbd")
                client_primary = val.get("client")
                primary_cluster = val.get("cluster")
            else:
                rbd_secondary = val.get("rbd")
                client_secondary = val.get("client")
                secondary_cluster = val.get("cluster")

        _ensure_rbd_pool_cleanup(rbd_primary, client_primary, ceph_version)
        _ensure_rbd_pool_cleanup(rbd_secondary, client_secondary, ceph_version)

        pool_cfg_for_verify = mirror_kwargs["config"]["rep_pool_config"][poolname]
        created_images = _list_mirror_image_names(pool_cfg_for_verify)

        LOG.info(f"Created {len(created_images)} mirrored images: {created_images}")

        # Deploy NVMe service on primary cluster
        LOG.info("Deploying NVMe service on primary cluster")
        # Deep copy to avoid config sharing between clusters
        primary_nvme_config = deepcopy(config.get("primary_nvme_config", config))
        primary_nvme_config["rbd_pool"] = poolname
        check_and_set_nvme_cli_image(
            primary_cluster, config=kwargs.get("test_data", {}).get("custom-config")
        )
        primary_nvme_service = deploy_nvme_service_on_cluster(
            primary_cluster, primary_nvme_config, rbd_primary
        )

        # Deploy NVMe service on secondary cluster
        LOG.info("Deploying NVMe service on secondary cluster")
        # Deep copy to avoid config sharing between clusters
        secondary_nvme_config = deepcopy(config.get("secondary_nvme_config", config))
        secondary_nvme_config["rbd_pool"] = poolname
        check_and_set_nvme_cli_image(
            secondary_cluster, config=kwargs.get("test_data", {}).get("custom-config")
        )
        secondary_nvme_service = deploy_nvme_service_on_cluster(
            secondary_cluster, secondary_nvme_config, rbd_secondary
        )

        # Configure NVMe entities on primary cluster
        LOG.info("Configuring NVMe entities on primary cluster")
        primary_subsystem_config = primary_nvme_config.get("subsystems", [])
        if primary_subsystem_config:
            # configure_gw_entities adds subsystems, listeners, hosts, and namespaces
            # Since bdevs count is 0, no namespaces will be created by configure_namespaces
            # So we only need to add namespaces for mirrored images after this
            configure_gw_entities(
                primary_nvme_service, rbd_obj=rbd_primary, cluster=primary_cluster
            )

            # Add namespaces for all mirrored images to each subsystem
            for subsys_cfg in primary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    primary_nvme_service,
                    rbd_primary,
                    poolname,
                    created_images,
                    subsys_cfg,
                )

        # Configure NVMe entities on secondary cluster
        LOG.info("Configuring NVMe entities on secondary cluster")
        secondary_subsystem_config = secondary_nvme_config.get("subsystems", [])
        if secondary_subsystem_config:
            # configure_gw_entities adds subsystems, listeners, hosts, and namespaces
            # Since bdevs count is 0, no namespaces will be created by configure_namespaces
            # So we only need to add namespaces for mirrored images after this
            configure_gw_entities(
                secondary_nvme_service,
                rbd_obj=rbd_secondary,
                cluster=secondary_cluster,
            )

            # Add namespaces for all mirrored images to each subsystem
            for subsys_cfg in secondary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    secondary_nvme_service,
                    rbd_secondary,
                    poolname,
                    created_images,
                    subsys_cfg,
                )

        LOG.info("NVMeoF RBD Mirror test completed successfully")
        return 0

    except Exception as err:
        LOG.error(f"Test failed with error: {err}")
        import traceback

        LOG.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup if specified
        if config.get("cleanup"):
            LOG.info("Performing cleanup")
            try:
                if "primary_nvme_service" in locals() and rbd_primary is not None:
                    teardown(primary_nvme_service, rbd_primary)
                if "secondary_nvme_service" in locals() and rbd_secondary is not None:
                    teardown(secondary_nvme_service, rbd_secondary)
            except Exception as cleanup_err:
                LOG.warning(f"Cleanup error: {cleanup_err}")
