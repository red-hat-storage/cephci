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
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from tests.rbd_mirror.rbd_mirror_utils import create_mirrored_images_with_user_specified
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def deploy_nvme_service_on_cluster(ceph_cluster, config, rbd_obj):
    """Deploy NVMe service on a cluster and configure gateway entities.

    Args:
        ceph_cluster: Ceph cluster object
        config: Test configuration
        rbd_obj: RBD object for the cluster

    Returns:
        NVMeService instance
    """
    LOG.info(f"Deploying NVMe service on cluster {ceph_cluster.name}")
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    LOG.info(f"NVMe service deployed successfully on cluster {ceph_cluster.name}")
    return nvme_service


def create_mirrored_images_with_snapshot_schedule(
    primary_mirror,
    secondary_mirror,
    poolname,
    image_config,
    **kw,
):
    """Create mirrored images on both clusters with snapshot schedules.

    Args:
        primary_mirror: RbdMirror instance for primary cluster
        secondary_mirror: RbdMirror instance for secondary cluster
        poolname: Pool name
        image_config: Configuration for images to create
        **kw: Additional keyword arguments

    Returns:
        List of created image names
    """
    image_count = image_config.get("count", 2)
    imagesize = image_config.get("size", "2G")
    snapshot_interval = image_config.get("snapshot_interval", "1h")
    mirrormode = kw.get("mirrormode", "snapshot")
    created_images = []

    # Create images on primary cluster
    for i in range(image_count):
        imagename = f"mirror_image_{generate_unique_id(length=4)}_{i}"
        imagespec = f"{poolname}/{imagename}"

        # Create image on primary cluster
        LOG.info(f"Creating image {imagename} on primary cluster")
        primary_mirror.create_image(imagespec=imagespec, size=imagesize)

        # Enable mirroring on the image
        primary_mirror.enable_mirror_image(poolname, imagename, mirrormode)

        # Add snapshot schedule using RbdMirror method
        LOG.info(f"Adding snapshot schedule for image {imagename}")
        primary_mirror.mirror_snapshot_schedule_add(
            poolname=poolname, imagename=imagename, interval=snapshot_interval
        )

        # Note: verify_snapshot_schedule waits for the interval duration, so we skip it
        # for additional images to avoid long test execution times
        # The schedule is verified for the first image in the main test flow

        created_images.append(imagename)

        # Wait for image to be mirrored to secondary
        LOG.info(f"Waiting for image {imagename} to be mirrored to secondary cluster")
        primary_mirror.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        secondary_mirror.wait_for_status(
            imagespec=imagespec, state_pattern="up+replaying"
        )

    # Create images on secondary cluster as well
    for i in range(image_config.get("secondary_count", 0)):
        imagename = f"mirror_image_sec_{generate_unique_id(length=4)}_{i}"
        imagespec = f"{poolname}/{imagename}"

        # Create image on secondary cluster
        LOG.info(f"Creating image {imagename} on secondary cluster")
        secondary_mirror.create_image(imagespec=imagespec, size=imagesize)

        # Enable mirroring on the image
        secondary_mirror.enable_mirror_image(poolname, imagename, mirrormode)

        # Add snapshot schedule using RbdMirror method
        LOG.info(f"Adding snapshot schedule for image {imagename}")
        secondary_mirror.mirror_snapshot_schedule_add(
            poolname=poolname, imagename=imagename, interval=snapshot_interval
        )

        # Note: verify_snapshot_schedule waits for the interval duration, so we skip it
        # for additional images to avoid long test execution times

        created_images.append(imagename)

        # Wait for image to be mirrored to primary
        LOG.info(f"Waiting for image {imagename} to be mirrored to primary cluster")
        secondary_mirror.wait_for_status(
            imagespec=imagespec, state_pattern="up+stopped"
        )
        primary_mirror.wait_for_status(
            imagespec=imagespec, state_pattern="up+replaying"
        )

    return created_images


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

        # Initialize RBD configurations for both clusters
        LOG.info("Initializing RBD configurations")
        primary_rbd_config = initial_rbd_config(ceph_cluster=primary_cluster, **kwargs)
        secondary_rbd_config = initial_rbd_config(
            ceph_cluster=secondary_cluster, **kwargs
        )

        # Get RBD pool objects for pool operations
        primary_rbd_obj = primary_rbd_config.get("rbd_reppool")
        secondary_rbd_obj = secondary_rbd_config.get("rbd_reppool")

        poolname = config.get("poolname", "rbd")
        mirrormode = config.get("mirrormode", "snapshot")
        mode = config.get("mode", "image")
        imagesize = config.get("image_config", {}).get("size", "2G")

        # Create RbdMirror instances
        primary_mirror = rbdmirror.RbdMirror(primary_cluster, config)
        secondary_mirror = rbdmirror.RbdMirror(secondary_cluster, config)

        # Configure RBD mirroring using initial_mirror_config (similar to test_configure_mirror_snapshot.py)
        # This will create pools, configure mirroring, and create the first image
        LOG.info("Configuring RBD snapshot-based mirroring between clusters")
        first_imagename = f"mirror_image_{primary_mirror.random_string(len=5)}"

        # Prepare kwargs for initial_mirror_config - it needs ceph_cluster and ceph_cluster_dict
        mirror_kwargs = kwargs.copy()
        mirror_kwargs["ceph_cluster"] = primary_cluster
        mirror_kwargs["ceph_cluster_dict"] = ceph_cluster_dict

        primary_mirror.initial_mirror_config(
            secondary_mirror,
            poolname=poolname,
            imagename=first_imagename,
            imagesize=imagesize,
            io_total=None,  # No IO execution - keep images fresh for other tests
            mode=mode,
            mirrormode=mirrormode,
            peer_mode=config.get("peer_mode", "bootstrap"),
            rbd_client=config.get("rbd_client", "client.admin"),
            build=config.get("build"),
            **mirror_kwargs,
        )

        # Add snapshot schedule for the first image
        primary_mirror.mirror_snapshot_schedule_add(
            poolname=poolname,
            imagename=first_imagename,
            interval=config.get("image_config", {}).get("snapshot_interval", "1h"),
        )
        primary_mirror.verify_snapshot_schedule(
            imagespec=f"{poolname}/{first_imagename}"
        )

        # Create additional mirrored images with snapshot schedules
        LOG.info("Creating additional mirrored images with snapshot schedules")
        image_config = config.get("image_config", {"count": 2, "size": "2G"})
        # Adjust count since we already created the first image
        image_config["count"] = max(0, image_config.get("count", 2) - 1)
        additional_images = create_mirrored_images_with_snapshot_schedule(
            primary_mirror,
            secondary_mirror,
            poolname,
            image_config,
            mirrormode=mirrormode,
            **mirror_kwargs,
        )

        # Combine all created images
        created_images = [first_imagename] + additional_images

        # Create user-specified count of mirrored images (if configured)
        if config.get("image_count"):
            config["image-count"] = config.get("image_count")
            create_mirrored_images_with_user_specified(**mirror_kwargs)

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
            primary_cluster, primary_nvme_config, primary_rbd_obj
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
            secondary_cluster, secondary_nvme_config, secondary_rbd_obj
        )

        # Configure NVMe entities on primary cluster
        LOG.info("Configuring NVMe entities on primary cluster")
        primary_subsystem_config = primary_nvme_config.get("subsystems", [])
        if primary_subsystem_config:
            # configure_gw_entities adds subsystems, listeners, hosts, and namespaces
            # Since bdevs count is 0, no namespaces will be created by configure_namespaces
            # So we only need to add namespaces for mirrored images after this
            configure_gw_entities(
                primary_nvme_service, rbd_obj=primary_rbd_obj, cluster=primary_cluster
            )

            # Add namespaces for all mirrored images to each subsystem
            for subsys_cfg in primary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    primary_nvme_service,
                    primary_rbd_obj,
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
                rbd_obj=secondary_rbd_obj,
                cluster=secondary_cluster,
            )

            # Add namespaces for all mirrored images to each subsystem
            for subsys_cfg in secondary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    secondary_nvme_service,
                    secondary_rbd_obj,
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
                if "primary_nvme_service" in locals():
                    teardown(primary_nvme_service, primary_rbd_obj)
                if "secondary_nvme_service" in locals():
                    teardown(secondary_nvme_service, secondary_rbd_obj)
            except Exception as cleanup_err:
                LOG.warning(f"Cleanup error: {cleanup_err}")
