import re
from json import loads

from ceph.waiter import WaitUntil
from cephci.prereq import setup_private_container_registry
from cephci.utils.configure import get_private_registry_image, setup_ssh_keys
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, NotSupportedError, OperationFailedError
from cli.utilities.containers import Container, Registry
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

PRIVATE_REG_USER = "myregistryusername"
PRIVATE_REG_PASS = "myregistrypassword"
PORT = "5000"


class ClusterCustomContainerError(Exception):
    pass


def get_registry_details(config):
    """Get registry and registry credentials
    Args:
        config (dict): Config parameters
    """
    build_type = "ibm" if config.get("ibm_build") else "rh"
    con = get_cephci_config()
    reg_cred = con.get(f"{build_type}_registry_credentials", {})
    return reg_cred.get("registry"), reg_cred.get("username"), reg_cred.get("password")


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get config file and node
    config = kw.get("config")

    # Get the rhbuild
    rhbuild = config.get("rhbuild")
    if rhbuild.startswith("8"):
        rhbuild = "squid"
    elif rhbuild.startswith("7"):
        rhbuild = "reef"
    elif rhbuild.startswith("6"):
        rhbuild = "quincy"
    elif rhbuild.startswith("5"):
        rhbuild = "pacific"
    else:
        raise NotSupportedError(f"Unsupported version: {rhbuild}")

    # Get dashboard image stack
    registry_image = config.get("dashboard_images")
    if not registry_image:
        raise ConfigError("Dashboard images are not provided")

    # Get registry credentials
    registry, reg_username, reg_password = get_registry_details(config)

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Define CephAdm class object
    cephadm = CephAdm(installer)

    # Get cluster nodes
    ceph_nodes = kw.get("ceph_nodes")

    # Get dashboard images
    dashboard_images = registry_image.values()

    # Get registry docker image
    docker_reg_image = config.get("registry_image").get("docker_registry_image")

    # Setup ssh key to all nodes
    setup_ssh_keys(installer, ceph_nodes)

    # Setup a private registry on installer node
    setup_private_container_registry(
        installer=installer,
        nodes=ceph_nodes,
        registry=registry,
        reg_username=reg_username,
        reg_password=reg_password,
        build_type=rhbuild,
        private_reg_username=PRIVATE_REG_USER,
        private_reg_password=PRIVATE_REG_PASS,
        docker_reg_image=docker_reg_image,
        images=dashboard_images,
    )

    # Get private registry catalog
    out = get_private_registry_image(
        installer, username=PRIVATE_REG_USER, password=PRIVATE_REG_PASS
    )
    images = out.get("repositories")

    # Pull images to nodes
    for ceph in ceph_nodes:
        Registry(ceph).login(
            registry=f"{installer.hostname}:{PORT}",
            username=PRIVATE_REG_USER,
            password=PRIVATE_REG_PASS,
        )
        for img in images:
            Container(ceph).pull(image=f"{installer.hostname}:{PORT}/{img}")

    # Define a regex for registry
    regex = r"(.*(.com|.io))\/(.*):(.*)"

    for config_key, image in registry_image.items():
        out = re.search(regex, image)
        if not out:
            raise ConfigError(f"{image} is not a valid image")

        # Set mgr config for dashboard images
        conf = {
            "key": "mgr",
            "value": f"mgr/cephadm/{config_key} {installer.hostname}:{PORT}/{out[3]}:{out[4]}",
        }
        cephadm.ceph.config.set(**conf)

        # Get mgr config for dashboard images
        conf = {
            "who": "mgr",
            "key": f"mgr/cephadm/{config_key}",
        }
        out = cephadm.ceph.config.get(**conf)[0]

        # Validate if dashboard images are updated correctly
        if installer.hostname not in out:
            raise OperationFailedError(
                f"Failed to set custom image mgr/cephadm/{config_key}"
            )

    for config_key, _ in registry_image.items():
        service = config_key.replace("container_image_", "")
        service = service.replace("_", "-")

        # Redeploy dashboard services
        conf = {"service": service}
        cephadm.ceph.orch.redeploy(**conf)

        timeout, interval = 30, 5
        for w in WaitUntil(timeout=timeout, interval=interval):
            conf = {"refresh": True}
            cephadm.ceph.orch.ps(**conf)

            # Verify dashboard services updated with private images
            service_info = loads(
                cephadm.ceph.orch.ps(service_name=service, format="json")
            )
            image = [c.get("container_image_name") for c in service_info][0]
            status = [c.get("status_desc") for c in service_info][0]
            if status == "running" and installer.hostname in image:
                log.info(
                    f"{service} has been re-deployed with custom-image {image} successfully"
                )
                break
            log.info(
                f"Service '{service}' is not in expected '{status}' state, retry after {interval} sec"
            )

        if w.expired:
            raise ClusterCustomContainerError(
                f"Monitor service deployment failed for {service}"
            )

    return 0
