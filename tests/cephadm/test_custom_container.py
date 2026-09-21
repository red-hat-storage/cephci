from json import loads
from shlex import join

from ceph.waiter import WaitUntil
from cephci.utils.configure import (
    DOMAIN_CERT_PATH_CERT,
    REGISTRY_CERTS,
    SKOPEO_IMAGE,
    add_cert_to_trusted_list,
    copy_cert_to_secondary_node,
    create_link_to_domain_cert,
    create_registry_directories,
    create_self_signed_certificate,
    get_private_registry_image,
    set_registry_credentials,
    setup_ssh_keys,
    start_local_private_registry,
)
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, NotSupportedError, OperationFailedError
from cli.utilities.containers import Container, Registry
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

PRIVATE_REG_USER = "myregistryusername"
PRIVATE_REG_PASS = "myregistrypassword"
PORT = "5000"
MONITORING_SERVICES = ("node_exporter", "grafana", "prometheus", "alertmanager")


class ClusterCustomContainerError(Exception):
    pass


def get_registry_image(image, registry):
    """Qualify an image with the default registry unless it already has a host."""
    if not isinstance(image, str) or not image.strip():
        raise ConfigError("A container image must be provided")

    image = image.strip()
    host, separator, _ = image.partition("/")
    if separator and ("." in host or ":" in host or host == "localhost"):
        return image
    if not registry:
        raise ConfigError(f"A registry is required for image '{image}'")
    return f"{registry.rstrip('/')}/{image}"


def setup_private_container_registry(
    installer,
    nodes,
    registry,
    reg_username,
    reg_password,
    build_type,
    private_reg_username,
    private_reg_password,
    docker_reg_image,
    images=None,
):
    """Set up a local private container registry on the installer node.

    Performs all steps required for a disconnected install: creates directories,
    configures credentials, generates a self-signed TLS certificate, distributes
    the certificate to every cluster node, starts the registry container, and
    copies the requested images into it.

    Args:
        installer (ceph.ceph.Ceph): Installer / registry host node
        nodes (list): All cluster nodes that need access to the registry
        registry (str): Source registry hostname (e.g. registry.redhat.io)
        reg_username (str): Username for the source registry
        reg_password (str): Password for the source registry
        build_type (str): Ceph build type (e.g. "reef", "squid")
        private_reg_username (str): Username for the private registry
        private_reg_password (str): Password for the private registry
        docker_reg_image (str): Container image used to run the registry daemon
        images (iterable, optional): Images to mirror into the private registry
    """
    # Step 1: Create folders for the private registry
    if not create_registry_directories(installer):
        return False

    # Step 2: Create credentials for accessing the private registry
    if not set_registry_credentials(
        installer, private_reg_username, private_reg_password
    ):
        return False

    # Step 3: Create a self-signed TLS certificate
    if not create_self_signed_certificate(installer):
        return False

    # Step 4: Symlink domain.cert so skopeo can locate the certificate
    if not create_link_to_domain_cert(installer):
        return False

    # Step 5: Add the certificate to the trusted list on the registry node
    if not add_cert_to_trusted_list(installer):
        return False

    # Step 6: Distribute the certificate to all other cluster nodes and update trust
    if not copy_cert_to_secondary_node(installer, nodes):
        return False

    # Step 7: Authenticate with the source registry
    Registry(installer).login(
        registry=registry, username=reg_username, password=reg_password
    )

    # Step 8: Start the local secure private registry container
    if not start_local_private_registry(installer, docker_reg_image):
        return False

    # Step 9: Keep the discovered source registry and full image reference.
    volumes = [
        f"{REGISTRY_CERTS}:/certs:Z",
        f"{DOMAIN_CERT_PATH_CERT}:/certs/domain.cert:Z",
    ]
    for image in images:
        source = get_registry_image(image, registry)
        source_registry, repository = source.split("/", 1)
        destination = f"{installer.hostname}:{PORT}/{repository}"
        args = [
            "skopeo",
            "copy",
            "--remove-signatures",
            "--dest-cert-dir=./certs/",
            "--dest-creds",
            f"{private_reg_username}:{private_reg_password}",
        ]
        if source_registry == registry.rstrip("/") and reg_username and reg_password:
            args.extend(["--src-creds", f"{reg_username}:{reg_password}"])
        if "@" in source:
            # Preserve the manifest/index addressed by a digest.
            args.extend(["--all", "--preserve-digests"])
        args.extend([f"docker://{source}", f"docker://{destination}"])
        Container(installer).run(
            long_running=False,
            volume=volumes,
            rm=True,
            image=SKOPEO_IMAGE,
            cmds=join(args),
        )

    # Step 10: Confirm images are visible in the private registry
    if not get_private_registry_image(
        installer, private_reg_username, private_reg_password
    ):
        return False

    log.info("Private registry setup successfully for monitoring images")
    return True


def get_registry_details(config):
    """Get registry and registry credentials
    Args:
        config (dict): Config parameters
    """
    build_type = "ibm" if config.get("ibm_build") else "rh"
    con = get_cephci_config()
    reg_cred = con.get(f"{build_type}_registry_credentials", {})
    return reg_cred.get("registry"), reg_cred.get("username"), reg_cred.get("password")


def get_dashboard_images(cephadm, config):
    """Resolve monitoring images from mgr configuration, honoring suite overrides."""
    images = dict(config.get("dashboard_images") or {})
    for service in MONITORING_SERVICES:
        key = f"container_image_{service}"
        if key not in images:
            images[key] = cephadm.ceph.config.get(who="mgr", key=f"mgr/cephadm/{key}")[
                0
            ].strip()
        if not isinstance(images[key], str) or not images[key].strip():
            raise ConfigError(f"No monitoring image configured for mgr/cephadm/{key}")
        images[key] = images[key].strip()
    return images


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images

    Monitoring images default to the cluster's mgr/cephadm configuration.
    Individual images can be overridden using config.dashboard_images.
    config.registry_image.docker_registry_image supplies the registry server image.

    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get config file and node
    config = kw.get("config")

    # Get the rhbuild
    rhbuild = config.get("rhbuild")
    if rhbuild.startswith("9"):
        rhbuild = "tentacle"
    elif rhbuild.startswith("8"):
        rhbuild = "squid"
    elif rhbuild.startswith("7"):
        rhbuild = "reef"
    elif rhbuild.startswith("6"):
        rhbuild = "quincy"
    elif rhbuild.startswith("5"):
        rhbuild = "pacific"
    else:
        raise NotSupportedError(f"Unsupported version: {rhbuild}")

    # Get registry credentials
    registry, reg_username, reg_password = get_registry_details(config)

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Define CephAdm class object
    cephadm = CephAdm(installer)

    # Resolve images before replacing mgr configuration with private references.
    registry_image = get_dashboard_images(cephadm, config)
    private_images = {
        key: f"{installer.hostname}:{PORT}/{get_registry_image(image, registry).split('/', 1)[1]}"
        for key, image in registry_image.items()
    }

    # Get cluster nodes
    ceph_nodes = kw.get("ceph_nodes")

    # Snapshot fully-qualified source images as a concrete list.
    dashboard_images = [
        get_registry_image(image, registry) for image in registry_image.values()
    ]

    # Get registry docker image
    docker_reg_image = (config.get("registry_image") or {}).get("docker_registry_image")
    if not docker_reg_image:
        raise ConfigError("registry_image.docker_registry_image is required")

    # Setup ssh key to all nodes
    setup_ssh_keys(installer, ceph_nodes)

    # Setup a private registry on installer node
    if not setup_private_container_registry(
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
    ):
        raise OperationFailedError("Failed to set up the private container registry")

    # Pull images to nodes
    for ceph in ceph_nodes:
        Registry(ceph).login(
            registry=f"{installer.hostname}:{PORT}",
            username=PRIVATE_REG_USER,
            password=PRIVATE_REG_PASS,
        )
        for image in private_images.values():
            Container(ceph).pull(image=image)

    for config_key, image in private_images.items():
        # Set mgr config for dashboard images
        conf = {
            "key": "mgr",
            "value": f"mgr/cephadm/{config_key} {image}",
        }
        cephadm.ceph.config.set(**conf)

        # Get mgr config for dashboard images
        conf = {
            "who": "mgr",
            "key": f"mgr/cephadm/{config_key}",
        }
        out = cephadm.ceph.config.get(**conf)[0]

        # Validate if dashboard images are updated correctly
        if out.strip() != image:
            raise OperationFailedError(
                f"Failed to set custom image mgr/cephadm/{config_key}"
            )

    for config_key, _ in registry_image.items():
        service = config_key.replace("container_image_", "")
        service = service.replace("_", "-")

        # Redeploy dashboard services
        conf = {"service": service}
        cephadm.ceph.orch.redeploy(**conf)

        timeout, interval = config.get("timeout", 300), 10
        for w in WaitUntil(timeout=timeout, interval=interval):
            # Verify dashboard services updated with private images
            service_info = loads(
                cephadm.ceph.orch.ps(service_name=service, format="json", refresh=True)
            )
            if service_info and all(
                daemon.get("status_desc") == "running"
                and (daemon.get("container_image_name") or "").startswith(
                    f"{installer.hostname}:{PORT}/"
                )
                for daemon in service_info
            ):
                log.info(
                    f"All {service} daemons have been re-deployed with private images"
                )
                break
            log.info(
                f"Waiting for all {service} daemons to run with private images; "
                f"retry after {interval} sec: {service_info}"
            )

        if w.expired:
            raise ClusterCustomContainerError(
                f"Monitor service deployment failed for {service}"
            )

    return 0
