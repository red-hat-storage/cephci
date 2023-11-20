from cli.utilities.containers import Container
from cli.utilities.packages import Package, PackageError, ReposError, Rpm
from cli.utilities.utils import get_custom_repo_url

YUM_REPO_DIR = "/etc/yum.repos.d"
REG_IMAGE = "registry.redhat.io/openshift4/{}:latest"


def compare_packages(node, pkgs):
    """Compare packages
    Args:
        node (ceph): ceph node objects
        pkgs (dict): packages and version
    """
    # Validate package version
    for pkg, ver in pkgs.items():
        if Package(node).compare(pkg, ver) == -1:
            raise PackageError(f"Expected version for {pkg} is {ver}")


def query_packages(node, pkgs):
    """Query packages to validate if package is installed
    Args:
        node (ceph): ceph node objects
        pkgs (dict): packages
    """
    # validate required packages
    for pkg, _ in pkgs.items():
        if not Rpm(node).query(pkg):
            raise PackageError(f"Required package {pkg} is not installed")


def install_packages(node, pkgs):
    """Install packages on node
    Args:
        node (ceph): ceph node objects
        pkgs (dict): packages
    """
    # Install required packages
    for pkg, _ in pkgs.items():
        if not Rpm(node).query(pkg):
            Package(node).install(pkg, nogpgcheck=True)


def remove_packages(node, pkgs):
    """remove packages from node
    Args:
        node (ceph): ceph node objects
        pkgs (dict): packages
    """
    # Install required packages
    for pkg, _ in pkgs.items():
        Package(node).remove(pkg)


def pull_images(node, images):
    """pull container images to node
    Args:
        node (ceph): ceph node objects
        images (dict): container images
    """
    for image, _ in images.items():
        Container(node).pull(REG_IMAGE.format(image))


def compare_images(node, images):
    """compare container images version
    Args:
        node (ceph): ceph node objects
        images (dict): container images
    """
    for image, ver in images.items():
        if Container(node).compare(REG_IMAGE.format(image), ver) == -1:
            raise PackageError(
                f"Expected version for {REG_IMAGE.format(image)} is {ver}"
            )


def validate_packages(node, pkgs):
    """Install and validate package version
    Args:
        node (ceph): ceph node objects
        pkgs (dict): packages and versions
    """
    try:
        # Install packages
        install_packages(node, pkgs)

        # Compare installed package version
        compare_packages(node, pkgs)

        # Check client additional package "ceph-diff-sorted" and "rgw-orphan-list"
        if "ceph-common" in pkgs:
            out = node.exec_command(
                sudo=True,
                cmd="ls -l /usr/bin/rgw-or* /usr/bin/ceph-diff*",
            )[0]
            if "rwxr-xr-x" not in out:
                raise PackageError(
                    "ceph-common not have additional package 'ceph-diff-sorted' and 'rgw-orphan-list'"
                )
    finally:
        # Remove installed packages
        remove_packages(node, pkgs)


def validate_images(node, images):
    """Validate the monitoring images
    Args:
        node (ceph): ceph node objects
        pkgs (dict): monitoring images and version
    """

    # Pull monitoring stack images
    pull_images(node, images)

    # validate expected image version
    compare_images(node, images)


def run(ceph_cluster, **kwargs):
    """Verify package version validation
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """
    # Get installer and client node
    client = ceph_cluster.get_nodes(role="client")[0]
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get config details
    config = kwargs.get("config")
    base_url = config.get("base_url")
    cloud_type = config.get("cloud_type")

    # Get expected package versions from config
    client_pkgs = config.get("client_packages")
    dependent_pkgs = config.get("dependent_packages")
    monitoring_images = config.get("monitoring_images")
    default_packages = config.get("default_packages")
    server_pkgs = config.get("server_packages")

    # Enable ceph tools repo on installer and client
    base_url = get_custom_repo_url(base_url, cloud_type)
    for node in {installer, client}:
        if not Package(node).add_repo(base_url):
            raise ReposError(f"Failed to enable {base_url} repo")

    # Get the Tools repo file name
    out = installer.get_dir_list(dir_path=YUM_REPO_DIR)
    file_name = [repo for repo in out if "Tools" in repo][0]

    try:
        # Validate if requied packages are installed
        query_packages(installer, default_packages)

        # Install and validate server packages
        validate_packages(installer, server_pkgs)

        # validate dependent package version
        compare_packages(installer, dependent_pkgs)

        # Install and validate client packages
        validate_packages(client, client_pkgs)

        # validate expected image version
        validate_images(installer, monitoring_images)

    finally:
        # Remove repo file from node
        installer.remove_file(file_path=f"{YUM_REPO_DIR}/{file_name}")

        # Remove yum cache
        for node in (installer, client):
            Package(node).clean()

    return 0
