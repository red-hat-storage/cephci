"""Resolution of the build under test.

The product, release, platform and build type select a section of the Ceph test
manifest, which states the compose repository and the container image to be used. The
command line can override any of the resolved artifacts.
"""

import re
from dataclasses import dataclass
from typing import List, Optional

from cephci.orchestration.config import RunConfig
from cephci.utils.build_info import CephTestManifest
from cephci.utils.utility import get_html_page
from utility.log import Log
from utility.utils import check_build_overrides

log = Log(__name__)

# Build types that do not publish a compose whose package list can be scraped.
UNSCRAPEABLE_BUILDS = ["released", "cvp", "upstream", None]


@dataclass(frozen=True)
class BuildArtifacts:
    """The artifacts a run installs and the version they carry."""

    manifest: CephTestManifest
    build: Optional[str]
    base_url: Optional[str]
    docker_registry: Optional[str]
    docker_image: Optional[str]
    docker_tag: Optional[str]
    ceph_version: str

    @property
    def container_image(self) -> str:
        """The fully qualified container image reference."""
        return f"{self.docker_registry}/{self.docker_image}:{self.docker_tag}"


def effective_build(config: RunConfig) -> Optional[str]:
    """Return the manifest section to read.

    A released build is not always published. Rather than failing the run, fall back to
    the nightly section, which is the long standing behaviour.

    Args:
        config (RunConfig): The run settings.

    Returns:
        str: The build type to use.
    """
    if config.build != "released":
        return config.build

    try:
        manifest = CephTestManifest(
            config.product, config.release, "released", config.platform
        )
        _ = manifest.build_info
    except RuntimeError:
        log.debug("Switching to nightly as released section is unavailable.")
        return "nightly"

    return config.build


def scrape_ceph_version(
    base_url: Optional[str], cloud_type: Optional[str]
) -> List[str]:
    """Read the Ceph version from the package listing of a compose.

    Args:
        base_url (str): Location of the compose under test.
        cloud_type (str): The infrastructure in use, which decides the layout of the
            compose.

    Returns:
        list: The versions found, which is empty when the listing is unavailable.
    """
    version_url = f"{base_url}/compose/Tools/x86_64/os/Packages/"

    # Incase we are executing from other environments
    # For now, only switch if type is IBMC
    if cloud_type and cloud_type.startswith("ibmc"):
        version_url = f"{base_url}/Tools/Packages/"

    listing = get_html_page(url=version_url)
    search_results = re.search(r"ceph-common-(.*?).x86", listing)

    return [search_results.group(1)] if search_results else []


def resolve(config: RunConfig) -> BuildArtifacts:
    """Determine the artifacts to be installed by this run.

    Args:
        config (RunConfig): The run settings.

    Returns:
        BuildArtifacts: The compose, container image and version under test.
    """
    build = effective_build(config)
    manifest = CephTestManifest(
        config.product, config.release, build, config.platform, crimson=config.crimson
    )

    base_url = config.rhs_ceph_repo
    docker_registry = config.docker_registry
    docker_image = config.docker_image
    docker_tag = config.docker_tag

    if not check_build_overrides(base_url, docker_registry, docker_image, docker_tag):
        # In case the overrides are not valid, we are switching them to other inputs.
        base_url = manifest.repository
        docker_registry = manifest.ceph_image_dtr
        docker_image = manifest.ceph_image_path
        docker_tag = manifest.ceph_image_tag

    versions = [manifest.ceph_version]
    if build not in UNSCRAPEABLE_BUILDS:
        versions.extend(scrape_ceph_version(base_url, config.cloud_type))

    if build == "upstream":
        versions.append(config.upstream_build)

    return BuildArtifacts(
        manifest=manifest,
        build=build,
        base_url=base_url,
        docker_registry=docker_registry,
        docker_image=docker_image,
        docker_tag=docker_tag,
        ceph_version=", ".join(list(set(versions))),
    )
