# -*- code: utf-8 -*-
"""Functions processing the ceph manifest file."""

from copy import deepcopy
from typing import Any, Dict, Optional

import requests
import yaml

from utility.log import Log
from utility.utils import get_cephci_config

logger = Log(__name__)


class CephTestManifest:
    """Class holding the test artifact information."""

    URI: str = (
        "https://raw.githubusercontent.com/ibmstorage/qe-ceph-manifest/refs/heads/main/"
    )
    SUPPORTED_PRODUCTS = ["community", "redhat", "ibm"]
    RELEASE_MAP = {"reef": 7, "squid": 8, "tentacle": 9}

    def __init__(
        self,
        product: str,
        release: str,
        build_type: str,
        platform: str,
        datacenter: Optional[str] = None,
    ) -> None:
        """Instance initialization.

        Args:
            product     The product can be community, redhat or ibm
            release     Refers the version of the product.
            build_type  Refers to section in the manifest file to be read.
            platform    The base operating system.
            datacenter  The location where the tests are being executed.
        """
        if product not in self.SUPPORTED_PRODUCTS:
            raise RuntimeError("Unsupported product")

        self.product: str = product
        self._release: str = release
        self._build_type: str = build_type
        self._platform: str = platform

        if datacenter is None:
            # Retrieve from config
            try:
                conf_dict = get_cephci_config()
                datacenter = conf_dict.get("datacenter", "default")
            except IOError:
                datacenter = "default"
        elif datacenter == "redhat":
            logger.debug("Using default as redhat is the datcenter.")
            datacenter = "default"

        self.datacenter = datacenter

    @property
    def platform(self) -> str:
        return self._platform

    @platform.setter
    def platform(self, platform: str) -> None:
        self._platform = platform

    @property
    def release(self) -> str:
        if self.product == "community":
            return self.RELEASE_MAP[self._release]

        return self._release

    @release.setter
    def release(self, release: str) -> None:
        self._release = release

    @property
    def build_type(self) -> None:
        # For backward compatability
        if self._build_type == "latest":
            return "nightly"

        if self._build_type == "sanity":
            return "stable"

        return self._build_type

    @build_type.setter
    def build_type(self, build_type: str) -> None:
        self._build_type = build_type

    @property
    def build_info(self) -> dict[str, Any]:
        return self.get_build_details()

    @property
    def ceph_version(self) -> str:
        return self.build_info["version"]

    @property
    def repository(self) -> Optional[str]:
        try:
            return self.build_info["repositories"][self.datacenter][self.platform]
        except KeyError:
            return None

    @property
    def repo_id(self) -> Optional[str]:
        if self.product != "redhat":
            return None

        return self.build_info["repo_ids"][self.platform]

    @property
    def images(self) -> dict[str, str]:
        return self.get_build_details()["images"]

    @property
    def ceph_image(self) -> str:
        return self.build_info["images"]["ceph-base"]

    @property
    def ceph_image_dtr(self) -> str:
        return self.ceph_image.split("/")[0]

    @property
    def ceph_image_tag(self) -> str:
        return self.ceph_image.split(":")[-1]

    @property
    def ceph_image_path(self) -> str:
        _image_fqdn = self.ceph_image
        _image_without_dtr = _image_fqdn.split("/", 1)[1]
        return _image_without_dtr.split(":")[0]

    @property
    def nvme_cli_image(self) -> str:
        return self.images.get("nvmeof_cli_image", "")

    @property
    def custom_images(self) -> dict[str, str]:
        """Custom images to be configured with the cluster.

        Parses through the images and removes the following
            - ceph-base
            - cephcsi
            - nvmeof-cli
        """
        remove_list = [
            "cephcsi",
            "nvmeof-cli",
            "crimson",
        ]
        rst = {}
        _images = deepcopy(self.images)

        # Remove ceph-base
        _images.pop("ceph-base")

        for image_key, image_value in _images.items():
            for rl in remove_list:
                if rl in image_value:
                    break
            else:
                rst.update({image_key: image_value})

        return rst

    def get_build_details(self) -> dict[str, Any]:
        """Retrieves build details from QE Ceph Manifest repository.

        The repository can be navigated using product/ceph-version. With the help
        of build, the build details are retrieved.

        For example: - redhat/8.1.yaml {nightly}

        Raises:

        Returns:
            Build details of the requested section. For example

            {
            "version": "19.1.2-100",
            "repositories": {
                "default": {
                    "rhel-9": "http://rpm/repo/product/ceph/verion/"
                }
            },
            "images": {
                "ceph-base": "quay.io/rhceph-ci/ceph@sha256:adgajsalkd",
                "snmp_image": "quay.io/rhceph-ci/snmp-notifier:@sha256"
            }
            }
        """
        # Please ensure to use _release instead of release, since release is a
        # property and can be overridden.
        _msg = f"Retreving build details of {self.product} - {self._release}. "
        _msg += f"Looking up {self.build_type} section."
        logger.debug(_msg)

        manifest_file: str = f"{self._release}.yaml"
        manifest_url: str = self.URI

        if self.product.lower() == "community":
            manifest_url += f"ceph/{manifest_file}"
        elif self.product.lower() == "ibm":
            manifest_url += f"ibm/{manifest_file}"
        else:
            manifest_url += f"redhat/{manifest_file}"

        try:
            data: requests.Response = requests.get(manifest_url, verify=False)
        except requests.RequestException as e:
            raise RuntimeError(
                "Unable to download the Ceph QE manifest file %s \n%s", manifest_url, e
            )

        try:
            yml_data: Dict[str, Any] = yaml.safe_load(data.text)
            build_data = yml_data[self.build_type]

            # Convert image digests from '@sha256:<digest>' format to ':<digest>' format if datacenter is 'eu-de'
            if self.datacenter == "eu-de":
                images = build_data.get("images", {})
                for k, v in images.items():
                    images[k] = v.replace("@sha256:", ":")

            return build_data

        except yaml.YAMLError:
            raise RuntimeError("Unable to process the Ceph QE manifest file")
        except KeyError:
            raise RuntimeError("Unknown build_type provided.")
