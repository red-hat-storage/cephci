import json

from ceph.rbd.utils import check_data_integrity, random_string
from utility.log import Log

log = Log(__name__)


def compare_image_size_primary_secondary(rbd_primary, rbd_secondary, image_spec_list):
    """
    Compare image sizes using rbd du on both primary and secondary cluster.
    Args:
        rbd_primary: Rbd object for primary cluster
        rbd_secondary: Rbd object for secondary cluster
        image_spec_list: list of images in spec format '<pool_name>/<image_list>'
    """
    for spec in list(json.loads(image_spec_list)):
        if "namespace" in spec.keys() and spec["namespace"] != "":
            image_spec = spec["pool"] + "/" + spec["namespace"] + "/" + spec["image"]
        else:
            image_spec = spec["pool"] + "/" + spec["image"]
        image_config = {"image-spec": image_spec}
        rbd_du = rbd_primary.image_usage(**image_config, format="json")
        for images in json.loads(rbd_du[0])["images"]:
            primary_image_size = images["used_size"]
            log.info(
                "Image size for "
                + image_spec
                + " at primary is: "
                + str(primary_image_size)
            )

        rbd_du = rbd_secondary.image_usage(**image_config, format="json")
        for images in json.loads(rbd_du[0])["images"]:
            secondary_image_size = images["used_size"]
            log.info(
                "Image size for "
                + image_spec
                + " at secondary is: "
                + str(secondary_image_size)
            )

        if primary_image_size != secondary_image_size:
            raise Exception(
                "Image size for " + image_spec + " does not match on both clusters"
            )


def check_mirror_consistency(
    rbd_primary, rbd_secondary, client_primary, client_secondary, image_spec_list
):
    """
    Verifies MD5sum hash matches for all images on both clusters.
    Args:
       rbd_primary: Rbd object for primary cluster
       rbd_secondary: Rbd object for secondary cluster
       client_primary: client object of primary cluster
       client_secondary: client object of secondary cluster
       image_spec_list: list of image in spec format <pool_name>/<image_name>
    """
    for spec in list(json.loads(image_spec_list)):
        if "namespace" in spec.keys() and spec["namespace"] != "":
            image_spec = spec["pool"] + "/" + spec["namespace"] + "/" + spec["image"]
        else:
            image_spec = spec["pool"] + "/" + spec["image"]

        data_integrity_spec = {
            "first": {
                "image_spec": image_spec,
                "rbd": rbd_primary,
                "client": client_primary,
                "file_path": "/tmp/" + random_string(len=3),
            },
            "second": {
                "image_spec": image_spec,
                "rbd": rbd_secondary,
                "client": client_secondary,
                "file_path": "/tmp/" + random_string(len=3),
            },
        }
        rc = check_data_integrity(**data_integrity_spec)
        if rc:
            raise Exception("Data consistency check failed for " + spec["image"])
        else:
            log.info("Data is consistent between the Primary and secondary clusters")
