import json

from ceph.rbd.utils import check_data_integrity, random_string
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from utility.log import Log

log = Log(__name__)


def compare_image_size_primary_secondary(rbd_primary, rbd_secondary, image_spec_list):
    """
    Compare image sizes using rbd du on both primary and secondary cluster.
    Returns: 1 if sizes do not match and 0 otherwise.
    Args:
        rbd_primary: Rbd object for primary cluster
        rbd_secondary: Rbd object for secondary cluster
        image_spec_list: list of images in spec format '<pool_name>/<image_list>'
    """
    for spec in list(json.loads(image_spec_list)):
        image_spec = spec["pool"] + "/" + spec["image"]
        image_config = {"image-spec": image_spec}
        out = rbd_primary.image_usage(**image_config)
        image_data = out[0]
        primary_image_size = image_data.split("\n")[1].split()[3].strip()
        log.info(
            "Image size for " + image_spec + " at primary is: " + primary_image_size
        )

        out = rbd_secondary.image_usage(**image_config)
        image_data = out[0]
        secondary_image_size = image_data.split("\n")[1].split()[3].strip()
        log.info(
            "Image size for " + image_spec + " at secondary is: " + secondary_image_size
        )

        if primary_image_size != secondary_image_size:
            return 1
    return 0


def run_IO(rbd, client, pool, image, **kw):
    """
    Run IO on an image (map, create file system, mount, run FIO)
    Args:
        rbd: Rbd object
        client: client object
        pool: pool in which image resides
        image: Image on which IO needs to be run
        **kw: FIO additional arguments for IO size
    """
    fio = kw.get("config", {}).get("fio", {})
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": fio["size"],
        "do_not_create_image": True,
        "runtime": "60",
        "num_jobs": "4",
        "iodepth": "32",
        "rwmixread": "70",
        "direct": "1",
        "invalidate": "1",
        "config": {
            "file_size": fio["size"],
            "file_path": ["/mnt/mnt_" + random_string(len=5) + "/file"],
            "get_time_taken": True,
            "image_spec": [pool + "/" + image],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "device_map": True,
            },
            "cmd_timeout": 2400,
            "io_type": "randrw",
        },
    }
    out, err = krbd_io_handler(**io_config)
    if err:
        log.error("Map, mount and run IOs failed for " + pool + "/" + image)
        return 1
    else:
        log.info("Map, mount and IOs successful for " + pool + "/" + image)


def check_mirror_consistency(
    rbd_primary, rbd_secondary, client_primary, client_secondary, image_spec_list
):
    """
    Verifies MD5sum hash matches for all images on both clusters.
    Returns: 1 if md5sum does not match for any of the image from image_spec_list and retruns 0 otherwise
    Args:
       rbd_primary: Rbd object for primary cluster
       rbd_secondary: Rbd object for secondary cluster
       client_primary: client object of primary cluster
       client_secondary: client object of secondary cluster
       image_spec_list: list of image in spec format <pool_name>/<image_name>
    """
    for spec in list(json.loads(image_spec_list)):
        data_integrity_spec = {
            "first": {
                "image_spec": spec["pool"] + "/" + spec["image"],
                "rbd": rbd_primary,
                "client": client_primary,
                "file_path": "/tmp/" + random_string(len=3),
            },
            "second": {
                "image_spec": spec["pool"] + "/" + spec["image"],
                "rbd": rbd_secondary,
                "client": client_secondary,
                "file_path": "/tmp/" + random_string(len=3),
            },
        }
        rc = check_data_integrity(**data_integrity_spec)
        if rc:
            log.error(
                "Data consistency check failed for "
                + spec["pool"]
                + "/"
                + spec["image"]
            )
            return 1
        else:
            log.info("Data is consistent between the Primary and secondary clusters")

    return 0
