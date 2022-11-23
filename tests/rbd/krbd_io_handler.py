from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(**kw):
    return krbd_io_handler(**kw)


def krbd_io_handler(**kw):
    """krbd io handler.
    Usage:
        config:
          operations:
            map: true
            # TBD:
            # mount and different options of mount command
            io: true
            fsck: true
            nounmap: false
          image_spec:
            - image_name/pool_name
            # TBD: options to provide images with different size, IO
    """

    rbd = Rbd(**kw)
    config = kw.get("config")

    operations = config["operations"]
    device_names = []

    # TBD: parallelism among images
    for image_spec in config["image_spec"]:
        pool_name = image_spec.split("/")[0]
        image_name = image_spec.split("/")[1]
    try:
        rbd.create_image(pool_name, image_name, config.get("size", "1G"))
        # TBD: skip map option
        if operations.get("map"):
            device_names.append(rbd.image_map(pool_name, image_name)[:-1])

        if operations.get("io"):
            run_fio(
                client_node=rbd.ceph_client,
                device_name=device_names[-1],
                runtime=config.get("runtime", 30),
            )

        if operations.get("fsck"):
            rbd.exec_cmd(cmd=f"fsck -N {device_names[-1]}")

    finally:
        for device_name in device_names:
            rbd.image_unmap(device_name)

    return 0
