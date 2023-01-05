from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import run_fio, run_mkfs

log = Log(__name__)


def run(**kw):
    return krbd_io_handler(**kw)


def krbd_io_handler(**kw):
    """krbd io handler.
    Usage:
        config:
          operations:
            map: true
            mount: true
            fs: ext4/xfs
            io: true
            fsck: true
            nounmap: false
          image_spec:
            - pool_name/image_name
          runtime: 30
          file_size: 100M
          rbd_obj: The RBD object to be used for testing (this is useful when the above method is called
                    from another test module which has already created an RBD object)
        Note:
        1) if mount is set to true, a mount point will be created and fs will be created on the device by default
        2) Required pool needs to be created.
        3) Run time is per image
    """
    if kw.get("rbd_obj"):
        rbd = kw.get("rbd_obj")
    else:
        rbd = Rbd(**kw)
    config = kw.get("config")
    log.debug("Config recieved for execution: ", config)

    operations = config["operations"]
    device_names = []
    mount_points = []
    return_flag = 0

    for image_spec in config["image_spec"]:
        pool_name = image_spec.split("/")[0]
        image_name = image_spec.split("/")[1]
        try:
            rbd.create_image(
                pool_name,
                image_name,
                config.get("size", "1G"),
                thick_provision=kw.get("config").get("thick_provision"),
            )

            if operations.get("map"):
                device_names.append(rbd.image_map(pool_name, image_name)[:-1])

            if operations.get("mount"):
                run_mkfs(
                    client_node=rbd.ceph_client,
                    device_name=device_names[-1],
                    type=operations.get("fs"),
                )
                # Creating mount directory and mouting device
                mount_points.append(f"/tmp/mp_{device_names[-1].split('/')[-1]}")
                rbd.exec_cmd(
                    cmd=f"mkdir {mount_points[-1]}; mount {device_names[-1]} {mount_points[-1]}",
                    sudo=True,
                )

            if operations.get("io"):
                if operations.get("mount"):
                    run_fio(
                        client_node=rbd.ceph_client,
                        filename=mount_points[-1],
                        runtime=config.get("runtime", 30),
                        size=config.get("file_size", "100M"),
                    )
                else:
                    run_fio(
                        client_node=rbd.ceph_client,
                        device_name=device_names[-1],
                        runtime=config.get("runtime", 30),
                    )

            if operations.get("fsck"):
                if rbd.exec_cmd(cmd=f"fsck -n {device_names[-1]}"):
                    log.debug("fsck failed")
                    return_flag = 1

        finally:
            if not operations.get("nounmap"):
                for device_name in device_names:
                    rbd.exec_cmd(
                        cmd=f"umount /tmp/mp_{device_names[-1].split('/')[-1]}",
                        sudo=True,
                    )
                    rbd.image_unmap(device_name)

    return return_flag
