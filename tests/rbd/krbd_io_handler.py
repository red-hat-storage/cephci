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
            device_map: true
          image_spec:
            - pool_name/image_name
          runtime: 30
          file_size: 100M
          device_names:
            - /dev/<>  # newly created devices will be returned in this parameter
          file_path:
            - /<path>/<name> # Pass the file path with file name where image needs to be mounted to run IOs
          encryption_config:
            - <encryption_type>: <passphrase> # List of encryptions and passphrases to be passed for device map
          skip_mkfs: true # Will skip creating new filesystem while mounting a device.
        rbd_obj: The RBD object to be used for testing, if not passed a new RBD object will be created.
        do_not_create_image: Default False, Set True if image on which operations need to be executed already exists
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
    log.debug(f"Config received for execution: {config}")

    operations = config["operations"]
    device_names = []
    mount_points = []
    return_flag = 0

    for image_spec in config["image_spec"]:
        pool_name = image_spec.split("/")[0]
        image_name = image_spec.split("/")[1]
        try:
            if not kw.get("do_not_create_image"):
                rbd.create_image(
                    pool_name,
                    image_name,
                    config.get("size", "1G"),
                    thick_provision=kw.get("config").get("thick_provision"),
                )

            if operations.get("map"):
                device_names.append(rbd.image_map(pool_name, image_name)[:-1])

            if operations.get("device_map"):
                out = rbd.exec_cmd(cmd="rpm -qa|grep rbd-nbd", sudo=True, output=True)
                if not out or out == 1:
                    rbd.exec_cmd(cmd="dnf install rbd-nbd -y", sudo=True)

                out, err = rbd.device_map(
                    "map",
                    f"{pool_name}/{image_name}",
                    config.get("device_type", "nbd"),
                    config.get("encryption_config"),
                )
                if err:
                    log.error(
                        f"RBD device map failed for {image_spec} and "
                        f"encryption_config: {config.get('encryption_config')}"
                    )
                    return 1
                device_names.append(out.strip())

            image_index = config["image_spec"].index(image_spec)

            if operations.get("mount"):
                if not config.get("skip_mkfs"):
                    run_mkfs(
                        client_node=rbd.ceph_client,
                        device_name=device_names[-1],
                        type=operations.get("fs"),
                    )
                # Creating mount directory and mouting device
                mount_point = (
                    config.get("file_path")[image_index].rsplit("/", 1)[0]
                    if config.get("file_path")
                    else f"/tmp/mp_{device_names[-1].split('/')[-1]}"
                )
                mount_points.append(mount_point)
                rbd.exec_cmd(
                    cmd=f"mkdir {mount_point}; mount {device_names[-1]} {mount_point}",
                    sudo=True,
                )

            if operations.get("io"):
                if operations.get("mount"):
                    file_name = (
                        config.get("file_path")[image_index]
                        if config.get("file_path")
                        else f"{mount_point}/{image_name}"
                    )
                    run_fio(
                        client_node=rbd.ceph_client,
                        filename=file_name,
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
                    if rbd.exec_cmd(
                        cmd=f"umount /tmp/mp_{device_name.split('/')[-1]}",
                        sudo=True,
                    ):
                        log.error(
                            f"Umount failed for /tmp/mp_{device_name.split('/')[-1]}"
                        )
                        return_flag = 1
                    if operations.get("device_map"):
                        if rbd.device_unmap(
                            "unmap",
                            f"{pool_name}/{image_name}",
                            config.get("device_type", "nbd"),
                            config.get("encryption_config"),
                        ):
                            log.error(
                                f"Device unmap failed for {pool_name}/{image_name} "
                                f"with encryption config {config.get('encryption_config')}"
                            )
                            return_flag = 1
                    else:
                        if rbd.image_unmap(device_name):
                            log.error(f"Unmap failed for {device_name}")
                            return_flag = 1
    kw["config"]["device_names"] = device_names
    return return_flag
