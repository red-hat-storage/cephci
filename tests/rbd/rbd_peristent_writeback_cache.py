"""RBD Persistent write back cache."""

import datetime
import re
from json import loads
from time import sleep

from utility.log import Log

log = Log(__name__)


class PWLException(Exception):
    pass


class PWLConfigurationError(Exception):
    pass


class PersistentWriteAheadLog:
    def __init__(self, rbd, client, drive):
        """Initialize PWL.

        Args:
             rbd: RBD object
             client: SSD/PMEM client node(CephNode)
             drive: Cache drive for persistent Write back cache.
        """
        self.rbd = rbd
        self.client = client
        self.drive = drive
        self.pwl_path = None

    def cleanup(self):
        """cleanup drive."""
        log.info("Starting to cleanup cache drive....")
        self.client.exec_command(cmd=f"wipefs -af {self.drive}", sudo=True)
        self.client.exec_command(
            cmd=f"umount -v {self.drive}", sudo=True, check_ec=False
        )
        self.client.exec_command(cmd=f"rm -rf {self.pwl_path}", sudo=True)

    def configure_cache_client(self):
        """Configure cache device with DAX.

        Configuration involves,
        - wipe drive
        - create mount directory.
        - mkfs.xfs <drive> or with ext4
        - mount drive with DAX(Direct Attached Access) option
        """
        log.info("Configuring SSD/PMEM cache client....")
        self.pwl_path = f"/mnt/{self.rbd.random_string()}"
        cmds = [
            f"mkdir -p {self.pwl_path}",
            f"mkfs.ext4 {self.drive}",
            f"mount -O dax {self.drive} {self.pwl_path}",
        ]

        # Cleanup drive and get ready for mount.
        self.cleanup()
        for cmd in cmds:
            self.client.exec_command(cmd=cmd, sudo=True)

    def configure_pwl_cache(self, mode, level, entity, size="1073741824"):
        """Set PWL cache mode (disabled, rwl, ssd).

        Args:
            level: cache mode applied at client or image or pool
            mode: cache mode ( disabled or rwl or ssd )
            entity: entity level ( client or image-name or pool-name )
            size: cache size ( default: 1073741824 )
        """
        log.info(f"Configuring RBD PWL cache setting at {level}:{entity}")
        configs = [
            ("global", "client", "rbd_cache", "false"),
            (level, entity, "rbd_plugins", "pwl_cache"),
            (level, entity, "rbd_persistent_cache_mode", mode),
            (level, entity, "rbd_persistent_cache_size", size),
            (level, entity, "rbd_persistent_cache_path", self.pwl_path),
        ]

        for config in configs:
            if self.rbd.set_config(*config):
                raise PWLConfigurationError(f"{config} - failed to add configuration")

    def remove_pwl_configuration(self, level, entity):
        """Unset PWL cache mode (disabled, rwl, ssd).

        Args:
            level: cache mode applied at client or image or pool
            entity: entity level ( client or image-name or pool-name )
        """
        log.info(f"Removing RBD PWL cache setting at {level}:{entity}")
        configs = [
            ("global", "client", "rbd_cache"),
            (level, entity, "rbd_plugins"),
            (level, entity, "rbd_persistent_cache_mode"),
            (level, entity, "rbd_persistent_cache_size"),
            (level, entity, "rbd_persistent_cache_path"),
        ]

        for config in configs:
            if self.rbd.remove_config(*config):
                raise PWLConfigurationError(
                    f"{config} - failed to remove configuration"
                )

    def get_image_cache_status(self, image):
        """Get image persistent cache status.

        Args:
            image: image name
        Returns:
            image_status
        """
        args = {"format": "json"}
        return loads(self.rbd.image_status(image, cmd_args=args, output=True))

    @staticmethod
    def validate_cache_size(rbd_status, cache_size):
        """Compare cache size."""
        configured_cache_size = rbd_status["persistent_cache"]["size"]
        if configured_cache_size != cache_size:
            raise PWLException(
                f"Cache size {configured_cache_size} from RBD status did not match to {cache_size}"
            )
        log.info(
            f"Cache size {configured_cache_size} from RBD status matched to {cache_size}"
        )

    def validate_cache_path(self, rbd_status):
        """Compare cache file path."""
        configured_cache_path = rbd_status["persistent_cache"]["path"]
        if self.pwl_path not in configured_cache_path:
            raise PWLException(
                f"{self.pwl_path} is not been used as cache path as configured {configured_cache_path}"
            )
        log.info(
            f"{self.pwl_path} is used as cache path as configured {configured_cache_path}"
        )

    def check_cache_file_exists(self, image, timeout=120, **kw):
        """Validate cache file existence.

        Args:
            image: name of the image.
            timeout: timeout in seconds
            kw: validate arguments
        Raises:
            PWLException
        """
        log.info("Validate RBD PWL cache file existence and size.")

        # Validate cache file
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while end_time > datetime.datetime.now():
            out = self.get_image_cache_status(image)
            log.debug(f"RBD status of image: {out}")

            cache_file_path = out.get("persistent_cache", {}).get("path")
            if cache_file_path:
                # validate cache from rbd status
                if kw.get("validate_cache_size"):
                    self.validate_cache_size(out, kw["cache_file_size"])
                if kw.get("validate_cache_path"):
                    self.validate_cache_path(out)

                try:
                    # validate cache file existence
                    self.client.exec_command(
                        cmd=f"ls -l {cache_file_path}",
                        check_ec=True,
                    )
                    log.info(
                        f"{self.client.hostname}:{cache_file_path} cache file found..."
                    )
                    break
                except Exception as err:
                    log.warning(err)
        else:
            raise PWLException(
                f"{self.client.hostname}:{self.pwl_path} cache file did not found!!!"
            )

    def flush(self, image_spec):
        """Perform cache flush on the image
        Args:
            image_spec: <pool>/<image> where cache flush needs to be performed.
        """
        log.info(f"Perform cache flush on image {image_spec}....")
        cmd = f"rbd persistent-cache flush {image_spec}"
        return self.client.exec_command(cmd=cmd, sudo=True)

    def invalidate(self, image_spec):
        """Perform cache invalidate on the image
        Args:
            image_spec: <pool>/<image> where cache invalidate needs to be performed.
        """
        log.info(f"Perform cache invalidate on image {image_spec}....")
        cmd = f"rbd persistent-cache invalidate {image_spec}"
        return self.client.exec_command(cmd=cmd, sudo=True)


# utils
def get_entity_level(config):
    """Method to get config level and entity."""
    config_level = config["level"]
    entity = "client"
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
    if config_level == "client":
        config_level = "global"
    elif config_level == "pool":
        entity = pool
    elif config_level == "image":
        entity = image

    return config_level, entity


def fio_ready(config, client, **kw):
    """Method to prepare FIO config args."""
    fio_args = config["fio"]
    fio_args["client_node"] = client
    fio_args["long_running"] = True

    if kw:
        fio_args.update(kw)
    return fio_args


def unmount(rbd, mount_point):
    """Unmount the mount point

    Args:
        rbd: RBD object.
        mount_point: directory where image is mounted
    """
    flag = 0
    umount_cmd = f"umount -f {mount_point}"
    if rbd.exec_cmd(cmd=umount_cmd, sudo=True):
        log.error(f"Umount failed for {mount_point}")
        flag = 1

    if rbd.exec_cmd(cmd=f"rm -rf {mount_point}", sudo=True):
        log.error(f"Remove dir failed for {mount_point}")
        flag = 1

    return flag


def device_cleanup(rbd):
    """Unmount and unmap the device.

    Args:
        rbd: RBD object
    """
    cmd = "lsblk --include 43 --json"
    out = rbd.exec_cmd(cmd=cmd, sudo=True, output=True)
    if out:
        rbd_devices = loads(out)

        for devices in rbd_devices.get("blockdevices"):
            device_name = f"/dev/{devices.get('name')}"
            mount_points = devices.get("mountpoints")
            for mount_point in mount_points:
                unmount(rbd=rbd, mount_point=mount_point)
            out_2 = rbd.device_map(
                operation="unmap",
                image_spec=f"{device_name}",
                device_type="nbd",
                encryption_config=[],
                long_running=True,
            )
            if out_2:
                log.error(f"RBD unmap failed for {device_name} ")


def kill_fio(rbd):
    """
    Kill the fio process running on the client
    """
    sleep(60)
    cmd = "ps -ef | grep fio"
    out = rbd.exec_cmd(cmd=cmd, sudo=True, output=True)
    if out and re.findall(r"(fio).*(--name)", out, re.I):
        proc_id = re.search(r"\d+", out).group()
        cmd = f"kill -9 {proc_id}"
        rbd.exec_cmd(cmd=cmd, sudo=True)
