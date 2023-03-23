import json
import random
import string
from time import sleep

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from tests.rbd.exceptions import (
    CreateFileError,
    ImageIsDeletedError,
    ImageNotFoundError,
    ImportFileError,
    RbdBaseException,
)
from utility.log import Log

log = Log(__name__)


class Rbd:
    def __init__(self, **kw):
        self.ceph_args = ""
        self.config = kw.get("config")
        self.ceph_version = int(self.config.get("rhbuild")[0])
        self.datapool = None
        self.flag = 0
        self.k_m = self.config.get("ec-pool-k-m", None)
        self.failure_domain = self.config.get("crush-failure-domain", None)

        if kw.get("req_cname"):
            self.ceph_nodes = kw["ceph_cluster_dict"][kw["req_cname"]]
        else:
            self.ceph_nodes = kw["ceph_nodes"]

        # Identifying Monitor And Client node
        for node in self.ceph_nodes:
            if node.role == "mon":
                self.ceph_mon = node
                continue
            if node.role == "client":
                self.ceph_client = node
                continue

        if self.ceph_version > 2 and self.k_m:
            self.datapool = (
                self.config["ec_pool_config"]["data_pool"]
                if self.config.get("ec_pool_config", {}).get("data_pool")
                else "rbd_test_data_pool_" + self.random_string()
            )
            if not self.k_m:
                self.k_m = "2,1"
            # Temporary change untill we get more info on default value of k and m
            if "," in self.k_m:
                self.ec_profile = self.config.get("ec_pool_config", {}).get(
                    "ec_profile", "rbd_ec_profile_" + self.random_string()
                )
                self.set_ec_profile(profile=self.ec_profile)
            else:
                self.ec_profile = ""

    def exec_cmd(self, **kw):
        """
        exec_command wrapper for rbd functions
        Args:
            pool_name: configs along with `cmd` - command
            output: True if command output needs to be returned

        Returns:  0 -> pass, 1 -> fail
        """
        try:
            cmd = kw.get("cmd", "")
            node = kw.get("node") if kw.get("node") else self.ceph_client
            if self.k_m and "rbd create" in cmd and "--data-pool" not in cmd:
                cmd = cmd + " --data-pool {}".format(self.datapool)

            if kw.get("long_running"):
                out = node.exec_command(
                    sudo=True,
                    cmd=cmd,
                    long_running=True,
                    check_ec=kw.get("check_ec", True),
                )
                return out

            out, err = node.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=False,
                check_ec=kw.get("check_ec", True),
            )

            if kw.get("output", False):
                return out

            if kw.get("all", False):
                return out, err

            log.info("Command execution complete")
            return 0

        except CommandFailed as e:
            log.error(f"Command {cmd} execution failed with error:\n{e}")
            self.flag = 1
            return 1

    def random_string(self):
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(10)])
        return temp_str

    def initial_rbd_config(self, rbd, pool, image, **kw):
        """
        Calls create_pool function on the clusters,
        creates an image in the pool,
        Args:
            **kw:
            pool - name for pool to be created on cluster
            image - name of images created on the pool
            size - size of the image to be created
        """
        size = kw.get("size", "10G")
        if not self.create_pool(poolname=pool):
            log.error(f"Pool creation failed for pool {pool}")
            return 1
        self.create_image(pool_name=pool, image_name=image, size=size)

    def create_pool(self, poolname):
        if self.ceph_version > 2 and self.k_m:
            self.create_ecpool(profile=self.ec_profile, poolname=self.datapool)
        if self.exec_cmd(cmd="ceph osd pool create {} 64 64".format(poolname)):
            log.error("Pool creation failed")
            return False
        if not self.check_pool_exists(pool_name=poolname):
            log.error("Pool not created")
            return False
        if self.ceph_version >= 3:
            self.exec_cmd(cmd="rbd pool init {}".format(poolname))
        return True

    def create_image(self, pool_name, image_name, size, **kw):
        cmd = f"rbd create {pool_name}/{image_name} --size {size}"
        if kw.get("image_feature"):
            image_feature = kw.get("image_feature")
            cmd += f" --image-feature {image_feature}"
        if kw.get("thick_provision"):
            cmd += " --thick-provision"
        if self.ceph_version > 2 and self.k_m:
            cmd += f" --data-pool {self.datapool}"
        self.exec_cmd(cmd=cmd)

    def set_ec_profile(self, profile):
        self.exec_cmd(cmd="ceph osd erasure-code-profile rm {}".format(profile))
        if self.failure_domain == "osd":
            self.exec_cmd(
                cmd="ceph osd erasure-code-profile set {} crush-failure-domain=osd k={} m={}".format(
                    profile, self.k_m[0], self.k_m[2]
                )
            )
        else:
            self.exec_cmd(
                cmd="ceph osd erasure-code-profile set {} k={} m={}".format(
                    profile, self.k_m[0], self.k_m[2]
                )
            )

    def create_ecpool(self, **kw):
        poolname = kw.get("poolname", self.datapool)
        profile = kw.get("profile", self.ec_profile)
        self.exec_cmd(
            cmd="ceph osd pool create {} 12 12 erasure {}".format(poolname, profile)
        )
        if not self.check_pool_exists(pool_name=poolname):
            log.error("Pool not created")
            return False
        self.exec_cmd(
            cmd="ceph osd pool set {} allow_ec_overwrites true".format(poolname)
        )
        return True

    def check_pool_exists(self, pool_name: str) -> bool:
        """
        recursively checks if the specified pool exists in the cluster
        Args:
            pool_name: Name of the pool to be checked

        Returns:  True -> pass, False -> fail

        """
        timeout, interval = 200, 2
        for w in WaitUntil(timeout=timeout, interval=interval):
            out = self.exec_cmd(cmd="ceph df -f json", output=True)
            if pool_name in [ele["name"] for ele in json.loads(out)["pools"]]:
                log.info(f"Pool '{pool_name}' present in the cluster")
                return True

            log.info(
                f"Pool '{pool_name}' not populated yet.\n"
                f"Waiting for {interval} seconds and retrying"
            )

        if w.expired:
            log.info(
                f"Failed to wait {timeout} seconds to pool '{pool_name}'"
                f" present on cluster"
            )
            return False

    def create_file_to_import(self, filename="dummy"):
        """
        Creates a dummy file on client node to import
        Args:
            filename: name of the dummy file
        Returns:  True -> pass, False -> fail
        """
        cmd = f"dd if=/dev/urandom of={filename} bs=4 count=5M"
        if self.exec_cmd(cmd=cmd, long_running=True):
            raise CreateFileError("Creating a file to import failed")

    def import_file(self, filename="dummy", pool_name="dummy", image_name="dummy"):
        """
        Imports a file as an image to specified pool name and image name
        Args:
            filename   : name of the file to be imported
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as

        Note: run create_file_to_import before this module in positive scenarios
        """
        cmd = f"rbd import {filename} {pool_name}/{image_name}"
        if self.exec_cmd(cmd=cmd, long_running=True):
            raise ImportFileError("Importing the file failed")

    def snap_create(self, pool_name, image_name, snap_name):
        """
        Creates a snap of an image in a specified pool name and image name
        Args:
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as
            snap_name  : name of the snapshot
        """

        cmd = f"rbd snap create {pool_name}/{image_name}@{snap_name}"
        return self.exec_cmd(cmd=cmd)

    def snap_ls(self, pool_name: str, image_name: str, snap_name=None, all=False):
        """
        Lists the snapshots present for an image and returns the given snap_name if present
        Args:
            pool_name: name of the pool containing snapshots
            image_name: name of the image whose snapshots are to be listed
            snap_name: if specified will return the snapshot if its present

        Returns:
            all the snaps for a pool and image specified
            the snap with the given snapname if pool, image and snapname are specified
            None if no specified snaps are present.
        """
        cmd = f"rbd snap ls {pool_name}/{image_name} --format json"
        if all:
            cmd += " --all"
        out = self.exec_cmd(cmd=cmd, output=True)
        if out == 1:
            log.error(
                f"Snap list command failed on pool {pool_name} and image {image_name}"
            )
            return None
        snaps = json.loads(out)
        if snaps and not snap_name:
            # returns all the listed snaps if snap_name is not specified
            return snaps
        else:
            snap_requested = [snap for snap in snaps if snap["name"] == snap_name]
            return snap_requested

    def snap_rename(self, pool_name, image_name, current_snap_name, new_snap_name):
        """
        Renames the snap of an image in a specified pool name and image name
        Args:
            pool_name: name of the pool where image snapshot is present
            image_name: name of the image where image whose snapshot is to be renamed
            current_snap_name: current name of the snapshot
            new_snap_name: new name to be given to the snapshot
        """
        cmd = f"rbd snap rename {pool_name}/{image_name}@{current_snap_name} {pool_name}/{image_name}@{new_snap_name}"
        if self.exec_cmd(cmd=cmd):
            log.error(f"Snapshot rename failed for {current_snap_name}")
            return 1

        if self.snap_ls(pool_name, image_name, new_snap_name) and not self.snap_ls(
            pool_name, image_name, current_snap_name
        ):
            log.info(
                f"Snapshot rename successful from {current_snap_name} to {new_snap_name}"
            )
            return 0
        else:
            log.error(f"Snapshot rename verification failed for {current_snap_name}")
            return 1

    def snap_remove(self, pool_name, image_name, snap_name, **kw):
        """
        Removes a snap of an image in a specified pool name and image name
        Args:
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as
            snap_name  : name of the snapshot
        """
        if kw.get("image_id"):
            cmd = f"rbd snap rm --image-id {kw['image_id']} --pool {pool_name} --snap {snap_name}"
        else:
            cmd = f"rbd snap rm {pool_name}/{image_name}@{snap_name}"
        return self.exec_cmd(cmd=cmd)

    def protect_snapshot(self, snap_name):
        """
        Protects the provided snapshot
        Args:
            snap_name : snapshot name in pool/image@snap format
        """
        cmd = f"rbd snap protect {snap_name}"
        return self.exec_cmd(cmd=cmd)

    def create_clone(self, snap_name, pool_name, image_name, **kw):
        """
        Creates a clone of an image from its snapshot
        in a specified pool name and image name
        Args:
            snap_name  : name of the snapshot of which a clone is to be created
            pool_name  : name of the pool where clone is to be created
            image_name : name of the cloned image
        Returns:
            0 on Success
            1 on Failure
        """
        if kw.get("clone_version") == "v2":
            cmd = "ceph osd set-require-min-compat-client mimic"
            self.exec_cmd(cmd=cmd)
        cmd = f"rbd clone {snap_name} {pool_name}/{image_name}"
        self.exec_cmd(cmd=cmd)
        if self.image_exists(pool_name, image_name):
            log.info("clone creation is successful")
            return 0
        else:
            log.info("clone creation is failed")
            return 1

    def flatten_clone(self, pool_name, image_name):
        """
        Flattens a clone of an image from parent image
        Args:
            pool_name: name of the pool which clone is created
            image_name: name of th clones image
        """
        log.info("start flatten")
        cmd = f"rbd flatten {pool_name}/{image_name}"
        rc = self.exec_cmd(cmd=cmd)
        if rc != 0:
            log.error(f"Error while flattening image {image_name}")
            return rc
        log.info(f"flatten completed for image {image_name}")
        return rc

    def remove_image(self, pool_name, image_name, **kw):
        """
        Remove image from the specified pool
        Args:
            kw: remove image config
            pool_name: name of the pool
            image_name: name of the image

        """
        log.info("Removal of image started")
        return self.exec_cmd(cmd=f"rbd rm {pool_name}/{image_name}", **kw)

    def move_image(self, image_spec, image_spec_new):
        """
        Move/Rename image from the specified spec to new spec
        Args:
            image_spec: pool_name/image_name
            image_spec_new: pool_name_new/image_name_new

        Returns:

        """
        return self.exec_cmd(cmd=f"rbd mv {image_spec} {image_spec_new}")

    def list_images(self, pool_name):
        """
        List images in the given pool
        Args:
            pool_name: name of the pool

        Returns:

        """
        out = self.exec_cmd(cmd=f"rbd ls {pool_name} --format json", output=True)
        images = json.loads(out)
        return images

    def image_info(self, pool_name, image_name):
        """
        Fetch image info for the given pool and image
        Args:
            pool_name: name of the pool
            image_name: image of the pool

        Returns:
            image_info (dict): if fetching image info was successful.
            1 (int): if fetching image info failed.
        """
        out = self.exec_cmd(
            cmd=f"rbd info {pool_name}/{image_name} --format json", output=True
        )
        try:
            image_info = json.loads(out)
        except TypeError:
            log.error("Failed to recieve json complaint image info")
            return 1
        return image_info

    def image_resize(self, pool_name, image_name, size):
        """
        Fetch image info for the given pool and image
        Args:
            pool_name: name of the pool
            image_name: image of the pool
            size: new size for the image

        Returns:

        """
        return self.exec_cmd(
            cmd=f"rbd resize -s {size} {pool_name}/{image_name} --allow-shrink"
        )

    def snap_rollback(self, snap_spec):
        """
        Fetch image info for the given pool and image
        Args:
            snap_spec: pool_name/image_name@snap_name

        Returns:

        """
        return self.exec_cmd(cmd=f"rbd snap rollback {snap_spec}")

    def unprotect_snapshot(self, snap_name, **kw):
        """
        UnProtects the provided snapshot
        Args:
            kw :
                snap_name : in pool/image@snap format or just snap name
                image_id : unique id of an image in trash
                pool : name of the pool where the snap is present
        """
        if kw.get("image_id"):
            cmd = f"rbd snap unprotect --image-id {kw['image_id']} --pool {kw.get('pool')} --snap {snap_name}"
        else:
            cmd = f"rbd snap unprotect {snap_name}"
        return self.exec_cmd(cmd=cmd)

    def image_exists(self, pool_name, image_name):
        """
        Verify if image exists in the specified pool
        Args:
            pool_name: name of the pool
            image_name: name of the image

        Returns:

        """
        out = self.exec_cmd(cmd=f"rbd ls {pool_name} --format json", output=True)
        images = json.loads(out)
        return True if any(image_name == image for image in images) else False

    def move_image_trash(self, pool_name, image_name):
        """
        Move images to trash from the specified pool
        Args:
            pool_name : name of the pool
            image_name : mane of the image
        Returns:
            0: on success
            1: on failure
        """
        cmd = f"rbd trash mv {pool_name}/{image_name}"
        rc = self.exec_cmd(sudo=True, cmd=cmd)
        if rc:
            log.error("Error while moving image to trash")
            return rc
        log.info("Moving image to trash is successful")
        return rc

    def remove_image_trash(self, pool_name, image_id):
        """
        Remove images from trash using image unique ID
        Args:
            pool_name : name of the pool
            image_id : unique id of the images generated after moving image to trash
        Returns:

        """
        return self.exec_cmd(cmd=f"rbd trash rm {pool_name}/{image_id}")

    def get_image_id(self, pool_name, image_name):
        """
        Returns the image_id of image obtained from images in trash
        for the provided poolname
        Args:
            pool_name : name of the pool
            image_name : name of the image used to verify the image_id of the image in trash
        Returns:
            image_id of the image

        """
        out = self.exec_cmd(
            cmd=f"rbd trash list {pool_name} --format json ", output=True
        )
        image_info = json.loads(out)
        for value in image_info:
            if value["name"] == image_name:
                return value["id"]

    def trash_restore(self, pool_name, image_id):
        """
         Move images to trash, restore them
        Args:
             pool_name: name of the pool
             image_id: image id of the image
         Returns:
        """
        return self.exec_cmd(cmd=f"rbd trash restore {pool_name}/{image_id}")

    def image_meta(self, **kw):
        """Manage image-meta.
        Args:
            kw:
                action: get, remove, set, list
                image_spec: image specification.
                key: meta-key.
                value: meta-value.
        """
        cmd = f"rbd image-meta {kw.get('action')} {kw.get('image_spec')}"
        if kw.get("key"):
            cmd += f" {kw.get('key')}"
            if kw.get("value"):
                cmd += f" {kw.get('value')}"

        return self.exec_cmd(cmd=cmd, output=True)

    def trash_exist(self, pool_name, image_name):
        out = self.exec_cmd(
            cmd=f"rbd trash list {pool_name} --format json ", output=True
        )
        image_info = json.loads(out)
        return any(image["name"] == image_name for image in image_info)

    def image_map(self, pool_name, image_name):
        """Map provided rbd image."""
        return self.exec_cmd(cmd=f"rbd map {pool_name}/{image_name}", output=True)

    def image_unmap(self, device_name):
        """UnMap provided rbd image."""
        return self.exec_cmd(cmd=f"rbd unmap {device_name}", output=True)

    def rbd_bench(self, **kw):
        """
        This method writes IOs using rbd bench
        Args:
            **kw: test data
                io: Amount of IO to be written, default size is 500MB
                imagespec: pool/image on which IO needs to be run
        """
        cmd = f"rbd bench --io-type {kw.get('io_type', 'write')} --io-threads {kw.get('io_threads', 16)}"
        cmd += f" --io-total {kw.get('io', '500M')} {kw.get('imagespec')}"
        return self.exec_cmd(
            cmd=cmd,
            long_running=True,
        )

    def clean_up(self, **kw):
        if kw.get("dir_name"):
            self.exec_cmd(cmd="rm -rf {}".format(kw.get("dir_name")))

        if kw.get("pools"):
            pool_list = kw.get("pools")
            if self.datapool:
                pool_list.append(self.datapool)

            # mon_allow_pool_delete must be True for removing pool
            if self.ceph_version >= 5:
                self.exec_cmd(cmd="ceph config set mon mon_allow_pool_delete true")
                sleep(20)

            for pool in pool_list:
                self.exec_cmd(
                    cmd="ceph osd pool delete {pool} {pool} "
                    "--yes-i-really-really-mean-it".format(pool=pool)
                )

    def migration_prepare(self, src_spec, dest_spec):
        """Migration Prepare.

        This method prepare the live migration of image from source to destination

        Args:
            src_spec : source image spec SOURCE_POOL_NAME/SOURCE_IMAGE_NAME
            dest_spec: Target image spec TARGET_POOL_NAME/SOURCE_IMAGE_NAME
        """
        log.info("Starting prepare Live migration of image ")
        return self.exec_cmd(cmd=f"rbd migration prepare {src_spec} {dest_spec}")

    def migration_action(self, action, dest_spec):
        """Migration action

        This method will execute/commit the live migration as per the provided action

        Args:
            action: execute or commit
            dest_spec: Target image spec in format of TARGET_POOL_NAME/SOURCE_IMAGE_NAME
        """
        log.info(f"Starting the {action} migration process")
        return self.exec_cmd(cmd=f"rbd migration {action} {dest_spec}")


def initial_rbd_config(**kw):
    """
    Configure replicated pools, ecpools or both based on arguments specified
    Args:
        **kw:

    Examples: In default configuration, pool and image names will be taken as random values.
        Default configuration for ecpools only :
            config:
                ec-pool-only: True
        Default configuration for replicated pools only :
            config:
                rep-pool-only: True
        Advanced configuration:
            config:
               do_not_create_image: True  # if not set then images will be created by default
               ec-pool-k-m: 2,1
               ec-pool-only: False
               ec_pool_config:
                  pool: rbd_pool_4
                  data_pool: rbd_ec_pool_4
                  ec_profile: rbd_ec_profile_4
                  image: rbd_image_4
                  size: 10G
               rep_pool_config:
                  pool: rbd_rep_pool
                  image: rbd_rep_image
                  size: 10G
    """
    rbd_obj = dict()
    log.debug(
        f'config recieved for rbd_config: {kw.get("config", "Config not recieved, assuming default values")}'
    )
    if not kw.get("config") or not kw.get("config").get("ec-pool-only"):
        ec_pool_k_m = kw.get("config", {}).pop("ec-pool-k-m", None)
        rbd_reppool = Rbd(**kw)

        if not kw.get("config"):
            kw["config"] = {
                "rep_pool_config": {
                    "pool": "rep_pool_" + rbd_reppool.random_string(),
                    "image": "rep_image_" + rbd_reppool.random_string(),
                    "size": "10G",
                }
            }
        elif kw.get("config").get("rep_pool_config"):
            kw["config"]["rep_pool_config"]["pool"] = kw["config"][
                "rep_pool_config"
            ].get("pool", "rep_pool_" + rbd_reppool.random_string())
            kw["config"]["rep_pool_config"]["image"] = kw["config"][
                "rep_pool_config"
            ].get("image", "rep_image_" + rbd_reppool.random_string())
            kw["config"]["rep_pool_config"]["size"] = kw["config"][
                "rep_pool_config"
            ].get("size", "10G")
        else:
            kw["config"]["rep_pool_config"] = {
                "pool": "rep_pool_" + rbd_reppool.random_string(),
                "image": "rep_image_" + rbd_reppool.random_string(),
                "size": "10G",
            }

        if not rbd_reppool.create_pool(
            poolname=kw["config"]["rep_pool_config"]["pool"]
        ):
            log.error(
                f"Pool creation failed for pool {kw['config']['rep_pool_config']['pool']}"
            )
            return None
        if not kw.get("config").get("do_not_create_image"):
            rbd_reppool.create_image(
                pool_name=kw["config"]["rep_pool_config"]["pool"],
                image_name=kw["config"]["rep_pool_config"]["image"],
                size=kw["config"]["rep_pool_config"]["size"],
            )
        kw["config"]["ec-pool-k-m"] = ec_pool_k_m
        rbd_obj.update({"rbd_reppool": rbd_reppool})

    if not kw.get("config").get("rep-pool-only"):
        if not kw.get("config").get("ec-pool-k-m"):
            kw["config"]["ec-pool-k-m"] = "go_with_default"

        rbd_ecpool = Rbd(**kw)

        if kw.get("config").get("ec_pool_config"):
            kw["config"]["ec_pool_config"]["pool"] = kw["config"]["ec_pool_config"].get(
                "pool", "ec_img_pool_" + rbd_ecpool.random_string()
            )
            kw["config"]["ec_pool_config"]["image"] = kw["config"][
                "ec_pool_config"
            ].get("image", "ec_image" + rbd_ecpool.random_string())
            kw["config"]["ec_pool_config"]["size"] = kw["config"]["ec_pool_config"].get(
                "size", "10G"
            )
        else:
            kw["config"]["ec_pool_config"] = {
                "pool": "ec_img_pool_" + rbd_reppool.random_string(),
                "image": "ec_image" + rbd_reppool.random_string(),
                "size": "10G",
            }

        if not rbd_ecpool.create_pool(poolname=kw["config"]["ec_pool_config"]["pool"]):
            log.error(
                f"Pool creation failed for pool {kw['config']['ec_pool_config']['pool']}"
            )
            return None
        if not kw.get("config").get("do_not_create_image"):
            rbd_ecpool.create_image(
                pool_name=kw["config"]["ec_pool_config"]["pool"],
                image_name=kw["config"]["ec_pool_config"]["image"],
                size=kw["config"]["ec_pool_config"]["size"],
            )
        rbd_obj.update({"rbd_ecpool": rbd_ecpool})

    return rbd_obj


def execute_dynamic(rbd, test, results: dict, **kwargs):
    """
    Executes the test specified in test parameter with inputs args and returns results
    Args:
        rbd: rbd object
        test: test to be executed
        results: test result
        **kwargs: input args required for the test

    Returns:
        results updated to results dict
    """
    method = getattr(rbd, test)
    rc = method(**kwargs)
    log.info(f"Return value for execution of method: {test} is {rc}")
    if rc:
        results.update({test: 1})
    else:
        results.update({test: 0})


def rbd_remove_image_negative_validate(rbd, pool, image):
    """
    This method validate the remove image when exclusive lock is enabled
    Args:
        rbd: rbd object
        pool: pool name
        image: image name
    """
    out, err = rbd.remove_image(pool, image, all=True, check_ec=False)
    if "rbd: error: image still has watchers" not in err:
        log.error(f"{out}")
        raise ImageIsDeletedError(f" RBD image is deleted: {out}")
    log.info(f"{err}")


def verify_migration_state(rbd, dest_spec):
    """verify the migration status.

    This method will verify the migration state for an image for destination pool after executing
    prepare migration and execute migration steps for live image migration.

    Args:
        rbd: rbd object
        dest_spec: Target_pool/image
    """
    log.info("verify migration process started")
    out = rbd.exec_cmd(cmd=f"rbd status {dest_spec} --format json", output=True)
    log.info(out)
    status = json.loads(out)
    try:
        if "prepared" in status["migration"]["state"]:
            log.info(f"Live Migration successfully prepared for {dest_spec}")
            return 0
        elif "executed" in status["migration"]["state"]:
            log.info(f"Live migration successfully executed for {dest_spec}")
            return 0
    except RbdBaseException as error:
        log.error(error.message)
        return 1


def verify_migration_commit(rbd, pool, image):
    """Verify migration commit.

    This method will verify for the commit migration is success or not.

    Args:
        rbd: rbd object
        pool_name: pool name
        image_name: image name
    """
    if rbd.image_exists(pool, image):
        log.info(f"Image migration is successful for image {image} to pool {pool}")
        return 0
    else:
        log.error((f"Image {image} is not found in pool {pool}"))
        raise ImageNotFoundError("Image Not found.")
