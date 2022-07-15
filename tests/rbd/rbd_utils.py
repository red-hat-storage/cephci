import json
import random
import string
from time import sleep

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from tests.rbd.exceptions import CreateFileError, ImportFileError, ProtectSnapError
from utility.log import Log

log = Log(__name__)


class Rbd:
    def __init__(self, **kw):
        self.ceph_args = ""
        self.config = kw.get("config")
        self.ceph_version = int(self.config.get("rhbuild")[0])
        self.datapool = None
        self.flag = 0
        self.k_m = self.config.get("ec-pool-k-m", False)

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
            self.datapool = "rbd_datapool"
            self.ec_profile = "rbd_ec_profile"
            self.set_ec_profile(profile=self.ec_profile)

    def exec_cmd(self, **kw):
        """
        exec_command wrapper for rbd functions
        Args:
            pool_name: configs along with `cmd` - command


        Returns:  0 -> pass, 1 -> fail
        """
        try:
            cmd = kw.get("cmd")
            node = kw.get("node") if kw.get("node") else self.ceph_client
            if self.k_m and "rbd create" in cmd and "--data-pool" not in cmd:
                cmd = cmd + " --data-pool {}".format(self.datapool)

            out, err = node.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=kw.get("long_running", False),
                check_ec=kw.get("check_ec", True),
            )

            if kw.get("output", False):
                return out

            return 0

        except CommandFailed as e:
            log.info(e)
            self.flag = 1
            return 1

    def random_string(self):
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(10)])
        return temp_str

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

    def create_image(self, pool_name, image_name, size):
        self.exec_cmd(cmd=f"rbd create {pool_name}/{image_name} --size {size}")

    def set_ec_profile(self, profile):
        self.exec_cmd(cmd="ceph osd erasure-code-profile rm {}".format(profile))
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
        self.exec_cmd(cmd="rbd pool init {}".format(poolname))
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
                f"Waitinig for {interval} seconds and retrying"
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
        if not self.exec_cmd(cmd, long_running=True):
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
        if not self.exec_cmd(cmd, long_running=True):
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
        self.exec_cmd(cmd=cmd)

    def snap_remove(self, pool_name, image_name, snap_name):
        """
        Removes a snap of an image in a specified pool name and image name
        Args:
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as
            snap_name  : name of the snapshot
        """

        cmd = f"rbd snap rm {pool_name}/{image_name}@{snap_name}"
        self.exec_cmd(cmd=cmd)

    def protect_snapshot(self, snap_name):
        """
        Protects the provided snapshot
        Args:
            snap_name : snapshot name in pool/image@snap format
        """
        cmd = f"rbd snap protect {snap_name}"
        if not self.exec_cmd(cmd):
            raise ProtectSnapError("Protecting the snapshot Failed")

    def create_clone(self, snap_name, pool_name, image_name):
        """
        Creates a clone of an image from its snapshot
        in a specified pool name and image name
        Args:
            snap_name  : name of the snapshot of which a clone is to be created
            pool_name  : name of the pool where clone is to be created
            image_name : name of the cloned image
        """
        cmd = f"rbd clone {snap_name} {pool_name}/{image_name}"
        self.exec_cmd(cmd=cmd)

    def flatten_clone(self, pool_name, image_name):
        """
        Flattens a clone of an image from parent image
        Args:
            pool_name: name of the pool which clone is created
            image_name: name of th clones image
        """
        log.info("start flatten")
        cmd = f"rbd flatten {pool_name}/{image_name}"
        self.exec_cmd(cmd=cmd)
        log.info("flatten completed")

    def remove_image(self, pool_name, image_name):
        """
        Remove image from the specified pool
        Args:
            pool_name: name of the pool
            image_name: name of the image

        Returns:

        """
        self.exec_cmd(cmd=f"rbd rm {pool_name}/{image_name}")

    def image_meta(self, **kw):
        """Manage image-meta.

        Args:
            action: get, remove, add.
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
                sleep(60)

            for pool in pool_list:
                self.exec_cmd(
                    cmd="ceph osd pool delete {pool} {pool} "
                    "--yes-i-really-really-mean-it".format(pool=pool)
                )
