import datetime
import json
from time import sleep

from ceph.cli import CephCLI
from utility.log import Log

log = Log(__name__)


class Rbd(CephCLI):
    def check_pool_exists(self, pool_name: str) -> bool:
        """
        recursively checks if the specified pool exists in the cluster
        Args:
            pool_name: Name of the pool to be checked

        Returns:  True -> pass, False -> fail

        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=200)
        while end_time > datetime.datetime.now():
            out = self.exec_cmd(cmd="ceph df -f json", output=True)
            existing_pools = json.loads(out)
            if pool_name not in [ele["name"] for ele in existing_pools["pools"]]:
                log.error(
                    f"Pool:{pool_name} not populated yet\n"
                    f"sleeping for 2 seconds and checking status again"
                )
                sleep(2)
            else:
                log.info(f"pool {pool_name} exists in the cluster")
                return True
        log.info(f"pool {pool_name} does not exist on cluster")
        return False

    def create_file_to_import(self, args):
        """
        Creates a dummy file on client node to import
        Args:
            filename: name of the dummy file
        Returns:
            out: output after execution of command
            err: error after execution of command
        """
        filename = args["file_name"]
        cmd = f"dd if=/dev/urandom of={filename} bs=4 count=5M"
        return self.exec_cmd(cmd=cmd, long_running=True)

    def create_pool(self, args):
        """
        Creates pool
        Args:
            pool_name  : name of the pool where image is to be imported
        Returns:  True -> pass, False -> fail
        """
        pool_name = args["pool_name"]
        cmd = f"ceph osd pool create {pool_name} 64 64"
        self.exec_cmd(cmd=cmd)
        if not self.check_pool_exists(pool_name=pool_name):
            log.error("Pool not created")
            return False
        self.exec_cmd(cmd="rbd pool init {}".format(pool_name))
        return True

    def create_ec_pool():
        pass

    def wait_for_operation():
        pass

    def benchwrite():
        pass

    def import_file(self, args):
        """
        Imports a file as an image to specified pool name and image name
        Args:
            filename   : name of the file to be imported
            pool_name  : name of the pool where image is to be imported
            image_name : name of the image file to be imported as

        Note: run create_file_to_import before this module in positive scenarios
        """
        filename = args["file_name"]
        pool_name = args["pool_name"]
        image_name = args["image_name"]
        cmd = f"rbd import {filename} {pool_name}/{image_name}"
        return self.exec_cmd(cmd=cmd, long_running=True)
