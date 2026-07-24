import json
import random
import string

from tests.io.io_utils import start_io
from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import convert_bytes, get_storage_stats, run_mkfs

log = Log(__name__)


class rbd_io:
    """
    This class is a tool for running different IOs on Ceph Block Device

    Functionalities:
    1. Creates the Rbd pool if not present
    2. Creates the image with size of the data that needs to be filled
    3. Mounts the image with extfs on the client nodes
    4. Runs the SmallFile IOs on the mounted directory

    Supported IO Tools :
    SmallFile
    """

    def __init__(self, client, config, **kw):
        self.client = client
        self.pool = config.get("pool", "rbd")
        self.image = config.get("image", "rbd_io")
        self.config = config
        self.fill_data = config.get("fill_data", 20)
        self.timeout = config.get("timeout", -1)
        self.io_tool = config.get("io_tool")
        self.rbd = Rbd(config=kw.get("cluster_config"), **kw)
        self.device_names = []

    def mount_rbd(self):
        """
        Creates RBD pool and Image inside it
        Mounts it on the client.

        returns:
        mounting directory
        """
        cluster_stats = get_storage_stats(self.client)
        total_size_in_gb = convert_bytes(cluster_stats.get("total_bytes"), "gb")
        self.rbd.create_pool(self.pool)
        out, rc = self.client.exec_command(
            sudo=True, cmd=f"ceph osd pool get {self.pool} size -f json"
        )
        pool = json.loads(out)
        pool_size = pool.get("size")
        total_space_to_fill = total_size_in_gb * 0.01 * (self.fill_data / pool_size)
        self.rbd.create_pool(self.pool)
        self.rbd.create_image(self.pool, self.image, f"{int(total_space_to_fill)}G")
        self.device_names.append(self.rbd.image_map(self.pool, self.image)[:-1])
        run_mkfs(
            client_node=self.client, device_name=self.device_names[-1], type="ext4"
        )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        mounting_dir_1 = f"/mnt/rbd_io_{mounting_dir}_1/"
        self.client.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir_1}")
        self.client.exec_command(
            sudo=True, cmd=f"mount {self.device_names[-1]} {mounting_dir_1}"
        )
        return mounting_dir_1

    def run_rbd_io(self):
        """
        Triggers IO on the block device mounted
        """
        log.info("Create all the pre-requisties and mount")
        mounting_dir = self.mount_rbd()
        start_io(
            self, mounting_dir, docker_compose=True, compose_file="rbd_compose.yaml"
        )
