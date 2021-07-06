"""
Rados Bench module to execute benchmark tests

RADOS bench testing uses the rados binary that comes with the ceph-common package.
It contains a benchmarking facility that exercises the cluster by way of librados,
the low level native object storage API provided by Ceph.

"""
import logging
from concurrent.futures import ThreadPoolExecutor
from time import time

from ceph.ceph_admin.common import config_dict_to_string

LOG = logging.getLogger(__name__)


class ClientNotFoundError(Exception):
    pass


class ClientLookupFailure(Exception):
    pass


class PoolNotFoundError(Exception):
    pass


class RadosBenchExecutionFailure(Exception):
    pass


def create_id(prefix=""):
    """
    Return unique name with prefix
    """
    return f"{prefix or 'test_run'}-{int(time())}"


def create_osd_pool(node, pool_name=None, pg_num=16):
    """
    Create OSD pool

    Args:
        node: node with Ceph CLI accessibility
        pool_name: pool name # preferred pool name else self.create_id()
        pg_num: placement group number

    Returns:
        pool_name
    """
    name = pool_name if pool_name else create_id("pool")
    node.exec_command(cmd=f"ceph osd pool create {name} {pg_num} {pg_num}", sudo=True)
    node.exec_command(cmd=f"ceph osd pool application enable {name} rados", sudo=True)
    return name


def create_pools(node, num_pools, pg_num=16):
    """
    Return list of OSD pools

    Args:
        node: node with Ceph CLI accessibility
        num_pools: required number of pool
        pg_num: placement group number

    Returns:
        pool_name
    """
    return [create_osd_pool(node=node, pg_num=pg_num) for _ in range(num_pools)]


def delete_osd_pool(node, pool_name):
    """
    Delete OSD pool
    Args:
        node: node with Ceph CLI accessibility
        pool_name: pool name
    """
    node.exec_command(
        cmd="ceph config set global mon_allow_pool_delete true", sudo=True
    )
    node.exec_command(
        cmd=f"ceph osd pool rm {pool_name} {pool_name} --yes-i-really-really-mean-it",
        sudo=True,
    )


class RadosBench:
    def __init__(self, mon_node, clients=[]):
        """
        Initialize Rados Benchmark

        Args:
            mon_node: monitor node
            clients: list of clients
        """
        self.SIGStop = False
        self.mon = mon_node
        self.clients = clients
        self.pools = []

    def fetch_client(self, node=""):
        """
        Returns node if node name provided else self.clients[0]

        Args:
            node: node name
        Returns:
            node
        """
        if not node:
            return self.clients[0]
        for client in self.clients:
            if node in client.shortname:
                return client
        raise ClientLookupFailure(f"{node} client node not found")

    @staticmethod
    def write(client, pool_name, **config):
        """
        Rados bench write test for provided time duration

        ex., rados bench 10 write -p test_bench  --no-cleanup --run-name test2

        Args:
            client: client node (CephVMNode else client will be picked from self.clients)
            pool_name: osd pool name (pool name else pool will be picked from self.pools)
            config: write ops command arguments

        config:
            seconds: duration of write ops (Default:10)
            run-name: benchmark run-name created based on (Boolean value : Optional)
            no-cleanup: no clean-up option (Boolean value, Default: false(clean-up))
            no-hints:  no-hint option (Boolean value, Default: false(hints))
            concurrent-ios: integer (String value)
            reuse-bench: bench name (String value)

        Returns:
            run_name
        """
        base_cmd = ["rados", "bench"]

        seconds = config.pop("seconds")
        base_cmd.extend(["-p", pool_name, seconds, "write"])

        run_name = config.pop("run-name", False)
        if run_name:
            run_name = create_id()
            base_cmd.append(f"--run-name {run_name}")

        base_cmd.append(config_dict_to_string(config))

        base_cmd = " ".join(base_cmd)

        client.exec_command(cmd=base_cmd, sudo=True)
        return run_name if run_name else None

    @staticmethod
    def sequential_read(client, pool_name, **config):
        """
        Rados bench sequential read test for provided time duration
            Note: there should be a write operation pre-executed.

        ex., rados bench 10 seq -p test_bench 10 seq --run-name test2

        Args:
            config: sequential read ops command arguments
            client: client node (CephVMNode)
            pool_name: osd pool name

        config:
            seconds: duration of write ops (Default:10)
            run-name: benchmark run-name created based on (Boolean value : Optional)
            no-cleanup: no clean-up option (Boolean value, Default: false(clean-up))
            no-hints:  no-hint option (Boolean value, Default: false(hints))
            concurrent-ios: integer (String value)
            reuse-bench: bench name (String value)

        Returns:
            run_name
        """
        base_cmd = ["rados", "bench"]

        run_name = config.get("run-name")

        seconds = config.pop("seconds")
        base_cmd.extend(["-p", pool_name, seconds, "seq"])

        base_cmd.append(config_dict_to_string(config))

        base_cmd = " ".join(base_cmd)

        client.exec_command(cmd=base_cmd, sudo=True)
        return run_name if run_name else None

    @staticmethod
    def cleanup(client, pool_name, run_name=""):
        """
        clean up benchmark operation

        ex., rados cleanup -p test_bench --run-name test1

        Args:
            client: client node to execute rados bench
            pool_name: osd pool name
            run_name: Rados benchmark run-name(Optional)
        """
        cmd = f"rados cleanup -p {pool_name}"
        cmd += "" if not run_name else f" --run-name {run_name}"

        client.exec_command(cmd=cmd, sudo=True)

    def stop_signal(self):
        return self.SIGStop

    def initiate_stop_signal(self):
        self.SIGStop = True

    def continuous_run(self, client, pool_name, duration):
        """
        run indefinite loop of rados bench IOs with data provided
            - write
            - sequential read
            - cleanup
        Args:
            client: client node to execute rados benchmark commands
            pool_name: ceph OSD pool name
            duration: duration of benchmark run in seconds
        """
        while not self.stop_signal():
            run_name = ""
            try:
                config = {
                    "seconds": str(duration),
                    "run-name": True,
                    "no-cleanup": True,
                }
                run_name = self.write(client, pool_name, **config)
                config["run-name"] = run_name
                self.sequential_read(client, pool_name, **config)
            except Exception:  # no qa
                raise RadosBenchExecutionFailure
            finally:
                self.cleanup(client, pool_name, run_name)
        # delete pool
        delete_osd_pool(node=self.mon, pool_name=pool_name)
        return "Done....."

    def teardown(self):
        """
        cleanup bench mark residues
        - remove pools
        """
        self.initiate_stop_signal()

    def run(self, config):
        """
        Execute benchmark,
           - Create necessary pools
           - Initiate executor
           - Submit thread
           - return

        Args:
            config: benchmark execution config

        config:
            duration: benchmark duration
            pg_num: placement group number
            pool_per_client: boolean
                # True - pool per client
                # False - one pool used by all clients
        """
        duration = config.get("duration")
        pg_num = config.get("pg_num")
        pool_per_placement = config.get("pool_per_placement")

        self.pools = create_pools(
            node=self.mon,
            num_pools=len(self.clients) if pool_per_placement else 1,
            pg_num=pg_num,
        )

        if len(self.pools) == 1:
            clients = dict((i, self.pools[0]) for i in self.clients)
        else:
            clients = zip(self.clients, self.pools)

        executor = ThreadPoolExecutor()
        try:
            for client, pool in clients.items():
                executor.submit(self.continuous_run, client, pool, duration)
        except BaseException as err:  # noqa
            LOG.error(err, exc_info=True)
            self.initiate_stop_signal()
            raise RadosBenchExecutionFailure(err)
