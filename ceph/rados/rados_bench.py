"""

RADOS bench testing uses the rados binary that comes with the ceph-common package.
It contains a benchmarking facility that exercises the cluster by way of librados,
the low level native object storage API provided by Ceph.

"""

from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from time import time

from ceph.ceph_admin.common import config_dict_to_string
from utility.log import Log

LOG = Log(__name__)


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

    Args:
        prefix (Str): required prefix

    Returns:
        Unique Id (Str)
    """
    return f"{prefix or 'test_run'}-{int(time())}"


def create_osd_pool(node, pool_name=None, pg_num=16):
    """
    Create OSD pool

    Args:
        node (CephNode): node with Ceph CLI accessibility
        pool_name (Str): pool name # preferred pool name else self.create_id()
        pg_num (Int): placement group number

    Returns:
        pool name (Str)
    """
    name = pool_name if pool_name else create_id("pool")
    node.exec_command(cmd=f"ceph osd pool create {name} {pg_num} {pg_num}", sudo=True)
    node.exec_command(cmd=f"ceph osd pool application enable {name} rados", sudo=True)
    return name


def create_pools(node, num_pools, pg_num=16):
    """
    Return list of OSD pools

    Args:
        node (CephNode): node with Ceph CLI accessibility
        num_pools (Int): required number of pool
        pg_num (Int): placement group number

    Returns:
        pool names (List)
    """
    return [create_osd_pool(node=node, pg_num=pg_num) for _ in range(num_pools)]


def delete_osd_pool(node, pool_name):
    """
    Delete OSD pool

    Args:
        node (CephNode): node with Ceph CLI accessibility
        pool_name (Str): pool name
    """
    node.exec_command(
        cmd="ceph config set global mon_allow_pool_delete true", sudo=True
    )
    node.exec_command(
        cmd=f"ceph osd pool rm {pool_name} {pool_name} --yes-i-really-really-mean-it",
        sudo=True,
    )


class RadosBench:
    """Rados Bench class to execute benchmark tests"""

    def __init__(self, mon_node, clients=[]):
        """
        Initialize Rados Benchmark

        Args:
            mon_node (CephNode): monitor node
            clients (List): list of clients

        """
        self.SIGStop = False
        self.mon = mon_node
        self.clients = clients
        self.pools = []

    def fetch_client(self, node=""):
        """
        Returns node if node name provided else self.clients[0]

        Args:
            node (Str): node name

        Returns:
            node (CephNode)

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

        Args:
            client (CephVMNode): client node
            pool_name (Str): osd pool name
            config (Dict): write ops command arguments

        Returns:
            run_name (Str)

        Example::

            rados bench 10 write -p test_bench  --no-cleanup --run-name test2

            config:
                seconds (Int) : duration of write ops (Default:10)
                run-name (Str) : benchmark run-name created based on (Optional)
                no-cleanup (Bool) : no clean-up option (Default: false (clean-up))
                no-hints (Bool) :  no-hint option (Default: false(hints))
                concurrent-ios (Str) : integer (String value)
                reuse-bench (Str) : bench name (String value)
                max-objects(Str) : max number of objects to be written

        """
        base_cmd = ["rados", "bench"]
        seconds = str(config.pop("seconds"))
        _timeout = config.get("timeout", int(seconds) + 100)
        base_cmd.extend(["-p", pool_name, seconds, "write"])

        run_name = config.pop("run-name", False)
        if run_name:
            run_name = create_id()
            base_cmd.append(f"--run-name {run_name}")

        base_cmd.append(config_dict_to_string(config))
        base_cmd = " ".join(base_cmd)

        client.exec_command(cmd=base_cmd, sudo=True, timeout=_timeout)
        return run_name if run_name else None

    @staticmethod
    def sequential_read(client, pool_name, **config):
        """
        Rados bench sequential read test for provided time duration

        Args:
            config (dict) : sequential read ops command arguments
            client (CephVMNode) : client node
            pool_name (str): osd pool name

        Returns:
            run_name (str)

        Example::

            rados bench 10 seq -p test_bench 10 seq --run-name test2

            config:
                seconds: duration of write ops (Default:10)
                run-name: benchmark run-name created based on (Boolean value : Optional)
                no-cleanup: no clean-up option (Boolean value, Default: false(clean-up))
                no-hints:  no-hint option (Boolean value, Default: false(hints))
                concurrent-ios: integer (String value)
                reuse-bench: bench name (String value)

        :warning: there should be a write operation pre-executed.

        """
        base_cmd = ["rados", "bench"]

        run_name = config.get("run-name")

        seconds = str(config.pop("seconds"))
        base_cmd.extend(["-p", pool_name, seconds, "seq"])

        base_cmd.append(config_dict_to_string(config))

        base_cmd = " ".join(base_cmd)

        client.exec_command(cmd=base_cmd, sudo=True)
        return run_name if run_name else None

    @staticmethod
    def cleanup(client, pool_name, run_name=""):
        """
        clean up benchmark operation

        Args:
            client (CephNode): client node to execute rados bench
            pool_name (Str): osd pool name
            run_name (Str): Rados benchmark run-name(Optional)

        Example::

            rados cleanup -p test_bench --run-name test1

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
            client (CephNode): client node to execute rados benchmark commands
            pool_name (Str): ceph OSD pool name
            duration (Int): duration of benchmark run in seconds

        """
        try:
            LOG.info(
                f"[ {pool_name}-{client.shortname} ] RadosBench execution Initiated...."
            )
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
        finally:
            LOG.info(
                f"[ {pool_name}-{client.shortname} ] RadosBench execution Ended...."
            )
            delete_osd_pool(node=self.mon, pool_name=pool_name)

        return "Done....."

    def wait_for_completion(self):
        """
        Wait for all tasks completion
        """
        wait(self.tasks, return_when=ALL_COMPLETED)

    def teardown(self):
        """
        cleanup bench mark residues
        - remove pools
        - wait for tasks completion with cleanup
        """
        self.initiate_stop_signal()
        self.wait_for_completion()
        LOG.info("RadosBench Execution Completed......")

    def run(self, config):
        """

        Execute benchmark

           - Create necessary pools
           - Initiate executor
           - Submit thread
           - return

        Args:
            config (dict): benchmark execution config

        Example::

            config:
                duration (int): benchmark duration
                pg_num (int): placement group number
                pool_per_client (bool): True - pool per client, False - one pool used by all clients
        """
        LOG.info("RadosBench Execution Started ......")
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

        self.executor = ThreadPoolExecutor()
        self.tasks = []
        try:
            for client, pool in clients.items():
                self.tasks.append(
                    self.executor.submit(self.continuous_run, client, pool, duration)
                )
        except BaseException as err:  # noqa
            LOG.error(err, exc_info=True)
            self.initiate_stop_signal()
            raise RadosBenchExecutionFailure(err)
