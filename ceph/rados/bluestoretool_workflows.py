"""
Module to perform specific functionalities of ceph-bluestore-tool.
 - ceph-bluestore-tool fsck|repair --path osd path [ --deep ]
 - ceph-bluestore-tool qfsck --path osd path
 - ceph-bluestore-tool allocmap --path osd path
 - ceph-bluestore-tool restore_cfb --path osd path
 - ceph-bluestore-tool show-label --dev device
 - ceph-bluestore-tool prime-osd-dir --dev device --path osd path
 - ceph-bluestore-tool bluefs-export --path osd path --out-dir dir
 - ceph-bluestore-tool bluefs-bdev-sizes --path osd path
 - ceph-bluestore-tool bluefs-bdev-expand --path osd path
 - ceph-bluestore-tool bluefs-bdev-new-wal --path osd path --dev-target new-device
 - ceph-bluestore-tool bluefs-bdev-new-db --path osd path --dev-target new-device
 - ceph-bluestore-tool bluefs-bdev-migrate --path osd path --dev-target new-device --devs-source device1
 - ceph-bluestore-tool free-dump|free-score --path osd path [ --allocator block/bluefs-wal/bluefs-db/bluefs-slow ]
 - ceph-bluestore-tool reshard --path osd path --sharding new sharding [ --sharding-ctrl control string ]
 - ceph-bluestore-tool show-sharding --path osd path
 - ceph-bluestore-tool bluefs-stats --path osd path
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class BluestoreToolWorkflows:
    """
    Contains various functions to verify ceph-bluestore-tool commands
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run Ceph-BlueStore-Tool commands
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.client = node.cluster.get_nodes(role="client")[0]

    def run_cbt_command(self, cmd: str, osd_id: int, timeout: int = 300) -> str:
        """
        Runs ceph-bluestore-tool commands within OSD container
        Args:
            cmd: command that needs to be run
            osd_id: daemon ID of target OSD
            timeout: Maximum time allowed for execution.
        Returns:
            output of respective ceph-bluestore-tool command in string format
        """
        osd_node = self.rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=str(osd_id)
        )
        base_cmd = f"cephadm shell --name osd.{osd_id} --"
        _cmd = f"{base_cmd} {cmd} --path /var/lib/ceph/osd/ceph-{osd_id}"
        try:
            self.rados_obj.change_osd_state(action="stop", target=osd_id)
            out, err = osd_node.exec_command(sudo=True, cmd=_cmd, timeout=timeout)
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            raise
        finally:
            self.rados_obj.change_osd_state(action="start", target=osd_id)
        return str(out)

    def help(self, osd_id: int):
        """Module to run help command with ceph-bluestore-tool to display usage
         Args:
            osd_id: OSD ID for which cbt will be executed

        Returns:
            Output of ceph-bluestore-tool usage
        """
        _cmd = "ceph-bluestore-tool --help"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def run_consistency_check(self, osd_id: int, deep: bool = False):
        """Module to run consistency check on BlueStore metadata.
        If deep is specified, also read all object data and verify checksums.
        Args:
            osd_id: OSD ID for which cbt will be executed
            deep: enables reading of all object data
        Returns:
            Returns the output of cbt quick fsck
        """
        # Extracting the ceush map from the cluster
        _cmd = "ceph-bluestore-tool fsck "
        if deep:
            _cmd = f"{_cmd} --deep true"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def run_quick_consistency_check(self, osd_id: int):
        """Module to run consistency check on BlueStore metadata comparing allocator
         data (from RocksDB CFB when exists and if not uses allocation-file) with ONodes state.
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt quick fsck
        """
        _cmd = "ceph-bluestore-tool qfsck"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def repair(self, osd_id: int):
        """Module to run a consistency check and repair any errors it can.
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt repair cmd
        """
        _cmd = "ceph-bluestore-tool repair"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def fetch_allocmap(self, osd_id: int):
        """Module to perform the same check done by qfsck and then stores
        a new allocation-file
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt allocmap cmd
        """
        _cmd = "ceph-bluestore-tool allocmap"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def restore_cfb(self, osd_id: int):
        """Module to Reverses changes done by the new NCB code
        (either through ceph restart or when running allocmap command) and
        restores RocksDB B Column-Family (allocator-map).
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt restore_cfb cmd
        """
        _cmd = "ceph-bluestore-tool restore_cfb"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def do_bluefs_export(self, osd_id: int, output_dir: str):
        """Module to export the contents of BlueFS (i.e., RocksDB files)
        to an output directory
        Args:
            osd_id: OSD ID for which cbt will be executed
            output_dir: Output directory for export
        Returns:
            Returns the output of cbt bluefs-export cmd
        """
        _cmd = f"ceph-bluestore-tool bluefs-export --out-dir {output_dir}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def block_device_sizes(self, osd_id: int):
        """Module to print the device sizes, as understood by BlueFS, to stdout.
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt bluefs-bdev-sizes cmd
        """
        _cmd = "ceph-bluestore-tool bluefs-bdev-sizes"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def block_device_expand(self, osd_id: int):
        """Module to instruct BlueFS to check the size of its block devices and,
        if they have expanded, make use of the additional space
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt bluefs-bdev-expand cmd
        """
        _cmd = "ceph-bluestore-tool bluefs-bdev-expand"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def add_wal_device(self, osd_id: int, new_device):
        """Module to add WAL device to BlueFS, fails if WAL device already exists.
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt bluefs-bdev-new-wal cmd
        """
        _cmd = f"ceph-bluestore-tool bluefs-bdev-new-wal --dev-target {new_device}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def add_db_device(self, osd_id: int, new_device):
        """Module to add DB device to BlueFS, fails if DB device already exists
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt bluefs-bdev-new-db cmd
        """
        _cmd = f"ceph-bluestore-tool bluefs-bdev-new-db --dev-target {new_device}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def block_device_migrate(self, osd_id: int, new_device, device_source):
        """Module to move BlueFS data from source device(s) to the target one,
        source devices (except the main one) are removed on success.
        Target device can be both already attached or new device. In the latter
        case it’s added to OSD replacing one of the source devices.
        Following replacement rules apply (in the order of precedence, stop on the first match):
            - if source list has DB volume - target device replaces it.
            - if source list has WAL volume - target device replace it.
            - if source list has slow volume only - operation isn’t permitted,
             requires explicit allocation via new-db/new-wal command.
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt bluefs-bdev-new-db cmd
        """
        _cmd = (
            f"ceph-bluestore-tool bluefs-bdev-migrate "
            f"--dev-target {new_device} --devs-source {device_source}"
        )
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def show_label(self, osd_id: int, device: str = None):
        """Module to show device label(s).
        Args:
            osd_id: OSD ID for which cbt will be executed
            device: device name | e.g. - block, wal, db
        Returns:
            Returns the output of cbt show-label cmd
        """
        _cmd = "ceph-bluestore-tool show-label"
        if device:
            _cmd = f"{_cmd} --dev {device}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def generate_prime_osd_dir(self, osd_id: int, device: str):
        """Module to generate the content for an OSD data directory that can start up a BlueStore OSD
        Args:
            osd_id: OSD ID for which cbt will be executed
            device: device name | e.g. - block, wal, db
        Returns:
            Returns the output of cbt prime-osd-dir cmd
        """
        _cmd = f"ceph-bluestore-tool prime-osd-dir --dev {device}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def get_free_dump(self, osd_id: int, allocator_type: str = None):
        """Module to dump all free regions in allocator
        Args:
            osd_id: OSD ID for which cbt will be executed
            allocator_type: type of device | accepted inputs - block,
                            bluefs-wal, bluefs-db, bluefs-slow
        Returns:
            Returns the output of cbt free-dump cmd
        """
        _cmd = "ceph-bluestore-tool free-dump"
        if allocator_type:
            _cmd = f"{_cmd} --allocator {allocator_type}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def get_free_score(self, osd_id: int, allocator_type: str = None):
        """Module to fetch a [0-1] number that represents quality of fragmentation in allocator.
        0 represents case when all free space is in one chunk.
        1 represents the worst possible fragmentation
        Args:
            osd_id: OSD ID for which cbt will be executed
            allocator_type: type of device | accepted inputs - block,
             bluefs-wal, bluefs-db, bluefs-slow
        Returns:
            Returns the output of cbt free-score cmd
        """
        _cmd = "ceph-bluestore-tool free-score"
        if allocator_type:
            _cmd = f"{_cmd} --allocator {allocator_type}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def do_reshard(self, osd_id: int, new_shard, control_string: str = None):
        """Module to change sharding of BlueStore’s RocksDB. Sharding is
        build on top of RocksDB column families. This option allows to test
        performance of new sharding without need to redeploy OSD.
        Resharding is usually a long process, which involves walking through
        entire RocksDB key space and moving some of them to different column families.
        Option --resharding-ctrl provides performance control over resharding process.
        Interrupted resharding will prevent OSD from running. Interrupted resharding
        does not corrupt data. It is always possible to continue previous resharding,
        or select any other sharding scheme, including reverting to original one
        Args:
            osd_id: OSD ID for which cbt will be executed
            new_shard:
            control_string: enables resharding control string
        Returns:
            Returns the output of cbt reshard cmd
        """
        _cmd = f"ceph-bluestore-tool reshard --sharding {new_shard}"
        if control_string:
            _cmd = f"{_cmd} --resharding-ctrl {control_string}"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def show_sharding(self, osd_id: int):
        """Module to show sharding that is currently applied to
        BlueStore's RocksDB
        Args:
            osd_id: OSD ID for which cbt will be executed
        Returns:
            Returns the output of cbt show-sharding cmd
        """
        _cmd = "ceph-bluestore-tool show-sharding"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)

    def show_bluefs_stats(self, osd_id: int):
        """
        Args:
            osd_id:
        Returns:
            Returns the output of cbt bluefs-stats cmd
        """
        _cmd = "ceph-bluestore-tool bluefs-stats"
        return self.run_cbt_command(cmd=_cmd, osd_id=osd_id)
