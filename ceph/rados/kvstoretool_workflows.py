"""
Module to perform specific functionalities of ceph-kvstore-tool.
ceph-kvstore-tool is a kvstore manipulation tool.
It allows users to manipulate RocksDB’s data (like OSD’s omap) offline.

ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> command [args…]


list [prefix]
    Print key of all KV pairs stored with the URL encoded prefix.

list-crc [prefix]
    Print CRC of all KV pairs stored with the URL encoded prefix.

dump [prefix]
    Print key and value of all KV pairs stored with the URL encoded prefix.

exists <prefix> [key]
    Check if there is any KV pair stored with the URL encoded prefix.
    If key is also specified, check for the key with the prefix instead.

get <prefix> <key> [out <file>]
    Get the value of the KV pair stored with the URL encoded prefix and key.
    If file is also specified, write the value to the file.

crc <prefix> <key>
    Get the CRC of the KV pair stored with the URL encoded prefix and key.

get-size [<prefix> <key>]
    Get estimated store size or size of value specified by prefix and key.

set <prefix> <key> [ver <N>|in <file>]
    Set the value of the KV pair stored with the URL encoded prefix and key. The value could be version_t or text.

rm <prefix> <key>
    Remove the KV pair stored with the URL encoded prefix and key.

rm-prefix <prefix>
    Remove all KV pairs stored with the URL encoded prefix.

store-copy <path> [num-keys-per-tx]
    Copy all KV pairs to another directory specified by path.
    [num-keys-per-tx] is the number of KV pairs copied for a transaction.

store-crc <path>
    Store CRC of all KV pairs to a file specified by path.

compac
    Subcommand compact is used to compact all data of kvstore.
    It will open the database, and trigger a database’s compaction.
    After compaction, some disk space may be released.

compact-prefix <prefix>
    Compact all entries specified by the URL encoded prefix.

compact-range <prefix> <start> <end>
    Compact some entries specified by the URL encoded prefix and range.

destructive-repair
    Make a (potentially destructive) effort to recover a corrupted database.
    Note that in the case of rocksdb this may corrupt an otherwise uncorrupted database--use this only as a last resort!

stats
    Prints statistics from underlying key-value database.
    This is only for informative purposes.
    Format and information content may vary between releases.
    For RocksDB information includes compactions stats, performance counters, memory usage and internal RocksDB stats.

histogram
    Presents key-value sizes distribution statistics from the underlying KV database.

"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class kvstoreToolWorkflows:
    """
    Contains various functions to verify ceph-kvstore-tool commands
    """

    def __init__(self, node: CephAdmin, nostop=None, nostart=None):
        """
        initializes the env to run Ceph-kvstore-Tool commands
        Args:
            node: CephAdmin obj
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.client = node.cluster.get_nodes(role="client")[0]
        self.nostop = nostop
        self.nostart = nostart

    def run_ckvt_command(
        self,
        cmd: str,
        osd_id: int,
        kv_store: str = "bluestore-kv",
        timeout: int = 300,
        mount: bool = False,
        exclude_stderr: bool = True,
    ) -> str:
        """
        Runs ceph-kvstore-tool commands within OSD container
        Args:
            cmd: command that needs to be run
            osd_id: daemon ID of target OSD
            kv_store: bluestore-kv or rocksdb
            timeout: Maximum time allowed for execution.
            mount: boolean to control mounting of /tmp directory
            to cephadm container
            exclude_stderr: flag to control logging of STDERR

        Returns:
            output of respective ceph-kvstore-tool command in string format
        """
        osd_node = self.rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=str(osd_id)
        )
        base_cmd = f"cephadm shell --name osd.{osd_id}"
        if mount:
            base_cmd = f"{base_cmd} --mount /tmp/"
        _cmd = f"{base_cmd} -- ceph-kvstore-tool {kv_store} /var/lib/ceph/osd/ceph-{osd_id} {cmd}"
        if exclude_stderr:
            _cmd += " 2> /dev/null"
        try:
            if not self.nostop:
                self.rados_obj.change_osd_state(action="stop", target=osd_id)
            out, err = osd_node.exec_command(sudo=True, cmd=_cmd, timeout=timeout)
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            raise
        finally:
            if not self.nostart:
                self.rados_obj.change_osd_state(action="start", target=osd_id)
        return str(out)

    def help(self, osd_id: int):
        """Module to run help command with ceph-kvstore-tool to display usage
         Args:
            osd_id: OSD ID for which ckvt will be executed

        Returns:
            Output of ceph-kvstore-tool usage
        """
        _cmd = "--help"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def list(self, osd_id: int, prefix: str = None):
        """Module to list keys of kV pairs
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> list <prefix>
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 list C
        """
        _cmd = "list"
        if prefix:
            _cmd = f"{_cmd} {prefix}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def list_crc(self, osd_id: int, prefix: str = None):
        """Module to list CRC of all KV pairs
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> list-crc <prefix>
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 list-crc C
        """
        _cmd = "list-crc"
        if prefix:
            _cmd = f"{_cmd} {prefix}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def dump(self, osd_id: int, prefix: str = None):
        """Module to dump all KV pairs
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> dump <prefix>
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 dump C
        """
        _cmd = "dump"
        if prefix:
            _cmd = f"{_cmd} {prefix}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def check_existence(self, osd_id: int, prefix: str, key: str = None):
        """Module to check if there is any KV pair stored with the URL encoded prefix.
        If key is also specified, check for the key with the prefix instead.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> exists <prefix>
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 dump C
        """
        _cmd = f"exists {prefix}"
        if key:
            _cmd = f"{_cmd} {key}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def get_value(self, osd_id: int, prefix: str, key: str, out_file=None):
        """Module to get the value of the KV pair stored with the URL encoded prefix and key.
         If file is also specified, write the value to the file.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
            out_file: output file to save stdout
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> get <prefix> <key> [out <file>]
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 get C
        """
        _cmd = f"get {prefix} {key}"
        if out_file:
            _cmd = f"out {out_file}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def get_crc(self, osd_id: int, prefix: str, key: str):
        """Module to get the CRC of the KV pair stored with the URL encoded prefix and key.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> crc <prefix> <key>
            e.g - ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 crc C
        """
        _cmd = f"crc {prefix} {key}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def get_size(self, osd_id: int, prefix: str = None, key: str = None):
        """Module to get estimated store size or size of value specified by prefix and key.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> get-size [<prefix> <key>]
            e.g -
            ceph-kvstore-tool bluestore-kv /var/lib/ceph/osd/ceph-6 get-size p
            %00%00%00%00%00%00%00%00%7b%3fC%c4%00%00%00%00%00%00%00%01.osd_superblock 2> /dev/null

            estimated store size: total: 0
            0
            (p,%00%00%00%00%00%00%00%00%7b%3fC%c4%00%00%00%00%00%00%00%01.osd_superblock) size 606 B
        """
        _cmd = f"get-size {prefix} {key}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def set_value(
        self, osd_id: int, prefix: str, key: str, version_t=None, in_file=None
    ):
        """Module to set the value of the KV pair stored with the URL encoded prefix and key.
        The value could be version_t or text.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
            version_t: version_t value to be set
            in_file: input file containing data to be set
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> set <prefix> <key> [ver <N>|in <file>]
        """
        _cmd = f"set {prefix} {key}"
        if in_file:
            _cmd = f"in {in_file}"
        elif version_t:
            _cmd = f"ver {version_t}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def remove(self, osd_id: int, prefix: str, key: str):
        """Module to remove the KV pair stored with the URL encoded prefix and key.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            key: ket of the KV pair
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> rm <prefix> <key>
        """
        _cmd = f"rm {prefix} {key}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def remove_prefix(self, osd_id: int, prefix: str):
        """Module to remove the KV pair stored with the URL encoded prefix.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> rm <prefix>
        """
        _cmd = f"rm {prefix}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def store_copy(self, osd_id: int, path: str, num_keys: int = None):
        """Module to copy all KV pairs to another directory specified by path.
         [num-keys-per-tx] is the number of KV pairs copied for a transaction.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            path: directory path where KV pairs should be copied
            num_keys: number of KV pairs copied for a transaction
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> store-copy <path> [num-keys-per-tx]
        """
        _cmd = f"store-copy {path}"
        if num_keys:
            _cmd = f"{_cmd} {num_keys}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def store_crc(self, osd_id: int, path: str):
        """Module to CRC of all KV pairs to a file specified by path
        Args:
            osd_id: OSD ID for which ckvt will be executed
            path: directory path where KV pairs should be copied
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> store-crc <path>
        """
        _cmd = f"store-crc {path}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def compact(self, osd_id: int):
        """Module to trigger subcommand compact is used to compact all data of kvstore.
        It will open the database, and trigger a database’s compaction. After compaction,
        some disk space may be released.
        Args:
            osd_id: OSD ID for which ckvt will be executed
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact
        """
        _cmd = "compact"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def compact_prefix(self, osd_id: int, prefix: str):
        """Module to compact all entries specified by the URL encoded prefix.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact-prefix <prefix>
        """
        _cmd = "compact-prefix"
        if prefix:
            _cmd = f"{_cmd} {prefix}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def compact_range(self, osd_id: int, prefix: str, start: str, end: str):
        """Module to compact some entries specified by the URL encoded prefix and range.
        Args:
            osd_id: OSD ID for which ckvt will be executed
            prefix: arg to filter entries
            start: start of range
            end: end of range
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> compact-range <prefix> <start> <end>
        """
        _cmd = f"compact-prefix {prefix} {start} {end}"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def destructive_repair(self, osd_id: int):
        """Module to recover a corrupted database even if it is potentially destructive
        Args:
            osd_id: OSD ID for which ckvt will be executed
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> destructive-repair
        """
        _cmd = "destructive-repair"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def print_stats(self, osd_id: int):
        """Module to print statistics from underlying key-value database.
        This is only for informative purposes. Format and information content may vary between releases.
        For RocksDB information includes compactions stats, performance counters,
        memory usage and internal RocksDB stats.
        Args:
            osd_id: OSD ID for which ckvt will be executed
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> stats
        """
        _cmd = "stats"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)

    def print_histogram(self, osd_id: int):
        """Module to present key-value sizes distribution statistics from the underlying KV database.
        Args:
            osd_id: OSD ID for which ckvt will be executed
        Returns:
            Returns the output of
            ceph-kvstore-tool <rocksdb|bluestore-kv> <store path> histogram
        """
        _cmd = "histogram"
        return self.run_ckvt_command(cmd=_cmd, osd_id=osd_id)
