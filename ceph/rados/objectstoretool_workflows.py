"""
Module to perform specific functionalities of ceph-objectstore-tool.

ceph-objectstore-tool --data-path path to osd [--op list ]

Possible obj operations:
    (get|set)-bytes [file]
    set-(attr|omap) [file]
    (get|rm)-attr|omap)
    get-omaphdr
    set-omaphdr [file]
    list-attrs
    list-omap
    remove|removeall
    dump
    set-size
    clear-data-digest
    remove-clone-metadata
    export
    meta-list
    get-osdmap
    get-superblock
    get-inc-osdmap

ceph-objectstore-tool --data-path path to osd [ --op list $obj_ID]
    info
    log
    remove
    mkfs
    fsck
    repair
    fuse
    dup
    export
    export-remove
    import
    list
    list-slow-omap
    fix-lost
    list-pgs
    dump-super
    meta-list
    get-osdmap
    set-osdmap
    get-superblock
    set-superblock
    get-inc-osdmap
    set-inc-osdmap
    mark-complete
    reset-last-complete
    update-mon-db
    dump-export
    trim-pg-log
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class objectstoreToolWorkflows:
    """
    Contains various functions to verify ceph-objectstore-tool commands
    """

    def __init__(self, node: CephAdmin, nostop=None, nostart=None):
        """
        initializes the env to run Ceph-objectstore-Tool commands
        Args:
            node: CephAdmin obj
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.client = node.cluster.get_nodes(role="client")[0]
        self.nostop = nostop
        self.nostart = nostart

    def run_cot_command(
        self,
        cmd: str,
        osd_id: int,
        timeout: int = 300,
        mount: bool = False,
        file_redirect: bool = False,
        return_err: bool = False,
    ) -> str:
        """
        Runs ceph-objectstore-tool commands within OSD container
        Args:
            cmd: command that needs to be run
            osd_id: daemon ID of target OSD
            timeout: Maximum time allowed for execution.
            mount: boolean to control mounting of /tmp directory
            to cephadm container
            file_redirect: flag to control redirecting stdout/stderr to file
        Returns:
            output of respective ceph-objectstore-tool command in string format
        """
        osd_node = self.rados_obj.fetch_host_node(
            daemon_type="osd", daemon_id=str(osd_id)
        )
        base_cmd = f"cephadm shell --name osd.{osd_id}"
        if mount:
            base_cmd = f"{base_cmd} --mount /tmp/"
        _cmd = f"{base_cmd} -- ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-{osd_id} {cmd}"
        if file_redirect:
            _cmd = f"{_cmd} > /tmp/cot_stdout"
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
        return str(err) if return_err else str(out)

    def help(self, osd_id: int):
        """Module to run help command with ceph-objectstore-tool to display usage
         Args:
            osd_id: OSD ID for which cot will be executed

        Returns:
            Output of ceph-objectstore-tool usage
        """
        _cmd = "--help"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def list_objects(
        self,
        osd_id: int,
        pgid: str = None,
        obj_name: str = None,
        file_redirect: bool = False,
    ):
        """Module to Identify all objs within an OSD
        or Identify all objs within a placement group
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: pg ID for which objs will be listed
            obj_name: name of a specific object to be listed
            file_redirect: flag to control stdout redirection to file
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op list
        """
        # Extracting the list of objects for an OSD
        _cmd = "--op list"
        if pgid:
            _cmd = f"{_cmd} --pgid {pgid}"
        if obj_name:
            _cmd = f"{_cmd} '{obj_name}'"
        return self.run_cot_command(
            cmd=_cmd, osd_id=osd_id, file_redirect=file_redirect
        )

    def list_pgs(self, osd_id: int, file_redirect: bool = False):
        """Module to identify all placement groups within an OSD
        Args:
            osd_id: OSD ID for which cot will be executed
            file_redirect: flag to control stdout redirection to file
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op list-pgs
        """
        # Extracting the list of objects for an OSD
        _cmd = "--op list-pgs"
        return self.run_cot_command(
            cmd=_cmd, osd_id=osd_id, file_redirect=file_redirect
        )

    def get_pg_from_object(self, osd_id: int, obj_id: str):
        """Module to get PG that an object belongs to
        by using an Object Identifier
        Args:
            osd_id: OSD ID for which cot will be executed
            obj_id: obj identifier ID
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --op list $OBJECT_ID
        """
        _cmd = f"--op list {obj_id}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def fix_lost_object(self, osd_id: int, pgid: str = None, obj_id: str = None):
        """Module to fix all the lost objects within a specified placement group
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj_id: obj identifier
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --op fix-lost $OBJECT_ID
        """
        _cmd = "--op fix-lost"
        if pgid:
            _cmd = f"{_cmd} --pgid {pgid}"
        if obj_id:
            _cmd = f"{_cmd} '{obj_id}'"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_bytes(self, osd_id: int, obj: str, out_file, pgid: str = None):
        """Module to extract byte data for a provided object
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            out_file: output file for redirection
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-bytes > $OBJECT_FILE_NAME
        """
        _cmd = f"'{obj}' get-bytes > {out_file}"
        if pgid:
            _cmd = f"--pgid {pgid} {_cmd}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id, mount=True)

    def set_bytes(
        self, osd_id: int, obj: str, in_file, pgid: str = None, start: bool = True
    ):
        """Module to set byte data for an object using input file
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            in_file: output file for redirection
            start: flag to control osd restart
        Returns:
            Returns the output of cbt set-bytes cmd
        """
        if not start:
            self.nostart = True
        _cmd = f"'{obj}' set-bytes < {in_file}"
        if pgid:
            _cmd = f"--pgid {pgid} {_cmd}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id, mount=True)

    def remove_object(self, osd_id: int, pgid: str, obj: str):
        """Module to remove an object within a placement group
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT remove
        """
        _cmd = f"--pgid {pgid} '{obj}' remove"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def list_omap(self, osd_id: int, pgid: str, obj: str, file_redirect: bool = False):
        """Module to list the contents of the object map (OMAP).
         The output is a list of keys.
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            file_redirect: flag to control stdout redirection to file
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-omap
        """
        _cmd = f"--pgid {pgid} '{obj}' list-omap"
        return self.run_cot_command(
            cmd=_cmd, osd_id=osd_id, file_redirect=file_redirect
        )

    def get_omap_header(self, osd_id: int, pgid: str, obj: str, out_file):
        """Module to fetch object map header for a specific omap object
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            out_file: Output redirection file.
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omaphdr > $OBJECT_MAP_FILE_NAME
        """
        _cmd = f"--pgid {pgid} '{obj}' get-omaphdr > {out_file}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id, mount=True)

    def get_omap(self, osd_id: int, pgid: str, obj: str, key: str):
        """Module to fetch the value of a particular omap key for a specific omap object
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            key: omap key for which value is to be extracted
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omap $KEY
        """
        _cmd = f"--pgid {pgid} '{obj}' get-omap {key}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def set_omap_header(self, osd_id: int, pgid: str, obj: str, in_file):
        """Module to set object map header for a specific omap object
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            in_file: input file having header data
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omaphdr < $OBJECT_MAP_FILE_NAME
        """
        _cmd = f"--pgid {pgid} '{obj}' set-omaphdr < {in_file}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id, mount=True)

    def set_omap(self, osd_id: int, pgid: str, obj: str, key: str, in_file):
        """Module to set the value of a particular omap key for a specific omap object
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            key: omap key for which value will be set
            in_file: input file containing desired omap value
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omap $KEY < $OBJECT_MAP_FILE_NAME
        """
        _cmd = f"--pgid {pgid} '{obj}' set-omap {key} < {in_file}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def remove_omap(self, osd_id: int, pgid: str, obj: str, key: str):
        """Module to remove a particular omap entry
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            key: omap key for removal
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-omap $KEY
        """
        _cmd = f"--pgid {pgid} '{obj}' rm-omap {key}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def list_attributes(self, osd_id: int, pgid: str, obj: str):
        """Module to list an object’s attributes
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-attrs
        """
        _cmd = f"--pgid {pgid} '{obj}' list-attrs"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_attribute(self, osd_id: int, pgid: str, obj: str, attr: str):
        """Module to fetch value of a particular object attribute
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            attr: attribute name
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-attr $KEY > $OBJECT_ATTRS_FILE
        """
        _cmd = f"--pgid {pgid} '{obj}' get-attr {attr}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def set_attribute(self, osd_id: int, pgid: str, obj: str, attr: str, in_file):
        """Module to set the value of a particular object attribute
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: obj identifier
            attr: attribute name
            in_file: input file containing value for object attribute
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT  set-attr $KEY < $OBJECT_ATTRS_FILE
        """
        _cmd = f"--pgid {pgid} '{obj}' set-attr {attr} < {in_file}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def remove_attribute(self, osd_id: int, pgid: str, obj: str, attr: str):
        """Module to remove an object attribute
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: object identifier
            attr: attribute name to be removed
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-attr $KEY
        """
        _cmd = f"--pgid {pgid} '{obj}' rm-attr {attr}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def fetch_object_dump(self, osd_id: int, obj: str, pgid: str = None):
        """Module to fetch object dump
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: Placement group ID
            obj: object identifier
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD $OBJECT dump
        """
        _cmd = f" '{obj}' dump"
        if pgid:
            _cmd = f" --pgid {pgid} {_cmd}"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_superblock(self, osd_id: int):
        """Module to retrieve objectstore superblock for an OSD
        Args:
            osd_id: OSD ID for which cot will be executed
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op get-superblock
        """
        # Extracting the superblock from the osd
        _cmd = "--op get-superblock"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_osdmap(self, osd_id: int, pgid: str = None, obj_name: str = None):
        """Module to retrieve osdmap for an object in input OSD
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: pg ID for which objs will be listed
            obj_name: name of a specific object to be listed
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op get-osdmap
        """
        # Extracting the osdmap for the input obj from the osd
        _cmd = "--op get-osdmap"
        if pgid:
            _cmd = f"{_cmd} --pgid {pgid}"
        if obj_name:
            _cmd = f"{_cmd} '{obj_name}'"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_inc_osdmap(self, osd_id: int, pgid: str = None, obj_name: str = None):
        """Module to retrieve inc-osdmap for an object in input OSD
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: pg ID for which objs will be listed
            obj_name: name of a specific object to be listed
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op get-inc-osdmap
        """
        # Extracting the incremental osdmap from the osd
        _cmd = "--op get-inc-osdmap"
        if pgid:
            _cmd = f"{_cmd} --pgid {pgid}"
        if obj_name:
            _cmd = f"{_cmd} '{obj_name}'"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def get_meta_list(self, osd_id: int, pgid: str = None, obj_name: str = None):
        """Module to retrieve meta-list for the input OSD
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: pg ID for which objs will be listed
            obj_name: name of a specific object to be listed
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op meta-list
        """
        # Extracting the meta-list for the object
        _cmd = "--op meta-list"
        if pgid:
            _cmd = f"{_cmd} --pgid {pgid}"
        if obj_name:
            _cmd = f"{_cmd} '{obj_name}'"
        return self.run_cot_command(cmd=_cmd, osd_id=osd_id)

    def export(self, osd_id: int, pgid: str, out_file: str):
        """Module to export content of input PG in an OSD
        Args:
            osd_id: OSD ID for which cot will be executed
            pgid: pg ID for which data will be exported
            out_file: file to save exported data
        Returns:
            Returns the output of
            ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op export --file outfile
        """
        # Extracting the content of a PG in an OSD
        _cmd = f"--op export --pgid {pgid} --file {out_file}"
        return self.run_cot_command(
            cmd=_cmd, osd_id=osd_id, mount=True, return_err=True
        )
