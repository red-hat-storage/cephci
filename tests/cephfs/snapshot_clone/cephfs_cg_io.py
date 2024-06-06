"""
This is cephfs consistency group snapshot feature IO module
It contains methods to start read and write IO on quiesce-set/consistency group.
Also, validates IO failure by checking current quiesce state on quiesce set.
"""
import datetime
import random
import string
import time
from multiprocessing import Value
from threading import Thread

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils as CG_snap_util
from utility.log import Log

log = Log(__name__)


class CG_snap_IO(object):
    def __init__(self, ceph_cluster):
        """
        FS Snapshot Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """

        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.cg_snap_util = CG_snap_util(ceph_cluster)
        self.fs_util = FsUtils(ceph_cluster)

    def start_cg_io(
        self, clients, qs_members, client_mnt_dict, cg_test_io_status, io_run_time
    ):
        """
        Start IO on quiesce-set members and validate IO progress based on quiesce state
        Args:
            1.client: ceph client to run IO and ceph commands
            2.qs_members : A list of quiesce members, each member in format subvol1 or group1/subvol1 if
                         subvol1 belongs to non-default group
            3.client_mnt_dict : A dict type input in below format, that was obtained as return variable from
            cg_snap_utils.mount_qs_members() when quiesce set subvolumes are mounted.
            {subvolume_name1 : {
            'group_name' : group_name,'mount_point':mount_point
            },
            subvolume2 : {'mount_point':mount_point},
            }
            so a prerequisite is to mount quiesce-set members with cg_snap_utils.mount_qs_members()
            4.cg_test_io_status : Value object instance of multiprocessing module, used to return start_cg_io
            exit status when start_cg_io used as thread to run in parallel to test.
            Eg., for usage,
            Test module:
                from multiprocessing import Value
                cg_test_io_status = Value("i", 0)
                p = Thread(
                target=cg_snap_io.start_cg_io,
                args=(clients, qs_set, client_mnt_dict, cg_test_io_status, io_run_time),
                )
            5.io_run_time : Run time for IO, Required param.
        Returns: if IO failed, cg_test_io_status.value = 1, return 1
                    IO passed, cg_test_io_status.value = 0, return 0
        """

        # start read and write_io in parallel
        io_procs = {}

        io_procs = []
        log.info(f"cg_test_io_status:{cg_test_io_status.value}")
        write_client = clients[0]
        qs_member_dict1 = client_mnt_dict[write_client.node.hostname]
        # To be used when multi-client IO support is added
        # read_client = clients[1]
        # qs_member_dict2=client_mnt_dict[read_client.node.hostname]
        cg_write_io_status = Value("i", 0)
        p = Thread(
            target=self.cg_write_io,
            args=(
                cg_write_io_status,
                write_client,
                int(io_run_time),
                qs_members,
                qs_member_dict1,
            ),
        )
        p.start()
        log.info(f"started process {p.name}")
        io_procs.append(p)
        cg_read_io_status = Value("i", 0)
        p = Thread(
            target=self.cg_read_io,
            args=(
                cg_read_io_status,
                write_client,
                int(io_run_time),
                qs_members,
                qs_member_dict1,
            ),
        )
        p.start()
        log.info(f"started process {p.name}")
        io_procs.append(p)

        for io_proc in io_procs:
            io_proc.join()
        if cg_read_io_status.value == 1 or cg_write_io_status.value == 1:
            cg_test_io_status.value = 1
            err_msg = f"CG IO test failed on quiesce-set members {qs_members}: "
            log.error(
                f"{err_msg}Read IO - {cg_read_io_status.value},Write IO - {cg_write_io_status.value}"
            )
            return 1
        cg_test_io_status.value = 0
        log.info(f"CG IO test passed on quiesce-set members {qs_members}")
        log.info(f"cg_test_io_status:{cg_test_io_status.value}")
        return 0

    ##########################
    # HELPER METHODS
    ##########################

    def cg_read_io(
        self, cg_read_io_status, write_client, io_run_time, qs_members, qs_member_dict
    ):
        """
        This is called by cg_start_io using Thread to run READ IO on quiesce set members
        in parallel.
        Params Description:
        1.cg_read_io_status : A Value object instance to store exit status of this module.
        2.write_client : A client object, same client that is used for write IO(due to current limitation with
        IO implementation)
        3.io_run_time : A run time for Read IO on quiesce set members in seconds
        4.qs_members : A list of quiesce members, each member in format subvol1 or groups/subvol1 if
                         subvol1 belongs to non-default group
        5.qs_member_dict : A dict type input in below format, that was obtained as return variable from
        cg_snap_utils.mount_qs_members() when quiesce set subvolumes are mounted.
        {subvolume_name1 : {
        'group_name' : group_name,'mount_point':mount_point
        },
        subvolume2 : {'mount_point':mount_point},
        }
        so a prerequisite is to mount quiesce-set members with cg_snap_utils.mount_qs_members()
        Returns:
        if Read IO failed, cg_read_io_status.value = 1, return 1
           Read IO passed, cg_read_io_status.value = 0, return 0

        """

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=io_run_time)
        read_procs = []
        read_fail = 0
        log.info("In cg_read_io function")
        proc_status_list = []
        while datetime.datetime.now() < end_time:
            for qs_member in qs_member_dict:
                qs_mnt_pt = qs_member_dict[qs_member]["mount_point"]
                read_proc_check_status = Value("i", 0)
                proc_status_list.append(read_proc_check_status)
                p = Thread(
                    target=self.cg_read_io_subvol,
                    args=(
                        read_proc_check_status,
                        write_client,
                        qs_member,
                        qs_mnt_pt,
                        qs_members,
                    ),
                )
                p.start()
                read_procs.append(p)

            for read_proc in read_procs:
                read_proc.join()
            read_fail = 0

            for proc_status in proc_status_list:
                if proc_status.value == 1:
                    log.error("Read IO failed on quiesce-set member")
                    read_fail = 1
            if read_fail == 1:
                log.info(f"cg_read_io_status before update : {cg_read_io_status.value}")
                cg_read_io_status.value = 1
                log.info(f"cg_read_io_status after update : {cg_read_io_status.value}")
                return 1

        log.info(f"cg_read_io_status before update : {cg_read_io_status.value}")
        cg_read_io_status.value = 0
        log.info(f"cg_read_io_status after update : {cg_read_io_status.value}")

        return 0

    def cg_write_io(
        self, cg_write_io_status, client, io_run_time, qs_members, qs_member_dict
    ):
        """
        This is called by cg_start_io using Thread to run WRITE IO on quiesce set members
        in parallel.
        Params Description:
        1.cg_write_io_status : A Value object instance to store exit status of this module.
        2.client : A client object to run IO
        3.io_run_time : A run time for Write IO on quiesce set members in seconds
        4.qs_members : A list of quiesce members, each member in format subvol1 or groups/subvol1 if
                         subvol1 belongs to non-default group
        5.qs_member_dict : A dict type input in below format, that was obtained as return variable from
        cg_snap_utils.mount_qs_members() when quiesce set subvolumes are mounted.
        {subvolume_name1 : {
        'group_name' : group_name,'mount_point':mount_point
        },
        subvolume2 : {'mount_point':mount_point},
        }
        so a prerequisite is to mount quiesce-set members with cg_snap_utils.mount_qs_members()
        Returns:
        if Write IO failed, cg_write_io_status.value = 1, return 1
           Write IO passed, cg_write_io_status.value = 0, return 0

        """

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=io_run_time)
        write_procs = []
        write_fail = 0
        proc_status_list = []
        log.info("In cg_write_io function")
        while datetime.datetime.now() < end_time:
            for qs_member in qs_member_dict:
                qs_mnt_pt = qs_member_dict[qs_member]["mount_point"]
                write_proc_check_status = Value("i", 0)
                proc_status_list.append(write_proc_check_status)
                p = Thread(
                    target=self.cg_write_io_subvol,
                    args=(
                        write_proc_check_status,
                        client,
                        qs_member,
                        qs_mnt_pt,
                        qs_members,
                    ),
                )
                p.start()
                write_procs.append(p)
            for write_proc in write_procs:
                write_proc.join()
            for proc_status in proc_status_list:
                if proc_status.value == 1:
                    log.error(f"Write IO failed on quiesce-set member {qs_member}")
                    write_fail = 1
            if write_fail == 1:
                log.info(
                    f"cg_write_io_status before update : {cg_write_io_status.value}"
                )
                cg_write_io_status.value = 1
                log.info(
                    f"cg_write_io_status after update : {cg_write_io_status.value}"
                )
                return 1

        log.info(f"cg_write_io_status before update : {cg_write_io_status.value}")
        cg_write_io_status.value = 0
        log.info(f"cg_write_io_status after update : {cg_write_io_status.value}")

        return 0

    def cg_read_io_subvol(
        self, read_proc_check_status, write_client, qs_member, mnt_pt, qs_members
    ):
        """
        This is called by cg_read_io using Thread to run READ IO on given quiesce member
        READ IO is run by sequentially executing IO tools - DD,Linux_cmds,FIO and SmallFile
        on subvolume and validating IO tool exit status after each run.
        Params Description:
        1.read_proc_check_status : A Value object instance to store exit status of this module.
        2.write_client : A client object to run Read IO
        3.qs_member : A subvolume name where READ IO occurs, this is a key name from qs_member_dict
        (obtained as return variable from cg_snap_utils.mount_qs_members()))
        4.mnt_pt : A mount point of 'qs_member' in write_client
        5.qs_members : A list of quiesce members, each member in format subvol1 or groups/subvol1 if
                         subvol1 belongs to non-default group
        Returns:
        if READ IO failed, read_proc_check_status.value = 1, return 1
           READ IO passed, read_proc_check_status.value = 0, return 0
        """
        log.info("In cg_read_io_subvol function")
        io_modules = {}

        read_status, end_time = self.cg_linux_cmds_read(write_client, mnt_pt)

        log.info(f"linux_cmds read end_time:{end_time}")
        io_modules.update({"cg_linux_cmds_read": read_status})
        log.info(f"io_modules:{io_modules}")
        lc_status = self.validate_exit_status(
            write_client, qs_member, io_modules, "read", end_time, qs_members
        )
        log.info(
            f"cg_linux_cmds_read, read_status:{read_status},read_io_validate_status : {lc_status}"
        )
        io_modules.clear()
        """
        BZ 2280603 : FIO Tool Read op not suceeding when subvolume in quiesced state.
        Not running FIO Read until BZ is resolved or read op blocked issue is analyzed and concluded
        if lc_status == 0:
            read_status, end_time = self.cg_fio_io_read(write_client, mnt_pt)
            log.info(
                f"{qs_member},{mnt_pt}-fio_read end_time:{end_time},fio_read status:{read_status}"
            )
            io_modules.update({"cg_fio_io_read": read_status})
            log.info(f"io_modules:{io_modules}")
            fio_status = self.validate_exit_status(
                write_client, qs_member, io_modules, "read", end_time, qs_members
            )
            log.info(
                f"cg_fio_io_read, read_status:{read_status},read_io_validate_status : {fio_status}"
            )
            io_modules.clear()
        """
        fio_status = 0
        if lc_status == 0 and fio_status == 0:
            read_status, end_time = self.cg_smallfile_io_read(write_client, mnt_pt)
            log.info(
                f"{qs_member},{mnt_pt}-smallfile read end_time:{end_time},smallfile read status:{read_status}"
            )
            io_modules.update({"cg_smallfile_io_read": read_status})
            log.info(f"io_modules:{io_modules}")
            sf_status = self.validate_exit_status(
                write_client, qs_member, io_modules, "read", end_time, qs_members
            )
            log.info(
                f"cg_smallfile_io_read, read_status:{read_status},read_io_validate_status : {sf_status}"
            )
            io_modules.clear()

        log.info(
            f"cg_read_io_subvol {qs_member} read_proc_check_status before update : {read_proc_check_status.value}"
        )
        if lc_status == 1 or fio_status == 1 or sf_status == 1:
            read_proc_check_status.value = 1
        else:
            read_proc_check_status.value = 0

        log.info(
            f"cg_read_io_subvol {qs_member} read_proc_check_status after update : {read_proc_check_status.value}"
        )
        return read_proc_check_status.value

    def cg_write_io_subvol(
        self, write_proc_check_status, client, qs_member, mnt_pt, qs_members
    ):
        """
        This is called by cg_write_io using Thread to run Write IO on given quiesce member
        Write IO is run by sequentially executing IO tools - DD,FIO,SmallFile,Linux_cmds,and Crefi
        on subvolume and validating IO tool exit status after each run.
        Params Description:
        1.write_proc_check_status : A Value object instance to store exit status of this module.
        2.client : A client object to run Write IO
        3.qs_member : A subvolume name where Write IO occurs, this is a key name from qs_member_dict
        (obtained as return variable from cg_snap_utils.mount_qs_members()))
        4.mnt_pt : A mount point of 'qs_member' in write_client
        5.qs_members : A list of quiesce members, each member in format subvol1 or groups/subvol1 if
                         subvol1 belongs to non-default group
        Returns:
        if Write IO failed, write_proc_check_status.value = 1, return 1
           Write IO passed, write_proc_check_status.value = 0, return 0
        """

        log.info("In cg_write_io_subvol function")
        io_modules = {}

        write_status, end_time = self.cg_dd_io_write(client, mnt_pt)

        log.info(f"dd write end_time:{end_time}")
        io_modules.update({"cg_dd_io_write": write_status})
        log.info(f"io_modules:{io_modules}")
        dd_status = self.validate_exit_status(
            client, qs_member, io_modules, "write", end_time, qs_members
        )
        log.info(
            f"cg_dd_io_write, write_status:{write_status},write_io_validate_status : {dd_status}"
        )

        if dd_status == 0:
            io_modules.clear()
            write_status, end_time = self.cg_fio_io_write(client, mnt_pt)

            log.info(f"fio write end_time:{end_time}")
            io_modules.update({"cg_fio_io_write": write_status})
            log.info(f"io_modules:{io_modules}")
            fio_status = self.validate_exit_status(
                client, qs_member, io_modules, "write", end_time, qs_members
            )
            log.info(
                f"cg_fio_io_write, write_status:{write_status},write_io_validate_status : {fio_status}"
            )
        if dd_status == 0 and fio_status == 0:
            io_modules.clear()
            write_status, end_time = self.cg_smallfile_io_write(client, mnt_pt)

            log.info(f"smallfile write end_time:{end_time}")
            io_modules.update({"cg_smallfile_io_write": write_status})
            log.info(f"io_modules:{io_modules}")
            sf_status = self.validate_exit_status(
                client,
                qs_member,
                io_modules,
                "write",
                end_time,
                qs_members,
            )
            log.info(
                f"cg_smallfile_io_write, write_status:{write_status},write_io_validate_status : {sf_status}"
            )
        if dd_status == 0 and fio_status == 0 and sf_status == 0:
            io_modules.clear()
            write_status, end_time = self.cg_linux_cmds_write(client, mnt_pt)

            log.info(f"linux_cmds write end_time:{end_time}")
            io_modules.update({"cg_linux_cmds_write": write_status})
            log.info(f"io_modules:{io_modules}")
            lc_status = self.validate_exit_status(
                client, qs_member, io_modules, "write", end_time, qs_members
            )
            log.info(
                f"cg_linux_cmds_write, write_status:{write_status},write_io_validate_status : {lc_status}"
            )
        if dd_status == 0 and fio_status == 0 and sf_status == 0 and lc_status == 0:
            io_modules.clear()
            write_status, end_time = self.cg_crefi_io(client, mnt_pt)

            log.info(f"crefi write end_time:{end_time}")
            io_modules.update({"cg_crefi_io": write_status})
            log.info(f"io_modules:{io_modules}")
            crefi_status = self.validate_exit_status(
                client, qs_member, io_modules, "write", end_time, qs_members
            )
            log.info(
                f"cg_crefi_io, write_status:{write_status},write_io_validate_status : {crefi_status}"
            )

        log.info(
            f"cg_write_io_subvol {qs_member} write_proc_check_status before update : {write_proc_check_status.value}"
        )
        if (
            dd_status == 1
            or fio_status == 1
            or sf_status == 1
            or lc_status == 1
            or crefi_status == 1
        ):
            write_proc_check_status.value = 1
        else:
            write_proc_check_status.value = 0

        log.info(
            f"cg_write_io_subvol {qs_member} write_proc_check_status after update : {write_proc_check_status.value}"
        )
        return write_proc_check_status.value

    def validate_exit_status(
        self, client, qs_member, io_modules, io_type, end_time, qs_members
    ):
        """
        This is called by cg_write_io_subvol and cg_read_io_subvol to validate exit status of IO tools.
        This is done by fetching current state of quiesce set with qs_members and evaluating against IO exit status.

        Params Description:
        2.client : A client object to run ceph commands
        3.qs_member : A subvolume name where IO occurs, this is a key name from qs_member_dict
        (obtained as return variable from cg_snap_utils.mount_qs_members()))
        4.io_modules : A dict type variable with key as io_tool name and value as io_tool exit status
        5.qs_members : A list of quiesce members, each member in format subvol1 or groups/subvol1 if
                         subvol1 belongs to non-default group
        Returns:
        if Validation results in failure, return 1, else return 0
        """
        log.info(f"In validate_exit_status function - {io_type} , {io_modules}")
        cg_status = 0

        for io_mod in io_modules:
            if io_type == "read":
                if io_modules[io_mod] == 1:
                    qs_id = self.cg_snap_util.get_qs_id(client, qs_members)
                    log.info(f"qs_id:{qs_id}")
                    if qs_id != 1:
                        qs_query = self.cg_snap_util.get_qs_query(client, qs_id=qs_id)
                        log.info(f"qs_query:{qs_query}")
                        state = qs_query["sets"][qs_id]["state"]["name"]
                        log.info(f"io_modules:{io_modules}")
                        log.error(
                            f"FAIL:{io_mod} io on {qs_member} failed when {qs_id} was in {state}"
                        )
                    else:
                        log.info(f"io_modules:{io_modules}")
                        log.error(
                            f"FAIL:{io_mod} io on {qs_member} failed when no QS exists"
                        )
                    cg_status = 1
            elif io_type == "write":
                if io_modules[io_mod] == 1:
                    qs_id = self.cg_snap_util.get_qs_id(client, qs_members)
                    log.info(f"qs_id:{qs_id}")
                    if qs_id != 1:
                        qs_query = self.cg_snap_util.get_qs_query(client, qs_id=qs_id)
                        log.info(f"qs_query:{qs_query}")
                        state = qs_query["sets"][qs_id]["state"]["name"]
                        age_ref = qs_query["sets"][qs_id]["age_ref"]
                        curr_time = time.time()
                        # Get time when state is achieved, add buffer 1secs
                        time_before_state = (float(curr_time) - float(age_ref)) + 1
                        for member_item in qs_query["sets"][qs_id]["members"]:
                            if qs_member in member_item:
                                excluded = qs_query["sets"][qs_id]["members"][
                                    member_item
                                ]["excluded"]
                                age = qs_query["sets"][qs_id]["members"][member_item][
                                    "state"
                                ]["age"]
                                break
                        if excluded is True:
                            # Get time when state is achieved, add buffer 1secs
                            time_before_state = (float(curr_time) - float(age)) + 1
                            log.info(
                                f"time_before_state:{time_before_state},end_time:{end_time}"
                            )
                            log.info(f"age:{age},curr_time:{curr_time}")
                            if float(time_before_state) > float(end_time):
                                log.info(
                                    f"WARN: {io_mod} {io_type} fails in QS state {state} on {qs_member}"
                                )
                                time_diff = int(
                                    float(time_before_state) - float(end_time)
                                )
                                log.info(
                                    f"{state} is achieved in {time_diff}secs after end_time,could be false positive"
                                )
                            else:
                                log.error(
                                    f"FAIL: {io_mod} {io_type} fails in {state} on {qs_member}, excluded-{excluded}"
                                )
                                cg_status = 1
                        elif state in "RELEASED|EXPIRED|TIMEDOUT|CANCELED":
                            log.info(
                                f"time_before_state:{time_before_state},end_time:{end_time}"
                            )
                            log.info(f"age_ref:{age_ref},curr_time:{curr_time}")
                            if state == "RELEASED" and (
                                float(time_before_state) > float(end_time)
                            ):
                                log.info(
                                    f"WARN: {io_mod} {io_type} fails in QS state {state} on {qs_member}"
                                )
                                time_diff = int(
                                    float(time_before_state) - float(end_time)
                                )
                                log.info(
                                    f"{state} is achieved in {time_diff}secs after end_time,could be false positive"
                                )
                            else:
                                log.error(
                                    f"FAIL: {io_mod} {io_type} fails when {state}  on {qs_member} - age:{age_ref}secs"
                                )
                                cg_status = 1
                        else:
                            log.info(
                                f"Expected: {io_mod} {io_type} fails in QS state {state} on {qs_member}"
                            )
                    if qs_id == 1:
                        log.error(
                            f"FAIL:{io_mod} {io_type} fails when no QS exists with {qs_member}"
                        )
                        cg_status = 1
                elif io_modules[io_mod] == 0:
                    qs_id = self.cg_snap_util.get_qs_id(client, qs_members)
                    log.info(f"qs_id:{qs_id}")
                    if qs_id != 1:
                        qs_query = self.cg_snap_util.get_qs_query(client, qs_id=qs_id)
                        log.info(f"qs_query:{qs_query}")
                        state = qs_query["sets"][qs_id]["state"]["name"]
                        curr_time = time.time()
                        for member_item in qs_query["sets"][qs_id]["members"]:
                            if qs_member in member_item:
                                excluded = qs_query["sets"][qs_id]["members"][
                                    member_item
                                ]["excluded"]
                                age = qs_query["sets"][qs_id]["members"][member_item][
                                    "state"
                                ]["age"]
                                # Get time when state is achieved, add buffer 1secs
                                time_before_state = (float(curr_time) - float(age)) + 1
                                log.info(
                                    f"time_before_state:{time_before_state},end_time:{end_time}"
                                )
                                log.info(f"curr_time:{curr_time},age:{age}")
                                break
                        if excluded is False:
                            if state in "QUIESCED|RELEASING|CANCELING":
                                if state == "QUIESCED" and (
                                    float(time_before_state) > float(end_time)
                                ):
                                    log.info(
                                        f"{io_mod} {io_type} passed on {qs_member} when {state},exclude-{excluded}"
                                    )
                                    log.info(
                                        f"{state} is achieved in {age}secs,could be false positive"
                                    )
                                else:
                                    log.error(
                                        f"FAIL:{io_mod} {io_type} pass on {qs_member} when {state},exclude-{excluded}"
                                    )
                                    cg_status = 1
                        else:
                            log.info(
                                f"Expected:{io_mod} {io_type} pass on {qs_member} when {state},exclude-{excluded}"
                            )
        return cg_status

    """
    def cg_dd_io_read(self,client,mnt_pt):
        io_type = 'read'
        log.info(f"In cg_dd_io function : {io_type}")
        try:
            #wait for files to be created with write_io
            time.sleep(10)
            #list files in mnt_pt/cg_io/dd_dir dir
            dir_path = f"{mnt_pt}/cg_io/dd_dir"
            rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
            )
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=60)
            while datetime.datetime.now() < end_time:
                out,rc = client.exec_command(sudo=True, cmd=f"ls {dir_path}")
                log.info(f"dd_io {io_type} cmd op : {out}")
                out = out.strip()
                files = out.split()
                log.info(f"dd_io {io_type} files op : {files}")
                if len(files) > 0:
                    break
                time.sleep(5)
            file_cnt = len(files)
            if file_cnt == 0:
                raise Exception(f"No files created in {dir_path} even after 60secs")
            max_cnt = 5
            if file_cnt < max_cnt:
                max_cnt = file_cnt
            read_files = [random.choice(files) for _ in range(max_cnt)]
            log.info(f"dd_io {io_type} read_files op : {read_files}")
            #on random max 5 files, perform dd
            bs_list = ['10K', '64K','256K']
            for file in read_files:
                bs = random.choice(bs_list)
                count = random.randrange(1,5)
                file_path = f"{dir_path}/{file}"
                cmd = f"touch /tmp/cg_dd_io_{rand_str}.log"
                log.info(f"dd_io {io_type} cmd to run: {cmd}")
                out,_ = client.exec_command(sudo=True, cmd=cmd)
                cmd = f"dd of={file_path} bs={bs} count={count} > /tmp/cg_dd_io_{rand_str}.log"
                log.info(f"dd_io {io_type} cmd to run: {cmd}")
                out,_ = client.exec_command(sudo=True, cmd=cmd)
                log.info(f"dd_io {io_type} {cmd} op : {out}")
            log.info(f"dd_io {io_type}, dd_io_read_status before update : {dd_io_read_status.value}")
            dd_io_read_status.value =  0
            log.info(f"dd_io {io_type}, dd_io_read_status after update : {dd_io_read_status.value}")
            return 0
        except Exception as ex:
            log.info(ex)
            log.info(f"dd_io {io_type}, dd_io_read_status before update : {dd_io_read_status.value}")
            dd_io_read_status.value = 1
            log.info(f"dd_io {io_type}, dd_io_read_status after update : {dd_io_read_status.value}")
            return 1
    """

    def cg_dd_io_write(self, client, mnt_pt):
        """
        IO tool called by cg_write_io_subvol to run DD write cmds on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where DD write cmds are executed
        Returns: 0 - Pass, 1 - Fail
        """

        io_type = "write"
        log.info(f"In cg_dd_io function : {io_type}")
        try:
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            sleep_time = random.randrange(1, 5)
            time.sleep(sleep_time)
            try:
                cmd = f"mkdir {mnt_pt}/cg_io/;mkdir {mnt_pt}/cg_io/dd_dir;"
                client.exec_command(sudo=True, cmd=cmd, timeout=15)
            except Exception as ex:
                log.info(f"dd_io {io_type} {cmd} failed on {mnt_pt}, retrying:{ex}")
                time.sleep(2)
                cmd = f"mkdir -p {mnt_pt}/cg_io/dd_dir"
                client.exec_command(sudo=True, cmd=cmd, timeout=15)
            file_path = f"{mnt_pt}/cg_io/dd_dir/dd_testfile_{rand_str}"
            log.info(f"file_path:{file_path}")
            # write 5 files using system files
            bs_list = ["10K", "64K", "256K"]
            bs = random.choice(bs_list)
            src_files = [
                "/var/log/messages",
                "/var/log/cloud-init.log",
                "/var/log/cron",
            ]
            src_file = random.choice(src_files)
            cmd = f"dd if={src_file} of={file_path} bs={bs}"
            log.info(f"dd_io {io_type} cmd to run: {cmd}")
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=True, timeout=15)
            log.info(f"dd_io {io_type} {cmd} op : {out}")
            client.exec_command(
                sudo=True,
                cmd=f"echo {src_file} >> {file_path}",
                check_ec=True,
                timeout=20,
            )
            out, _ = client.exec_command(
                sudo=True, cmd=f"ls -l {file_path}", check_ec=True, timeout=15
            )

            end_time = time.time()
            log.info(
                f"dd_io {io_type} {cmd} passed on {file_path} with end_time {end_time}: {out}"
            )
            return (0, end_time)
        except Exception as ex:
            end_time = time.time()
            log.info(
                f"dd_io {io_type} {cmd} failed on {file_path} with end_time {end_time}:{ex}"
            )
            return (1, end_time)

    def cg_smallfile_io_read(self, client, mnt_pt):
        """
        IO tool called by cg_read_io_subvol to run SmallFile on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where smallfile executes
        Returns: 0 - Pass, 1 - Fail
        """

        io_type = "read"
        log.info(f"In cg_smallfile_io function:{io_type}")
        time.sleep(10)
        dir_path = f"{mnt_pt}/cg_io/smallfile_io_dir"
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=60)

        while datetime.datetime.now() < end_time:
            try:
                out, rc = client.exec_command(
                    sudo=True, cmd=f"ls -d {dir_path}/smallfile_*/", timeout=15
                )
                log.info(f"smallfile_io {io_type} cmd op : {out}")
                out = out.strip()
                smallfile_dirs = out.split()
                log.info(f"smallfile_io {io_type} smallfile_dirs op : {smallfile_dirs}")
                if len(smallfile_dirs) > 0:
                    break
            except Exception as ex:
                log.info(ex)
                time.sleep(5)
        dir_cnt = len(smallfile_dirs)
        if dir_cnt == 0:
            raise Exception(
                f"No smallfile dirs created in {dir_path} even after 60secs"
            )
        smallfile_dir = random.choice(smallfile_dirs)
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        i = 0
        retry_cnt = 2
        while i < retry_cnt:
            try:
                out_tmp, _ = client.exec_command(
                    sudo=True,
                    cmd=f"mkdir -p /var/tmp/smallfile_dir_{rand_str}",
                    timeout=10,
                )
                log.info(out_tmp)
                log.info(f"Smallfile read on {mnt_pt}, Iteration {i}")
                out, _ = client.exec_command(
                    sudo=True,
                    cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
                    f"--operation read --threads 1 --file-size 1024 "
                    f"--files 1 --top {smallfile_dir} --network-sync-dir /var/tmp/smallfile_dir_{rand_str}",
                    timeout=30,
                )
                log.info(f"smallfile_io {io_type} cmd op : {out}")
                end_time = time.time()
                log.info(
                    f"Iter{i}:smallfile_io_read passed on {smallfile_dir} with end_time {end_time}"
                )
                return (0, end_time)
            except Exception as ex:
                exp_str = (
                    "No such file or directory: '/var/tmp/starting_gate.tmp.notyet'"
                )
                if exp_str in ex:
                    end_time = time.time()
                    log.info(
                        f"smallfile_io_read passed on {smallfile_dir} with end_time {end_time}"
                    )
                    return (0, end_time)
                else:
                    log.info(
                        f"smallfile_io_read failed on {smallfile_dir} in Iter{i}:{ex}"
                    )
                    i += 1

        end_time = time.time()
        log.info(
            f"smallfile_io_read failed on {smallfile_dir} with end_time {end_time}"
        )
        return (1, end_time)

    def cg_smallfile_io_write(self, client, mnt_pt):
        """
        IO tool called by cg_write_io_subvol to run SmallFile on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where smallfile executes
        Returns: 0 - Pass, 1 - Fail
        """
        io_type = "write"
        log.info(f"In cg_smallfile_io function:{io_type}")
        try:
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            dir_path = f"{mnt_pt}/cg_io/smallfile_io_dir/smallfile_{rand_str}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
            cmd_list = [
                "create",
                "append",
                "delete",
                "create",
                "overwrite",
                "rename",
                "delete-renamed",
                "mkdir",
                "rmdir",
                "create",
                "symlink",
                # "chmod",
            ]

            for i in cmd_list:
                out_list = []
                out_list = client.exec_command(
                    sudo=True,
                    cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
                    f"--operation {i} --threads 1 --file-size 1024 "
                    f"--files 1 --top {dir_path}",
                    long_running=True,
                    timeout=20,
                )
                log.info(f"smallfile write output for {i} on {dir_path}:{out_list}")
            end_time = time.time()
            log.info(
                f"smallfile_io_write passed on {dir_path} with end_time {end_time}"
            )
            return (0, end_time)
        except Exception as ex:
            end_time = time.time()
            log.info(
                f"smallfile_io_write failed on {dir_path} with end_time {end_time}:{ex}"
            )
            return (1, end_time)

    def cg_fio_io_read(self, client, mnt_pt):
        """
        IO tool called by cg_read_io_subvol to run FIO on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where fio executes
        Returns: 0 - Pass, 1 - Fail
        """
        io_type = "read"
        log.info(f"In cg_fio_io function:{io_type}")

        time.sleep(10)
        dir_path = f"{mnt_pt}/cg_io/fio_io_dir"
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=100)
        fio_files = []
        while datetime.datetime.now() < end_time:
            try:
                out, rc = client.exec_command(
                    sudo=True, cmd=f"ls {dir_path}/fio_cg_test_*", timeout=15
                )
                log.info(f"fio_io {io_type} cmd op : {out}")
                out = out.strip()
                fio_files = out.split()
                log.info(f"fio_io {io_type} op : {fio_files}")
                if len(fio_files) > 0:
                    break
                time.sleep(5)
            except Exception as ex:
                log.info(ex)
                time.sleep(5)

        fio_cnt = len(fio_files)
        if fio_cnt == 0:
            raise Exception(f"No fio files created in {dir_path} even after 60secs")
        fio_file = random.choice(fio_files)
        retry_cnt = 2
        i = 0
        while i < retry_cnt:
            try:
                log.info(f"fio read on {mnt_pt}, Iteration {i}")
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"fio --name={fio_file} --ioengine=libaio --size 2M --rw=read --bs=1M --direct=1 "
                    f"--numjobs=1 --iodepth=5 --runtime=10 --debug=io",
                    timeout=30,
                )
                end_time = time.time()
                log.info(
                    f"Iter{i}:fio_io read passed on {fio_file} with end_time {end_time}: {out}"
                )
                return (0, end_time)

            except Exception as ex:
                log.info(f"fio_io read failed on {fio_file} in Iter{i}: {ex} ")
                i += 1

        end_time = time.time()
        log.info(f"fio_io read failed on {fio_file} with end_time {end_time}")
        return (1, end_time)

    def cg_fio_io_write(self, client, mnt_pt):
        """
        IO tool called by cg_write_io_subvol to run FIO on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where fio executes
        Returns: 0 - Pass, 1 - Fail
        """
        io_type = "write"
        log.info(f"In cg_fio_io function:{io_type}")
        try:
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            dir_path = f"{mnt_pt}/cg_io/fio_io_dir"
            client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
            file_name = f"fio_cg_test_{rand_str}"
            file_path = f"{dir_path}/{file_name}"
            client.exec_command(
                sudo=True,
                cmd=f"fio --name={file_path} --ioengine=libaio --size 2M --rw=write --bs=1M --direct=1 "
                f"--numjobs=1 --iodepth=5 --runtime=10",
                timeout=20,
                long_running=True,
            )
            end_time = time.time()
            log.info(f"fio write passed on {file_path} with end_time {end_time} ")
            return (0, end_time)
        except Exception as ex:
            end_time = time.time()
            log.info(f"fio write failed on {file_path} with end_time {end_time}: {ex}")
            return (1, end_time)

    def cg_crefi_io(self, client, mnt_pt):
        """
        IO tool called by cg_write_io_subvol to run CREFI on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where Crefi executes
        Returns: 0 - Pass, 1 - Fail
        """
        log.info("In cg_crefi_io function")
        try:
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            dir_path = f"{mnt_pt}/cg_io/crefi_io_dir/crefi_{rand_str}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}", timeout=15)
            file_type_list = ["binary", "text", "tar"]
            file_type = random.choice(file_type_list)
            cmd_list = [
                "create",
                "rename",
                "chmod",
                "chown",
                "chgrp",
                "symlink",
                "hardlink",
                # "truncate",
            ]
            for i in cmd_list:
                out_list = []
                out_list = client.exec_command(
                    sudo=True,
                    cmd=f"python3 /home/cephuser/Crefi/crefi.py "
                    f"-n 5 --max 100k --min 10k -t {file_type} -b 2 -d 2 --multi --fop {i} -T 2 {dir_path}",
                    long_running=True,
                    timeout=20,
                )
                log.info(f"after crefi_io cmd on {dir_path}:{out_list}")

            end_time = time.time() - 1
            log.info(f"Crefi write passed on {dir_path} with end_time {end_time}")
            return (0, end_time)
        except Exception as ex:
            end_time = time.time()
            log.info(f"Crefi write failed on {dir_path} with end_time {end_time}:{ex}")
            return (1, end_time)

    def cg_linux_cmds_read(self, client, mnt_pt):
        """
        IO tool called by cg_read_io_subvol to run Read type linux cmds on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where linux read cmds are executed
        Returns: 0 - Pass, 1 - Fail
        """
        io_type = "read"
        log.info(f"In cg_linux_cmds function : {io_type}")

        try:
            dir_path = f"{mnt_pt}/cg_io/dd_dir"
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
            cmd_pass = 0
            while datetime.datetime.now() < end_time and cmd_pass == 0:
                try:
                    out, _ = client.exec_command(
                        sudo=True, cmd=f"ls {dir_path}", timeout=15
                    )
                    log.info(f"linux_cmds {io_type} cmd op : {out}")
                    out = out.strip()
                    files = out.split()
                    cmd_pass = 1
                except Exception as ex:
                    log.info(ex)
                    time.sleep(5)
            retry_cnt = 5
            while retry_cnt > 0:
                file_name = random.choice(files)
                retry_cnt -= 1
                if "_copy" not in file_name:
                    retry_cnt = 0
            client_name = client.node.hostname
            client_name_1 = f"{client_name[0: -1]}"
            read_cmd_dict = {
                "grep": f"grep -m 2 -H {client_name_1} {dir_path}/{file_name}",
                "cat": f"touch /tmp/cg_dd_io_cmds.log;cat {dir_path}/{file_name} > /tmp/cg_dd_io_cmds.log",
                "getfacl": f"getfacl {dir_path}/{file_name}",
                "dir": f"dir {dir_path}",
                "ls -l": f"ls -l {mnt_pt}/cg_io/*",
            }
            for cmd in read_cmd_dict:
                log.info(
                    f"linux_cmds {io_type} cmd to run on {mnt_pt}: {read_cmd_dict[cmd]}"
                )
                out, _ = client.exec_command(
                    sudo=True, cmd=read_cmd_dict[cmd], timeout=30
                )
                log.info(
                    f"linux_cmds read passed on {mnt_pt} for {read_cmd_dict[cmd]} : {out}"
                )

            end_time = time.time()
            log.info(f"linux_cmds read passed on {mnt_pt} with end_time {end_time}")
            return (0, end_time)
        except Exception as ex:
            log.info(ex)
            end_time = time.time()
            log.info(f"linux_cmds read failed on {mnt_pt},end_time-{end_time},out-{ex}")
            return (1, end_time)

    def cg_linux_cmds_write(self, client, mnt_pt):
        """
        IO tool called by cg_write_io_subvol to run write type linux cmds on given mountpoint
        Params:
        client - A client object where mount point exists
        mnt_pt - A mountpoint os subvolume where linux write cmds are executed
        Returns: 0 - Pass, 1 - Fail
        """
        io_type = "write"
        log.info(f"In cg_linux_cmds function : {io_type}")

        try:
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            time.sleep(2)
            dir_path = f"{mnt_pt}/cg_io/dd_dir"
            out, _ = client.exec_command(sudo=True, cmd=f"ls {dir_path}/", timeout=15)
            log.info(f"linux_cmds {io_type} cmd op : {out}")
            out = out.strip()
            files = out.split()
            file_name = random.choice(files)
            log.info(f"linux_cmds {io_type} filename chosen : {file_name}")

            write_cmd_dict = {
                "cp": f"cp -f {dir_path}/{file_name} {dir_path}/{file_name}_copy",
                "mkdir": f"mkdir {dir_path}/testdir_{rand_str}",
                "rmdir": f"rmdir {dir_path}/testdir_{rand_str}",
                "setfacl": f"setfacl -m u::rw {dir_path}/{file_name}",
                "tar": f"tar -cvf {dir_path}/{file_name}_copy.tar {dir_path}/{file_name}_copy",
                "mv": f"mv {dir_path}/{file_name}_copy {dir_path}/{file_name}_copy_renamed",
                "rm": f"rm -f {dir_path}/{file_name}_copy_renamed",
            }
            for cmd in write_cmd_dict:
                log.info(f"linux_cmds {io_type} cmd to run : {write_cmd_dict[cmd]}")
                out, _ = client.exec_command(
                    sudo=True, cmd=write_cmd_dict[cmd], timeout=20
                )
                log.info(f"linux_cmds {io_type} cmd op : {out}")
            end_time = time.time()
            return (0, end_time)
        except Exception as ex:
            log.info(ex)
            end_time = time.time()
            return (1, end_time)
