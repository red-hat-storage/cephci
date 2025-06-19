"""
This is cephfs recovery utility module
It contains all the re-useable functions related to cephfs recovery workflow

"""

import datetime
import json
import time

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class FSRecovery(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.result_vals = {}
        self.return_counts = {}
        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.installer = ceph_cluster.get_nodes(role="installer")[0]
        self.fs_util = FsUtils(ceph_cluster)

    def fs_recovery(
        self, client, active_mds_ranks, fs_name="cephfs", debug_mds=None, **kwargs
    ):
        """
        This method will execute complete FS recovery steps -
            Journal back & recovery,
            Reset cephfs-table-tool,
            journal reset
            data scan tool
            Bring FS online
            Scrub start and wait for completion
        Required params:
        mds_ranks - list of ranks of active mdses
        client - client obj to run ceph cmds
        Other params:
        fs_name - FS name, default - cephfs
        debug_mds - debug level of mds logging, default - None, type - int
        kwargs could be as,
            {debug_client : debug level for client
            log_file : log_file_name for data scan logging}
        """
        log.info("Start journal backup and recovery")
        self.journal_ops(client, fs_name=fs_name, mds_ranks=active_mds_ranks)
        log.info("Start Data scan")
        self.data_scan(client, fs_name=fs_name, debug_mds=debug_mds, **kwargs)
        log.info(f"Bring FS {fs_name} online")
        cmd = f"ceph fs set {fs_name} joinable true"
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Wait for expected mdses to be active")
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        exp_status = 1
        while datetime.datetime.now() < end_time and exp_status:
            out, rc = client.exec_command(
                cmd=f"ceph fs status {fs_name} -f json", client_exec=True
            )
            log.info(out)
            parsed_data = json.loads(out)
            active_mdses = [
                mds["name"]
                for mds in parsed_data.get("mdsmap", [])
                if mds.get("rank", -1) in active_mds_ranks
                and mds.get("state") == "active"
            ]
            if len(active_mdses) == len(active_mds_ranks):
                exp_status = 0  # active MDS found
            else:
                time.sleep(10)  # Retry after the specified interval
        log.info("Start scrub ops")
        self.scrub_ops(client, active_mdses)
        log.info("Check Ceph Status after FS recovery")
        self.fs_util.get_ceph_health_status(client)

        return 0

    def data_scan(
        self,
        client,
        fs_name="cephfs",
        ops="all",
        debug_mds=None,
        force_init=False,
        **kwargs,
    ):
        """
        Run the data_scan module, either with all ops - init,scan_extents,scan_inodes and scan_links in order or
        just the selected op
        Args:
            client: client node , type - client object
            ops: mentions all or specific data_scan op - init,extents,inodes,links,type - str,default - all
            debug_mds : If debug_mds required pass debug_level as value, type - int,default-None
            **kwargs:
                debug_client : debug level for client, type - int
                log_file : file to log debug info,type - str
                example on method usage: kwargs = {'log_file' : 'foo.log','debug_client':10}
                fs_recovery.data_scan(client,debug_mds=20,**kwargs)
        Return: None
        """
        try:

            def data_scan_init():
                log.info("data scan phase : init")
                cmd_main = f"cephfs-data-scan init --filesystem {fs_name}"
                cmd = data_scan_params(cmd_main, debug_mds=debug_mds, **kwargs)
                if force_init:
                    cmd += " --force-init"
                client.exec_command(sudo=True, cmd=cmd)

            def data_scan_extents():
                log.info("data scan phase : extents")
                cmd_main = f"cephfs-data-scan scan_extents --filesystem {fs_name}"
                cmd = data_scan_params(cmd_main, debug_mds=debug_mds, **kwargs)
                client.exec_command(sudo=True, cmd=cmd)

            def data_scan_inodes():
                log.info("data scan phase : inodes")
                cmd_main = f"cephfs-data-scan scan_inodes --filesystem {fs_name}"
                cmd = data_scan_params(cmd_main, debug_mds=debug_mds, **kwargs)
                client.exec_command(sudo=True, cmd=cmd)

            def data_scan_links():
                log.info("data scan phase : links")
                cmd_main = f"cephfs-data-scan scan_links --filesystem {fs_name}"
                cmd = data_scan_params(cmd_main, debug_mds=debug_mds, **kwargs)
                client.exec_command(sudo=True, cmd=cmd)

            def data_scan_params(cmd_str, debug_mds=debug_mds, **kwargs):
                if debug_mds:
                    cmd_str += f" --debug_mds={debug_mds} --debug_ms=1"
                if kwargs.get("debug_client"):
                    cmd_str += f" --debug_client={kwargs['debug_client']} --debug_ms=1"
                if kwargs.get("log_file"):
                    cmd_str += f" --log_file={kwargs['log_file']}"
                return cmd_str

            data_scan_ops_obj = {
                "init": data_scan_init,
                "extents": data_scan_extents,
                "inodes": data_scan_inodes,
                "links": data_scan_links,
            }
            if ops == "all":
                for op in data_scan_ops_obj:
                    data_scan_ops_obj[op]()
            else:
                data_scan_ops_obj[ops]()
        except Exception as ex:
            log.info(ex)

    def journal_ops(self, client, fs_name="cephfs", ops="all", mds_ranks=["0"]):
        """
        Run the journal module, either with all ops - backup,recover and reset in order or
        just the selected op
        Args:
            client: client node , type - client object
            ops: mentions all or specific data_scan op - backup,recover,reset,type - str,default - all
        Return: None
        """
        try:

            def journal_backup():
                log.info("journal tool phase : backup")
                for mds_rank in mds_ranks:
                    cmd = f"cephfs-journal-tool --rank {fs_name}:{mds_rank} journal export backup.bin"
                    out, ec = client.exec_command(sudo=True, cmd=cmd)
                    return out, ec

            def journal_recover():
                log.info("journal tool phase : recover dentries")
                for mds_rank in mds_ranks:
                    cmd = f"cephfs-journal-tool --rank {fs_name}:{mds_rank} event recover_dentries list"
                    out, ec = client.exec_command(sudo=True, cmd=cmd)
                    return out, ec

            def journal_reset():
                log.info("journal tool phase : reset")
                cephfs_table_tool(client, fs_name)
                for mds_rank in mds_ranks:
                    cmd = f"cephfs-journal-tool --rank {fs_name}:{mds_rank} journal reset --yes-i-really-really-mean-it"
                    out, ec = client.exec_command(sudo=True, cmd=cmd)
                    return out, ec

            def journal_flush():
                log.info("journal tool phase : flush")
                for mds_rank in mds_ranks:
                    cmd = f"ceph tell mds.{fs_name}:{mds_rank} flush journal"
                    out, ec = client.exec_command(sudo=True, cmd=cmd)
                    return out, ec

            def cephfs_table_tool(client, fs_name):
                reset_list = ["session", "snap", "inode"]
                log.info(f"cephfs-table-tool ops, Reset: {reset_list}")
                for i in reset_list:
                    for mds_rank in mds_ranks:
                        cmd = f"cephfs-table-tool {fs_name}:{mds_rank} reset {i}"
                        out, ec = client.exec_command(sudo=True, cmd=cmd)
                        return out, ec

            journal_ops_obj = {
                "backup": journal_backup,
                "recover": journal_recover,
                "reset": journal_reset,
                "flush": journal_flush,
            }
            log.info(f"ops:{ops}")
            if ops == "all":
                result = {}
                for op in journal_ops_obj:
                    if "flush" not in op:
                        log.info(f"op:{op}")
                        result[op] = journal_ops_obj[op]()
                return result
            else:
                result = journal_ops_obj[ops]()
                if result is None:
                    log.error(f"Journal operation {ops} failed")
                    return 1
                return result
        except Exception as ex:
            log.error(ex)
            return 1

    def scrub_ops(self, client, active_mds_list, ops="all", wait_for="not_running"):
        """
        Run the scrub module, either with all ops - start,wait_status in order or
        just the selected op
        Args:
            client: client node , type - client object
            active_mds_list : List of active mds names to run scrub ops
            ops: mentions all or specific data_scan op - scrub_start,wait_status - str,default - all
        Return: None
        """
        try:

            def scrub_start():
                log.info("scrub phase : start")
                log.info(f"active_mds_list:{active_mds_list}")
                for active_mds in active_mds_list:
                    cmd = f"ceph tell mds.{active_mds} scrub start / recursive repair"
                    client.exec_command(sudo=True, cmd=cmd)

            def wait_status():
                log.info(f"wait for status : {wait_for}")
                status_msgs = {"not_running": "no active scrubs running"}
                exp_msg = status_msgs[wait_for]
                for active_mds in active_mds_list:
                    end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
                    exp_status = 1
                    while datetime.datetime.now() < end_time and exp_status:
                        cmd = f"ceph tell mds.{active_mds} scrub status"
                        out, _ = client.exec_command(sudo=True, cmd=cmd)
                        log.info(out)
                        if exp_msg in out:
                            exp_status = 0
                    if exp_status == 1:
                        log.error(
                            f"Scrub status is not as expected.Expected:{wait_for},Current status:{out}"
                        )

            scrub_ops = {
                "scrub_start": scrub_start,
                "wait_status": wait_status,
            }
            if ops == "all":
                for op in scrub_ops:
                    scrub_ops[op]()
            else:
                scrub_ops[ops]()
        except Exception as ex:
            log.info(ex)

    def get_active_mds_ranks(self, client, fs_name):
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        output = json.loads(out)
        active_mds = [
            mds["rank"] for mds in output["mdsmap"] if mds["state"] == "active"
        ]
        return active_mds
