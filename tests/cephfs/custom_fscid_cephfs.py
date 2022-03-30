import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
        Pre-requisites :
        1. creats fs volume create cephfs if the volume is not there

        Custom fscid cephfs operation:
        1. Create data & metadata pools for cephfs
        Ex. ceph osd pool create cephfs_data 64 64
            ceph osd pool create cephfs_metadata 8 8
        2. Create Cephfs with data & metadata pool with specific fscid
        ceph fs new <cephfs_name> <metada_pool> <data_pool> --fscid <fscid> --force
        Ex. ceph fs new cephfs cephfs_metadata cephfs_data --fscid {fscid} --force
        3. Verify cephfs is created with specified fscid
        ceph fs ls
        ceph fs dump
        4. Verify that another Cephfs is getting not created with same fscid
        """
        tc = "CEPH-83574632"
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        rhbuild = config.get("rhbuild")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fscid = "300"
        fs_name = "fs2"
        if "4." in rhbuild:
            client1.exec_command(sudo=True, cmd="ceph fs flag set enable_multiple true")
        commands = [
            "ceph osd pool create fs_data 64 64",
            "ceph osd pool create fs_metadata 8 8",
            f"ceph fs new {fs_name} fs_metadata fs_data --fscid {fscid} --force",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        if "5." in rhbuild:
            mdss = ceph_cluster.get_ceph_objects("mds")
            mds1 = mdss[0].node.hostname
            mds2 = mdss[1].node.hostname
            cmd = f"ceph orch apply mds {fs_name} --placement='2 {mds1} {mds2}'"
            client1.exec_command(sudo=True, cmd=cmd)
        out, err = client1.exec_command(sudo=True, cmd="ceph fs ls | grep cephfs")
        if "cephfs" in out:
            log.info("CephFs is successfully created")
        else:
            log.error("CephFs creation failed")
            return 1
        cmd = f"ceph fs dump | grep Filesystem | grep {fs_name}"
        out, err = client1.exec_command(sudo=True, cmd=cmd)
        if fscid in out:
            log.info("CephFs is successfully created with specific fscid")
        else:
            log.error("custom fscid setting failed")
            return 1
        fs_name = "fs3"
        commands = [
            "ceph osd pool create cfs_data 64 64",
            "ceph osd pool create cfs_metadata 8 8",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        try:
            cmd = f"ceph fs new {fs_name} cfs_metadata cfs_data --fscid {fscid} --force"
            client1.exec_command(sudo=True, cmd=cmd)
        except CommandFailed as e:
            log.info(e)
            log.info("Cephfs creation failed with same fscid as expected")
        else:
            log.error("Cephfs did not failed with same fscid as expected")
            return 1
        log.info("Cleaning up")
        fs_name = "fs2"
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            f"ceph fs volume rm {fs_name} --yes-i-really-mean-it",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
