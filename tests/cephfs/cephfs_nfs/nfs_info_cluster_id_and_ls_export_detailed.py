import json
import random
import string
import traceback

from ceph.ceph_admin import CephAdmin
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
"""
pre-requisites:
1. nfs cluster ready
2. export the nfs cluster
Test operation:
1. check if input and --detailed value match
2. check if nfs clsuter info using its cluster_id
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83573992"
        log.info(f"Running CephFS tests for BZ-{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)

        instance = CephAdmin(cluster=ceph_cluster, **config)
        host_cmd = "cephadm shell ceph orch ps --format json-pretty"
        res, ec = instance.installer.exec_command(sudo=True, cmd=host_cmd)
        results = json.loads(res)
        nfs_hosts_name = []
        log.info(print(results))
        nfs_target_node = ""
        for ps in results:
            log.info(print(ps))
            if "nfs" in ps["service_name"]:
                nfs_hosts_name.append(ps["hostname"])
            if "mds" in ps["service_name"]:
                nfs_target_node = ps["hostname"]
        log.info(print("NFS service is being hosted by ", nfs_hosts_name))

        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        cluster_id = f"test-nfs_{rand}"
        path = "/"
        export_id = "1"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        bind = "/ceph"
        user_id = (
            f"nfs.{cluster_id}.{fs_name}"
            if int(build.split(".")[0]) == 7
            else f"nfs.{cluster_id}.{export_id}"
        )

        passed = []
        client1.exec_command(
            sudo=True, cmd=f'ceph nfs cluster create {cluster_id} "{nfs_target_node}"'
        )
        cmd_nfs_export = (
            f"ceph nfs export create cephfs {cluster_id} {bind} {fs_name} --path={path}"
        )

        client1.exec_command(sudo=True, cmd=cmd_nfs_export)

        res, ec = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export ls {cluster_id} --detailed --format json"
        )
        results = json.loads(res)
        log.info(print(results))
        res2, ec2 = client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster info {cluster_id}"
        )

        if res2:
            results2 = json.loads(res2)
            log.info(print(results2))
            passed.append(True)
        else:
            log.error("Getting cluster info using the cluster ID has failed")
            passed.append(False)

        do_not_match = []
        result = results[0]

        if result["fsal"]["name"] != "CEPH":
            do_not_match.append("[fsal][name]")

        elif result["fsal"]["user_id"] != user_id:
            do_not_match.append("[fsid][user_id]")

        elif result["fsal"]["fs_name"] != fs_name:
            do_not_match.append("[fsal][fs_name")

        elif result["path"] != path:
            do_not_match.append("[path]")

        elif result["cluster_id"] != cluster_id:
            do_not_match.append("[cluster_id]")

        elif result["pseudo"] != bind:
            do_not_match.append("[pseudo]")

        if len(do_not_match) != 0:
            passed.append(False)
            log.error("There is some mismatch")
            log.error(do_not_match)
            log.error(result)
        else:
            passed.append(True)

        if passed[0] is False and passed[1] is True:
            log.error("Failed to load info using cluster ID")
            return 1
        elif passed[0] is True and passed[1] is False:
            log.error("Failed to get right details from --detailed option")
            return 1
        elif passed[0] is False and passed[1] is False:
            log.error("Failed to load info using cluster ID")
            log.error("Failed to get right details from --detailed option")
            return 1
        else:
            return 0

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
