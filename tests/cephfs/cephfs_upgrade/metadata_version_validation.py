import json
import traceback

from utility.log import Log

log = Log(__name__)
"""
What it does:
1. Stores ceph versions before upgrade
2. Compares ceph versions after upgrade
3. Stores stat data before upgrade (Permission , Permissions bits and file type, User ID, User Name, Time of birth etc)
4. Compares file stat data after upgrade
5. It is free to add additional file stats as needed

This Script needs 2 times of run:

1. Before upgrade to store ceph version data and file stat data
2. After upgrade to compared those data
"""


def run(ceph_cluster, **kw):
    ceph_version_path = "/tmp/ceph_versions.json"
    log.info("Upgrade checking initiated")
    client = ceph_cluster.get_ceph_objects("client")[0]
    check_list = ["mon", "mgr", "osd", "mds", "overall"]
    cluster_stat_commands = [
        "ceph fs ls",
        "ceph fs status",
        "ceph mds stat",
        "ceph fs dump",
    ]
    try:
        out0, err0 = client.exec_command(
            sudo=True, cmd=f"stat {ceph_version_path}", check_ec=False
        )

        def version_compare(v1, v2):
            arr1 = v1.split(".")[:-1]
            arr2 = v2.split(".")[:-1]
            n = len(arr1)
            m = len(arr2)
            arr1 = [int(i) for i in arr1]
            arr2 = [int(i) for i in arr2]
            if n > m:
                for i in range(m, n):
                    arr2.append(0)
            elif m > n:
                for i in range(n, m):
                    arr1.append(0)
            for i in range(len(arr1)):
                if arr1[i] > arr2[i]:
                    return 1
                elif arr2[i] > arr1[i]:
                    return -1
            return 0

        if out0:
            log.info("Loading previous ceph versions")
            with client.remote_file(
                file_name=ceph_version_path, file_mode="r"
            ) as before_file:
                pre_data = json.load(before_file)
            out1, err1 = client.exec_command(
                sudo=True, cmd="ceph versions --format json"
            )
            upgraded_data = json.loads(out1)
            fail_daemon = []
            for daemon in check_list:
                pre_version = str(pre_data[daemon]).split()[2]
                cur_version = str(upgraded_data[daemon]).split()[2]
                log.info(f"Previous ceph {daemon} version is {pre_version}")
                log.info(f"Current ceph {daemon} version is {cur_version}")
                pre_version = pre_version.replace("-", ".")
                cur_version = cur_version.replace("-", ".")
                result = version_compare(cur_version, pre_version)
                if result == -1 or result == 0:
                    fail_daemon.append(daemon)
                    log.error(f"Upgrade failed for {daemon}")
            before_file.close()
            if len(fail_daemon) > 0:
                log.error(f"Upgrade failed for these daemons: {str(fail_daemon)}")
                return 1
        else:
            log.info("---------Writing ceph versions--------------")

            client.exec_command(
                sudo=True, cmd=f"ceph versions --format json > {ceph_version_path}"
            )
            out2, err2 = client.exec_command(sudo=True, cmd=f"cat {ceph_version_path}")
            log.info(out2)

        file_stat_path = "/tmp/file_stat.txt"
        testing_file_path = "/tmp/stat_testing.txt"
        out0, err0 = client.exec_command(
            sudo=True, cmd=f"stat {file_stat_path}", check_ec=False
        )
        log.info(out0)
        if out0:
            log.info("---------Comparing stats of a file after upgrade--------------")
            log.info(
                "File stats to compare: Permission , Permissions bits and file type, User ID, User Name, Time of birth"
            )

            out1, err1 = client.exec_command(
                sudo=True, cmd=f"stat --format=%a,%A,%u,%U,%w {testing_file_path}"
            )
            out2, err2 = client.exec_command(sudo=True, cmd=f"cat {file_stat_path}")
            cur_stat = out1.split(",")
            pre_stat = out2.split(",")
            log.info("Current testing file stats: ", cur_stat)
            log.info("Previous testing file stats: ", pre_stat)
            for idx in range(4):
                if cur_stat[idx] != pre_stat[idx]:
                    return 1
            log.info("After upgrade file stats are identical")
            after_upgrade_file = client.remote_file(
                sudo=True,
                file_name="/home/cephuser/ceph_cluster_after_upgrade.txt",
                file_mode="w",
            )
            for cmd in cluster_stat_commands:
                out, rc = client.exec_command(
                    sudo=True, cmd=f"{cmd} --format json-pretty"
                )
                log.info(out)
                after_upgrade_file.write(f"{cmd}\n")
                output = json.loads(out)
                after_upgrade_file.write(json.dumps(output, indent=4))
                after_upgrade_file.write("\n")
            after_upgrade_file.flush()
            before_upgrade_file = client.remote_file(
                sudo=True,
                file_name="/home/cephuser/ceph_cluster_before_upgrade.txt",
                file_mode="r",
            )
            after_upgrade_file = client.remote_file(
                sudo=True,
                file_name="/home/cephuser/ceph_cluster_after_upgrade.txt",
                file_mode="r",
            )
            log.info(
                "----------------Before Upgrade Cluster Status-----------------------------------"
            )
            for line in before_upgrade_file.readlines():
                log.info(line)
            log.info(
                "----------------After Upgrade cluster status------------------------------------"
            )
            for line in after_upgrade_file.readlines():
                log.info(line)
            clients = ceph_cluster.get_ceph_objects("client")
            cmd = (
                "dnf clean all;dnf -y install ceph-common --nogpgcheck;"
                "dnf -y update ceph-common --nogpgcheck; "
                "dnf -y install ceph-fuse --nogpgcheck;"
                "dnf -y update ceph-fuse --nogpgcheck;"
            )
            for client in clients:
                client.exec_command(sudo=True, cmd=cmd)
        else:
            log.info("---------Writing stats of a file before upgrade--------------")
            log.info(
                "File stats to compare: Permission , Permissions bits and file type, User ID, User Name, Time of birth"
            )
            client.exec_command(sudo=True, cmd=f"touch {testing_file_path}")
            out1, err1 = client.exec_command(
                sudo=True,
                cmd=f"stat --format=%a,%A,%u,%U,%w {testing_file_path} > {file_stat_path}",
            )
            log.info(out1)
            before_upgrade_file = client.remote_file(
                sudo=True,
                file_name="/home/cephuser/ceph_cluster_before_upgrade.txt",
                file_mode="w",
            )
            for cmd in cluster_stat_commands:
                out, rc = client.exec_command(
                    sudo=True, cmd=f"{cmd} --format json-pretty"
                )
                log.info(out)
                before_upgrade_file.write(f"{cmd}\n")
                output = json.loads(out)
                before_upgrade_file.write(json.dumps(output, indent=4))
                before_upgrade_file.write("\n")
            before_upgrade_file.flush()
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
