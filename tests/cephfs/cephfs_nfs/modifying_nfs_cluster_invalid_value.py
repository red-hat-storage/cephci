import json
import secrets
import string
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. change the access_type to RW
    2. apply the changes
    3. check if it throws an error with invalid value
    4. try to change other values using steps above
    """
    try:
        tc = "CEPH-83574014"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            raise CommandFailed("Cluster has not been created")
        try:
            out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
            output = json.loads(out)
        except JSONDecodeError:
            output = json.dumps([out])
        if nfs_name in output:
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )

        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export get {nfs_name} {nfs_export_name} > export_original.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export_original.conf",
        )

        access_type_value = '"access_type": "RW"'
        squash_value = '"squash": "none"'
        security_value = '"security_label": true'

        access_type = '"access_type": '
        security_label = '"security_label": '
        squash = '"squash": '

        access_type_fault = "fault_value"
        squash_fault = "fault_value"
        security_fault = "fault_value"

        log.info(f"config file for {nfs_export_name}: {out}")

        client1.exec_command(sudo=True, cmd="cp export_original.conf export1.conf")
        client1.exec_command(sudo=True, cmd="cp export_original.conf export2.conf")
        client1.exec_command(sudo=True, cmd="cp export_original.conf export3.conf")
        client1.exec_command(sudo=True, cmd="cp export_original.conf export4.conf")
        client1.exec_command(sudo=True, cmd="cp export_original.conf export5.conf")
        # changing access type to invalid value
        client1.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{access_type_value}/{access_type}{access_type_fault}/g' export1.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export1.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config1")
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export1.conf",
            check_ec=False,
        )
        if out:
            raise CommandFailed("NFS export has permission to export1")
        else:
            log.info(ec)
        log.info("changing squash to invalid value")
        client1.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{squash_value}/{squash}{squash_fault}/g' export2.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export2.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config2")
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export2.conf",
            check_ec=False,
        )
        log.info(out)
        if out:
            raise CommandFailed("NFS export has permission to export2")
        else:
            log.info(ec)

        log.info("changing security_value to invalid value")

        client1.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{security_value}/{security_label}{security_fault}/g' export3.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export3.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config3")
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export3.conf",
            check_ec=False,
        )
        if out:
            raise CommandFailed("Security label fault value should not be applied")
        else:
            log.info(ec)
        log.info("changing protocols to invalid value")
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/4/-1/g' export4.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export4.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config4")
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export4.conf",
            check_ec=False,
        )
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export4.conf",
            check_ec=False,
        )
        log.info(out)
        log.info(ec)
        if "Invalid protocol" not in out:
            if "Invalid protocol" not in ec:
                raise CommandFailed("Protocols fault value should not be applied")
        else:
            log.info(ec)

        log.info("changing transport to invalid value")
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/TCP/fault_value/g' export5.conf",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="cat export5.conf",
        )
        log.info(f"config file for {nfs_export_name}: {out}")
        log.info("Apply the config5")
        out, ec = client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i export5.conf",
            check_ec=False,
        )
        log.info(out)
        log.info(ec)
        if "is not a valid transport protocol" not in out:
            if "is not a valid transport protocol" not in ec:
                raise CommandFailed("Transport fault value should not be applied")
        else:
            log.info(ec)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {nfs_name}",
            check_ec=False,
        )
