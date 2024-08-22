import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. Create a nfs cluster
    2. Create a nfs export
    3. Create an export json file with multiple exports
    4. Apply the export json file
    5. Check if the exports are created
    """
    try:
        tc = "CEPH-83575082"
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
        out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls")
        output = out.split()
        if nfs_name in str(output):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        # create nfs
        export_id1, export_id2 = random.sample(range(1000), 2)
        psuedo1 = f"/cephfs1_{export_id1}"
        psuedo2 = f"/cephfs2_{export_id2}"
        user_id1 = f"nfs.{nfs_name}.{export_id1}"
        user_id2 = f"nfs.{nfs_name}.{export_id2}"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        data = [
            {
                '"export_id"': export_id1,
                '"path"': '"/"',
                '"cluster_id"': f'"{nfs_name}"',
                '"pseudo"': f'"{psuedo1}"',
                '"access_type"': '"RW"',
                '"squash"': '"none"',
                '"security_label"': "true",
                '"protocols"': "[4]",
                '"transports"': '["TCP"]',
                '"fsal"': {
                    '"name"': '"CEPH"',
                    '"user_id"': f'"{user_id1}"',
                    '"fs_name"': f'"{fs_name}"',
                },
                '"clients"': "[]",
            },
            {
                '"export_id"': export_id2,
                '"path"': '"/"',
                '"cluster_id"': f'"{nfs_name}"',
                '"pseudo"': f'"{psuedo2}"',
                '"access_type"': '"RW"',
                '"squash"': '"none"',
                '"security_label"': "true",
                '"protocols"': "[4]",
                '"transports"': '["TCP"]',
                '"fsal"': {
                    '"name"': '"CEPH"',
                    '"user_id"': f'"{user_id2}"',
                    '"fs_name"': f'"{fs_name}"',
                },
                '"clients"': "[]",
            },
        ]
        client1.exec_command(sudo=True, cmd=f"echo {data} > export.json")
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs export apply {nfs_name} -i export.json"
        )
        out1, rc1 = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
        )
        output1 = out1.split()[1:-1]
        log.info(output1)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        # mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
        fs_util.cephfs_nfs_mount(
            client1, nfs_server, psuedo1, nfs_mounting_dir, check_ec=False
        )
        dir_name = "smallfile_dir"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}{dir_name}")
        smallfile(client1, nfs_mounting_dir, "smallfile")
        tf1 = False
        tf2 = False
        for ss in output1:
            if psuedo1 in ss:
                tf1 = True
            if psuedo2 in ss:
                tf2 = True
            log.info(ss)

        if tf1 and tf2:
            log.info("export created successfully")
        else:
            raise CommandFailed("Failed to export configs")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}*")
        log.info("Unmount NFS export")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {export_id1}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {export_id2}",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph nfs cluster rm {nfs_name}")


def smallfile(client, mounting_dir, dir_name):
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={mounting_dir}{dir_name} bs=100M " "count=10",
    )
