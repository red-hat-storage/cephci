import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        client1 = clients[0]
        # get nfs nodes
        nfs_list = ceph_cluster.get_ceph_objects("nfs")
        log.info(nfs_list)
        virtual_ip = "10.8.128.100"
        subnet = "21"
        nfs_ID = "cephfs_nfs"
        port = "12345"
        data = {
            "service_type": "nfs",
            "service_id": nfs_ID,
            "placement": {
                "hosts": [nfs_list[0].node.hostname, nfs_list[1].node.hostname],
                "count": 1,
            },
            "spec": {"port": port},
        }
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        client1.exec_command(sudo=True, cmd=f"echo '{data}' > /tmp/nfs.yaml")
        client1.exec_command(sudo=True, cmd="ceph orch apply -i /tmp/nfs.yaml")
        # validate_service
        fs_util.validate_services(client1, f"nfs.{nfs_ID}")
        ingress = {
            "service_type": "ingress",
            "service_id": f"nfs.{nfs_ID}",
            "placement": {
                "hosts": [nfs_list[0].node.hostname, nfs_list[1].node.hostname],
                "count": 1,
            },
            "spec": {
                "backend_service": f"nfs.{nfs_ID}",
                "frontend_port": "2049",
                "monitor_port": "9000",
                "virtual_ip": f"{virtual_ip}/{subnet}",
            },
        }
        client1.exec_command(sudo=True, cmd=f"echo '{ingress}' > /tmp/ingress.yaml")
        client1.exec_command(sudo=True, cmd="ceph orch apply -i /tmp/ingress.yaml")
        # validate_service
        fs_util.validate_services(client1, f"ingress.nfs.{nfs_ID}")
        out2, ec2 = client1.exec_command(
            sudo=True,
            cmd=f"ceph orch ls --service_name=ingress.nfs.{nfs_ID} --format json-pretty",
        )
        output2 = json.loads(out2)
        log.info(output2[0])
        if f"ingress.nfs.{nfs_ID}" in output2[0]["service_name"]:
            log.info("ingress service created successfully")
        else:
            raise CommandFailed("Failed to create ingress service")
        if output2[0]["placement"]["count"] == 1:
            log.info("count number is correct")
        else:
            raise CommandFailed("count number is incorrect")
        if output2[0]["spec"]["frontend_port"] == 2049:
            log.info("port number is correct")
        else:
            raise CommandFailed("port number is incorrect")
        if output2[0]["spec"]["monitor_port"] == 9000:
            log.info("monitor port number correct")
        else:
            raise CommandFailed("monitor port number is incorrect")
        if output2[0]["spec"]["virtual_ip"] == f"{virtual_ip}/{subnet}":
            log.info("virtual ip is correct")
        else:
            raise CommandFailed("virtual ip is incorrect")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create cephfs {nfs_ID} "
            f"{nfs_export_name} {fs_name} path={export_path}",
        )
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_ID}")
        if nfs_export_name not in out:
            raise CommandFailed("Failed to create nfs export")
        log.info("ceph nfs export created successfully")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export get {nfs_ID} {nfs_export_name}"
        )
        json.loads(out)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {virtual_ip}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)
        dir_name = "smallfile_dir"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}{dir_name}")
        smallfile(clients[0], nfs_mounting_dir, dir_name)
        log.info("Test completed successfully")
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
            cmd=f"ceph nfs export delete {nfs_ID} {nfs_export_name}",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph nfs cluster rm {nfs_ID}")


def smallfile(client, mounting_dir, dir_name):
    client.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 8 --file-size 10240 "
        f"--files 10 --top {mounting_dir}{dir_name}",
    )
