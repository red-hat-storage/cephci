import json
import random
import string
import traceback
from time import sleep

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_system.osd_node_failure_ops import object_compare
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11265 - Complete Lab shutdown(Ordered) and cluster recovery test with cephfs, no active IO.

    Steps Performed:
    1. Create FS and mount on all the ways (Fuse, kernel, NFS)
    2. Run IOs on all mounts
    3. Power off the clients
    4. power off Mds
    5. power off OSD
    6. power off Mon
    sleep for 15 min
    Bring back the cluster in Reverse order of power off operation

    Args:
        ceph_cluster:
        **kw:

    Returns:
        0 --> if test PASS
        1 --> if test FAIL

    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        mon_nodes = ceph_cluster.get_ceph_objects("mon")

        osd_nodes_list = ceph_cluster.get_ceph_objects("osd")
        unique_objects = []
        for obj in osd_nodes_list:
            if not any(object_compare(obj, u_obj) for u_obj in unique_objects):
                unique_objects.append(obj)
        osd_nodes = unique_objects

        osp_cred = config.get("osp_cred")
        if config.get("cloud-type") == "openstack":
            os_cred = osp_cred.get("globals").get("openstack-credentials")
            params = {}
            params["username"] = os_cred["username"]
            params["password"] = os_cred["password"]
            params["auth_url"] = os_cred["auth-url"]
            params["auth_version"] = os_cred["auth-version"]
            params["tenant_name"] = os_cred["tenant-name"]
            params["service_region"] = os_cred["service-region"]
            params["domain_name"] = os_cred["domain"]
            params["tenant_domain_id"] = os_cred["tenant-domain-id"]
            params["cloud_type"] = "openstack"
        elif config.get("cloud-type") == "ibmc":
            pass
        else:
            pass
        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        ceph_version = json.loads(version)
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        fs_util.prepare_clients([clients[0]], build)
        fs_util.auth_list([clients[0], clients[1]])
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount([clients[0], clients[1]], fuse_mounting_dir, fstab=True)

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0], clients[1]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=",fs=cephfs",
            fstab=True,
        )

        if "nautilus" not in ceph_version["version"]:
            nfs_server = ceph_cluster.get_ceph_objects("nfs")
            nfs_client = ceph_cluster.get_ceph_objects("client")
            fs_util.auth_list(nfs_client)
            nfs_name = "cephfs-nfs"
            fs_name = "cephfs"
            nfs_export_name = "/export1"
            path = "/"
            nfs_servers = ceph_cluster.get_ceph_objects("nfs")
            nfs_server = nfs_servers[0].node.hostname
            # Create ceph nfs cluster
            nfs_client[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            out, rc = nfs_client[0].exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster create {nfs_name} {nfs_servers[0].node.hostname},{nfs_servers[1].node.hostname}",
            )
            # Verify ceph nfs cluster is created
            if wait_for_process(
                client=nfs_client[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully")
            else:
                raise CommandFailed("Failed to create nfs cluster")
            # Create cephfs nfs export
            if "5.0" in build:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                    f"{nfs_export_name} path={path}",
                )
            else:
                nfs_client[0].exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {nfs_name} "
                    f"{nfs_export_name} {fs_name} path={path}",
                )

            # Verify ceph nfs export is created
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )
            if nfs_export_name in out:
                log.info("ceph nfs export created successfully")
            else:
                raise CommandFailed("Failed to create nfs export")
            # Mount ceph nfs exports
            nfs_client[0].exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            rc = fs_util.cephfs_nfs_mount(
                nfs_client[0], nfs_server, nfs_export_name, nfs_mounting_dir, fstab=True
            )
            if not rc:
                log.error("cephfs nfs export mount failed")
                return 1
            out, rc = nfs_client[0].exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output
            log.info("Creating Directory")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )

        commands = [
            f"mkdir {fuse_mounting_dir}/dir_fuse {kernel_mounting_dir}/dir_kernel {nfs_mounting_dir}/dir_nfs",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {fuse_mounting_dir}/dir_fuse",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mounting_dir}/dir_kernel",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir_nfs",
        ]

        for command in commands:
            clients[0].exec_command(sudo=True, cmd=command, long_running=True)

        log.info("Shutting down the cluster")
        for client in clients:
            fs_util.node_power_off(node=client.node, sleep_time=120, **params)

        log.info("Client Nodes Powered OFF Successfully")

        for mds in mds_nodes:
            fs_util.node_power_off(node=mds.node, sleep_time=120, **params)

        log.info("MDS Nodes Powered OFF Successfully")

        for mon in mon_nodes:
            fs_util.node_power_off(node=mon.node, sleep_time=120, **params)

        log.info("Mon Nodes Powered OFF Successfully")
        for osd in osd_nodes:
            fs_util.node_power_off(node=osd.node, sleep_time=120, **params)
        log.info("OSD Nodes Powered OFF Successfully")
        log.info("Sleeping for 10 min")
        sleep(600)

        log.info("Bring the Cluster back")
        for mon in mon_nodes:
            fs_util.node_power_on(node=mon.node, sleep_time=120, **params)
        log.info("Mon Nodes Powered ON Successfully")
        for osd in osd_nodes:
            fs_util.node_power_on(node=osd.node, sleep_time=120, **params)
        log.info("OSD Nodes Powered OFF Successfully")

        for mds in mds_nodes:
            fs_util.node_power_on(node=mds.node, sleep_time=120, **params)
        log.info("MDS Nodes Powered ON Successfully")
        for client in clients:
            fs_util.node_power_on(node=client.node, sleep_time=120, **params)
        log.info("Clients Nodes Powered ON Successfully")

        out, rc = clients[0].exec_command(sudo=True, cmd="ceph -s -f json")
        cluster_info = json.loads(out)
        if cluster_info.get("health").get("status") != "HEALTH_OK":
            log.error(
                f"Cluster helath is in : {cluster_info.get('health').get('status')}"
            )
            out, rc = clients[0].exec_command(
                sudo=True, cmd="ceph health detail -f json"
            )
            error_summary = json.loads(out)
            log.error(f"{error_summary}")
            return 1

        commands = [
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {fuse_mounting_dir}/dir_fuse",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {kernel_mounting_dir}/dir_kernel",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir_nfs",
        ]

        clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir}/dir_fuse {kernel_mounting_dir}/dir_kernel "
            f"{nfs_mounting_dir}/dir_nfs",
        )

        for command in commands:
            clients[0].exec_command(sudo=True, cmd=command, long_running=True)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("---clean up---------")
        fs_util.client_clean_up(
            "umount",
            fuse_clients=[clients[0], clients[1]],
            mounting_dir=fuse_mounting_dir,
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[clients[0], clients[1]],
            mounting_dir=kernel_mounting_dir,
        )
        for client in [clients[0], clients[1]]:
            client.exec_command(
                sudo=True, cmd="mv /etc/fstab.backup /etc/fstab", check_ec=False
            )
