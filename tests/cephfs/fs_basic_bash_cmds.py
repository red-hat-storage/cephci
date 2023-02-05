import json
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
    CEPH-11300: Running basic bash commands on fuse,Kernel and NFS mounts

    Steps Performed:
    1. Create FS and mount on all the ways (Fuse, kernel, NFS)
    2. Run directory_cmds, file_cmds, search_cmds
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
        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        ceph_version = json.loads(version)
        nfs_mounting_dir = "/mnt/nfs/"
        dir_name = "dir"
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount([clients[0]], fuse_mounting_dir)

        mount_test_case(
            [clients[0]], fuse_mounting_dir, "fuse_dir", "fuse_test", config
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([clients[0]], kernel_mounting_dir, ",".join(mon_node_ips))

        mount_test_case(
            [clients[0]], kernel_mounting_dir, "kernel_dir", "kernel_test", config
        )

        if "nautilus" not in ceph_version["version"]:
            nfs_server = ceph_cluster.get_ceph_objects("nfs")
            nfs_client = ceph_cluster.get_ceph_objects("client")
            fs_util.auth_list(nfs_client)
            nfs_name = "cephfs-nfs"
            fs_name = "cephfs"
            nfs_export_name = "/export1"
            path = "/"
            nfs_server_name = nfs_server[0].node.hostname
            # Create ceph nfs cluster
            nfs_client[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server_name}"
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
            assert fs_util.wait_for_cmd_to_succeed(
                nfs_client[0],
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            nfs_client[0].exec_command(
                sudo=True,
                cmd=f"mount -t nfs -o port=2049 {nfs_server_name}:{nfs_export_name} {nfs_mounting_dir}",
            )
            out, rc = nfs_client[0].exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if nfs mount is is passed of failed:")
            assert nfs_mounting_dir.rstrip("/") in mount_output
            log.info("Creating Directory")
            out, rc = nfs_client[0].exec_command(
                sudo=True, cmd=f"mkdir {nfs_mounting_dir}{dir_name}"
            )

            mount_test_case(
                [clients[0]], nfs_mounting_dir, "nfs_dir", "nfs_test", config
            )
            return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("---clean up---------")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mounting_dir,
        )


def mount_test_case(clients, mounting_dir, dir_name, filename, config):
    """
    Running commands on mounted directory
    Args:
        clients:
        mounting_dir:
        filename:

    Returns:

    """
    log.info("Basic directory operations")
    directory_cmds = [
        f"cd {mounting_dir}",
        "pwd",
        f"mkdir -p {mounting_dir}/{dir_name}",
        f"cd {mounting_dir}/{dir_name}",
    ]
    file_operations = [
        f"cd {mounting_dir}",
        f"touch {mounting_dir}/{filename}.txt",
        f"echo 'writing data' >  {mounting_dir}/{filename}.txt",
        f"head {mounting_dir}/{filename}.txt",
        f"tail -n3 {mounting_dir}/{filename}.txt",
        f"ln -s {mounting_dir}/{filename}.txt {mounting_dir}/{filename}_ln.txt",
        f"ls -lhaF {mounting_dir}//{filename}_ln.txt | grep ^l",
    ]
    search_cmds = [
        f"find {mounting_dir} -maxdepth 1 -type f -name {filename}.txt",
        f"cat {mounting_dir}/{filename}.txt | grep writing",
    ]
    for client in clients:
        for dir_cmd in directory_cmds:
            client.exec_command(sudo=True, cmd=dir_cmd)
        for file_cmd in file_operations:
            client.exec_command(sudo=True, cmd=file_cmd)
        out, rc = client.exec_command(sudo=True, cmd=search_cmds[0])
        log.info(out)
        if f"{filename}.txt" not in out:
            log.error("unable to find the file that has been created")
            return 1
        out, rc = client.exec_command(sudo=True, cmd=search_cmds[1])
        log.info(out)
        if "writing" not in out:
            log.error("unable to find the file that has been created")
            return 1

        client.exec_command(sudo=True, cmd=f"touch {mounting_dir}/{filename}{{1..100}}")

        cmd = (
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 5 "
            f"--file-size {config.get('size_of_files', 1024)} "
            f"--files {config.get('no_of_files', 100)} --files-per-dir 10 "
            f"--dirs-per-dir 2 --top {mounting_dir}{dir_name}"
        )

        clients[0].exec_command(sudo=True, cmd=cmd)
        clients[0].exec_command(
            sudo=True,
            cmd=f"cp -r {mounting_dir}/{dir_name} {mounting_dir}/{dir_name}_1",
        )
        clients[0].exec_command(
            sudo=True, cmd=f"mv {mounting_dir}/{dir_name} {mounting_dir}/{dir_name}_2"
        )

        clients[0].exec_command(
            sudo=True, cmd=f"chmod +777 {mounting_dir}/{dir_name}_2"
        )
        out, rc = clients[0].exec_command(
            sudo=True, cmd=f"stat -c '%a' {mounting_dir}/{dir_name}_2"
        )
        if out.strip() != "777":
            log.error("chmod is not working on the directory")
            return 1
        out1, rc = clients[0].exec_command(
            sudo=True, cmd=f"ls {mounting_dir}/{dir_name}_2 | wc -l"
        )
        out2, rc = clients[0].exec_command(
            sudo=True, cmd=f"ls {mounting_dir}/{dir_name}_1 | wc -l"
        )
        if out1 != out2:
            log.error("cp and mv are not working")
            return 1
        client.exec_command(sudo=True, cmd=f"rm -rf {mounting_dir}/*")
