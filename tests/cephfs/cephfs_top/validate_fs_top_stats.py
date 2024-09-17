import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def setup_fs_top(client, **kwargs):
    log.info("Enable mgr stats")
    client.exec_command(sudo=True, cmd="ceph mgr module enable stats")
    log.info("Install FS top")
    client.exec_command(sudo=True, cmd="dnf install cephfs-top -y")
    if kwargs.get("user"):
        client.exec_command(
            sudo=True,
            cmd=f"ceph auth get-or-create client.{kwargs.get('user')} mon 'allow r' mds 'allow r' "
            f"osd 'allow r' mgr 'allow r' > /etc/ceph/ceph.client.{kwargs.get('user')}.keyring",
        )
    else:
        client.exec_command(
            sudo=True,
            cmd="ceph auth get-or-create client.fstop mon 'allow r' mds 'allow r' "
            "osd 'allow r' mgr 'allow r' > /etc/ceph/ceph.client.fstop.keyring",
        )


def extract_mount_info_by_id(data, client_id):
    """
    This function extracts the mount_point and root based on the provided client_id.
    """
    for entry in data:
        if entry.get("id") == client_id:
            client_metadata = entry.get("client_metadata", {})
            mount_point = client_metadata.get("mount_point")
            root = client_metadata.get("root")
            return root, mount_point
    return None, None


@retry(CommandFailed, tries=3, delay=60)
def extract_client_id_by_mount_point(client, active_mds, mount_point):
    """
    This function extracts the client_id based on the provided mount_point.
    Assumes data is a list of client dictionaries.
    """

    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
    )
    client_details = json.loads(out)
    mount_point_normalized = mount_point.rstrip("/")
    for entry in client_details:
        client_metadata = entry.get("client_metadata", {})
        actual_mount_point = client_metadata.get("mount_point", "")

        # Compare the actual mount point with the provided mount point
        if actual_mount_point == mount_point_normalized:
            return entry.get("id")
    raise CommandFailed("Client details Not loaded yet")


@retry(CommandFailed, tries=3, delay=60)
def validate_mount_info(
    client, client_id, expected_mount_root, expected_mount_point, fs_name="cephfs-top"
):
    """
    This function validates the extracted mount_point and root values with the cephfs data based on client_id.
    """
    out, rc = client.exec_command(sudo=True, cmd=f"cephfs-top --dumpfs {fs_name}")
    top_details = json.loads(out)
    log.info(f"{top_details}")
    cephfs_data = top_details.get(fs_name, {})
    if not cephfs_data:
        raise CommandFailed(f"Unable to load the cephfs_data: {cephfs_data}")
    log.info(f"JSON data for {fs_name}: {cephfs_data}")

    # Get the specific client data using client_id
    client_data = cephfs_data.get(str(client_id))

    if client_data is None:
        log.error(f"No data found for client_id {client_id}")
        raise CommandFailed(f"No data found for client_id {client_id}")

    actual_mount_root = client_data.get("mount_root")
    actual_mount_point = client_data.get("mount_point@host/addr")
    actual_mount_point_extracted = actual_mount_point.split("@")[0]

    # Validate mount_root
    if actual_mount_root != expected_mount_root:
        log.error(
            f"Validation failed for 'mount_root' for client_id {client_id}: "
            f"Expected {expected_mount_root}, but got {actual_mount_root}"
        )
        raise CommandFailed(
            f"Validation failed for 'mount_root' for client_id {client_id}: "
            f"Expected {expected_mount_root}, but got {actual_mount_root}"
        )
    else:
        log.info(
            f"'mount_root' is valid for client_id {client_id}: {actual_mount_root}"
        )

    # Validate mount_point
    if actual_mount_point_extracted != expected_mount_point:
        log.error(
            f"Validation failed for 'mount_point' for client_id {client_id}: "
            f"Expected {expected_mount_point}, but got {actual_mount_point}"
        )
        raise CommandFailed(
            f"Validation failed for 'mount_point' for client_id {client_id}: "
            f"Expected {expected_mount_point}, but got {actual_mount_point}"
        )
    else:
        log.info(
            f"'mount_point' is valid for client_id {client_id}: {actual_mount_point}"
        )


def run(ceph_cluster, **kw):
    """
    Steps:
    Scenario 1: cephfs-top should not display any entries when no clients are mounted.
    Scenario 2: After mounting and writing data using both mounts,
                cephfs-top should reflect the details of the active mounts.
    Scenario 3: After rebooting the client, verify that cephfs-top no longer shows any entries.
    Scenario 4: Configure the mount via fstab entries, reboot the client, and
                ensure that cephfs-top displays the same clients with nonidentical IDs as before the reboot.
    """
    try:
        tc = "CEPH-83573838"
        log.info(f"Running CephFS tests for -{tc}")
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
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        client2 = clients[1]
        for client in [client1, client2]:
            setup_fs_top(client)

        fs_name = "cephfs-top" if not erasure else "cephfs-top-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)
        if not fs_details:
            fs_util.create_fs(client1, fs_name)

        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        log.info("Scenario 1: cephfs-top should be empty when no clients have mounted")
        out, rc = client2.exec_command(sudo=True, cmd=f"cephfs-top --dumpfs {fs_name}")
        top_details = json.loads(out)
        is_empty = (
            "Empty"
            if isinstance(top_details.get(fs_name), dict) and not top_details[fs_name]
            else "Not Empty"
        )
        if is_empty != "Empty":
            raise CommandFailed(
                f"Top is populating data without mounting the clients, populated data : {top_details}"
            )

        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")

        log.info(
            "Scenario 2: Cephfs-top should have details of the mount after running the writing the data using "
            "both clients"
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            mon_node_ip,
            extra_params=f",fs={fs_name}",
        )
        active_mds = fs_util.get_active_mdss(client1, fs_name)
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
        )
        client_details = json.loads(out)
        kernel_client_ls = [item["id"] for item in client_details]
        kernel_client_id = kernel_client_ls[0]
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {fs_name}"
        )

        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
        )
        client_details = json.loads(out)
        fuse_client_ls = [item["id"] for item in client_details]
        fuse_client_id = [
            item for item in fuse_client_ls if item not in kernel_client_ls
        ][0]
        log.info(f"fuse_client_id : {fuse_client_id}")

        with parallel() as p:
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_1, ["dd", "smallfile"])
            p.spawn(
                fs_util.run_ios, client1, kernel_mounting_dir_1, ["dd", "smallfile"]
            )

            log.info("Validate mount points and client ids")
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
            )
            client_details = json.loads(out)

            out, rc = client2.exec_command(
                sudo=True, cmd=f"cephfs-top --dumpfs {fs_name}"
            )
            top_details = json.loads(out)
            log.info(f"{top_details}")

            expected_mount_root, expected_mount_point = extract_mount_info_by_id(
                client_details, fuse_client_id
            )
            validate_mount_info(
                client2,
                fuse_client_id,
                expected_mount_root,
                expected_mount_point,
                fs_name,
            )
            expected_mount_root, expected_mount_point = extract_mount_info_by_id(
                client_details, kernel_client_id
            )
            # validate_mount_info(
            #     client2,
            #     kernel_client_id,
            #     expected_mount_root,
            #     expected_mount_point,
            #     fs_name,
            # )
        log.info(
            "scenario 3: Reboot client and validate the cephfs-top dump it should be empty"
        )
        fs_util.reboot_node(client1)
        out, rc = client2.exec_command(sudo=True, cmd=f"cephfs-top --dumpfs {fs_name}")
        top_details = json.loads(out)
        is_empty = (
            "Empty"
            if isinstance(top_details.get(fs_name), dict) and not top_details[fs_name]
            else "Not Empty"
        )
        if is_empty != "Empty":
            raise CommandFailed(
                f"Top is populating data without mounting the clients, populated data : {top_details}"
            )

        log.info(
            "Scenario 4: Mount using fstab entries reboot the client and validate cephfs-top displays "
            "same clients with same IDs"
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_2,
            mon_node_ip,
            extra_params=f",fs={fs_name}",
            fstab=True,
        )
        active_mds = fs_util.get_active_mdss(client1, fs_name)
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
        )
        client_details = json.loads(out)
        kernel_client_ls = [item["id"] for item in client_details]
        kernel_client_id = kernel_client_ls[0]
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_2,
            extra_params=f" --client_fs {fs_name}",
            fstab=True,
        )

        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
        )
        client_details = json.loads(out)
        fuse_client_ls = [item["id"] for item in client_details]
        fuse_client_id = [
            item for item in fuse_client_ls if item not in kernel_client_ls
        ][0]
        log.info(f"fuse_client_id : {fuse_client_id}")
        fs_util.reboot_node(client1)

        fuse_client_id = extract_client_id_by_mount_point(
            client2, active_mds, fuse_mounting_dir_2
        )

        log.info(f"fuse_client_id after reboot : {fuse_client_id}")
        with parallel() as p:
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_2, ["dd", "smallfile"])
            p.spawn(
                fs_util.run_ios, client1, kernel_mounting_dir_2, ["dd", "smallfile"]
            )
            log.info("Validate mount points and client ids")
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph tell mds.{active_mds[0]} client ls --format json"
            )
            client_details = json.loads(out)
            expected_mount_root, expected_mount_point = extract_mount_info_by_id(
                client_details, fuse_client_id
            )
            validate_mount_info(
                client2,
                fuse_client_id,
                expected_mount_root,
                expected_mount_point,
                fs_name,
            )
            expected_mount_root, expected_mount_point = extract_mount_info_by_id(
                client_details, kernel_client_id
            )
            # validate_mount_info(
            #     client2,
            #     kernel_client_id,
            #     expected_mount_root,
            #     expected_mount_point,
            #     fs_name,
            # )

        log.info("Successfully set all the client config values")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Unmounting the clients")
        fs_util.client_clean_up(
            client1,
            [
                fuse_mounting_dir_1,
                kernel_mounting_dir_1,
                fuse_mounting_dir_2,
                kernel_mounting_dir_2,
            ],
        )
        fs_util.remove_fs(client1, fs_name)
        log.info("Successfully unmounted the clients")
        client1.exec_command(
            sudo=True, cmd="mv /etc/fstab.backup /etc/fstab", check_ec=False
        )
