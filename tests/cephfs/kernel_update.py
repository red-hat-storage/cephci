import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Check current kernel version
    Check the requirement is for pre-verification or verification
    Update the kernel of client nodes
    Reboot the client nodes
    Verify kernel is updated
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")

        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        repo_file = clients[0].remote_file(
            sudo=True, file_name="/etc/yum.repos.d/rh_add_repo.repo", file_mode="r"
        )
        file_contents = repo_file.readlines()
        url = file_contents[1]
        kernel_package = url.split("/")[-1]

        if "p" in file_contents[0]:
            url = file_contents[2].split("=", 1)[1]
            log.info(url)
            kernel_package = url.split("/")[-1]
            for client in clients:
                kernel_version, _ = client.exec_command(sudo=True, cmd="uname -r")
                log.info(f"Current kernel version is {kernel_version}")
                kernel_version = kernel_version.rstrip()
                kernel_version = kernel_version.rstrip(".x86_64")
                kernel_package = kernel_package.rstrip()
                log.info(f"Current kernel version is {kernel_version}+")
                log.info(f" kernel package {kernel_package}+")
                if kernel_version not in kernel_package:
                    log.info(f"Updating kernel using private repo {kernel_package}")
                    client.exec_command(
                        sudo=True, cmd="yum update kernel -y --nogpgcheck"
                    )
                    fs_util.reboot_node(client)
                    kernel_version, _ = client.exec_command(sudo=True, cmd="uname -r")
                    kernel_version = kernel_version.rstrip()
                    kernel_version = kernel_version.rstrip(".x86_64")
                    kernel_package = kernel_package.rstrip()
                    log.info(f" kernel package {kernel_package}")
                    log.info(f"Updated kernel version is {kernel_version}")
                    if kernel_version not in kernel_package:
                        log.error("Kernel update failed")
                        return 1
            log.info("kernel update success")
            return 0

        elif "v" in file_contents[0]:
            url = file_contents[1]
            kernel_package = url.split("/")[-1]
            url = url.strip(kernel_package)
            kernel_core_package = "kernel-core" + kernel_package.strip("kernel")
            kernel_modules_package = "kernel-modules" + kernel_package.strip("kernel")
            log.info(url + kernel_core_package)
            log.info(url + kernel_modules_package)
            log.info(url + kernel_package)
            for client in clients:
                kernel_version, _ = client.exec_command(sudo=True, cmd="uname -r")
                log.info(f"Current kernel version is {kernel_version}")
                log.info("Updating kernel using below packages")
                client.exec_command(
                    sudo=True, cmd=f"rpm -ivh {url + kernel_core_package}"
                )
                client.exec_command(
                    sudo=True, cmd=f"rpm -ivh {url + kernel_modules_package}"
                )
                client.exec_command(sudo=True, cmd=f"rpm -ivh {url + kernel_package}")
                fs_util.reboot_node(client)
                kernel_version, rc = client.exec_command(sudo=True, cmd="uname -r")
                kernel_version = kernel_version.rstrip()
                kernel_package = kernel_package.rstrip()
                log.info(f"Updated kernel version is {kernel_version}")
                if kernel_version not in kernel_package:
                    log.error("Kernel update failed")
                    return 1
        log.info("kernel update success")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
