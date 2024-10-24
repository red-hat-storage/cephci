import time
import traceback

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Check current kernel version
    Check the requirement is for pre-verification or verification
    Update the kernel of client nodes
    Reboot the client nodes
    Verify kernel is updated
    RHEL-8 package installation order:
    kernel-core
    kernel-modules
    kernel
    RHEL-9 package installation order:
    kernel-modules-core
    kernel-core
    kernel-modules
    kernel
    """
    try:
        clients = ceph_cluster.get_ceph_objects("client")
        test_data = kw.get("test_data")
        log.info("Test data is " + str(test_data))
        custom_config = test_data.get("custom-config")[0].split("=")
        pre_or_post = custom_config[0]
        url = custom_config[1]
        url = url.rstrip("/")
        # removing space from the url
        url = url.replace(" ", "")
        log.info("Given Kernel Information is " + str(url))
        verification_type = []
        if "pre" in pre_or_post:
            log.info("This is a pre-verification")
            verification_type.append("pre")
        elif "post" in pre_or_post:
            log.info("This is a Post verification")
            verification_type.append("post")
        else:
            log.error("Invalid repo file")
            log.error("The repo file should contain [pre] or [post]")
            return 1
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        ceph_nodes = ceph_cluster.get_nodes()
        kernel_package = url.split("/")[-1]
        if "pre" in verification_type:
            log.info(url)
            kernel_package = url.split("/")[-1]
            for cnode in ceph_nodes:
                cnode.exec_command(
                    sudo=True,
                    cmd=f"echo -e '[rh_add_repo]\nbaseurl={url}\ngpgcheck=0\nenabled=1\n"
                    f"name=Add Repo' > /etc/yum.repos.d/rh_add_repo.repo",
                )
                kernel_version, _ = cnode.exec_command(sudo=True, cmd="uname -r")
                log.info(f"Current kernel version is {kernel_version}")
                if kernel_package in kernel_version:
                    log.info("Kernel is already updated")
                    continue
                kernel_version = kernel_version.rstrip()
                kernel_version = kernel_version.rstrip(".x86_64")
                kernel_package = kernel_package.rstrip()
                log.info(f"Current kernel version is {kernel_version}+")
                log.info(f" kernel package {kernel_package}+")
                if kernel_version not in kernel_package:
                    log.info(f"Updating kernel using private repo {kernel_package}")
                    cnode.exec_command(
                        sudo=True, cmd="yum update kernel -y --nogpgcheck"
                    )
                    cnode.exec_command(sudo=True, cmd="sudo reboot", check_ec=False)
                    time.sleep(60)
                    cnode.reconnect()
                    kernel_version, _ = cnode.exec_command(sudo=True, cmd="uname -r")
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
        elif "post" in verification_type:
            log.info(url)
            kernel_cmds = ["rpm", "-ivh"]
            if "rhel-8" in url:
                kernel_package = url.split("/")[-1]
                url = url.strip(kernel_package)
                kernel_core_package = (
                    url + "kernel-core" + kernel_package.strip("kernel")
                )
                kernel_modules_package = (
                    url + "kernel-modules" + kernel_package.strip("kernel")
                )
                log.info(url + kernel_core_package)
                log.info(url + kernel_modules_package)
                log.info(url + kernel_package)
                kernel_cmds.append(kernel_core_package)
                kernel_cmds.append(kernel_modules_package)
                kernel_cmds.append(url + kernel_package)
            if "rhel-9" in url:
                kernel_package = url.split("/")[-1]
                url = url.strip(kernel_package)
                kernel_modules_core = "kernel-modules-core" + kernel_package.strip(
                    "kernel"
                )
                kernel_core_package = "kernel-core" + kernel_package.strip("kernel")
                kernel_modules_package = "kernel-modules" + kernel_package.strip(
                    "kernel"
                )
                log.info(url + kernel_modules_core)
                log.info(url + kernel_core_package)
                log.info(url + kernel_modules_package)
                log.info(url + kernel_package)
                kernel_cmds.append(url + kernel_modules_core)
                kernel_cmds.append(url + kernel_core_package)
                kernel_cmds.append(url + kernel_modules_package)
                kernel_cmds.append(url + kernel_package)
            kernel_update_cmd = " ".join(kernel_cmds).replace("\n", "")
            for cnode in ceph_nodes:
                kernel_version, _ = cnode.exec_command(sudo=True, cmd="uname -r")
                if kernel_package in kernel_version:
                    log.info("Kernel is already updated")
                    continue
                log.info(f"Current kernel version is {kernel_version}")
                log.info("Updating kernel using below packages")
                log.info(kernel_update_cmd)
                cnode.exec_command(sudo=True, cmd=kernel_update_cmd)
                cnode.exec_command(sudo=True, cmd="sudo reboot", check_ec=False)
                time.sleep(60)
                cnode.reconnect()
                kernel_version, rc = cnode.exec_command(sudo=True, cmd="uname -r")
                kernel_version = kernel_version.rstrip()
                kernel_package = kernel_package.rstrip()
                log.info(f"Updated kernel version is {kernel_version}")
                if kernel_version not in kernel_package:
                    log.error("Kernel update failed")
                    return 1
        else:
            log.error("Invalid repo file")
            log.error("The repo file should start with either [pre] or [post]")
            return 1
        log.info("kernel update success")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
