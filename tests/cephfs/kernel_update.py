import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import parse_custom_config_list

log = Log(__name__)

_REBOOT_RECONNECT_TIMEOUT = 300
_RECONNECT_INTERVAL = 20
_PER_REBOOT_HEALTH_TIMEOUT = 600

_DIAGNOSTIC_COMMANDS = (
    "cat /etc/os-release",
    "uname -r",
    "ceph versions",
)

_PRE_KERNEL_YUM_CMD = (
    "yum update kernel -y --nogpgcheck " "--disablerepo='*' --enablerepo=rh_add_repo"
)

_REPO_FILE = "/etc/yum.repos.d/rh_add_repo.repo"


def _print_system_info(client, label):
    """Run OS/kernel/ceph version commands on client.1 and log output."""
    log.info("=" * 20 + f" {label} " + "=" * 20)
    log.info("Client node: %s", client.node.hostname)
    for cmd in _DIAGNOSTIC_COMMANDS:
        log.info("Executing: %s", cmd)
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        log.info("Output:\n%s", out)
    log.info("=" * 20 + f" End {label} " + "=" * 20)


def _node_os_major(node):
    """Return the OS major version (e.g. 8, 9) for a cluster node."""
    return node.distro_info["VERSION_ID"].split(".")[0]


def _is_mixed_os_cluster(ceph_cluster):
    """True when client nodes run a different OS major version than other nodes."""
    client_nodes = ceph_cluster.get_nodes(role="client")
    all_nodes = ceph_cluster.get_nodes()
    if not client_nodes or len(client_nodes) == len(all_nodes):
        return False

    client_hostnames = {node.hostname for node in client_nodes}
    client_majors = {_node_os_major(node) for node in client_nodes}
    non_client_majors = {
        _node_os_major(node)
        for node in all_nodes
        if node.hostname not in client_hostnames
    }
    return bool(non_client_majors) and client_majors != non_client_majors


def _kernel_update_nodes(ceph_cluster):
    """Select nodes for kernel update: clients only on mixed OS, else all nodes."""
    if _is_mixed_os_cluster(ceph_cluster):
        nodes = ceph_cluster.get_nodes(role="client")
        scope = "Mixed OS cluster detected; kernel update limited to client nodes"
    else:
        nodes = ceph_cluster.get_nodes()
        scope = "Homogeneous OS cluster; kernel update targets all nodes"
    log.info("%s: %s", scope, ", ".join(node.hostname for node in nodes))
    return nodes


def _reboot_and_reconnect(node, timeout=_REBOOT_RECONNECT_TIMEOUT):
    """Reboot a node and retry SSH reconnect until timeout."""
    node.exec_command(sudo=True, cmd="sudo reboot", check_ec=False)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(_RECONNECT_INTERVAL)
        try:
            node.reconnect()
            log.info("Reconnected to %s after reboot", node.hostname)
            return
        except Exception:
            log.info("Waiting for %s to come back after reboot", node.hostname)
    raise RuntimeError(f"Failed to reconnect to {node.hostname} after reboot")


def _wait_for_cluster_healthy(cephfs_common_utils, client, node_hostname):
    """Wait for HEALTH_OK after a node reboot before continuing."""
    log.info(
        "Waiting up to %ss for HEALTH_OK after rebooting %s",
        _PER_REBOOT_HEALTH_TIMEOUT,
        node_hostname,
    )
    if cephfs_common_utils.wait_for_healthy_ceph(client, _PER_REBOOT_HEALTH_TIMEOUT):
        log.error("Cluster did not reach HEALTH_OK after rebooting %s", node_hostname)
        return 1
    log.info("Cluster is healthy after rebooting %s", node_hostname)
    return 0


def _reboot_node_and_wait_for_healthy(cephfs_common_utils, client, node):
    """Reboot a node, reconnect, and wait for the cluster to recover."""
    _reboot_and_reconnect(node)
    return _wait_for_cluster_healthy(cephfs_common_utils, client, node.hostname)


def _resolve_kernel_repo_config(test_data):
    """
    Resolve kernel repo URL from --custom-config pre=<url> or post=<url>.

    Prefers parsing the custom-config list (split on first '=') so other
    entries such as ocpvirt_namespace do not shadow pre/post, and URLs that
    contain '=' are preserved. Falls back to custom_config_dict.
    """
    test_data = test_data or {}
    custom_dict = parse_custom_config_list(test_data.get("custom-config"))
    if not custom_dict:
        custom_dict = test_data.get("custom_config_dict") or {}

    for key in ("pre", "post"):
        value = custom_dict.get(key)
        if value:
            return key, str(value).rstrip("/").replace(" ", "")
    return None, None


def _repo_base_and_package(url):
    """Split a kernel package URL into base directory URL and rpm basename."""
    package = url.rsplit("/", 1)[-1]
    base = (
        url[: -len(package)] if url.endswith(package) else f"{url.rsplit('/', 1)[0]}/"
    )
    return base, package


def _rpm_version_suffix(kernel_package):
    """Turn kernel-<ver>.rpm into -<ver>.rpm for sibling rpm name construction."""
    if kernel_package.startswith("kernel"):
        return kernel_package[len("kernel") :]
    return kernel_package


def _running_kernel(node):
    """Return uname -r output with trailing whitespace removed."""
    kernel_version, _ = node.exec_command(sudo=True, cmd="uname -r")
    return kernel_version.rstrip()


def _normalize_kernel_id(kernel_version):
    """Drop arch suffix used when comparing uname -r to package names."""
    return kernel_version.rstrip(".x86_64")


def _kernel_already_updated(kernel_version, kernel_package):
    """True when running kernel already matches the target package/repo id."""
    package = kernel_package.rstrip()
    return package in kernel_version or _normalize_kernel_id(kernel_version) in package


def _kernel_update_succeeded(kernel_version, kernel_package):
    """True when normalized uname -r is present in the target package name."""
    return _normalize_kernel_id(kernel_version) in kernel_package.rstrip()


def _write_pre_repo(node, url):
    """Install a temporary yum repo pointing at the pre-verification kernel URL."""
    node.exec_command(
        sudo=True,
        cmd=(
            f"printf '%s\\n' '[rh_add_repo]' 'baseurl={url}' 'gpgcheck=0' "
            f"'enabled=1' 'name=Add Repo' > {_REPO_FILE}"
        ),
    )


def _build_post_rpm_install_cmd(url):
    """
    Build `rpm -ivh` command with RHEL-8/RHEL-9 package install order.

    RHEL-8: kernel-core, kernel-modules, kernel
    RHEL-9+: kernel-modules-core, kernel-core, kernel-modules, kernel
    """
    base, kernel_package = _repo_base_and_package(url)
    suffix = _rpm_version_suffix(kernel_package)
    packages = []
    if "rhel-8" in url:
        packages = [
            f"{base}kernel-core{suffix}",
            f"{base}kernel-modules{suffix}",
            f"{base}{kernel_package}",
        ]
    else:
        packages = [
            f"{base}kernel-modules-core{suffix}",
            f"{base}kernel-core{suffix}",
            f"{base}kernel-modules{suffix}",
            f"{base}{kernel_package}",
        ]
    for package in packages:
        log.info(package)
    return " ".join(["rpm", "-ivh", *packages]), kernel_package


def _update_nodes(nodes, client, cephfs_common_utils, kernel_package, install_fn):
    """Apply install_fn on each node, reboot, and verify the running kernel."""
    for node in nodes:
        kernel_version = _running_kernel(node)
        log.info("Current kernel version is %s", kernel_version)
        if _kernel_already_updated(kernel_version, kernel_package):
            log.info("Kernel is already updated on %s", node.hostname)
            continue

        install_fn(node)
        if _reboot_node_and_wait_for_healthy(cephfs_common_utils, client, node):
            return 1

        kernel_version = _running_kernel(node)
        log.info("Updated kernel version is %s", kernel_version)
        if not _kernel_update_succeeded(kernel_version, kernel_package):
            log.error("Kernel update failed on %s", node.hostname)
            return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Update cluster/client kernels for pre- or post-verification.

    pre: yum update from a private repo (--custom-config pre=<repo-url>)
    post: rpm -ivh ordered packages (--custom-config post=<kernel-rpm-url>)

    On mixed-OS clusters only client nodes are updated; otherwise all nodes.
    """
    client1 = None
    diagnostics_started = False
    try:
        clients = ceph_cluster.get_ceph_objects("client")
        test_data = kw.get("test_data")
        log.info("Test data is %s", test_data)

        pre_or_post, url = _resolve_kernel_repo_config(test_data)
        if not pre_or_post or not url:
            log.error(
                "Missing kernel repo config. Pass --custom-config pre=<url> "
                "or --custom-config post=<url>"
            )
            return 1
        if pre_or_post not in ("pre", "post"):
            log.error("Invalid kernel repo config key: %s", pre_or_post)
            log.error("Expected --custom-config pre=<url> or post=<url>")
            return 1

        log.info("Given Kernel Information is %s", url)
        log.info(
            "This is a %s-verification",
            "pre" if pre_or_post == "pre" else "Post",
        )

        if not clients:
            log.info(
                "This test requires minimum 1 client nodes. This has only %s clients",
                len(clients),
            )
            return 1

        client1 = clients[0]
        nodes = _kernel_update_nodes(ceph_cluster)
        _print_system_info(client1, "Before start of TC")
        diagnostics_started = True
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)

        if pre_or_post == "pre":
            _, kernel_package = _repo_base_and_package(url)

            def _install_pre(node):
                _write_pre_repo(node, url)
                log.info("Updating kernel using private repo %s", kernel_package)
                retry(CommandFailed, tries=3, delay=60)(node.exec_command)(
                    sudo=True, cmd=_PRE_KERNEL_YUM_CMD
                )

            rc = _update_nodes(
                nodes, client1, cephfs_common_utils, kernel_package, _install_pre
            )
        else:
            kernel_update_cmd, kernel_package = _build_post_rpm_install_cmd(url)

            def _install_post(node):
                log.info("Updating kernel using below packages")
                log.info(kernel_update_cmd)
                node.exec_command(sudo=True, cmd=kernel_update_cmd)

            rc = _update_nodes(
                nodes, client1, cephfs_common_utils, kernel_package, _install_post
            )

        if rc:
            return rc
        log.info("kernel update success")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if diagnostics_started and client1:
            _print_system_info(client1, "After TC")
