from cli.utilities.packages import Repos, SubscriptionManager


def enable_rhel_rpms(ceph, distro_ver):
    """
    Enable required RHEL repositories based on the distro version.

    Args:
        ceph: The node or context to run commands on.
        distro_ver (str): The major RHEL version (e.g., '7', '8', '9')
    """
    repos = {
        "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
        "8": ["rhel-8-for-x86_64-appstream-rpms", "rhel-8-for-x86_64-baseos-rpms"],
        "9": ["rhel-9-for-x86_64-appstream-rpms", "rhel-9-for-x86_64-baseos-rpms"],
    }

    sm = SubscriptionManager(ceph)
    repos = Repos(ceph)

    sm.set(distro_ver)

    for repo in repos.get(distro_ver[0]):
        repos.enable(repos=repo)


def enable_rhel_eus_rpms(ceph, distro_ver):
    """
    Enable Extended Update Support (EUS) repositories for RHEL 7.

    Args:
        ceph: The node or object to run commands on.
        distro_ver (str): The major RHEL version (e.g., '7').

    Raises:
        NotImplementedError: If the version is not supported for EUS.
    """

    sm = SubscriptionManager(ceph)
    repos = Repos(ceph)

    eus_repos = {"7": ["rhel-7-server-eus-rpms", "rhel-7-server-extras-rpms"]}
    for repo in eus_repos.get(distro_ver[0]):
        repos.enable(repos=repo)

    if distro_ver[0] == "7":
        release = "7.7"
    else:
        raise NotImplementedError(f"Cannot set EUS repos for {distro_ver[0]}")

    sm.set(release)
    ceph.exec_command(sudo=True, cmd="yum clean all", long_running=True)
