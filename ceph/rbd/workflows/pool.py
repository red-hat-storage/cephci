import json

from ceph.rbd.utils import exec_cmd
from ceph.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)


def check_pool_exists(pool_name, client, timeout=10, interval=10):
    """
    recursively checks if the specified pool exists in the cluster
    Args:
        pool_name: Name of the pool to be checked
        timeout: number in seconds to wait until pool exists
                  default will be 10
        interval: number in seconds to check for pool existence until timeout
                  default will be 10

    Returns:
        0 if pool created successfully,
        1 if pool creation failed.

    """
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = exec_cmd(node=client, cmd="ceph df -f json", output=True)
        if pool_name in [ele["name"] for ele in json.loads(out)["pools"]]:
            log.info(f"Pool '{pool_name}' present in the cluster")
            return 0
        elif timeout == 0:
            log.error(f"Pool '{pool_name}' not present in the cluster")
            return 1
        log.info(
            f"Pool '{pool_name}' not populated yet.\n"
            f"Waiting for {interval} seconds and retrying"
        )

    if w.expired:
        log.info(
            f"Failed to wait {timeout} seconds to pool '{pool_name}'"
            f" present on cluster"
        )
        return 1


def set_ec_profile(k_m, profile, failure_domain, client):
    """
    Sets erasure code profile for the given k_m and profile values

    Args:
        k_m: Comma separated values of k and m. Ex: 2,1
        profile: EC profile name to be created
        failure_domain: osd

    Returns:
        0 if ec profile creation is successful
        1 if ec profile creation fails
    """
    exec_cmd(node=client, cmd=f"ceph osd erasure-code-profile rm {profile}")

    cmd = f"ceph osd erasure-code-profile set {profile} k={k_m[0]} m={k_m[2]}"
    if failure_domain:
        cmd += f" crush-failure-domain={failure_domain}"
    return exec_cmd(node=client, cmd=cmd)


def create_ecpool(
    pool,
    client,
    k_m="",
    profile="use_default",
    pg_num=16,
    pgp_num=16,
    failure_domain="",
    cluster="ceph",
):
    """
    Creates an ec pool with the given parameters

    Args:
        pool: Pool name to be created
        client: client node to be used to execute commands
        pg_num: pg_num value for the pool, default 16
        pgp_num: pgp_num value for the pool, default 16
        k_m: Comma separated values of k and m. Ex: 2,1, default ""
        profile: EC profile name to be created default "use_default"
        failure_domain: osd default ""

    Returns:
        0 if ec pool creation is successful
        1 if ec pool creation fails
    """
    cmd = f"ceph osd pool create {pool} {pg_num} {pgp_num} erasure --cluster {cluster} "
    if profile != "use_default":
        cmd += profile
        if set_ec_profile(
            client=client, k_m=k_m, profile=profile, failure_domain=failure_domain
        ):
            log.error("EC profile creation failed")
            return 1
    if exec_cmd(node=client, cmd=cmd):
        log.error("Pool creation failed")
        return 1
    if check_pool_exists(pool_name=pool, client=client, timeout=200, interval=2):
        log.error("Pool not created")
        return 1
    if exec_cmd(node=client, cmd=f"ceph osd pool set {pool} allow_ec_overwrites true"):
        log.error("Set allow_ec_overwrites failed")
        return 1
    return 0


def create_pool(pool, client, pg_num=64, pgp_num=64, cluster="ceph"):
    """
    Creates a pool with the given name

    Args:
        pool: name of the pool to be created
        client: client node to be used to execute commands
        pg_num: pg_num value for the pool, default 64
        pgp_num: pgp_num value for the pool, default 64

    Returns:
        0 if pool creation is successful
        1 if pool creation fails
    """
    if exec_cmd(
        node=client,
        cmd=f"ceph osd pool create {pool} {pg_num} {pgp_num} --cluster {cluster}",
    ):
        log.error("Pool creation failed")
        return 1
    if check_pool_exists(client=client, pool_name=pool):
        log.error("Pool not created")
        return 1
    return 0
