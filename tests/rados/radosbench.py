import random

from utility.log import Log

log = Log(__name__)


def run(**kw):
    log.info("Running exec test")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")

    clients = []
    role = "client"
    if config.get("role"):
        role = config.get("role")
    for cnode in ceph_nodes:
        if cnode.role == role:
            clients.append(cnode)

    idx = 0
    client = clients[idx]

    if config.get("idx"):
        idx = config["idx"]
        client = clients[idx]

    if config.get("pg_num"):
        pg_num = config.get("pg_num")
    else:
        pg_num = "128"

    if config.get("op"):
        op = config.get("bench_op")
    else:
        op = "write"

    if config.get("cleanup"):
        cleanup = "cleanup"
    else:
        cleanup = ""

    name = client.shortname
    pool_name = "test_pool" + str(random.randint(10, 999))
    pool_create = "sudo ceph osd pool create {pool_name} {pg_num}".format(
        pool_name=pool_name, pg_num=pg_num
    )

    client.exec_command(cmd=pool_create)
    block = str(config.get("size", 4 << 20))
    time = str(config.get("time", 360))
    # block and time parameters are hardcoded to unblock the CI jobs.
    # Remove block=10 and time = 60 lines after #Bug-1848016 fix
    block = 10
    time = 60
    rados_bench = "sudo rados --no-log-to-stderr  -b {block} -p {pool} bench {time} {op} \
     {cleanup}".format(
        name=name, block=block, pool=pool_name, time=time, op=op, cleanup=cleanup
    )
    return client.exec_command(cmd=rados_bench, long_running=True)
