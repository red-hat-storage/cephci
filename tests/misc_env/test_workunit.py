from utility.log import Log

log = Log(__name__)


def run(**kw):
    log.info("Running workunit test")
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

    if config.get("repo"):
        repo = config.get("repo")
    else:
        repo = "git://git.ceph.com/ceph.git"
    if config.get("downstream"):
        repo = "http://gitlab.cee.redhat.com/ceph/workunits.git"

    if config.get("branch"):
        branch = config.get("branch")
    else:
        branch = "master"

    git_cmd = "git clone -b " + branch + " " + repo
    if config.get("test_name"):
        test_name = config.get("test_name")

    cmd1 = "mkdir cephtest ; cd cephtest ; {git_cmd}".format(git_cmd=git_cmd)
    client.exec_command(cmd="rm -rf cephtest", timeout=60)
    out, err = client.exec_command(cmd=cmd1, timeout=600)
    log.info(out)
    if client.exit_status != 0:
        log.error("Failed during git clone")
        return 1
    cmd2 = "CEPH_REF={ref} sudo -E sh cephtest/ceph/qa/workunits/{name}".format(
        ref=branch, name=test_name
    )
    if config.get("downstream"):
        cmd2 = "CEPH_REF={ref} sudo -E sh cephtest/workunits/{name}".format(
            ref=branch, name=test_name
        )
    ec = client.exec_command(cmd=cmd2, long_running=True)

    if ec == 0:
        log.info("Workunit completed successfully")
    else:
        log.info("Error during workunit")
    return ec
