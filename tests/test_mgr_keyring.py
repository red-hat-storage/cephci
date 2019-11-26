import logging
logger = logging.getLogger(__name__)
log = logger


def run(**kw):

    log.info("Running exec test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    clients = []
    role = 'mgr'
    if config.get('role'):
        role = config.get('role')
    for cnode in ceph_nodes:
        if cnode.role == role:
            clients.append(cnode)

    idx = 0
    client = clients[idx]

    host_name = client.hostname
    cmd = "getfacl /etc/ceph/ceph.mgr.{}.keyring".format(host_name)
    out = client.exec_command(cmd=cmd, long_running=True)
    out = str(out)

    if "user::r--" in out and "group::---" in out and "other::---" in out:
        rc = 0
    else:
        rc = 1

    return rc
