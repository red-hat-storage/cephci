import logging
import time

from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    # config = kw.get('config')

    with parallel() as p:
        for ceph in ceph_nodes:
            distro_info = ceph.distro_info
            distro_ver = distro_info['VERSION_ID']
            if distro_ver.startswith('8'):
                p.spawn(add_recent_rhel8_product_cert, ceph)
            time.sleep(5)
    return 0


def add_recent_rhel8_product_cert(ceph):
    log.info("Adding recent rhel8 GAed product certificate for subscription manager on {sn}".format(sn=ceph.shortname))
    ceph.exec_command(sudo=True, cmd='rm /etc/pki/product-default/*', long_running=True, check_ec=False)
    ceph.exec_command(sudo=True, cmd='rm /etc/pki/product/*', long_running=True, check_ec=False)
    ceph.exec_command(sudo=True,
                      cmd='curl http://magna002.ceph.redhat.com/cephci-jenkins/rhceph-qe-team-files/rhpc_8_2.sh -O')
    ceph.exec_command(sudo=True, cmd='bash rhpc_8_2.sh', long_running=True)
