import logging
import timeit
import traceback
import re

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = '11221'
        log.info('Running cephfs %s test case' % tc)
        fs_util = FsUtils(ceph_cluster)
        config = kw.get('config')
        build = config.get('build', config.get('rhbuild'))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info('Got client info')
        else:
            raise CommandFailed('fetching client info failed')
        c1 = 1
        client1 = []
        client2 = []
        client3 = []
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)

        print(rc1, rc2, rc3)
        if rc1 == 0 and rc2 == 0 and rc3 == 0:
            log.info('got auth keys')
        else:
            raise CommandFailed('auth list failed')
        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])
        if rc1 == 0 and rc2 == 0:
            log.info('Fuse mount passed')
        else:
            raise CommandFailed('Fuse mount failed')

        rc3 = fs_util.kernel_mount(
            client3,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])

        if rc3 == 0:
            log.info('kernel mount passed')
        else:
            raise CommandFailed('kernel mount failed')

        while c1:

            with parallel() as p:
                p.spawn(fs_util.read_write_IO, client1,
                        client_info['mounting_dir'], 'g', 'write')
                p.spawn(fs_util.read_write_IO, client2,
                        client_info['mounting_dir'], 'g', 'read')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    '',
                    0,
                    2,
                    iotype='crefi',
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    '',
                    0,
                    2,
                    iotype='crefi',
                )
                p.spawn(fs_util.read_write_IO, client3,
                        client_info['mounting_dir'])
                for op in p:
                    (return_counts, rc) = op
            c1 = ceph_df(ceph_cluster)

        check_health(ceph_cluster)
        log.info('Test completed for CEPH-%s' % tc)
        print('Results:')
        result = fs_util.rc_verify(tc, return_counts)
        print(result)
        print('Script execution time:------')
        stop = timeit.default_timer()
        total_time = stop - start
        (mins, secs) = divmod(total_time, 60)
        (hours, mins) = divmod(mins, 60)
        print('Hours:%d Minutes:%d Seconds:%f' % (hours, mins, secs))
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         client_info['kernel_clients'],
                                         client_info['mounting_dir'], 'umount')
        else:
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         '',
                                         client_info['mounting_dir'], 'umount')
        if rc == 0:
            log.info('Cleaning up successfull')

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def ceph_df(ceph_cluster):
    for mnode in ceph_cluster.get_nodes('mon'):
        if mnode.role == 'mon':
            m = mnode.role
            log.info('%s' % m)
            out, rc = mnode.exec_command(cmd='sudo ceph df ')
            output = out.read()
            str = output.decode()
            print(str)
            l = []
            for s in str.split():
                if re.findall(r'-?\d+\.?\d*', s):
                    l.append(s)
            print(l[3])
            cluster_filled_perc = float(l[3])
            if cluster_filled_perc > 30:
                return 0
            return 1


def check_health(ceph_cluster):
    for mnode in ceph_cluster.get_nodes('mon'):
        if mnode.role == 'mon':
            m = mnode.role
            log.info('%s' % m)
            out, rc = mnode.exec_command(cmd='sudo ceph -s ')
            output = out.read()
            out1 = output.decode()
            print(out1)
            out2 = out1.split()
            if 'HEALTH_OK' not in out2:
                log.warning('Ceph Health is NOT OK')
                return 0
            else:
                log.info('Health IS OK')
                return 1
