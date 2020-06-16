import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = '11219,11224'
        dir_name = 'dir'
        log.info("Running cephfs %s test case" % (tc))
        config = kw.get('config')
        num_of_osds = config.get('num_of_osds')
        fs_util = FsUtils(ceph_cluster)
        build = config.get('build', config.get('rhbuild'))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            log.error("fetching client info failed")
            return 1
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])
        cluster_health_beforeIO = check_ceph_healthly(
            client_info['mon_node'][0], num_of_osds, len(
                client_info['mon_node']), build, None, 300)
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
        print(rc1, rc2, rc3, rc4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1

        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            log.error("Fuse mount failed")
            return 1
        rc3 = fs_util.kernel_mount(
            client3,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        rc4 = fs_util.kernel_mount(
            client4,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        if rc3 == 0 and rc4 == 0:
            log.info("kernel mount passed")
        else:
            log.error("kernel mount failed")
            return 1
        rc = fs_util.activate_multiple_mdss(client_info['mds_nodes'])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            log.error("Activate multiple mdss failed")
            return 1
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                2,
                iotype='crefi'
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'g', 'readwrite')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'])
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify('', return_counts)
        if result == 'Data validation success':
            print("Data validation success")
            dirs, rc = fs_util.mkdir(
                client1, 0, 3, client_info['mounting_dir'], dir_name)
            if rc == 0:
                log.info("Directories created")
            else:
                raise CommandFailed("Directory creation failed")
            dirs = dirs.split('\n')
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='fio')
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    100,
                    iotype='touch')
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[1],
                    0,
                    1,
                    iotype='dd')
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='crefi')
                for op in p:
                    return_counts, rc = op

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_create', fnum=10, fsize=1024)
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[1],
                    0,
                    1,
                    iotype='smallfile_create', fnum=10, fsize=1024)
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='smallfile_create', fnum=10, fsize=1024)
                for op in p:
                    return_counts, rc = op
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_delete', fnum=10, fsize=1024)
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[1],
                    0,
                    1,
                    iotype='smallfile_delete')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='smallfile_delete', fnum=10, fsize=1024)
                for op in p:
                    return_counts, rc = op
            cluster_health_afterIO = check_ceph_healthly(
                client_info['mon_node'][0], num_of_osds, len(
                    client_info['mon_node']), build, None, 300)

            log.info("Execution of Test case CEPH-%s ended" % (tc))
            print("Results:")
            result = fs_util.rc_verify(tc, return_counts)
            if cluster_health_beforeIO == cluster_health_afterIO:
                print(result)
            print('-----------------------------------------')
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    5,
                    iotype='fio')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    10,
                    iotype='dd')
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='crefi')
            print('-------------------------------------------------------')
            with parallel() as p:
                p.spawn(
                    fs_util.read_write_IO,
                    client1,
                    client_info['mounting_dir'],
                    'g',
                    'read',
                    dir_name=dirs[0])
                p.spawn(
                    fs_util.read_write_IO,
                    client2,
                    client_info['mounting_dir'],
                    'g',
                    'read',
                    dir_name=dirs[0])
            print('-------------------------------------------------------')
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    5,
                    iotype='fio')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    10,
                    iotype='dd')
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='crefi')
            print('-------------------------------------------------------')
            with parallel() as p:
                p.spawn(
                    fs_util.read_write_IO,
                    client1,
                    client_info['mounting_dir'],
                    'g',
                    'read',
                    dir_name=dirs[0])
                p.spawn(
                    fs_util.read_write_IO,
                    client2,
                    client_info['mounting_dir'],
                    'g',
                    'read',
                    dir_name=dirs[0])
            print('-------------------------------------------------------')
            log.info('Cleaning up!-----')
            if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
                rc = fs_util.client_clean_up(
                    client_info['fuse_clients'],
                    client_info['kernel_clients'],
                    client_info['mounting_dir'],
                    'umount')
                if rc == 0:
                    log.info('Cleaning up successfull')
                else:
                    return 1
            else:
                rc = fs_util.client_clean_up(
                    client_info['fuse_clients'],
                    '',
                    client_info['mounting_dir'],
                    'umount')
                if rc == 0:
                    log.info('Cleaning up successfull')
                else:
                    return 1

        log.info("Execution of Test case CEPH-%s ended" % (tc))
        print("Results:")
        result = fs_util.rc_verify(tc, return_counts)
        print(result)
        print('Script execution time:------')
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
        else:
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                '',
                client_info['mounting_dir'],
                'umount')
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
