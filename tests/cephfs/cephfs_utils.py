import datetime
import random
import re
import string
import time
from distutils.version import LooseVersion

from ceph.ceph import CommandFailed
from utility.log import Log

log = Log(__name__)


class FsUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.ceph_cluster = ceph_cluster
        self.clients = []
        self.result_vals = {}
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.return_counts = {}
        self.mounting_dir = ""
        self.dirs = ""
        self.rc_list = []
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.__install_python3()

    def __install_python3(self):
        """
        Installation of python3 and pip3
        """
        clients = self.ceph_cluster.get_ceph_objects("client")
        packages = ["python3", "python3-pip"]
        for client in clients:
            for package in packages:
                client.exec_command(
                    sudo=True,
                    cmd="yum install -y {package} --nogpgcheck".format(package=package),
                    long_running=True,
                    check_ec=False,
                )

            # verify python3 and pip3 existence
            client.exec_command(cmd="python3 --version; pip3 --version", check_ec=True)

    @staticmethod
    def _setup_crefi(node):
        """
        Setup crefi using Crefi repository
        """
        # clone crefi repository
        node.exec_command(
            cmd="cd /home/cephuser/; git clone {}".format(
                "https://github.com/yogesh-mane/Crefi.git"
            ),
            long_running=True,
        )

        # Setup Crefi pre-requisites : pyxattr
        node.exec_command(sudo=True, cmd="pip3 install pyxattr", long_running=True)

    def get_clients(self, build):
        log.info("Getting Clients")

        self.clients = self.ceph_cluster.get_ceph_objects("client")

        for node in self.clients:
            out, rc = node.exec_command(
                sudo=True, cmd="ceph mon dump  | awk {'print $2'} "
            )
            self.mon_node_ip = out.rstrip("\n")
            self.mon_node_ip = self.mon_node_ip.split("\n")
            self.mon_node_ip = (
                self.mon_node_ip[-3].strip("/0")
                + " "
                + self.mon_node_ip[-2].strip("/0")
                + " "
                + self.mon_node_ip[-1].strip("/0")
            )
            self.mon_node_ip = self.mon_node_ip.split(" ")
            if LooseVersion(build) > LooseVersion("3"):
                self.mon_node_ip[0] = re.search(
                    r"\W+v\w+:(\d+.\d+.\d+.\d+:\d+)/0,v\w+:(\d+.\d+.\d+.\d+:\d+)/0\W",
                    self.mon_node_ip[0],
                ).group(2)
                self.mon_node_ip[1] = re.search(
                    r"\W+v\w+:(\d+.\d+.\d+.\d+:\d+)/0,v\w+:(\d+.\d+.\d+.\d+:\d+)/0\W",
                    self.mon_node_ip[1],
                ).group(2)
                self.mon_node_ip[2] = re.search(
                    r"\W+v\w+:(\d+.\d+.\d+.\d+:\d+)/0,v\w+:(\d+.\d+.\d+.\d+:\d+)/0\W",
                    self.mon_node_ip[2],
                ).group(2)
            break
        for client in self.clients:
            node = client.node
            if node.pkg_type == "rpm":
                out, rc = node.exec_command(
                    sudo=True, cmd="rpm -qa | grep -w 'attr\\|xattr'"
                )
                if "attr" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y attr")
                node.exec_command(
                    sudo=True, cmd="yum install -y gcc python3-devel", check_ec=False
                )

                if "xattr" not in out:
                    pkgs = [
                        "@development",
                        "rh-python36",
                        "rh-python36-numpy rh-python36-scipy",
                        "rh-python36-python-tools rh-python36-python-six",
                        "libffi libffi-devel",
                    ]
                    if build.endswith("7") or build.startswith("3"):
                        cmd = "yum install -y " + " ".join(pkgs)
                        node.exec_command(sudo=True, cmd=cmd, long_running=True)

                    node.exec_command(
                        sudo=True, cmd="pip3 install xattr", long_running=True
                    )

                out, rc = node.exec_command(sudo=True, cmd="ls /home/cephuser")
                log.info("ls /home/cephuser : {}".format(out))

                if "Crefi" not in out:
                    self._setup_crefi(node)

                if "smallfile" not in out:
                    node.exec_command(
                        cmd="git clone https://github.com/bengland2/" "smallfile.git"
                    )

                out, rc = node.exec_command(sudo=True, cmd="rpm -qa")
                if "fio" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y fio")
                if "fuse-2" not in out:
                    node.exec_command(sudo=True, cmd="yum install -y fuse")
                if "ceph-fuse" not in out:
                    node.exec_command(
                        sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse"
                    )

            elif node.pkg_type == "deb":
                node.exec_command(sudo=True, cmd="pip3 install --upgrade pip3")
                out, rc = node.exec_command(sudo=True, cmd="apt list libattr1-dev")
                out = out.split()
                if "libattr1-dev/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y libattr1-dev")
                out, rc = node.exec_command(sudo=True, cmd="apt list attr")
                out = out.split()
                if "attr/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y attr")
                out, rc = node.exec_command(sudo=True, cmd="apt list fio")
                out = out.split()
                if "fio/xenial,now" not in out:
                    node.exec_command(sudo=True, cmd="apt-get install -y fio")
                out, rc = node.exec_command(sudo=True, cmd="pip3 list")
                if "crefi" not in out:
                    node.exec_command(sudo=True, cmd="pip3 install crefi")

                out, rc = node.exec_command(sudo=True, cmd="ls /home/cephuser")
                if "smallfile" not in out:
                    node.exec_command(
                        cmd="git clone " "https://github.com/bengland2/smallfile.git"
                    )
        self.mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        self.mounting_dir = "/mnt/cephfs_" + self.mounting_dir + "/"
        # separating clients for fuse and kernel
        self.fuse_clients = self.clients[0:2]
        self.kernel_clients = self.clients[2:4]
        self.result_vals.update({"clients": self.clients})
        self.result_vals.update({"fuse_clients": self.fuse_clients})
        self.result_vals.update({"kernel_clients": self.kernel_clients})
        self.result_vals.update({"mon_node_ip": self.mon_node_ip})
        self.result_vals.update({"mon_node": self.mons})
        self.result_vals.update({"osd_nodes": self.osds})
        self.result_vals.update({"mds_nodes": self.mdss})
        self.result_vals.update({"mgr_nodes": self.mgrs})
        self.result_vals.update({"mounting_dir": self.mounting_dir})

        return self.result_vals, 0

    def auth_list(self, clients, **kwargs):
        mons = self.ceph_cluster.get_ceph_objects("mon")
        self.path = ""
        self.permission = ""
        self.osd_perm = False
        self.mds_perm = False
        self.layout_quota = False
        p_flag = None
        fs_info = self.get_fs_info(mons[0])
        if kwargs:
            for i, j in list(kwargs.items()):
                if i == "path":
                    self.path = j
                if i == "permission":
                    self.permission = j
                if i == "osd":
                    self.osd_perm = True
                if i == "mds":
                    self.mds_perm = True
                if i == "layout_quota":
                    self.layout_quota = True
                    if j == "p_flag":
                        p_flag = "rwp"
                    elif j == "!p_flag":
                        p_flag = "rw"
            for client in clients:
                out, rc = client.exec_command(sudo=True, cmd="ceph auth list")
                out = out.rstrip("\n").split()
                if "client.%s_%s" % (client.node.hostname, self.path) in out:
                    client.exec_command(
                        sudo=True,
                        cmd="ceph auth del client.%s_%s"
                        % (client.node.hostname, self.path),
                    )

            for client in clients:
                log.info("Giving the required permissions for clients from MON node:")
                for mon in mons:
                    if self.mds_perm:
                        mon.exec_command(
                            sudo=True,
                            cmd="ceph auth get-or-create client.%s_%s"
                            " mon 'allow r' mds "
                            "'allow %s path=/%s' osd 'allow "
                            "rw pool=%s'"
                            " -o /etc/ceph/ceph.client.%s_%s.keyring"
                            % (
                                client.node.hostname,
                                self.path,
                                self.permission,
                                self.path,
                                fs_info.get("data_pool_name"),
                                client.node.hostname,
                                self.path,
                            ),
                        )
                    elif self.osd_perm:
                        mon.exec_command(
                            sudo=True,
                            cmd="ceph auth get-or-create client.%s_%s"
                            " mon 'allow r' mds "
                            "'allow r, allow rw  path=/' osd 'allow "
                            "%s pool=%s'"
                            " -o /etc/ceph/ceph.client.%s_%s.keyring"
                            % (
                                client.node.hostname,
                                self.path,
                                self.permission,
                                fs_info.get("data_pool_name"),
                                client.node.hostname,
                                self.path,
                            ),
                        )

                    elif self.layout_quota:
                        mon.exec_command(
                            sudo=True,
                            cmd="ceph auth get-or-create client.%s_%s"
                            " mon 'allow r' mds "
                            "'allow %s' osd 'allow "
                            " rw tag cephfs data=cephfs'"
                            " -o /etc/ceph/ceph.client.%s_%s.keyring"
                            % (
                                client.node.hostname,
                                self.path,
                                p_flag,
                                client.node.hostname,
                                self.path,
                            ),
                        )
                    self.rc_list.append(mon.node.exit_status)
                    keyring, rc = mon.exec_command(
                        sudo=True,
                        cmd="cat /etc/ceph/ceph.client.%s_%s.keyring"
                        % (client.node.hostname, self.path),
                    )
                    self.rc_list.append(mon.node.exit_status)
                    key_file = client.remote_file(
                        sudo=True,
                        file_name="/etc/ceph/ceph.client.%s_%s.keyring"
                        % (client.node.hostname, self.path),
                        file_mode="w",
                    )
                    key_file.write(keyring)
                    key_file.flush()
                    self.rc_list.append(client.node.exit_status)
                    client.exec_command(
                        sudo=True,
                        cmd="chmod 644 /etc/ceph/ceph.client.%s_%s.keyring"
                        % (client.node.hostname, self.path),
                    )
                    self.rc_list.append(client.node.exit_status)
                    rc_set = set(self.rc_list)
                    assert len(rc_set) == 1
                    return 0
        else:
            for client in clients:
                out, rc = client.exec_command(sudo=True, cmd="ceph auth list")
                out = out.rstrip("\n").split()
                if "client.%s" % client.node.hostname in out:
                    client.exec_command(
                        sudo=True, cmd="ceph auth del client.%s" % client.node.hostname
                    )

            for client in clients:
                log.info("Giving required permissions for clients from MON node:")
                for mon in mons:
                    mon.exec_command(
                        sudo=True,
                        cmd="ceph auth get-or-create client.%s"
                        " mon 'allow *' mds "
                        "'allow *, allow * path=/' osd 'allow "
                        "rw pool=%s'"
                        " -o /etc/ceph/ceph.client.%s.keyring"
                        % (
                            client.node.hostname,
                            fs_info.get("data_pool_name"),
                            client.node.hostname,
                        ),
                    )
                    self.rc_list.append(mon.node.exit_status)
                    keyring, rc = mon.exec_command(
                        sudo=True,
                        cmd="cat /etc/ceph/ceph.client.%s.keyring"
                        % client.node.hostname,
                    )
                    self.rc_list.append(mon.node.exit_status)
                    key_file = client.remote_file(
                        sudo=True,
                        file_name="/etc/ceph/ceph.client.%s.keyring"
                        % client.node.hostname,
                        file_mode="w",
                    )
                    key_file.write(keyring)
                    key_file.flush()
                    self.rc_list.append(client.node.exit_status)
                    client.exec_command(
                        sudo=True,
                        cmd="chmod 644 /etc/ceph/ceph.client.%s.keyring"
                        % client.node.hostname,
                    )
                    self.rc_list.append(client.node.exit_status)
                    rc_set = set(self.rc_list)
                    assert len(rc_set) == 1

                    return 0

    def fuse_mount(self, fuse_clients, mounting_dir, **kwargs):
        self.sub_dir = ""
        new_client_hostname = ""
        if kwargs:
            for key, val in list(kwargs.items()):
                if key == "new_client":
                    new_client_hostname = val
                if key == "sub_dir":
                    self.sub_dir = val

            for client in fuse_clients:
                log.info("Creating mounting dir:")
                client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
                log.info(
                    "Mounting fs with ceph-fuse on client %s:" % client.node.hostname
                )
                if self.sub_dir != "":
                    client.exec_command(
                        sudo=True,
                        cmd="ceph-fuse -n client.%s %s -r /%s "
                        % (new_client_hostname, mounting_dir, self.sub_dir),
                    )
                else:
                    client.exec_command(
                        sudo=True,
                        cmd="ceph-fuse -n client.%s %s "
                        % (new_client_hostname, mounting_dir),
                    )
                out, rc = client.exec_command(cmd="mount")
                mount_output = out.split()
                log.info("Checking if fuse mount is is passed of failed:")
                assert mounting_dir.rstrip("/") in mount_output

                return 0
        else:
            for client in fuse_clients:
                try:
                    out, rc = client.exec_command(
                        sudo=True, cmd="mount | grep '/mnt' | awk {'print $3'}"
                    )
                    out = out.rstrip("\n").split()
                    if mounting_dir.rstrip("/") not in out:
                        for op in out:
                            client.exec_command(
                                sudo=True, cmd="rm -rf %s/*" % op, timeout=300
                            )
                            client.exec_command(sudo=True, cmd="umount %s -l" % op)
                            client.exec_command(sudo=True, cmd="rm -rf  %s " % op)
                            client.exec_command(sudo=True, cmd="rm -rf /mnt/*")
                except CommandFailed as e:
                    log.info(e)
                    pass
            for client in fuse_clients:
                log.info("Creating mounting dir:")
                client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
                log.info(
                    "Mounting fs with ceph-fuse on client %s:" % client.node.hostname
                )
                client.exec_command(
                    sudo=True,
                    cmd="ceph-fuse -n client.%s %s"
                    % (client.node.hostname, mounting_dir),
                )
                out, rc = client.exec_command(cmd="mount")
                mount_output = out.rstrip("\n").split()
                log.info("Checking if fuse mount is is passed of failed:")
                assert mounting_dir.rstrip("/") in mount_output
                return 0

    @staticmethod
    def kernel_mount(kernel_clients, mounting_dir, mon_node_ip, **kwargs):
        sub_dir = ""
        new_client_hostname = ""
        if kwargs:
            for key, val in list(kwargs.items()):
                if key == "new_client":
                    new_client_hostname = val
                if key == "sub_dir":
                    sub_dir = val
            for client in kernel_clients:
                if client.pkg_type == "rpm":
                    log.info("Creating mounting dir:")
                    client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
                    secret_key, rc = client.exec_command(
                        sudo=True,
                        cmd="ceph auth get-key client.%s" % new_client_hostname,
                    )
                    key_file = client.remote_file(
                        sudo=True,
                        file_name="/etc/ceph/%s.secret" % new_client_hostname,
                        file_mode="w",
                    )
                    key_file.write(secret_key.rstrip("\n"))
                    key_file.flush()
                    client.exec_command(
                        sudo=True,
                        cmd="mount -t ceph %s,%s,%s:/%s "
                        "%s -o name=%s,secretfile=/etc/ceph/%s.secret"
                        % (
                            mon_node_ip[0],
                            mon_node_ip[1],
                            mon_node_ip[2],
                            sub_dir,
                            mounting_dir,
                            new_client_hostname,
                            new_client_hostname,
                        ),
                    )
                    out, rc = client.exec_command(cmd="mount")
                    mount_output = out.split()
                    log.info("Checking if kernel mount is is passed of failed:")
                    assert mounting_dir.rstrip("/") in mount_output

                    return 0
                else:
                    log.info("Kernel mount is not supported for Ubuntu")
                    return 0

        else:
            for client in kernel_clients:
                try:
                    out, rc = client.exec_command(
                        sudo=True, cmd="mount | grep '/mnt' | awk {'print $3'}"
                    )
                    out = out.rstrip("\n").split()
                    if mounting_dir.rstrip("/") not in out:
                        for op in out:
                            try:
                                client.exec_command(
                                    sudo=True, cmd="rm -rf %s/*" % op, timeout=300
                                )
                            except CommandFailed as e:
                                log.info(e)
                                pass
                            client.exec_command(sudo=True, cmd="umount %s -l" % op)
                            client.exec_command(sudo=True, cmd="rm -rf  %s " % op)
                            client.exec_command(sudo=True, cmd="rm -rf /mnt/*")

                except CommandFailed as e:
                    log.info(e)
                    pass

            for client in kernel_clients:
                if client.pkg_type == "rpm":
                    log.info("Creating mounting dir:")
                    client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
                    secret_key, rc = client.exec_command(
                        sudo=True,
                        cmd="ceph auth get-key client.%s" % client.node.hostname,
                    )
                    key_file = client.remote_file(
                        sudo=True,
                        file_name="/etc/ceph/%s.secret" % client.node.hostname,
                        file_mode="w",
                    )
                    key_file.write(secret_key.rstrip("\n"))
                    key_file.flush()

                    client.exec_command(
                        sudo=True,
                        cmd="mount -t ceph %s,%s,%s:/ "
                        "%s -o name=%s,secretfile=/etc/ceph/%s.secret"
                        % (
                            mon_node_ip[0],
                            mon_node_ip[1],
                            mon_node_ip[2],
                            mounting_dir,
                            client.node.hostname,
                            client.node.hostname,
                        ),
                    )
                    out, rc = client.exec_command(cmd="mount")
                    mount_output = out.rstrip("\n").split()
                    log.info("Checking if kernel mount is is passed of failed:")
                    assert mounting_dir.rstrip("/") in mount_output

                    return 0
                else:
                    log.info("Kernel mount is not supported for Ubuntu")
                    return 0

    @staticmethod
    def nfs_ganesha_install(ceph_demon):
        if ceph_demon.pkg_type == "rpm":
            ceph_demon.exec_command(
                sudo=True, cmd="yum install --nogpgcheck nfs-ganesha-ceph -y"
            )
            ceph_demon.exec_command(sudo=True, cmd="systemctl start rpcbind")
            ceph_demon.exec_command(sudo=True, cmd="systemctl stop nfs-server.service")
            ceph_demon.exec_command(
                sudo=True, cmd="systemctl disable nfs-server.service"
            )
            assert ceph_demon.node.exit_status == 0
        return 0

    @staticmethod
    def nfs_ganesha_conf(node, nfs_client_name):
        out, rc = node.exec_command(
            sudo=True, cmd="ceph auth get-key client.%s" % nfs_client_name
        )
        secret_key = out.rstrip("\n")

        conf = """
    NFS_CORE_PARAM
    {
        Enable_NLM = false;
        Enable_RQUOTA = false;
        Protocols = 4;
    }

    NFSv4
    {
        Delegations = true;
        Minor_Versions = 1, 2;
    }

    CACHEINODE {
        Dir_Max = 1;
        Dir_Chunk = 0;
        Cache_FDs = true;
        NParts = 1;
        Cache_Size = 1;
    }

    EXPORT
    {
        Export_ID=100;
        Protocols = 4;
        Transports = TCP;
        Path = /;
        Pseudo = /ceph/;
        Access_Type = RW;
        Attr_Expiration_Time = 0;
        Delegations = R;
        Squash = "None";

        FSAL {
            Name = CEPH;
            User_Id = "%s";
            Secret_Access_key = "%s";
        }

    }
    CEPH
    {
        Ceph_Conf = /etc/ceph/ceph.conf;
    }
         """ % (
            nfs_client_name,
            secret_key,
        )
        conf_file = node.remote_file(
            sudo=True, file_name="/etc/ganesha/ganesha.conf", file_mode="w"
        )
        conf_file.write(conf)
        conf_file.flush()
        node.exec_command(sudo=True, cmd="systemctl enable nfs-ganesha")
        node.exec_command(sudo=True, cmd="systemctl start nfs-ganesha")
        return 0

    @staticmethod
    def nfs_ganesha_mount(client, mounting_dir, nfs_server):
        if client.pkg_type == "rpm":
            client.exec_command(sudo=True, cmd="yum install nfs-utils -y")
            client.exec_command(sudo=True, cmd="mkdir %s" % mounting_dir)
            client.exec_command(
                sudo=True,
                cmd="mount -t nfs -o nfsvers=4,sync,noauto,soft,proto=tcp %s:/ %s"
                % (nfs_server, mounting_dir),
            )

        return 0

    def read_write_IO(self, clients, mounting_dir, *args, **kwargs):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                log.info("Performing read and write on clients")
                rand_num = random.randint(1, 5)
                fio_read = (
                    "sudo fio --name=global --rw=read --size=%d%s "
                    "--name=%s_%d_%d_%d --directory=%s%s --runtime=300"
                )
                fio_write = (
                    "sudo fio --name=global --rw=write --size=%d%s "
                    "--name=%s_%d_%d_%d --directory=%s%s "
                    "--runtime=300 --verify=meta"
                )
                fio_readwrite = (
                    "sudo fio --name=global --rw=readwrite "
                    "--size=%d%s"
                    " --name=%s_%d_%d_%d --directory=%s%s "
                    "--runtime=300 "
                    "--verify=meta"
                )
                if kwargs:
                    for i, j in list(kwargs.items()):
                        self.dir_name = j
                else:
                    self.dir_name = ""
                if args:
                    if "g" in args:
                        size = "g"
                    elif "m" in args:
                        size = "m"
                    else:
                        size = "k"
                    for arg in args:
                        if arg == "read":
                            if size == "g":
                                rand_size = random.randint(1, 5)
                                client.exec_command(
                                    cmd=fio_read
                                    % (
                                        rand_size,
                                        size,
                                        client.node.hostname,
                                        rand_size,
                                        rand_size,
                                        rand_num,
                                        mounting_dir,
                                        self.dir_name,
                                    ),
                                    long_running=True,
                                    timeout=900,
                                )
                                self.return_counts = self.io_verify(client)
                            elif size == "m":
                                for num in range(0, 10):
                                    rand_size = random.randint(1, 5)
                                    client.exec_command(
                                        cmd=fio_read
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            rand_size,
                                            num,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                            else:
                                for num in range(0, 500):
                                    rand_size = random.randint(50, 100)
                                    client.exec_command(
                                        cmd=fio_read
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            rand_size,
                                            num,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                        elif arg == "write":
                            if size == "g":
                                rand_size = random.randint(1, 5)
                                client.exec_command(
                                    cmd=fio_write
                                    % (
                                        rand_size,
                                        size,
                                        client.node.hostname,
                                        rand_size,
                                        rand_size,
                                        rand_num,
                                        mounting_dir,
                                        self.dir_name,
                                    ),
                                    long_running=True,
                                    timeout=900,
                                )
                                self.return_counts = self.io_verify(client)
                                break

                            elif size == "m":
                                for num in range(0, 10):
                                    rand_size = random.randint(1, 5)
                                    client.exec_command(
                                        cmd=fio_write
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            rand_size,
                                            num,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                            else:
                                for num in range(0, 500):
                                    rand_size = random.randint(50, 100)
                                    client.exec_command(
                                        cmd=fio_write
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            rand_size,
                                            num,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                        elif arg == "readwrite":
                            if size == "g":
                                rand_size = random.randint(1, 5)

                                client.exec_command(
                                    cmd=fio_readwrite
                                    % (
                                        rand_size,
                                        size,
                                        client.node.hostname,
                                        rand_num,
                                        rand_num,
                                        rand_size,
                                        mounting_dir,
                                        self.dir_name,
                                    ),
                                    long_running=True,
                                    timeout=900,
                                )
                                self.return_counts = self.io_verify(client)
                                break

                            elif size == "m":
                                for num in range(0, 10):
                                    rand_size = random.randint(50, 100)
                                    client.exec_command(
                                        cmd=fio_readwrite
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            num,
                                            rand_size,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                            else:
                                for num in range(0, 500):
                                    rand_size = random.randint(50, 100)
                                    client.exec_command(
                                        cmd=fio_readwrite
                                        % (
                                            rand_size,
                                            size,
                                            client.node.hostname,
                                            rand_size,
                                            num,
                                            mounting_dir,
                                            self.dir_name,
                                        ),
                                        long_running=True,
                                        timeout=900,
                                    )
                                    self.return_counts = self.io_verify(client)
                                break

                else:
                    size = "k"
                    for num in range(0, 500):
                        rand_size = random.randint(50, 100)
                        client.exec_command(
                            cmd=fio_readwrite
                            % (
                                rand_size,
                                size,
                                client.node.hostname,
                                rand_size,
                                rand_size,
                                num,
                                mounting_dir,
                                self.dir_name,
                            ),
                            long_running=True,
                            timeout=900,
                        )
                        self.return_counts = self.io_verify(client)
        return self.return_counts, 0

    @staticmethod
    def file_locking(clients, mounting_dir):
        for client in clients:
            to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('%sto_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print("locking file:--------------------------------")
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=%sto_test_file_lock","bs=10M","count=1"])
except IOError as e:
    print(e)
finally:
    print("Unlocking file:------------------------------")
    fcntl.lockf(f,fcntl.LOCK_UN)
                        """ % (
                mounting_dir,
                mounting_dir,
            )
            to_lock_code = client.remote_file(
                sudo=True, file_name="/home/cephuser/file_lock.py", file_mode="w"
            )
            to_lock_code.write(to_lock_file)
            to_lock_code.flush()
            out, rc = client.exec_command(
                sudo=True, cmd="python3 /home/cephuser/file_lock.py"
            )

            if "Errno 11" in out:
                log.info("File locking achieved, data is not corrupted")
            elif "locking" in out:
                log.info("File locking achieved, data is not corrupted")
            else:
                log.error("Data is corrupted")

            md5sum_file_lock, rc = client.exec_command(
                sudo=True,
                cmd="md5sum %sto_test_file_lock | awk '{print $1}'" % mounting_dir,
            )
            return md5sum_file_lock, 0

    def mkdir_bulk(self, clients, range1, range2, mounting_dir, dir_name):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                log.info("Creating Directories")
                client.exec_command(
                    sudo=True,
                    cmd="mkdir %s%s_{%d..%d}"
                    % (mounting_dir, dir_name, range1, range2),
                )
        return 0

    def pinning(self, clients, range1, range2, mounting_dir, dir_name, pin_val):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                for num in range(int(range1), int(range2)):
                    client.exec_command(
                        sudo=True,
                        cmd="setfattr -n ceph.dir.pin -v %s %s%s_%d"
                        % (pin_val, mounting_dir, dir_name, num),
                    )
                return 0

    def mkdir(self, clients, range1, range2, mounting_dir, dir_name):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                for num in range(int(range1), int(range2)):
                    log.info("Creating Directories")
                    out, rc = client.exec_command(
                        sudo=True, cmd="mkdir %s%s_%d" % (mounting_dir, dir_name, num)
                    )
                    print(out)
                    self.dirs, rc = client.exec_command(
                        sudo=True, cmd="ls %s | grep %s" % (mounting_dir, dir_name)
                    )
            break
        return self.dirs, 0

    def activate_multiple_mdss(self, mds_nodes):
        for node in mds_nodes:
            fs_info = self.get_fs_info(node)
            log.info("Activating Multiple MDSs:")
            out, rc = node.exec_command(cmd="ceph -v | awk {'print $3'}")
            if out.startswith("10."):
                log.info("multimds is not supported in 2.x")
                return 0
            else:
                node.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s allow_multimds true "
                    "--yes-i-really-mean-it" % fs_info.get("fs_name"),
                    check_ec=False,
                )
                log.info("Setting max mdss 2:")
                node.exec_command(
                    sudo=True, cmd="ceph fs set %s max_mds 2" % fs_info.get("fs_name")
                )
                return 0

    def allow_dir_fragmentation(self, mds_nodes, fs_name="cephfs"):
        log.info("Allowing directorty fragmenation for splitting and merging")
        for node in mds_nodes:
            node.exec_command(
                sudo=True,
                cmd=f"ceph fs set {fs_name} allow_dirfrags 1",
            )
            break
        return 0

    @staticmethod
    def mds_fail_over(mds_nodes):
        rand = random.randint(0, 1)
        timeout = 120
        timeout = datetime.timedelta(seconds=timeout)
        starttime = datetime.datetime.now()
        for node in mds_nodes:
            while True:
                out, rc = node.exec_command(
                    sudo=True,
                    cmd="ceph mds stat --format=json-pretty | grep active"
                    " | awk {'print $2'}",
                )
                count = out.count("active")
                if count == 2:
                    log.info("Failing MDS %d" % rand)
                    node.exec_command(sudo=True, cmd="ceph mds fail %d" % rand)
                    break
                else:
                    log.info("waiting for active-active mds state")
                    if datetime.datetime.now() - starttime > timeout:
                        log.error("Failed to get active-active mds")
                        return 1
            break
        return 0

    @staticmethod
    def get_active_mdss(mdss):
        active_mds_1_name, active_mds_2_name = str(), str()
        for mds in mdss:
            out, rc = mds.exec_command(
                sudo=True,
                cmd="ceph mds stat | grep -o -P '(?<=0=)." "*(?==up:active,)'",
            )
            active_mds_1_name = out.rstrip("\n")
            out, rc = mds.exec_command(
                sudo=True, cmd="ceph mds stat | grep -o -P '(?<=1=)." "*(?==up:active)'"
            )
            active_mds_2_name = out.rstrip("\n")
            break

        active_mds_1, active_mds_2 = str(), str()
        for mds in mdss:
            node = mds.node
            if node.hostname == active_mds_1_name:
                active_mds_1 = mds
            if node.hostname == active_mds_2_name:
                active_mds_2 = mds
        return active_mds_1, active_mds_2, 0

    @staticmethod
    def get_mds_info(active_mds_node_1, active_mds_node_2, **kwargs):
        for key, val in list(kwargs.items()):
            if val == "get subtrees":
                out_1, err_1 = active_mds_node_1.exec_command(
                    sudo=True,
                    cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s | grep path" % (active_mds_node_1.node.hostname, val),
                )
                out_2, err_2 = active_mds_node_2.exec_command(
                    sudo=True,
                    cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s| grep path" % (active_mds_node_2.node.hostname, val),
                )
                return (
                    out_1.rstrip("\n"),
                    out_2.rstrip("\n"),
                    0,
                )

            elif val == "session ls":
                out_1, err_1 = active_mds_node_1.exec_command(
                    sudo=True,
                    cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s" % (active_mds_node_1.node.hostname, val),
                )
                out_2, err_2 = active_mds_node_2.exec_command(
                    sudo=True,
                    cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                    "asok %s" % (active_mds_node_2.node.hostname, val),
                )
                return (
                    out_1.rstrip("\n"),
                    out_2.rstrip("\n"),
                    0,
                )

    def stress_io(self, clients, mounting_dir, dir_name, range1, range2, **kwargs):
        for client in clients:
            self.num_files = 0
            self.file_size = 0
            self.file_name = ""
            rc = self.check_mount_exists(client)
            if rc == 0:
                if "fnum" in kwargs:
                    self.num_files = kwargs["fnum"]
                    print(self.num_files)
                if "fsize" in kwargs:
                    self.file_size = kwargs["fsize"]
                if "fname" in kwargs:
                    self.file_name = kwargs["fname"]
                for key, val in list(kwargs.items()):
                    if val == "touch":
                        if self.file_name != "":
                            client.exec_command(
                                sudo=True,
                                cmd="touch %s%s/%s"
                                % (mounting_dir, dir_name, self.file_name),
                            )
                        else:
                            client.exec_command(
                                sudo=True,
                                cmd="sh -c 'cd %s%s && sudo touch {%d..%d}.txt'"
                                % (mounting_dir, dir_name, range1, range2),
                            )

                        self.return_counts = self.io_verify(client)
                    elif val == "fio":
                        for num in range(int(range1), int(range2)):
                            rand_num = random.randint(1, 5)
                            client.exec_command(
                                sudo=True,
                                cmd="fio --name=global --rw=write "
                                "--size=%dm --name=%s_%d_%d_%d"
                                " --directory=%s%s "
                                "--runtime=10 --verify=meta"
                                % (
                                    rand_num,
                                    client.node.hostname,
                                    rand_num,
                                    range2,
                                    range1,
                                    mounting_dir,
                                    dir_name,
                                ),
                                long_running=True,
                                timeout=300,
                            )
                            self.return_counts = self.io_verify(client)

                    elif val == "dd":
                        for num in range(int(range1), int(range2)):
                            rand_bs = random.randint(1, 5)
                            rand_count = random.randint(1, 5)
                            client.exec_command(
                                sudo=True,
                                cmd="dd if=/dev/zero "
                                "of=%s%s/%s_%d_%d_%d.txt "
                                "bs=%dM count=%d"
                                % (
                                    mounting_dir,
                                    dir_name,
                                    client.node.hostname,
                                    range1,
                                    num,
                                    rand_bs,
                                    rand_bs,
                                    rand_count,
                                ),
                                long_running=True,
                                timeout=300,
                            )
                            self.return_counts = self.io_verify(client)
                    elif val == "crefi":
                        client.exec_command(
                            sudo=True,
                            cmd="%s %s%s --fop create -t %s "
                            "--multi -b 10 -d 10 -n 10 -T 10 "
                            "--random --min=1K --max=%dK"
                            % (
                                "python3 /home/cephuser/Crefi/crefi.py",
                                mounting_dir,
                                dir_name,
                                "text",
                                5,
                            ),
                            long_running=True,
                            timeout=300,
                        )
                        for i in range(0, 6):
                            ops = [
                                "create",
                                "rename",
                                "chmod",
                                "chown",
                                "chgrp",
                                "setxattr",
                            ]
                            rand_ops = random.choice(ops)
                            ftypes = ["text", "sparse", "binary", "tar"]
                            rand_filetype = random.choice(ftypes)
                            rand_count = random.randint(2, 10)
                            client.exec_command(
                                sudo=True,
                                cmd="%s %s%s --fop %s -t %s "
                                "--multi -b 10 -d 10 -n 10 -T 10 "
                                "--random --min=1K --max=%dK"
                                % (
                                    "python3 /home/cephuser/Crefi/crefi.py",
                                    mounting_dir,
                                    dir_name,
                                    rand_ops,
                                    rand_filetype,
                                    rand_count,
                                ),
                                long_running=True,
                                timeout=300,
                            )
                            self.return_counts = self.io_verify(client)

                    elif val == "smallfile":
                        # Crefi equivalent command for file creation using smallfile tool
                        for i in range(0, 5):
                            ops = ["create", "setxattr", "getxattr", "chmod", "rename"]
                            client.exec_command(
                                sudo=True,
                                cmd="python3 /home/cephuser/smallfile/"
                                "smallfile_cli.py "
                                "--operation %s --threads 10 "
                                "--file-size 4 --files 1000 "
                                "--files-per-dir 10 --dirs-per-dir 2"
                                " --top %s%s" % (ops[i], mounting_dir, dir_name),
                                long_running=True,
                                timeout=300,
                            )
                            self.return_counts = self.io_verify(client)

                    elif val == "smallfile_create":
                        client.exec_command(
                            sudo=True,
                            cmd="python3 /home/cephuser/smallfile/"
                            "smallfile_cli.py --operation create "
                            "--threads 4 --file-size %d --files %d"
                            " --top %s%s "
                            % (self.file_size, self.num_files, mounting_dir, dir_name),
                            long_running=True,
                            timeout=600,
                        )
                        self.return_counts = self.io_verify(client)

                    elif val == "smallfile_rename":
                        client.exec_command(
                            sudo=True,
                            cmd="python3 /home/cephuser/smallfile/"
                            "smallfile_cli.py --operation rename "
                            "--threads 4 --file-size %d --files %d"
                            " --top %s%s"
                            % (self.file_size, self.num_files, mounting_dir, dir_name),
                            long_running=True,
                            timeout=600,
                        )
                        self.return_counts = self.io_verify(client)
                    elif val == "smallfile_delete":
                        client.exec_command(
                            sudo=True,
                            cmd="python3 /home/cephuser/smallfile/"
                            "smallfile_cli.py --operation delete "
                            "--threads 4 --file-size %d --files %d"
                            " --top %s%s "
                            % (self.file_size, self.num_files, mounting_dir, dir_name),
                            long_running=True,
                            timeout=600,
                        )
                        self.return_counts = self.io_verify(client)
                    elif val == "smallfile_delete-renamed":
                        client.exec_command(
                            sudo=True,
                            cmd="python3 /home/cephuser/smallfile/"
                            "smallfile_cli.py "
                            "--operation delete-renamed "
                            "--threads 4 --file-size %d --files %d"
                            " --top %s%s "
                            % (self.file_size, self.num_files, mounting_dir, dir_name),
                            long_running=True,
                            timeout=600,
                        )
                        self.return_counts = self.io_verify(client)

        return self.return_counts, 0

    def max_dir_io(self, clients, mounting_dir, dir_name, range1, range2, num_of_files):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                range_diff = range2 - range1
                total_of_files = range_diff * num_of_files
                client.exec_command(
                    sudo=True,
                    cmd="python3 /home/cephuser/smallfile/smallfile_cli.py "
                    "--operation create --threads 1 "
                    "--file-size 4 --files %d "
                    "--files-per-dir %d --top %s%s"
                    % (total_of_files, total_of_files, mounting_dir, dir_name),
                    timeout=300,
                    long_running=True,
                )
                self.return_counts = self.io_verify(client)
        return self.return_counts, 0

    def pinned_dir_io_mdsfailover(
        self,
        clients,
        mounting_dir,
        dir_name,
        range1,
        range2,
        num_of_files,
        mds_fail_over,
        mds_nodes,
    ):
        log.info("Performing IOs on clients")
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                for num in range(int(range1), int(range2)):
                    working_dir = dir_name + "_" + str(num)
                    out, rc = client.exec_command("sudo ls %s" % (mounting_dir))
                    if working_dir not in out:
                        client.exec_command(
                            cmd="mkdir %s%s_%d" % (mounting_dir, dir_name, num)
                        )
                    log.info("Performing MDS failover:")
                    mds_fail_over(mds_nodes)
                    client.exec_command(
                        sudo=True,
                        cmd="python3 %s --operation create "
                        "--threads 1 --file-size 100 --files %d"
                        " --top %s%s_%d"
                        % (
                            "/home/cephuser/smallfile/smallfile_cli.py",
                            num_of_files,
                            mounting_dir,
                            dir_name,
                            num,
                        ),
                        timeout=300,
                    )
                    self.return_counts = self.io_verify(client)
                break
        return self.return_counts, 0

    def filesystem_utilities(self, clients, mounting_dir, dir_name, range1, range2):
        commands = ["ls", "rm -rf", "ls -l"]
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                for num in range(int(range1), int(range2)):
                    if random.choice(commands) == "ls":
                        out, rc = client.exec_command(
                            sudo=True, cmd="ls %s%s_%d" % (mounting_dir, dir_name, num)
                        )
                    elif random.choice(commands) == "ls -l":
                        out, rc = client.exec_command(
                            sudo=True,
                            cmd="ls -l %s%s_%d/" % (mounting_dir, dir_name, num),
                        )
                    else:
                        out, rc = client.exec_command(
                            sudo=True,
                            cmd="rm -rf %s%s_%d/*" % (mounting_dir, dir_name, num),
                        )
                    print(out)
                    self.return_counts = self.io_verify(client)

            break
        return self.return_counts, 0

    @staticmethod
    def fstab_entry(clients, mounting_dir, **kwargs):
        for key, val in list(kwargs.items()):
            if val == "doEntry":
                for client in clients:
                    out, rc = client.exec_command(cmd="mount")
                    mount_output = out.split()
                    if "fuse" in mount_output:
                        client.exec_command(sudo=True, cmd="cp /etc/fstab /etc/fstab1")
                        out, rc = client.exec_command(sudo=True, cmd="cat /etc/fstab")
                        fuse_fstab = """
{old_entry}
#DEVICE         PATH                 TYPE           OPTIONS
none           {mounting_dir}       {fuse}          ceph.id={client_hostname},\
ceph.conf=/etc/ceph/ceph.conf,_netdev,defaults  0 0
                                """.format(
                            old_entry=out,
                            fuse="fuse.ceph",
                            mounting_dir=mounting_dir,
                            client_hostname=client.node.hostname,
                        )
                        fstab = client.remote_file(
                            sudo=True, file_name="/etc/fstab", file_mode="w"
                        )
                        fstab.write(fuse_fstab)
                        fstab.flush()
                        return 0
                    else:
                        client.exec_command(
                            sudo=True,
                            cmd="ceph auth get-key client.%s" % client.node.hostname,
                        )
                        client.exec_command(sudo=True, cmd="cp /etc/fstab /etc/fstab1")
                        out, rc = client.exec_command(sudo=True, cmd="cat /etc/fstab")
                    mon_node_ip = str()
                    if kwargs:
                        for key1, val1 in list(kwargs.items()):
                            if key1 == "mon_node_ip":
                                mon_node_ip = val1

                        kernel_fstab = """
{old_entry}
#DEVICE              PATH                TYPE          OPTIONS
{mon_ip1},{mon_ip2},{mon_ip2}:/      {mounting_dir}      {ceph}        name={client_hostname},\
secretfile={secret_key},_netdev,noatime 00
                                """.format(
                            old_entry=out,
                            ceph="ceph",
                            mon_ip1=mon_node_ip[0],
                            mon_ip2=mon_node_ip[1],
                            mon_ip3=mon_node_ip[2],
                            mounting_dir=mounting_dir,
                            client_hostname=client.node.hostname,
                            secret_key="/etc/ceph/%s.secret" % client.node.hostname,
                        )
                        fstab = client.remote_file(
                            sudo=True, file_name="/etc/fstab", file_mode="w"
                        )
                        fstab.write(kernel_fstab)
                        fstab.flush()
            elif val == "revertEntry":
                for client in clients:
                    client.exec_command(sudo=True, cmd="mv /etc/fstab1 /etc/fstab")
        return 0

    @staticmethod
    def osd_flag(mon, flag, action):
        if action == "set":
            mon.exec_command(
                sudo=True, cmd="ceph osd %s %s --yes-i-really-mean-it" % (action, flag)
            )

        mon.exec_command(sudo=True, cmd="ceph osd %s %s" % (action, flag))
        return 0

    @staticmethod
    def network_disconnect(ceph_object):
        script = """
import time,os
os.system('sudo systemctl stop network')
time.sleep(20)
os.system('sudo systemctl start  network')
"""
        node = ceph_object.node
        nw_disconnect = node.remote_file(
            sudo=True, file_name="/home/cephuser/nw_disconnect.py", file_mode="w"
        )
        nw_disconnect.write(script)
        nw_disconnect.flush()

        log.info("Stopping the network..")
        node.exec_command(sudo=True, cmd="yum install -y python3")
        node.exec_command(sudo=True, cmd="python3 /home/cephuser/nw_disconnect.py")
        log.info("Starting the network..")
        return 0

    def reboot_node(self, ceph_demon):
        node = ceph_demon.node
        timeout = 600
        node.exec_command(sudo=True, cmd="reboot", check_ec=False)
        self.return_counts.update({node.hostname: node.exit_status})
        timeout = datetime.timedelta(seconds=timeout)
        starttime = datetime.datetime.now()
        while True:
            try:
                node.reconnect()
                break
            except BaseException:
                if datetime.datetime.now() - starttime > timeout:
                    log.error(
                        "Failed to reconnect to the"
                        " node {node} after"
                        "reboot ".format(node=node.ip_address)
                    )
                    time.sleep(5)
                    log.error(
                        "Failed to reconnect to the node "
                        "{node} after reboot ".format(node=node.ip_address)
                    )
                    return 1

    def reboot(self, ceph_daemon):
        if ceph_daemon.role == "client":
            rc = self.check_mount_exists(ceph_daemon)
            if rc == 0:
                self.reboot_node(ceph_daemon)
        elif ceph_daemon.role == "mds":
            out, rc = ceph_daemon.exec_command(sudo=True, cmd="ceph -s")
            if ceph_daemon.node.hostname in out:
                self.reboot_node(ceph_daemon)

        else:
            self.reboot_node(ceph_daemon)

        return 0

    @staticmethod
    def daemon_systemctl(ceph_daemon, daemon_name, op):
        if ceph_daemon.role == "mds" and op == "active_mds_restart":
            try:
                out, rc = ceph_daemon.exec_command(sudo=True, cmd="ceph -s")
                out = out.rstrip("\n")
                if ceph_daemon.node.hostname in out:
                    ceph_daemon.node.exec_command(
                        sudo=True,
                        cmd="systemctl restart ceph-%s@%s.service"
                        % ("mds", ceph_daemon.node.hostname),
                    )
            except CommandFailed:
                ceph_daemon.node.exec_command(
                    sudo=True,
                    cmd="systemctl reset-failed ceph-%s@%s.service"
                    % ("mds", ceph_daemon.node.hostname),
                )
                ceph_daemon.node.exec_command(
                    sudo=True,
                    cmd="systemctl start ceph-%s@%s.service"
                    % ("mds", ceph_daemon.node.hostname),
                )
        else:
            try:
                ceph_daemon.node.exec_command(
                    sudo=True,
                    cmd="systemctl %s ceph-%s@%s.service"
                    % (op, daemon_name, ceph_daemon.node.hostname),
                )
            except CommandFailed:
                ceph_daemon.node.exec_command(
                    sudo=True,
                    cmd="systemctl reset-failed ceph-%s@%s.service"
                    % (daemon_name, ceph_daemon.node.hostname),
                )
                ceph_daemon.node.exec_command(
                    sudo=True,
                    cmd="systemctl start ceph-%s@%s.service"
                    % (daemon_name, ceph_daemon.node.hostname),
                )

    def standby_rank(self, mds_nodes, mon_nodes, **kwargs):
        host_names = []
        for mds in mds_nodes:
            host_names.append(mds.node.hostname)

        standby_rank = """
[mds.%s]
mds standby replay = true
mds standby for rank = 0
[mds.%s]
mds standby replay = true
mds standby for rank = 1
[mds.%s]
mds standby replay = true
mds standby for rank = 0
[mds.%s]
mds standby replay = true
mds standby for rank = 1
""" % (
            host_names[0],
            host_names[1],
            host_names[2],
            host_names[3],
        )
        """
        for mds nodes
        """
        for key, val in list(kwargs.items()):
            if val == "add_rank":
                for mds in mds_nodes:
                    mds.exec_command(
                        sudo=True, cmd="cp /etc/ceph/ceph.conf" " /etc/ceph/ceph1.conf"
                    )
                    mds_conf_file, rc = mds.exec_command(
                        sudo=True, cmd="cat /etc/ceph/ceph.conf"
                    )
                    key_file = mds.remote_file(
                        sudo=True, file_name="/etc/ceph/ceph.conf", file_mode="w"
                    )
                    key_file.write(mds_conf_file)
                    key_file.write(standby_rank)
                    key_file.flush()
                    self.daemon_systemctl(mds, "mds", "restart")
                time.sleep(50)
                """
                for mon node
                """
                for mon in mon_nodes:
                    mon.exec_command(
                        sudo=True, cmd="cp /etc/ceph/ceph.conf " "/etc/ceph/ceph1.conf"
                    )
                    mon_conf_file, rc = mon.exec_command(
                        sudo=True, cmd="cat /etc/ceph/ceph.conf"
                    )
                    key_file = mon.remote_file(
                        sudo=True, file_name="/etc/ceph/ceph.conf", file_mode="w"
                    )
                    key_file.write(mon_conf_file)
                    key_file.write(standby_rank)
                    key_file.flush()
                    self.daemon_systemctl(mon, "mon", "restart")

            elif val == "add_rank_revert":
                for mds in mds_nodes:
                    mds.exec_command(
                        sudo=True, cmd="mv /etc/ceph/ceph1.conf" " /etc/ceph/ceph.conf"
                    )
                    self.daemon_systemctl(mds, "mds", "restart")
                time.sleep(50)

                for mon in mon_nodes:
                    mon.exec_command(
                        sudo=True, cmd="mv /etc/ceph/ceph1.conf " "/etc/ceph/ceph.conf"
                    )
                    self.daemon_systemctl(mon, "mon", "restart")
        return 0

    @staticmethod
    def pid_kill(node, daemon):
        out, rc = node.exec_command(cmd="pgrep %s " % daemon, container_exec=False)
        out = out.split("\n")
        out.pop()
        for pid in out:
            node.exec_command(sudo=True, cmd="kill -9 %s" % pid, container_exec=False)
            time.sleep(10)
        return 0

    def check_mount_exists(self, client):
        out, rc = client.exec_command(cmd="mount")
        mount_output = out.split()
        if self.result_vals["mounting_dir"].rstrip("/") in mount_output:
            log.info("Mount exists : {}".format(mount_output))
            return 0
        else:
            log.info("Mount does not exists")
            return 1

    def io_verify(self, client):
        if client.node.exit_status == 0:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            log.info("Client IO is going on,success")
        else:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            print("------------------------------------")
            print(self.return_counts)
            print("------------------------------------")
            log.error("Client IO got interrupted")
        return self.return_counts

    @staticmethod
    def rc_verify(tc, return_counts):
        return_codes_set = set(return_counts.values())
        if len(return_codes_set) == 1:
            out = "Test case %s Passed" % tc
            if tc == "":
                output = "Data validation success"
                return output
            else:
                return out

        else:
            return 1

    @staticmethod
    def get_fs_info(mon):
        out, rc = mon.exec_command(cmd=" sudo ceph fs ls | awk {' print $2'} ")
        fs_name = out.rstrip().strip(",")
        out, rc = mon.exec_command(cmd=" sudo ceph fs ls | awk {' print $5'} ")
        metadata_pool_name = out.rstrip().strip(",")
        out, rc = mon.exec_command(cmd=" sudo ceph fs ls | awk {' print $8'} ")
        data_pool_name = out.rstrip().strip("[")
        output_dict = {
            "fs_name": fs_name,
            "metadata_pool_name": metadata_pool_name,
            "data_pool_name": data_pool_name,
        }
        return output_dict

    def del_cephfs(self, mds_nodes, fs_name):
        for mds in mds_nodes:
            if mds.containerized:
                mds.node.exec_command(
                    sudo=True,
                    cmd="systemctl stop ceph-mds@{hostname}".format(
                        hostname=mds.node.hostname
                    ),
                )
            else:
                mds.node.exec_command(sudo=True, cmd="systemctl stop ceph-mds.target")
            self.clients[0].exec_command(sudo=True, cmd="ceph mds fail 0")
        log.info("sleeping for 50sec")
        time.sleep(50)
        for _ in mds_nodes:
            log.info("Deleting fs:")
            try:
                self.clients[0].exec_command(
                    sudo=True, cmd="ceph fs rm %s --yes-i-really-mean-it" % fs_name
                )
                return self.return_counts, 0
            except CommandFailed:
                self.clients[0].exec_command(sudo=True, cmd="ceph mds fail 0")
                time.sleep(30)

    def create_fs(self, mds_nodes, fs_name, data_pool, metadata_pool, **kwargs):
        self.clients[0].exec_command("sudo ceph fs flag set enable_multiple true")
        if kwargs:
            for k, v in list(kwargs.items()):
                if v == "erasure_pool":
                    for mds in mds_nodes:
                        log.info("starting mds service on %s" % mds.node.hostname)
                        self.daemon_systemctl(mds, "mds", "start")
                        log.info("started  mds service on %s" % mds.node.hostname)
            for mds in mds_nodes:
                mds.exec_command(
                    sudo=True,
                    cmd="ceph fs new %s %s %s --force "
                    "--allow-dangerous-metadata-overlay"
                    % (fs_name, metadata_pool, data_pool),
                )
                break
            log.info("sleeping for 50sec")
            time.sleep(50)

            for mds in mds_nodes:
                out, rc = mds.exec_command(sudo=True, cmd="ceph fs ls")
                if fs_name in out:
                    log.info("New cephfs created")
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    return self.return_counts, 0
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    return self.return_counts, 0

        else:
            for mds in mds_nodes:
                log.info("starting mds service on %s" % mds.node.hostname)
                self.daemon_systemctl(mds, "mds", "start")
                log.info("started  mds service on %s" % mds.node.hostname)

            for mds in mds_nodes:
                mds.exec_command(
                    sudo=True,
                    cmd="ceph fs new %s %s %s --force "
                    "--allow-dangerous-metadata-overlay"
                    % (fs_name, metadata_pool, data_pool),
                )
                break
            for mds in mds_nodes:
                out, rc = mds.exec_command(sudo=True, cmd="ceph fs ls")
                if fs_name in out:
                    log.info("New cephfs created")
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    return self.return_counts, 0

    @staticmethod
    def create_pool(mon_node, pool_name, pg, pgp, **kwargs):
        if kwargs:
            mon_node.exec_command(
                sudo=True,
                cmd="ceph osd pool create %s %s %s %s %s"
                % (
                    pool_name,
                    pg,
                    pgp,
                    kwargs.get("pool_type"),
                    kwargs.get("profile_name"),
                ),
            )
            mon_node.exec_command(
                sudo=True,
                cmd="ceph osd pool set %s allow_ec_overwrites true" % pool_name,
            )
        else:
            mon_node.exec_command(
                sudo=True, cmd="ceph osd pool create %s %s %s" % (pool_name, pg, pgp)
            )

    @staticmethod
    def create_erasure_profile(mon_node, profile_name, k, m):
        mon_node.exec_command(
            sudo=True,
            cmd="ceph osd erasure-code-profile set %s k=%s m=%s" % (profile_name, k, m),
        )
        return profile_name

    def add_pool_to_fs(self, mon, fs_name, pool_name):
        mon.exec_command(
            sudo=True, cmd="ceph fs add_data_pool %s  %s" % (fs_name, pool_name)
        )
        out, rc = mon.exec_command(sudo=True, cmd="ceph fs ls")
        output = out.split()
        if pool_name in output:
            log.info("adding new pool to cephfs successfull")
            self.return_counts.update({mon.node.hostname: mon.node.exit_status})
            return self.return_counts, 0

    def remove_pool_from_fs(self, ceph_object, fs_name, pool_name):
        ceph_object.exec_command(
            sudo=True, cmd="ceph fs rm_data_pool %s %s" % (fs_name, pool_name)
        )
        out, rc = ceph_object.exec_command(sudo=True, cmd="ceph fs ls")
        output = out.split()
        if pool_name not in output:
            log.info("removing pool %s to cephfs successfull" % pool_name)
            self.return_counts.update(
                {ceph_object.node.hostname: ceph_object.node.exit_status}
            )
        return self.return_counts, 0

    def set_attr(self, mdss, fs_name):
        max_file_size = "1099511627776"
        for mds in mdss:
            attrs = [
                "max_mds",
                "max_file_size",
                "allow_new_snaps",
                "inline_data",
                "cluster_down",
                "allow_multimds",
                "allow_dirfrags",
                "balancer",
                "standby_count_wanted",
            ]
            if attrs[0]:
                mds.exec_command(
                    sudo=True, cmd="ceph fs set  %s %s 2" % (fs_name, attrs[0])
                )
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[0])
                )
                out = out.rstrip().replace("\t", "")
                if "max_mds2" in out:
                    log.info("max mds attr passed")
                    log.info("Reverting:")
                    mds.exec_command(
                        sudo=True, cmd="ceph fs set  %s %s 1" % (fs_name, attrs[0])
                    )
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[0])
                    )
                    out = out.rstrip().replace("\t", "")
                    if "max_mds1" in out:
                        log.info("Setting max mds to 1")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("Setting max mds attr failed")
                        return self.return_counts, 1
            if attrs[1]:
                mds.exec_command(
                    sudo=True, cmd="ceph fs set  %s %s 65536" % (fs_name, attrs[1])
                )
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[1])
                )
                out = out.rstrip()
                print(out)
                if "max_file_size	65536" in out:
                    log.info("max file size attr tested successfully")
                    log.info("Reverting:")
                    mds.exec_command(
                        sudo=True,
                        cmd="ceph fs set  %s %s %s"
                        % (fs_name, attrs[1], max_file_size),
                    )
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[1])
                    )
                    if max_file_size in out:
                        log.info("max file size attr reverted successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("max file size attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    return self.return_counts, 1

            if attrs[2]:
                out, rc = mds.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s %s 1 --yes-i-really-mean-it"
                    % (fs_name, attrs[2]),
                )
                if "enabled new snapshots" in rc:
                    log.info("allow new snap flag is set successfully")
                    log.info("Reverting:")
                    out, rc = mds.exec_command(
                        sudo=True,
                        cmd="ceph fs set %s %s 0 --yes-i-really-mean-it"
                        % (fs_name, attrs[2]),
                    )
                    if "disabled new snapshots" in rc:
                        print(out)
                        log.info("Reverted allow_new_snaps successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("failed to revert new snap shots attr")
                        return self.return_counts, 1
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("failed to enable new snap shots")
                    return self.return_counts, 1

            if attrs[3]:
                out, rc = mds.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s %s 1 --yes-i-really-mean-it"
                    % (fs_name, attrs[3]),
                )
                if "inline data enabled" in rc:
                    log.info("inline data set successfully")
                    log.info("Reverting:")
                    out, rc = mds.exec_command(
                        sudo=True,
                        cmd="ceph fs set %s %s 0 --yes-i-really-mean-it"
                        % (fs_name, attrs[3]),
                    )
                    if "inline data disabled" in rc:
                        log.info("inline data disabled successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )

                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("inline data attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("inline data attr failed")
                    return self.return_counts, 1

            if attrs[4]:
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs set %s %s 1" % (fs_name, attrs[4])
                )
                if "marked down" in rc:
                    log.info("cluster_down attr set successfully")
                    log.info("Reverting:")
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs set %s %s 0" % (fs_name, attrs[4])
                    )
                    if "marked up" in rc:
                        log.info("cluster_down attr reverted successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("cluster_down attr set failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("cluster_down attr set failed")
                    return self.return_counts, 1

            if attrs[5]:
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs set %s  %s 1" % (fs_name, attrs[5])
                )
                if "enabled creation of more than 1 active MDS" in rc:
                    log.info("allow_multimds attr set successfully")
                    log.info("Reverting:")
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs set %s %s 0" % (fs_name, attrs[5])
                    )
                    if "disallowed increasing the cluster size past " "1" in rc:
                        log.info("allow_multimds attr reverted successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )

                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("allow_multimds attr failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("allow_multimds attr failed")
                    return self.return_counts, 1

            if attrs[6]:
                log.info(
                    "Allowing directory fragmentation for splitting " "and merging"
                )
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs set %s  %s 1" % (fs_name, attrs[6])
                )
                if "enabled directory fragmentation" in rc:
                    log.info("directory fragmentation enabled successfully")
                    log.info("disabling directory fragmentation")
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs set %s %s 0" % (fs_name, attrs[6])
                    )
                    if "disallowed new directory fragmentation" in rc:
                        log.info("directory fragmentation disabled successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )

                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("allow_dirfrags set attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("allow_dirfrags set attr failed")
                    return self.return_counts, 1

            if attrs[7]:
                log.info("setting the metadata load balancer")
                mds.exec_command(
                    sudo=True, cmd="ceph fs set %s  %s 2" % (fs_name, attrs[7])
                )
                out, rc = mds.exec_command(
                    sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[7])
                )
                out = out.rstrip()
                if "balancer	2" in out:
                    log.info("metadata load balancer attr set successfully ")
                    log.info("reverting:")
                    mds.exec_command(
                        sudo=True, cmd="ceph fs set %s %s 1" % (fs_name, attrs[7])
                    )
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs get %s| grep %s" % (fs_name, attrs[7])
                    )
                    out = out.rstrip()

                    if "balancer	1" in out:
                        log.info(
                            "metadata load balancer attr reverted " "successfully "
                        )
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("metadata load balancer attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("metadata load balancer attr failed")
                    return self.return_counts, 1

            if attrs[8]:
                log.info("setting standby_count_wanted")
                mds.exec_command(
                    sudo=True, cmd="ceph fs set %s %s 2" % (fs_name, attrs[8])
                )
                out, rc = mds.exec_command(sudo=True, cmd="ceph fs get %s" % fs_name)
                out = out.rstrip()
                if "standby_count_wanted	2" in out:
                    log.info("standby_count_wanted attr set successfully")
                    log.info("Reverting:")
                    mds.exec_command(
                        sudo=True, cmd="ceph fs set %s %s 1" % (fs_name, attrs[8])
                    )
                    out, rc = mds.exec_command(
                        sudo=True, cmd="ceph fs get %s" % fs_name
                    )
                    out = out.rstrip()
                    if "standby_count_wanted	1" in out:
                        log.info("standby_count_wanted attr reverted successfully")
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                    else:
                        self.return_counts.update(
                            {mds.node.hostname: mds.node.exit_status}
                        )
                        print(self.return_counts)
                        log.error("standby_count_wanted setting failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update({mds.node.hostname: mds.node.exit_status})
                    print(self.return_counts)
                    log.error("standby_count_wanted setting failed")
                    return self.return_counts, 1

            return self.return_counts, 0

    @staticmethod
    def heartbeat_map(mds):
        try:
            mds.exec_command(
                sudo=True,
                cmd="cat /var/log/ceph/ceph-mds.%s.log "
                "| grep heartbeat_map" % mds.node.hostname,
            )
            return 1
        except CommandFailed as e:
            log.info(e)
            return 0

    def rsync(self, clients, source_dir, dest_dir):
        for client in clients:
            rc = self.check_mount_exists(client)
            if rc == 0:
                client.exec_command(
                    sudo=True, cmd="rsync -zvh %s %s" % (source_dir, dest_dir)
                )
                if client.node.exit_status == 0:
                    log.info("Files synced successfully")
                else:
                    raise CommandFailed("File sync failed")
                break
        return self.return_counts, 0

    @staticmethod
    def auto_evict(active_mds_node, clients, rank):
        grep_pid_cmd = """sudo ceph tell mds.%d client ls | grep '"pid":'"""
        out, rc = active_mds_node.exec_command(cmd=grep_pid_cmd % rank)
        client_pid = re.findall(r"\d+", out)
        while True:
            for client in clients:
                try:
                    for pid in client_pid:
                        client.exec_command(
                            sudo=True, cmd="kill -9 %s" % pid, container_exec=False
                        )
                        return 0
                except Exception as e:
                    print(e)
                    pass

    @staticmethod
    def manual_evict(active_mds_node, rank):
        grep_cmd = f""" sudo ceph tell mds.{rank} client ls | grep '"id":'"""
        out, rc = active_mds_node.exec_command(cmd=grep_cmd)
        client_ids = re.findall(r"\d+", out)
        grep_cmd = f""" sudo ceph tell mds.{rank} client ls | grep '"inst":'"""
        log.info("Getting IP address of Evicted client")
        out, rc = active_mds_node.exec_command(cmd=grep_cmd)
        op = re.findall(r"\d+.+\d+.", out)
        ip_add = op[0]
        ip_add = ip_add.split(" ")
        ip_add = ip_add[1].strip('",')
        for client_id in client_ids:
            active_mds_node.exec_command(
                cmd=f"sudo ceph tell mds.{rank} client evict id={client_id}"
            )
            break

        return ip_add

    @staticmethod
    def osd_blacklist(active_mds_node, ip_add):
        out, rc = active_mds_node.exec_command(sudo=True, cmd="ceph osd blacklist ls")
        if ip_add in out:
            active_mds_node.exec_command(
                sudo=True, cmd="ceph osd blacklist rm %s" % ip_add
            )
            if "listed 0 entries" in out:
                log.info("Evicted client %s unblacklisted successfully" % ip_add)
        return 0

    def config_blacklist_auto_evict(self, active_mds, rank, **kwargs):
        if kwargs:
            active_mds.exec_command(
                sudo=True,
                cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok"
                " config set mds_session_blacklist_on_timeout true"
                % active_mds.node.hostname,
            )
            return 0
        else:
            active_mds.exec_command(
                sudo=True,
                cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok "
                "config set mds_session_blacklist_on_timeout false"
                % active_mds.node.hostname,
            )
            self.auto_evict(active_mds, self.fuse_clients, rank)
            log.info("Waiting 300 seconds for auto eviction---")
            time.sleep(300)
            return 0

    def config_blacklist_manual_evict(self, active_mds, rank, **kwargs):
        if kwargs:
            active_mds.exec_command(
                sudo=True,
                cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok"
                " config set mds_session_blacklist_on_evict true"
                % active_mds.node.hostname,
            )
            return 0
        else:
            active_mds.exec_command(
                sudo=True,
                cmd="ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok "
                "config set mds_session_blacklist_on_evict false"
                % active_mds.node.hostname,
            )
            ip_add = self.manual_evict(active_mds, rank)
            out, rc = active_mds.exec_command(sudo=True, cmd="ceph osd blacklist ls")
            print(out)
            if ip_add not in out:
                return 0

    def getfattr(self, clients, mounting_dir, file_name):
        for client in clients:
            client.exec_command(sudo=True, cmd="touch %s%s" % (mounting_dir, file_name))
            out, rc = client.exec_command(
                sudo=True,
                cmd="getfattr -n ceph.file.layout %s%s" % (mounting_dir, file_name),
            )
            out = out.split()
            out[3] = out[3].strip("ceph.file.layout=")
            self.result_vals.update({"stripe_unit": out[3]})
            self.result_vals.update({"stripe_count": out[4]})
            self.result_vals.update({"object_size": out[5]})
            self.result_vals.update({"pool": out[6]})
            return self.result_vals, 0

    def setfattr(self, clients, ops, val, mounting_dir, file_name):
        if ops == "max_bytes" or ops == "max_files":
            for client in clients:
                rc = self.check_mount_exists(client)
                if rc == 0:
                    client.exec_command(
                        sudo=True,
                        cmd="setfattr -n ceph.quota.%s -v %s %s%s"
                        % (ops, val, mounting_dir, file_name),
                    )
            return 0
        else:
            for client in clients:
                rc = self.check_mount_exists(client)
                if rc == 0:
                    client.exec_command(
                        sudo=True,
                        cmd="setfattr -n ceph.file.layout.%s -v %s %s%s"
                        % (ops, val, mounting_dir, file_name),
                    )
                return 0

    @staticmethod
    def get_osd_count(mon_node):
        out, rc = mon_node.exec_command(
            sudo=True, cmd="ceph -s| grep osds| awk {'print $2'}"
        )
        osd_count = out.rstrip("\n")
        return osd_count

    @staticmethod
    def wait_until_umount_succeeds(client, mount_point, timeout=180, interval=5):
        """
        Checks for the mount point and returns the status based on mount command
        :param client:
        :param mount_point:
        :param timeout:
        :param interval:
        :return: boolean
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info("Wait for the mount to appear")
        while end_time > datetime.datetime.now():
            out, rc = client.exec_command(sudo=True, cmd="mount", check_ec=False)
            mount_output = out.rstrip("\n").split()
            log.info(mount_output)
            log.info("Validate Un mount:")
            if mount_point.rstrip("/") not in mount_output:
                return True
            time.sleep(interval)
        return False

    @staticmethod
    def client_clean_up(fuse_clients, kernel_clients, mounting_dir, *args, **kwargs):
        if kwargs:
            client_name = str()
            for k, v in list(kwargs.items()):
                if k == "client_name":
                    client_name = v
            for client in fuse_clients:
                log.info("Removing files:")
                client.exec_command(
                    sudo=True,
                    cmd="find %s -type f -delete" % mounting_dir,
                    timeout=3600,
                    check_ec=False,
                )
                client.exec_command(
                    sudo=True,
                    cmd="rm -rf %s*" % mounting_dir,
                    timeout=3600,
                    check_ec=False,
                )
                if args:
                    if "umount" in args:
                        log.info("Unmounting fuse client:")
                        client.exec_command(
                            sudo=True,
                            cmd="fusermount -u %s -z" % mounting_dir,
                            check_ec=False,
                        )
                        FsUtils.wait_until_umount_succeeds(client, mounting_dir)
                        log.info("Removing mounting directory:")
                        client.exec_command(sudo=True, cmd="rmdir %s" % mounting_dir)
                        log.info("Removing keyring file:")
                        client.exec_command(
                            sudo=True,
                            cmd="rm -rf /etc/ceph/ceph.client.%s.keyring" % client_name,
                            check_ec=False,
                        )
                        log.info("Removing permissions:")
                        client.exec_command(
                            sudo=True, cmd="ceph auth del client.%s" % client_name
                        )
                        client.exec_command(
                            cmd="find /home/cephuser -type f -not -name 'authorized_keys' "
                            " -name 'Crefi' -name 'smallfile' -delete",
                            timeout=3600,
                            check_ec=False,
                        )
                        client.exec_command(
                            cmd="cd /home/cephuser && ls -a | grep -v 'authorized_keys' |"
                            "xargs sudo rm -f",
                            timeout=3600,
                            check_ec=False,
                        )
                        client.exec_command(
                            sudo=True, cmd="iptables -F", check_ec=False
                        )
            for client in kernel_clients:
                if client.pkg_type == "deb":
                    pass
                else:
                    log.info("Removing files:")
                    client.exec_command(
                        sudo=True,
                        cmd="find %s -type f -delete" % mounting_dir,
                        timeout=3600,
                        check_ec=False,
                    )
                    client.exec_command(
                        sudo=True,
                        cmd="rm -rf %s*" % mounting_dir,
                        timeout=3600,
                        check_ec=False,
                    )
                    if args:
                        if "umount" in args:
                            log.info("Unmounting kernel client:")
                            client.exec_command(
                                sudo=True, cmd="umount %s -l" % mounting_dir
                            )
                            FsUtils.wait_until_umount_succeeds(client, mounting_dir)
                            client.exec_command(
                                sudo=True, cmd="rmdir %s" % mounting_dir
                            )
                            log.info("Removing keyring file:")
                            client.exec_command(
                                sudo=True,
                                cmd="rm -rf /etc/ceph/ceph.client.%s.keyring"
                                % client_name,
                            )
                            log.info("Removing permissions:")
                            client.exec_command(
                                sudo=True, cmd="ceph auth del client.%s" % client_name
                            )
                            client.exec_command(
                                cmd="find /home/cephuser -type f -not -name 'authorized_keys' "
                                " -name 'Crefi' -name 'smallfile' -delete",
                                timeout=3600,
                                check_ec=False,
                            )
                            client.exec_command(
                                cmd="cd /home/cephuser && ls -a | grep -v 'authorized_keys' | "
                                "xargs sudo rm -f",
                                timeout=3600,
                                check_ec=False,
                            )

        else:
            for client in fuse_clients:
                log.info("Removing files:")
                client.exec_command(
                    sudo=True,
                    cmd="find %s -type f -delete" % mounting_dir,
                    timeout=3600,
                    check_ec=False,
                )
                client.exec_command(
                    sudo=True,
                    cmd="rm -rf %s*" % mounting_dir,
                    timeout=3600,
                    check_ec=False,
                )
                if args:
                    if "umount" in args:
                        log.info("Unmounting fuse client:")
                        client.exec_command(
                            sudo=True, cmd="fusermount -u %s -z" % mounting_dir
                        )
                        log.info("Removing mounting directory:")
                        client.exec_command(sudo=True, cmd="rmdir %s" % mounting_dir)
                        log.info("Removing keyring file:")
                        client.exec_command(
                            sudo=True,
                            cmd="rm -rf /etc/ceph/ceph.client.%s.keyring"
                            % client.node.hostname,
                        )
                        log.info("Removing permissions:")
                        client.exec_command(
                            sudo=True,
                            cmd="ceph auth del client.%s" % client.node.hostname,
                        )
                        client.exec_command(
                            cmd="find /home/cephuser -type f -not -name 'authorized_keys' "
                            "-name 'Crefi' -name 'smallfile' -delete",
                            timeout=3600,
                        )
                        client.exec_command(
                            cmd="cd /home/cephuser && ls -a | grep -v 'authorized_keys' | "
                            "xargs sudo rm -f",
                            timeout=3600,
                            check_ec=False,
                        )
                        client.exec_command(
                            sudo=True, cmd="iptables -F", check_ec=False
                        )
            for client in kernel_clients:
                if client.pkg_type == "deb":
                    pass
                else:
                    log.info("Removing files:")
                    client.exec_command(
                        sudo=True,
                        cmd="find %s -type f -delete" % mounting_dir,
                        timeout=3600,
                        check_ec=False,
                    )
                    client.exec_command(
                        sudo=True,
                        cmd="rm -rf %s*" % mounting_dir,
                        timeout=3600,
                        check_ec=False,
                    )
                    if args:
                        if "umount" in args:
                            log.info("Unmounting kernel client:")
                            client.exec_command(
                                sudo=True, cmd="umount %s -l" % mounting_dir
                            )
                            client.exec_command(
                                sudo=True, cmd="rmdir %s" % mounting_dir
                            )
                            log.info("Removing keyring file:")
                            client.exec_command(
                                sudo=True,
                                cmd="rm -rf "
                                "/etc/ceph/ceph.client.%s.keyring"
                                % client.node.hostname,
                            )
                            log.info("Removing permissions:")
                            client.exec_command(
                                sudo=True,
                                cmd="ceph auth del client.%s" % client.node.hostname,
                            )
        return 0

    def mds_cleanup(self, mds_nodes, dir_fragmentation):
        log.info("Deactivating Multiple MDSs")
        for node in mds_nodes:
            fs_info = self.get_fs_info(node)
            log.info("Deactivating Multiple MDSs")
            node.exec_command(
                sudo=True,
                cmd="ceph fs set %s allow_multimds false "
                "--yes-i-really-mean-it" % fs_info.get("fs_name"),
                check_ec=False,
            )
            log.info("Setting Max mds to 1:")
            node.exec_command(
                sudo=True, cmd="ceph fs set %s max_mds 1" % fs_info.get("fs_name")
            )
            if dir_fragmentation is not None:
                log.info("Disabling directory fragmentation")
                node.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s allow_dirfrags 0" % fs_info.get("fs_name"),
                )
            break
        time.sleep(120)
        return 0

    def create_file_data(self, client, directory, no_of_files, file_name, data):
        """
        This function will write files to the directory with the data given
        :param client:
        :param directory:
        :param no_of_files:
        :param file_name:
        :param data:
        :return:
        """
        files = [f"{file_name}_{i}" for i in range(0, no_of_files)]
        client.exec_command(
            sudo=True,
            cmd=f"cd {directory};echo {data * random.randint(100, 500)} | tee {' '.join(files)}",
        )

    def get_files_and_checksum(self, client, directory):
        """
        This will collect the filenames and their respective checksums and returns the dictionary
        :param client:
        :param directory:
        :return:
        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"cd {directory};ls -lrt |  awk {{'print $9'}}"
        )
        file_list = out.strip().split()
        file_dict = {}
        for file in file_list:
            out, rc = client.exec_command(sudo=True, cmd=f"md5sum {directory}/{file}")
            md5sum = out.strip().split()
            file_dict[file] = md5sum[0]
        return file_dict
