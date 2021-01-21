"""
Ceph Administrator
"""
import re
import json
import logging

from utility.utils import get_cephci_config
from ceph.ceph import CommandFailed
from time import sleep

logger = logging.getLogger(__name__)


class CephAdmin:

    TIMEOUT = 300
    ROLES = ["mon", "mgr", "osd", "mds", "rgw", "iscsi", "client"]
    BOOT_DISK = "findmnt -v -n -T / -o SOURCE "
    GET_DISKS = "lsblk -np -r -o {} "

    def __init__(self, name, ceph_cluster, ceph_installer, **kwargs):
        """
        Initialize Cephadm with ceph_cluster object

        Args:
            name: cluster name
            ceph_cluster: Ceph cluster object
            ceph_installer: Ceph Installer node
            kwargs: Key-value arguments (test configuration)

        kwargs:
            container_image: container_image_url
            registry: boolean # registry support for default image
        """
        self.cluster = ceph_cluster
        self.cluster_name = name
        self.installer = ceph_installer
        self.config = kwargs
        self.first_mon = self.installer.node
        self.first_mgr = self.installer.node
        self.image = kwargs.get("container_image", None)
        self.registry = kwargs.get("registry", None)

    def get_image(self):
        """
        Retrieve ceph container image
         this method able to get image only from bootstrapped cluster

        Returns:
            image: ceph container image
        """
        out, _ = self.installer.exec_command(cmd="sudo cephadm ls")
        out = json.loads(out.read().decode().strip())
        self.image = [
            con["container_image_name"] for con in out if "mon" in con["name"]
        ][-1]
        logger.info("cluster image(default) is been used %s", self.image)
        return self.image

    def image(self):
        """
        Returns cluster container image
        """
        return self.image

    def repo(self):
        """
        Returns ceph compose base URL
        """
        return self.config.get("base_url")

    def check_exist(self, daemon, ids, timeout=None, interval=5):
        """
        Validate daemon existence using daemon ids

        this method uses `ceph orch ls -f json`
        to get JSON formatted output and
        validates provided daemon ids with timeout.

        Args:
            daemon: daemon name
            ids: daemon ids
            timeout: timeout in seconds
            interval: interval in seconds

        Returns:
            boolean
        """
        timeout = self.TIMEOUT if not timeout else timeout
        checks = timeout / interval

        while checks:
            checks -= 1

            out, _ = self.shell(
                remote=self.installer,
                args=["ceph", "orch", "ps", "-f", "json-pretty"],
            )

            out = json.loads(out)
            daemons = dict(
                (i["daemon_id"], i["status_desc"])
                for i in out
                if i.get("daemon_type") == daemon
            )

            count = 0
            for _id in ids:
                if _id in daemons:
                    if daemons[_id] == "running":
                        count += 1

            logger.info("%s/%s %s daemon(s) up...." % (count, len(ids), daemon))
            if count == len(ids):
                return True

            logger.info("re-checking... %s times left", checks)
            sleep(interval)
        return False

    def shell(self, remote, args, **kwargs):
        """
        Ceph orchestrator shell interface to run ceph commands

        Args:
            remote: remote host
            args: list arguments
            kwargs: key-value arguments

        Returns:
            out: stdout
            err: stderr
        """
        if not remote:
            remote = self.installer

        if not self.image:
            self.get_image()

        cmd = ["sudo cephadm -v", "--image {}".format(self.image), "shell -- "] + args
        [cmd.extend([k, v]) for k, v in kwargs]

        out, err = remote.exec_command(
            cmd=" ".join(cmd),
            timeout=self.TIMEOUT,
        )

        out = out.read().decode().strip()
        err = err.read().decode().strip()
        logger.info(out)
        return out, err

    def set_tool_repo(self):
        """
        Enable Ceph tool repo on every cluster node
        """
        for node in self.cluster.get_nodes():
            node.exec_command(
                cmd="sudo yum-config-manager --add"
                " {}compose/Tools/x86_64/os/".format(self.repo())
            )

    def install_cephadm(self, **kw):
        """
        Install cephadm on installer node

        Args:
          kw: key-value configuration

        kw:
          upgrade: boolean # to upgrade cephadm RPM package

        """
        logger.info("Installing cephadm")
        self.installer.exec_command(
            sudo=True,
            cmd="yum install cephadm -y --nogpgcheck",
            long_running=True,
        )

        if kw.get("upgrade"):
            self.installer.exec_command(cmd="sudo yum update metadata")
            self.installer.exec_command(cmd="sudo yum update -y cephadm")

        out, rc = self.installer.exec_command(cmd="rpm -qa | grep cephadm")
        output = out.read().decode().rstrip()
        logger.info("Installed cephadm: %s", output)

    def bootstrap(self):
        """
        Bootstrap the ceph cluster with supported options

        Bootstrap involves,
          - Creates /etc/ceph directory with permissions
          - CLI creation with bootstrap options with custom/default image
          - Execution of bootstrap command

        """
        # Create and set permission to ceph directory
        self.installer.exec_command(cmd="sudo mkdir -p /etc/ceph")
        self.installer.exec_command(cmd="sudo chmod 777 /etc/ceph")

        # Execute bootstrap with MON ip-address
        # Construct bootstrap command
        # 1) Skip default mon, mgr & crash specs
        # 2) Skip automatic dashboard provisioning
        cdn_cred = get_cephci_config().get("cdn_credentials")

        cmd = "sudo cephadm -v "
        if not self.registry and self.image:
            cmd += "--image {image} ".format(image=self.image)

        cmd += (
            "bootstrap "
            "--registry-url registry.redhat.io "
            "--registry-username {user} "
            "--registry-password {password} "
            "--orphan-initial-daemons "
            "--skip-monitoring-stack "
            "--mon-ip {mon_ip}"
        )

        cmd = cmd.format(
            user=cdn_cred.get("username"),
            password=cdn_cred.get("password"),
            mon_ip=self.first_mon.ip_address,
        )

        out, err = self.installer.exec_command(
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out.read().decode())
        logger.error("Bootstrap error: %s", err.read().decode())

        if not self.image:
            self.get_image()

    def manage_hosts(self, **kwargs):
        """
        Manage nodes in ceph cluster using cephadm
        Args:
            kwargs: key-value arguments

        kwargs:
            nodes: node-list # ['node1.obj', 'node2.obj', 'node{n}.obj']
            op: action # ("add"|"remove")

        Raises:
            AssertionError

        Returns:
            None
        """
        op = kwargs.get("op", "add").lower()
        nodes = kwargs.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]

        if op == "add":
            self.installer.distribute_cephadm_gen_pub_key(self.cluster, nodes)
            for node in nodes:
                # Add host
                self.shell(
                    remote=self.installer,
                    args=["ceph", "orch", "host", op, node.shortname],
                )

                # Check host in the orch host list
                out, rc = self.shell(
                    remote=self.installer,
                    args=["ceph", "orch", "host", "ls", "--format=json"],
                )

                j = [i["hostname"] for i in json.loads(out)]
                assert node.shortname in j
        elif op == "remove":
            # todo: add code for removing host
            raise NotImplementedError

    def add_daemons(self):
        """
        Add all daemons from cluster configuration
        one method to deploy all daemons

        Adds service daemon(s) using roles defined in configuration
          - collects nodes based on the role
          - calls respective method to deploy services
          - service/daemons deployed
               (MON, MGR, OSD, MDS, ISCSI, RGW, Clients)
          - deploys dashboard services
               (prometheus, node-exporter, alert-manager, grafana)

        Returns:
            None
        """
        self.ceph_mon(self.cluster.get_nodes(role="mon"))
        self.ceph_mgr(self.cluster.get_nodes(role="mgr"))
        self.ceph_osd(self.cluster.get_nodes(role="osd"))
        self.ceph_mds(self.cluster.get_nodes(role="mds"))
        self.ceph_iscsi(self.cluster.get_nodes(role="iscsi"))
        self.ceph_client(self.cluster.get_nodes(role="client"))

        # monitoring tools
        daemons = ["prometheus", "node-exporter", "alertmanager", "grafana"]
        for daemon in daemons:
            self.ceph_monitoring(daemon, self.cluster.get_nodes(role=daemon))

        self.ceph_rgw(self.cluster.get_nodes(role="rgw"))

    def ceph_mon(self, nodes, op="create"):
        """
        Deploy MON

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Returns:
            None
        """
        if not nodes:
            return

        timeout = self.TIMEOUT
        sleep_time = 1

        nodes = nodes if isinstance(nodes, list) else [nodes]

        # Bootstrap already created one MON node
        num_mons = 1

        if op == "create":
            for mon in nodes:
                if mon.hostname == self.first_mon.hostname:
                    continue
                logger.info("adding mon on %s-%s" % (mon.hostname, mon.ip_address))
                num_mons += 1
                self.shell(
                    remote=self.installer,
                    args=[
                        "ceph",
                        "orch",
                        "daemon",
                        "add",
                        "mon",
                        "{hostname}:{ip}=mon.{hostname}".format(
                            hostname=mon.shortname,
                            ip=mon.ip_address,
                        ),
                    ],
                )

                while timeout:
                    logger.info("waiting for %d mons in mon-map", num_mons)
                    out, rc = self.shell(
                        remote=self.installer,
                        args=["ceph", "mon", "dump", "-f", "json"],
                    )
                    j = json.loads(out)
                    if len(j["mons"]) == num_mons:
                        break
                    sleep(sleep_time)
                    timeout -= sleep_time

        elif op == "purge":
            # todo: Handle mon purge
            raise NotImplementedError

    def ceph_mgr(self, nodes, op="create"):
        """
        Deploy MGR

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Raises:
            AssertionError
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":
            mgr = []
            for node in nodes:
                if node.hostname == self.first_mgr.hostname:
                    continue
                logger.info("adding mgr on %s-%s" % (node.hostname, node.ip_address))
                mgr.append("{host}=mgr.{host}".format(host=node.hostname))

            # Since mgr already deployed at bootstrap,
            # empty list can be expected at `mgr`.
            if mgr:
                self.shell(
                    remote=self.installer,
                    args=[
                        "ceph",
                        "orch",
                        "apply",
                        "mgr",
                        "'{};{}'".format(len(mgr) + 1, ";".join(mgr)),
                    ],
                )

                # check daemon existence
                mgr = [id_.split("=", 1)[-1] for id_ in mgr]
                assert self.check_exist(
                    daemon="mgr",
                    ids=mgr,
                )

        elif op == "purge":
            # todo: Handle mgr purge
            raise NotImplementedError

    def ceph_mds(self, nodes, op="create"):
        """
        Deploy MDS

        This method involves,
            - creates "cephfs" file system volume
            - uses "cephfs" fs volume to deploy mds service daemons

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Raises:
            AssertionError

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":
            mds = []
            for node in nodes:
                logger.info("adding mds on %s-%s" % (node.hostname, node.ip_address))
                mds.append("{host}=cephfs.{host}".format(host=node.hostname))

            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "orch",
                    "apply",
                    "mds",
                    "cephfs",
                    "'{};{}'".format(len(mds), ";".join(mds)),
                ],
            )

            # workaround, ceph-orch should create fs volume
            try:
                self.shell(
                    remote=self.installer,
                    args=[
                        "ceph",
                        "fs",
                        "volume",
                        "create",
                        "cephfs",
                    ],
                )
            except CommandFailed as warn:
                logger.warning(warn)

            # check daemon existence
            mds = [id_.split("=", 1)[-1] for id_ in mds]
            assert self.check_exist(
                daemon="mds",
                ids=mds,
            )

        elif op == "purge":
            # todo: Handle mds purge
            raise NotImplementedError

    def ceph_rgw(self, nodes, op="create"):
        """
        Deploy RGW

        This method involves,
            - collects realm, zone list
            - uses realm.zone config to deploy RGW

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Raises:
            AssertionError

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]

        if op == "create":
            rgw = {}
            daemons = []
            for node in nodes:
                logger.info("adding rgw on %s-%s" % (node.hostname, node.ip_address))
                realm_zone = "realm.zone"

                if realm_zone not in rgw:
                    rgw[realm_zone] = []
                id_ = "{}.{}.rgw".format(realm_zone, node.hostname)
                rgw[realm_zone].append("{host}={id}".format(host=node.hostname, id=id_))
                daemons.append(id_)

            for realm_zone, nodes in rgw.items():
                (realm, zone) = realm_zone.split(".")
                self.shell(
                    remote=self.installer,
                    args=[
                        "ceph",
                        "orch",
                        "apply",
                        "rgw",
                        realm,
                        zone,
                        "--placement",
                        '"{};{}"'.format(str(len(nodes)), ";".join(nodes)),
                    ],
                )

            # check daemon existence
            assert self.check_exist(
                daemon="rgw",
                ids=daemons,
                timeout=len(rgw) * self.TIMEOUT,
            )

        elif op == "purge":
            # todo: Handle rgw purge
            raise NotImplementedError

    def get_disk_info(self, node):
        """
        Get node disk(s) info

        Args:
            node: remote node object
        Returns:
            disks: remote disk details
        """
        # get boot disk
        out, _ = node.exec_command(cmd="%s" % self.BOOT_DISK)
        out = out.read().decode().strip()

        boot_disk = re.sub(r"\d", "", out)
        logger.info("Boot disk found : %s", boot_disk)

        # get disk and skip boot disk
        headers = ["name", "type"]
        out, _ = node.exec_command(
            cmd="{} | grep disk".format(self.GET_DISKS.format(",".join(headers)))
        )
        disks_info = out.read().decode().strip().split("\n")

        disks = []
        for disk in list([x for x in disks_info if x]):
            disk_info = dict(list(zip(headers, re.split(r"\s+", disk))))
            disk_name = disk_info["name"]

            if boot_disk in disk_name:
                continue
            disks.append(disk_name)

        return disks

    def ceph_osd(self, nodes, op="create"):
        """
        Deploy OSD

        This method involves,
            - Collects devices from node
            - Zap each device
            - Deploy OSD per device in each node

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":
            for osd in nodes:
                logger.info("adding osd on %s-%s" % (osd.hostname, osd.ip_address))
                disks = self.get_disk_info(osd)

                for disk in disks:
                    # zap device
                    logger.info("Zap %s on %s" % (disk, osd.hostname))
                    self.shell(
                        remote=self.installer,
                        args=[
                            "ceph",
                            "orch",
                            "device",
                            "zap",
                            "--force",
                            "{} {}".format(osd.hostname, disk),
                        ],
                    )
                    # add osd
                    logger.info("adding osd %s:%s" % (disk, osd.hostname))
                    self.shell(
                        remote=self.installer,
                        args=[
                            "ceph",
                            "orch",
                            "daemon",
                            "add",
                            "osd",
                            "{}:{}".format(osd.hostname, disk),
                        ],
                    )

        elif op == "purge":
            # todo: Handle osd purge
            raise NotImplementedError

    def ceph_monitoring(self, daemon, nodes, op="create"):
        """
        Deploy Monitoring services
        (prometheus, node-exporter, alert-manager, grafana)

        Args:
            daemon: dashboard service name (example: grafana)
            nodes: node list
            op: action (create|purge) # default: "create"

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":
            host = []
            for node in nodes:
                logger.info(
                    "adding %s on %s-%s" % (daemon, node.hostname, node.ip_address)
                )
                host.append(
                    "{host}={daemon}.{host}".format(host=node.hostname, daemon=daemon)
                )

            # Deploy service
            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "orch",
                    "apply",
                    daemon,
                    "'{};{}'".format(len(host), ";".join(host)),
                ],
            )

            # Check daemon existence
            host = [id_.split("=")[-1] for id_ in host]
            assert self.check_exist(
                daemon=daemon,
                ids=host,
                timeout=900,
            )
        elif op == "purge":
            # todo: Handle mgr purge
            raise NotImplementedError

    def ceph_client(self, nodes, op="create"):
        """
        Configure Ceph client node

        Create client involves,
          - create auth key using name
          - copy created keyring to client node
          - set right permissions(0644)

        Args:
            nodes: node list
            op: action (create|purge) # default: "create"

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":

            for client in nodes:
                logger.info(
                    "setting up client on %s-%s" % (client.hostname, client.ip_address)
                )
                name = "client.{}".format(client.hostname)
                client_keyring = "/etc/ceph/{}.{}.keyring".format(
                    self.cluster_name, name
                )

                out, _ = self.shell(
                    remote=self.installer,
                    args=[
                        "ceph",
                        "auth",
                        "get-or-create",
                        name,
                        'mon "allow *"',
                        'osd "allow *"',
                        'mds "allow *"',
                        'mgr "allow *"',
                    ],
                )

                client.exec_command(cmd="sudo mkdir -p /etc/ceph")
                client.exec_command(cmd="sudo chmod 777 /etc/ceph")

                keyring_file = client.write_file(
                    sudo=True,
                    file_name=client_keyring,
                    file_mode="w",
                )
                keyring_file.write(out)
                keyring_file.flush()

                client.exec_command(cmd=f"sudo chmod 0644 {client_keyring}")

        elif op == "purge":
            # todo: Handle mgr purge
            raise NotImplementedError

    def ceph_iscsi(self, nodes, op="create"):
        """
        Manage Ceph ISCSI targets

        Create ISCSI targets involves,
          - Creates ISCSI replicated pool
          - Associates pool to RBD application
          - Deploy ISCSI targets on nodes provided
          - Validates deployed ISCSI targets using
             orchestration services list.

        Args:
            nodes: ISCSI node list
            op: action (op: "create|purge", default: create)

        Returns:
            None
        """
        if not nodes:
            return

        nodes = nodes if isinstance(nodes, list) else [nodes]
        if op == "create":
            iscsi = []
            for node in nodes:
                logger.info("adding iscsi on %s-%s" % (node.hostname, node.ip_address))
                iscsi.append("{host}=iscsi.{host}".format(host=node.hostname))

            pool_name = "iscsi"

            # create ISCSI replicated pool
            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "osd",
                    "pool",
                    "create",
                    pool_name,
                    "3",
                    "3",
                    "replicated",
                ],
            )

            # Associate pool to RBD application
            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "osd",
                    "pool",
                    "application",
                    "enable",
                    pool_name,
                    "rbd",
                ],
            )

            # create ISCSI targets
            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "orch",
                    "apply",
                    "iscsi",
                    pool_name,
                    "user",
                    "password",
                    "--placement",
                    "'{};{}'".format(len(iscsi), ";".join(iscsi)),
                ],
            )

            # check daemon existence
            iscsi = [id_.split("=", 1)[-1] for id_ in iscsi]
            assert self.check_exist(
                daemon="iscsi",
                ids=iscsi,
                timeout=900,
            )
        elif op == "purge":
            # todo: Handle iscsi purge
            raise NotImplementedError

    def deploy(self):
        """
        Deploy ceph cluster with all daemons

        one method to setup cephadm cluster which involves
          - setup ceph compose on every node
          - cephadm installation
          - bootstrap
          - add hosts
          - add all daemons
        """
        # set tool download repository
        self.set_tool_repo()

        # install/download cephadm package on installer
        self.install_cephadm()

        # bootstrap cluster
        self.bootstrap()

        # add all hosts
        self.manage_hosts()

        # add all daemons
        self.add_daemons()
