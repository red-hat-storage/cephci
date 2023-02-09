import ast
import datetime
import json
import random
import string
import time

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.exceptions import IOonSecondaryError
from utility.log import Log

log = Log(__name__)


class RbdMirror:
    def __init__(self, cluster, config):
        self.ceph_nodes = cluster
        self.k_m = config.get("ec-pool-k-m", None)
        self.ceph_version = int(config.get("rhbuild")[0])
        self.cluster_name = "ceph"
        self.rbd_client = "client.admin"
        self.ceph_args = " --cluster {}".format(self.cluster_name)
        self.cluster_spec = self.rbd_client + "@" + self.cluster_name
        self.datapool = None
        self.flag = 0
        self.ceph_rbdmirror = self.ceph_nodes.get_ceph_objects("rbd-mirror")[0]

        # Identifying Monitor And Client node
        for node in self.ceph_nodes:
            if node.role == "mon":
                self.ceph_mon = node
                continue
            if node.role == "client":
                self.ceph_client = node
                continue

        if self.ceph_version > 2 and self.k_m:
            self.datapool = (
                config["ec_pool_config"]["data_pool"]
                if config.get("ec_pool_config", {}).get("data_pool")
                else "rbd_test_data_pool_" + self.random_string()
            )
            if "," in self.k_m:
                self.ec_profile = config.get("ec_pool_config", {}).get(
                    "ec_profile", "rbd_ec_profile_" + self.random_string()
                )
                self.set_ec_profile(profile=self.ec_profile)
            else:
                self.ec_profile = ""

    def exec_cmd(self, **kw):
        try:
            cmd = kw.get("cmd")
            node = kw.get("node") if kw.get("node") else self.ceph_client

            if kw.get("ceph_args", True):
                cmd = cmd + self.ceph_args

            out = node.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=kw.get("long_running", False),
                check_ec=kw.get("check_ec", True),
            )
            log.info(f"Output of command {cmd}: {out}")

            if kw.get("output", False):
                if isinstance(out, tuple):
                    return out[0]
                return out

            return 0

        except CommandFailed:
            raise

    def copy_file(self, file_name, src, dest):
        contents, err = src.exec_command(sudo=True, cmd="cat {}".format(file_name))
        key_file = dest.remote_file(sudo=True, file_name=file_name, file_mode="w")
        key_file.write(contents)
        key_file.flush()

    def value(self, key, dictionary):
        """Retrieve required details from json output."""
        return str(list(self.find(key, dictionary))[0])

    def find(self, key, dictionary):
        """Finding required details from json output."""
        for k, v in dictionary.items():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in self.find(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    if isinstance(d, dict):
                        for result in self.find(key, d):
                            yield result
                    else:
                        yield d

    # Handling of clusters with same name
    def handle_same_name(self, name):
        self.cluster_name = name
        self.cluster_spec = self.rbd_client + "@" + self.cluster_name
        self.ceph_args = " --cluster {}".format(self.cluster_name)
        self.exec_cmd(
            ceph_args=False,
            cmd="[ -e /etc/sysconfig/ceph ] && sed -i 's/CLUSTER=ceph/CLUSTER={name}/' /etc/sysconfig/ceph "
            "|| echo 'CLUSTER={name}' >> /etc/sysconfig/ceph".format(name=name),
        )
        self.exec_cmd(
            ceph_args=False,
            cmd="ln -s /etc/ceph/ceph.conf /etc/ceph/{}.conf".format(name),
        )
        self.exec_cmd(
            ceph_args=False,
            cmd="ln -s /etc/ceph/ceph.client.admin.keyring /etc/ceph/{}.client.admin.keyring".format(
                name
            ),
        )

    def mirror_daemon(self, enable=None, start=None, stop=None, restart=None):
        """Enable, Start or Stop Rbd Mirror Daemon
        Note: This method cannot be used for RHCS 4.x and beyond.
        (For mirroring daemons configured using ceph-ansible/cephadm)
        """
        if enable:
            self.exec_cmd(
                ceph_args=False, cmd="systemctl enable ceph-rbd-mirror.target"
            )
            self.exec_cmd(ceph_args=False, cmd="systemctl enable ceph-rbd-mirror@admin")
        if start:
            self.exec_cmd(ceph_args=False, cmd="systemctl start ceph-rbd-mirror@admin")
        if stop:
            self.exec_cmd(ceph_args=False, cmd="systemctl stop ceph-rbd-mirror@admin")
        if restart:
            self.exec_cmd(
                ceph_args=False, cmd="systemctl restart ceph-rbd-mirror@admin"
            )

    # Initial setup of mirroring host
    def setup_mirror(self, peer_cluster, **kw):
        """Exchanges conf file and keyring file and installs rbd-mirror daemon."""

        self.copy_file(
            file_name="/etc/ceph/{}.conf".format(peer_cluster.cluster_name),
            src=peer_cluster.ceph_client,
            dest=self.ceph_mon,
        )
        self.copy_file(
            file_name="/etc/ceph/{}.client.admin.keyring".format(
                peer_cluster.cluster_name
            ),
            src=peer_cluster.ceph_client,
            dest=self.ceph_mon,
        )
        self.copy_file(
            file_name="/etc/ceph/{}.conf".format(peer_cluster.cluster_name),
            src=peer_cluster.ceph_client,
            dest=self.ceph_client,
        )
        self.copy_file(
            file_name="/etc/ceph/{}.client.admin.keyring".format(
                peer_cluster.cluster_name
            ),
            src=peer_cluster.ceph_client,
            dest=self.ceph_client,
        )

        if not kw.get("daemon_configured"):
            self.exec_cmd(ceph_args=False, cmd="yum install -y rbd-mirror")
            self.mirror_daemon(enable=True, start=True)

    def config_mirror(self, peer_cluster, **kw):
        """
        Configure mirroring on RBD clusters based on the parameters provided
        Args:
            peer_cluster: peer_cluster object for the secondary RBD cluster
            **kw:
                poolname: poolname to be used for creating pool
                mode: mirroring mode, pool or image to be used
                way: one-way or two-way mirroring
        """
        poolname = kw.get("poolname")
        primary_cluster = kw.get("ceph_cluster")
        primary_cluster_name = primary_cluster.name
        secondary_cluster_name = [
            cluster_name
            for cluster_name in kw.get("ceph_cluster_dict").keys()
            if cluster_name != primary_cluster_name
        ][0]
        secondary_cluster = kw.get("ceph_cluster_dict")[secondary_cluster_name]
        mode = kw.get("mode")
        peer_mode = kw.get("peer_mode")
        rbd_client = kw.get("rbd_client")
        build = kw.get("build")
        wait_for_status = kw.get("wait_for_status", True)

        self.enable_mirroring(mirror_level="pool", specs=poolname, mode=mode)
        peer_cluster.enable_mirroring(mirror_level="pool", specs=poolname, mode=mode)

        if self.ceph_version >= 4:
            if peer_mode == "bootstrap":
                self.bootstrap_peers(
                    poolname=poolname, cluster_name=primary_cluster_name
                )
                self.copy_file(
                    file_name="/root/bootstrap_token_primary",
                    src=self.ceph_client,
                    dest=peer_cluster.ceph_client,
                )
                peer_cluster.import_bootstrap(
                    poolname=poolname, cluster_name=secondary_cluster_name
                )
            else:
                primary_mon = ",".join(
                    [
                        obj.node.ip_address
                        for obj in primary_cluster.get_ceph_objects(role="mon")
                    ]
                )
                secondary_mon = ",".join(
                    [
                        obj.node.ip_address
                        for obj in secondary_cluster.get_ceph_objects(role="mon")
                    ]
                )
                primary_fsid = peer_cluster.ceph_nodes.get_cluster_fsid(build)
                secondary_fsid = self.ceph_nodes.get_cluster_fsid(build)
                secret = self.exec_cmd(
                    cmd=f"ceph auth get-or-create {rbd_client}", output=True
                )
                secret = secret.split(" ")[-1].strip()
                key_file_path = "/etc/ceph/secret_key"
                self.exec_cmd(cmd=f"echo {secret} > {key_file_path}")
                peer_cluster.exec_cmd(cmd=f"echo {secret} > {key_file_path}")
                primary_cluster_spec = (
                    f"{rbd_client}@{primary_fsid} --remote-mon-host {secondary_mon} "
                    f"--remote-key-file {key_file_path}"
                )
                secondary_cluster_spec = (
                    f"{rbd_client}@{secondary_fsid} --remote-mon-host {primary_mon} "
                    f"--remote-key-file {key_file_path}"
                )
                if "one-way" in kw.get("way", ""):
                    peer_cluster.peer_add(
                        poolname=poolname, cluster_spec=secondary_cluster_spec
                    )
                else:
                    self.peer_add(poolname=poolname, cluster_spec=primary_cluster_spec)
                    peer_cluster.peer_add(
                        poolname=poolname, cluster_spec=secondary_cluster_spec
                    )
        else:
            if "one-way" in kw.get("way", ""):
                peer_cluster.peer_add(poolname=poolname, cluster_spec=self.cluster_spec)
            else:
                self.peer_add(poolname=poolname, cluster_spec=peer_cluster.cluster_spec)
                peer_cluster.peer_add(poolname=poolname, cluster_spec=self.cluster_spec)

        if (
            self.mirror_info(poolname, "peers") is not None
            and peer_cluster.mirror_info(poolname, "peers") is not None
        ):
            log.info("Peers were successfully added")

        else:
            log.error("Peers were not added")

        # Waiting for OK pool mirror status to be okay based on user input as in image based
        # mirorring status wouldn't reach OK without enabling mirroing on individual images
        if wait_for_status:
            self.wait_for_status(poolname=poolname, health_pattern="OK")
            peer_cluster.wait_for_status(poolname=poolname, health_pattern="OK")

    # configure initial mirroring steps
    def initial_mirror_config(self, mirror2, poolname, imagename, **kw):
        """
        Calls create_pool function on both the clusters,
        creates an image on primary cluster and configure the mirroring
        waits for image to be present in secondary cluster with replying status
        Args:
            **kw:
            mirror1 - primary cluster
            mirror2 - secondary cluster
            poolname - name for pool to be created on primary and secondary cluster
            imagename - name of images created on primary and secondary cluster
            imagesize - size of the image created
            mode - mirror mode to be configured pool or image
            mirrormode - type of mirror configured journal or snapshot
        """
        log.debug(
            f"Config Recieved for initial mirror config: poolname:{poolname}, imagename:{imagename}\nkw:{kw}"
        )
        imagespec = poolname + "/" + imagename
        imagesize = kw.get("imagesize")
        io = kw.get("io_total")
        with parallel() as p:
            p.spawn(self.create_pool, poolname=poolname)
            p.spawn(mirror2.create_pool, poolname=poolname)

        self.create_image(imagespec=imagespec, size=imagesize)
        if kw.get("image_feature"):
            image_feature = kw.get("image_feature")
            self.image_feature_enable(imagespec=imagespec, image_feature=image_feature)

        if kw.get("mode"):
            kw["peer_mode"] = kw.get("peer_mode", "bootstrap")
            kw["rbd_client"] = kw.get("rbd_client", "client.admin")
            if kw.get("mode") == "image":
                kw["wait_for_status"] = False
            self.config_mirror(mirror2, poolname=poolname, **kw)

        # Enable image level mirroring only when mode is image type
        if kw.get("mirrormode") and kw.get("mode") == "image":
            mirrormode = kw.get("mirrormode")
            self.enable_mirror_image(poolname, imagename, mirrormode)
            self.wait_for_status(poolname=poolname, health_pattern="OK")
            mirror2.wait_for_status(poolname=poolname, health_pattern="OK")

        # TBD: We need to override wait_for_status to match images in cluster1==cluster2
        # ITs failing here when the pool contains more than 1 image
        # Using same image pool for replicated and ec pool
        # mirror2.wait_for_status(poolname=poolname, images_pattern=1)

        if kw.get("io_total"):
            self.benchwrite(imagespec=imagespec, io=io)
            time.sleep(60)
        with parallel() as p:
            p.spawn(
                self.wait_for_status, imagespec=imagespec, state_pattern="up+stopped"
            )
            p.spawn(
                mirror2.wait_for_status,
                imagespec=imagespec,
                state_pattern="up+replaying",
            )

    # Wait for required status
    def wait_for_status(self, **kw):
        # Waiting for cluster to be aware of new event
        starttime = datetime.datetime.now()
        if kw.get("tout"):
            tout = kw.get("tout")
        else:
            tout = datetime.timedelta(seconds=1200)
        time.sleep(20)
        while True:
            if kw.get("poolname", False):
                if kw.get("health_pattern"):
                    out = self.mirror_status("pool", kw.get("poolname"), "health")
                    log.info(
                        "Health of {} pool in {} cluster: {}".format(
                            kw.get("poolname"), self.cluster_name, out
                        )
                    )
                    if kw.get("health_pattern") in out:
                        return 0
                if kw.get("images_pattern"):
                    out = self.mirror_status("pool", kw.get("poolname"), "states")
                    out = ast.literal_eval(out)
                    state_pattern = kw.get("state", "total")
                    num_image = 0
                    if "total" in state_pattern:
                        for k, v in out.items():
                            num_image = num_image + v
                    else:
                        num_image = out[state_pattern]
                    log.info(
                        "Images in {} pool in {} cluster {}: {}, expected is :{}".format(
                            kw.get("poolname"),
                            self.cluster_name,
                            state_pattern,
                            num_image,
                            kw.get("images_pattern"),
                        )
                    )
                    if kw.get("images_pattern") == num_image:
                        return 0
            else:
                if kw.get("state_pattern"):
                    out = self.mirror_status("image", kw.get("imagespec"), "state")
                    log.info(
                        "State of {} image in {} cluster: {}".format(
                            kw.get("imagespec"), self.cluster_name, out
                        )
                    )
                    if kw.get("state_pattern") in out:
                        return 0
                if kw.get("description_pattern"):
                    out = self.get_position(
                        imagespec=kw.get("imagespec"),
                        pattern=kw.get("description_pattern"),
                    )
                    log.info(
                        "Description of {} image in {} cluster: {}".format(
                            kw.get("imagespec"), self.cluster_name, out
                        )
                    )
                    return out
            if datetime.datetime.now() - starttime <= tout:
                time.sleep(20)
            else:
                raise Exception("Required status can not be attained")

    # Wait for replay to complete, check every 30 seconds
    def wait_for_replay_complete(self, imagespec):
        while True:
            time.sleep(30)
            out = self.wait_for_status(
                imagespec=imagespec, description_pattern="entries"
            )
            if self.ceph_version >= 4:
                out1 = out.split('entries_behind_primary":')
                out2 = out1[1].split(",")
                log.info(f"entries_behind_primary : {out2[0]}")
                if int(out2[0]) == 0:
                    time.sleep(30)
                    return out2[0]
            else:
                if int(out.split("=")[-1]) == 0:
                    return 0

    # Get Position
    def get_position(self, imagespec, pattern=None):
        out = self.mirror_status("image", imagespec, "description")
        if pattern is not None:
            primary_pos = out.find("primary_position")
            mirror_pos = out.find("mirror_position")
            entries_behind = out.find("entries")
            pos = [
                out[primary_pos : mirror_pos - 2],
                out[mirror_pos : entries_behind - 2],
                out[entries_behind:],
            ]
            if "primary" in pattern:
                return pos[0]
            elif "mirror" in pattern:
                return pos[1]
            else:
                return pos[2]
        else:
            return out

    def get_mirror_mode(self, *args):
        output = self.exec_cmd(
            output=True, cmd=f"rbd --image {args[0]} info --format=json"
        )
        json_dict = json.loads(output)
        mirroring_details = json_dict.get("mirroring", None)
        if mirroring_details:
            return mirroring_details.get("mode")
        return None

    def mirror_snapshot_schedule_add(self, **kwargs):
        """
        This will schedule snapshot based on below kwargs
        Args:
            **kwargs:
                poolname: if pool name is specified schedule will be done per pool, if not will be done globally
                imagename : if image name is specified schedule will be done per image if not will be done per pool or
                            globally
                interval : Positional argument need to be specified, default will be 1minute
                starttime : optional if specified will be configured
        Returns:
        """
        cmd1 = "rbd mirror snapshot schedule add"
        if kwargs.get("poolname"):
            cmd1 += f" --pool {kwargs.get('poolname', '')}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')}"
        cmd1 += f" {kwargs.get('interval')}" if kwargs.get("interval") else " 1m"
        cmd1 += f" {kwargs.get('starttime')}" if kwargs.get("starttime") else ""
        self.exec_cmd(cmd=cmd1)

    def verify_snapshot_schedule(self, imagespec, interval=1):
        """
        This will verify the snapshot roll overs on the image which is snapshot based mirroring
        Args:
            imagespec:
            interval : this is int and specified in min
        Returns:

        """
        output = self.exec_cmd(
            output=True, cmd=f"rbd mirror image status {imagespec} --format=json"
        )
        json_dict = json.loads(output)
        snapshot_ids = [i["id"] for i in json_dict.get("snapshots")]
        log.info(f"snapshot_ids Before : {snapshot_ids}")
        time.sleep(interval * 120)
        output = self.exec_cmd(
            output=True, cmd=f"rbd mirror image status {imagespec} --format=json"
        )
        json_dict = json.loads(output)
        snapshot_ids_1 = [i["id"] for i in json_dict.get("snapshots")]
        log.info(f"snapshot_ids After : {snapshot_ids_1}")
        if snapshot_ids != snapshot_ids_1:
            return 0
        raise Exception("snapshots not generated after the intervel")

    def mirror_snapshot_schedule_list(self, poolname, **kwargs):
        """
        This will list the mirror snapshot schedule based on below kwargs
        Args:
            poolname: name of the pool
            **kwargs:
                imagename : if imagename is specified list will be done per image if not will done globally
        Returns:
        """
        cmd1 = f"rbd mirror snapshot schedule list --pool {poolname}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')} --format=json --recursive"
        self.exec_cmd(cmd=cmd1)

    def mirror_snapshot_schedule_status(self, poolname, **kwargs):
        """
        This will show the status of mirror snapshot schedule based on below kwargs
        Args:
            poolname:
            **kwargs:
                imagename : if imagename is specified status will be done per image if not will done globally

        Returns:
        """
        cmd1 = f"rbd mirror snapshot schedule status --pool {poolname}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')} --format=json"
        self.exec_cmd(cmd=cmd1)

    def mirror_snapshot_schedule_remove(self, **kwargs):
        """
        This will remove the mirror snapshot schedule based on below kwargs
        Args:
            **kwargs:
                poolname : if poolname is specified schedule will be removed at pool level else cluster level
                imagename : if imagename is specified schedule will be removed per image if not will done globally
                interval : optional if specified will be configured
                starttime : optional if specified will be configured
        Returns:
        """
        cmd1 = "rbd mirror snapshot schedule remove"
        if kwargs.get("poolname"):
            cmd1 += f" --pool {kwargs.get('poolname')}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')}"
        cmd1 += f" {kwargs.get('interval')}" if kwargs.get("interval") else ""
        cmd1 += f" {kwargs.get('starttime')}" if kwargs.get("starttime") else ""
        self.exec_cmd(cmd=cmd1)

    def verify_snapshot_schedule_remove(self, **kwargs):
        """
        This will verify snapshot schedule got removed successfuly or not
        Args:
        **kwargs:
            poolname : if poolname is specified schedule will be removed at pool level else cluster level
            imagename : if imagename is specified schedule will be removed per image if not will done globally
        Returns:
            0 on Success
            1 on Failure
        """
        cmd1 = "rbd mirror snapshot schedule list"
        if kwargs.get("poolname"):
            cmd1 += f" --pool {kwargs.get('poolname')}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')}"
        output = self.exec_cmd(output=True, cmd=cmd1, check_ec=False)
        if not output:
            log.info("snapshot schedule got removed successfully")
            return 0
        else:
            log.error("Failed to remove snapshot schedule")
            return 1

    # Check data consistency
    def check_data(self, peercluster, imagespec):
        self.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        peercluster.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        if self.get_mirror_mode(imagespec) != "snapshot":
            peercluster.wait_for_replay_complete(imagespec)
        export_path = "/home/cephuser/image.export_" + self.random_string()
        self.export_image(imagespec=imagespec, path=export_path)
        peercluster.export_image(imagespec=imagespec, path=export_path)
        local_md5 = self.exec_cmd(
            ceph_args=False, output=True, cmd="md5sum {}".format(export_path)
        )
        rmt_md5 = peercluster.exec_cmd(
            ceph_args=False, output=True, cmd="md5sum {}".format(export_path)
        )
        log.info(local_md5)
        log.info(rmt_md5)
        if local_md5 == rmt_md5:
            log.info("Data is consistent")
            self.exec_cmd(ceph_args=False, cmd="rm -f {}".format(export_path))
            peercluster.exec_cmd(ceph_args=False, cmd="rm -f {}".format(export_path))
            return 0
        else:
            raise Exception("Data Inconsistency found")

    # CLIs
    def benchwrite(self, **kw):
        if self.ceph_version < 3:
            self.exec_cmd(
                cmd="rbd bench-write --io-total {} {}".format(
                    kw.get("io", "500M"), kw.get("imagespec")
                ),
                long_running=True,
            )
        else:
            self.exec_cmd(
                cmd="rbd bench --io-type write --io-threads 16 --io-total {} {}".format(
                    kw.get("io", "500M"), kw.get("imagespec")
                ),
                long_running=True,
            )

    def create_pool(self, **kw):
        if self.ceph_version > 2 and self.k_m:
            self.create_ecpool(profile=self.ec_profile, poolname=self.datapool)
        self.exec_cmd(cmd="ceph osd pool create {} 64 64".format(kw.get("poolname")))
        if self.ceph_version >= 3:
            self.exec_cmd(cmd="rbd pool init {}".format(kw.get("poolname")))

    def create_image(self, **kw):
        """
        Create image on provided pool with default image feature
        like exclusive-lock, layering, objectmap, fast-diff, deepflatten
        Args:
            **kw:
                imagespec: pool/image_name
                size: size of the image
        """
        cmd = "rbd create {} --size {}".format(
            kw.get("imagespec"), kw.get("size", "2G")
        )
        if kw.get("datapool", self.datapool):
            cmd = cmd + " --data-pool {}".format(kw.get("datapool", self.datapool))
        self.exec_cmd(cmd=cmd)

    def image_feature_enable(self, **kw):
        """
        enable image features on the existing image
        Args:
            **kw:
                imagespec: pool/image_name on which image features will enable
                image_features: comma seperated value for image features
        """
        cmd = "rbd feature enable {} {}".format(
            kw.get("imagespec"), kw.get("image_feature")
        )
        self.exec_cmd(cmd=cmd)

    def image_feature_disable(self, **kw):
        """
        Disable image features on the existing image
        Args:
            **kw:
                imagespec: pool/image_name on which image features will disable
                image_features: comma seperated value for image features
        """
        cmd = "rbd feature disable {} {}".format(
            kw.get("imagespec"), kw.get("image_feature")
        )
        self.exec_cmd(cmd=cmd)

    def export_image(self, **kw):
        self.exec_cmd(
            cmd="rbd export {} {}".format(kw.get("imagespec"), kw.get("path")),
            long_running=True,
        )

    # Enable Pool or Image Mirroring
    def enable_mirroring(self, **kw):
        """
        Enable mirroring on provided pool with provided mode.
        Args:
            **kw:
                mirror_level: pool if mirroring has to be enabled on a pool,
                              image if mirroring has to be enabled only on a specific image in a pool
                              this is a necessary parameter
                specs: pool_name if mirroring has to be enabled on a pool,
                       pool_name/image_name if mirroring has to be enabled on an image
                       this is a necessary parameter
                mode: pool if mirroring has to be enabled on all images in a pool by default,
                      image if mirroring has to be enabled explicitly on images in a pool
        """
        self.exec_cmd(
            cmd="rbd mirror {} enable {} {}".format(
                kw.get("mirror_level", "pool"),
                kw.get("specs", "rbd"),
                kw.get("mode", ""),
            )
        )

    # Disable Pool or Image Mirroring
    def disable_mirroring(self, *args):
        self.exec_cmd(cmd="rbd mirror {} disable {}".format(args[0], args[1]))

    # Mirroring Info
    def mirror_info(self, *args):
        output = self.exec_cmd(
            output=True, cmd="rbd mirror pool info {} --format=json".format(args[0])
        )
        json_dict = json.loads(output)
        return self.value(args[1], json_dict)

    # Mirroring Status
    def mirror_status(self, *args):
        output = self.exec_cmd(
            output=True,
            cmd="rbd mirror {} status {} --format=json".format(args[0], args[1]),
        )
        json_dict = json.loads(output)
        return self.value(args[2], json_dict)

    # Add Peer
    def peer_add(self, **kw):
        return self.exec_cmd(
            cmd=f"rbd mirror pool peer add {kw['poolname']} {kw['cluster_spec']}"
        )

    def bootstrap_peers(self, **kw):
        """Create a bootstrap token on primary cluster and store under root"""
        cmd = "rbd mirror pool peer bootstrap create"
        cmd += " --site-name primary"
        cmd += f" {kw['poolname']} > /root/bootstrap_token_primary"
        return self.exec_cmd(cmd=cmd)

    def import_bootstrap(self, **kw):
        """Import bootstrap token that is created on primary from scecondary cluster"""
        if "one-way" in kw.get("way", ""):
            cmd = "rbd mirror pool peer bootstrap import"
            cmd += " --site-name secondary --direction rx-only"
            cmd += f" {kw['poolname']} /root/bootstrap_token_primary"
            return self.exec_cmd(cmd=cmd)
        else:
            cmd = "rbd mirror pool peer bootstrap import"
            cmd += " --site-name secondary"
            cmd += f" {kw['poolname']} /root/bootstrap_token_primary"
            return self.exec_cmd(cmd=cmd)

    # Remove Peer
    def peer_remove(self, **kw):
        peer_uuid = self.mirror_info(kw.get("poolname"), "uuid")
        return self.exec_cmd(
            cmd="rbd mirror pool peer remove {} {}".format(
                kw.get("poolname"), peer_uuid
            )
        )

    # Promote Image
    def promote(self, **kw):
        if kw.get("force"):
            return self.exec_cmd(
                output=True,
                cmd="rbd mirror image promote --force {}".format(kw.get("imagespec")),
            )
        else:
            return self.exec_cmd(
                output=True,
                cmd="rbd mirror image promote {}".format(kw.get("imagespec")),
            )

    # Demote Image
    def demote(self, imagespec):
        return self.exec_cmd(cmd="rbd mirror image demote {}".format(imagespec))

    # Resync Image
    def resync(self, imagespec):
        self.exec_cmd(cmd="rbd mirror image resync {}".format(imagespec))

    def random_string(self):
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(10)])
        return temp_str

    def delete_pool(self, poolname):
        self.exec_cmd(
            cmd="ceph osd pool delete {pool} {pool} "
            "--yes-i-really-really-mean-it".format(pool=poolname)
        )

    def delete_image(self, imagespec):
        self.exec_cmd(cmd="rbd rm {}".format(imagespec))

    def set_ec_profile(self, profile):
        self.exec_cmd(cmd="ceph osd erasure-code-profile rm {}".format(profile))
        self.exec_cmd(
            cmd="ceph osd erasure-code-profile set {} k={} m={}".format(
                profile, self.k_m[0], self.k_m[2]
            )
        )

    def create_ecpool(self, **kw):
        poolname = kw.get("poolname", self.datapool)
        profile = kw.get("profile", self.ec_profile)
        self.exec_cmd(
            cmd="ceph osd pool create {} 12 12 erasure {}".format(poolname, profile)
        )
        self.exec_cmd(cmd="rbd pool init {}".format(poolname))
        self.exec_cmd(
            cmd="ceph osd pool set {} allow_ec_overwrites true".format(poolname)
        )

    def enable_mirror_image(self, poolname, imagename, mode):
        """

        Args:
            poolname:
            imagename:
            mode:
            Allowed modes journal,snapshot

        Returns:

        """
        self.exec_cmd(cmd=f"rbd mirror image enable {poolname}/{imagename} {mode}")

    def clean_up(self, peercluster, **kw):
        if kw.get("dir_name"):
            self.exec_cmd(cmd="rm -rf {}".format(kw.get("dir_name")))
            peercluster.exec_cmd(cmd="rm -rf {}".format(kw.get("dir_name")))
        if kw.get("pools"):
            pool_list = kw.get("pools")
            if self.datapool:
                pool_list.append(self.datapool)
            for pool in pool_list:
                self.delete_pool(poolname=pool)
                peercluster.delete_pool(poolname=pool)

    def get_rbd_service_name(self, service_name):
        """
        Gets the rbd mirror service name where the rbd mirror daemon running
        Args:
            service_name:

        Returns:
            service_name --> str
        """
        service_name, rc = self.ceph_rbdmirror.exec_command(
            sudo=True,
            cmd=f"systemctl list-units --all | grep {service_name} | grep -Ev \\.target | awk {{'print $1'}}",
        )
        log.info(f"Service name : {service_name} ")
        return service_name

    def change_service_state(self, service_name, operation):
        """
        Starts or Stops the given service name
        Args:
            service_name:
            operation:

        Returns:
            None
        """
        log.info(f"{operation}ing the service : {service_name} ")
        self.ceph_rbdmirror.exec_command(
            sudo=True, cmd=f"systemctl {operation} {service_name}"
        )

    def image_exists(self, imagespec):
        """
        Checks whether given image exists or not
        Args:
            imagespec: image spec of image to be checked for existence
        Returns:
            0 : if image exists
            1 : if image doesn't exist
        """
        cmd = f"rbd --image {imagespec} info"
        try:
            self.exec_cmd(sudo=True, cmd=cmd)

        except CommandFailed as failed:
            if "No such file" in failed.args[0]:
                log.info("No such image found")
                return 1
            else:
                raise CommandFailed
        log.info("Image is found")
        return 0

    def rename_primary_image(self, source_imagespec, dest_imagespec, **kw):
        """
        Rename the primary image and check from secondary for the changes
        Args:
           source_imagespec: primary image
           dest_imagespec: rename image
        Returns:
              None
        """
        pool_name = kw.get("poolname")
        cmd1 = f"rbd mv {source_imagespec} {pool_name}/{dest_imagespec}"
        self.exec_cmd(cmd=cmd1)
        cmd2 = f"rbd info {pool_name}/{dest_imagespec} --format=json"
        out1 = self.exec_cmd(cmd=cmd2)
        log.info(out1)

    def create_mirror_snapshot(self, imagespec):
        """
        create snapshot on the image to reflect the changes to secondary
        Args:
            imagespec
        Returns:
              None
        """
        cmd1 = f"rbd mirror image snapshot {imagespec}"
        self.exec_cmd(cmd=cmd1)

    def resize_image(self, imagespec, size):
        """
        Resize provided image
        Args:
            imagespec: image-spec of the image to be resized
            size: size of the image to be updated to
        Returns:
            None
        Note:
            Function raises IOonSecondaryError if resize is tried on
        secondary image
        """
        log.info(f"Resizing image {imagespec} to size {size}")
        cmd = f"rbd resize {imagespec} -s {size} --allow-shrink"
        try:
            self.exec_cmd(sudo=True, cmd=cmd)

        except CommandFailed as resize_failed:
            if "Read-only file system" in resize_failed.args[0]:
                raise IOonSecondaryError("Detected I/O Operation on secondary")
            else:
                raise

    def mirror_daemon_status(self, daemon):
        """
        Gets the rbd mirror daemon status where the rbd mirror daemon running
        Args:
            daemon
        Returns:
            0 - if daemon_status is active
            1 - it daemon_status is not active
        """
        daemon_status, rc = self.ceph_rbdmirror.exec_command(
            sudo=True,
            cmd=f"systemctl | grep {daemon} | awk {{'print $3'}}",
        )
        log.info(f"Daemon Runing Status : {daemon_status}")
        if daemon_status.strip("\n") != "active":
            return 1
        return 0

    def get_mirror_pool_daemon_status(self, poolname):
        """Refresh object argument ceph_rbdmirror based on mirror daemon status present in given pool's status.
        Args:
            poolname: name of the pool based on which status of daemon needs to be updated.
        returns:
            dict: dictionary of daemon details
            1: if no daemons are up
        """
        out = self.exec_cmd(
            cmd=f"rbd mirror pool status {poolname} --verbose --format json",
            output=True,
        )
        json_dict = json.loads(out)

        return json_dict.get("daemons", 1)

    def update_ceph_rbdmirror(self, poolname, leader=True):
        """Update ceph_rbdmirror value based on the provided values.
        Args:
            poolname: poolname to fetch mirror status.
            leader: which daemon to be updated.
            # currently it assumes that only 2 per cluster.
        returns:
            1: upon failure
        """
        mirror_daemon_dict = self.get_mirror_pool_daemon_status(poolname)
        if mirror_daemon_dict == 1:
            return 1
        log.debug(f"mirror_daemon_dict: {mirror_daemon_dict}")
        for each_daemon in mirror_daemon_dict:
            if each_daemon["leader"] == leader:
                self.ceph_rbdmirror = get_node_by_id(
                    self.ceph_nodes, each_daemon["hostname"].split("-")[-1]
                )
                break


def rbd_mirror_config(**kw):
    """
    Configure RBD mirroring on replicated pools, ecpools or both based on arguments specified
    Args:
        **kw:

    Examples: In default configuration, pool and image names will be taken as random values.
        Configuration for ecpools only :
            config:
                ec-pool-only: True
        Configuration for replicated pools only :
            config:
                rep-pool-only: True
        Advanced configuration:
            config:
               do_not_create_image: True  # if not set then images will be created by default
               ec-pool-k-m: 2,1
               ec-pool-only: False
               ec_pool_config:
                  pool: rbd_pool_4
                  data_pool: rbd_ec_pool_4
                  ec_profile: rbd_ec_profile_4
                  image: rbd_image_4
                  size: 10G
                  io_total: 1G
                  mode: image (default pool)
                  mirrormode: snapshot (default journal)
               rep_pool_config:
                  pool: rbd_rep_pool
                  image: rbd_rep_image
                  size: 10G
                  io_total: 1G
                  mode: image (default pool)
                  mirrormode: snapshot (default journal)
    Args:
        **kw:

    Returns:
        RBD mirror objects for ecpool and replicated pool
    """
    rbd_obj = dict()
    log.debug(
        f'config recieved for rbd_mirror_config: {kw.get("config", "Config not recieved, assuming default values")}'
    )
    # Create all the necessary configuration for replicated pool if not specified by user
    if not kw.get("config") or not kw.get("config").get("ec-pool-only"):
        ec_pool_k_m = kw.get("config", {}).pop("ec-pool-k-m", None)

        rep_mirror1, rep_mirror2 = [
            RbdMirror(cluster, kw.get("config", {}))
            for cluster in kw.get("ceph_cluster_dict").values()
        ]

        # Create rep pool config when no configuration is specified for the module
        if not kw.get("config"):
            kw["config"] = {
                "rep_pool_config": {
                    "pool": "rep_pool_" + rep_mirror1.random_string(),
                    "image": "rep_image_" + rep_mirror1.random_string(),
                    "size": "1G",
                    "io_total": "1G",
                    "mode": "pool",
                }
            }
        # Create rep pool config with all necessary config when only some are specified
        elif kw.get("config").get("rep_pool_config"):
            kw["config"]["rep_pool_config"]["pool"] = kw["config"][
                "rep_pool_config"
            ].get("pool", "rep_pool_" + rep_mirror1.random_string())
            kw["config"]["rep_pool_config"]["image"] = kw["config"][
                "rep_pool_config"
            ].get("image", "rep_image_" + rep_mirror1.random_string())
            kw["config"]["rep_pool_config"]["size"] = kw["config"][
                "rep_pool_config"
            ].get("size", "1G")
            kw["config"]["rep_pool_config"]["io_total"] = kw["config"][
                "rep_pool_config"
            ].get("io_total", "1G")
            kw["config"]["ec_pool_config"]["mode"] = kw["config"][
                "rep_pool_config"
            ].get("mode", "pool")
        # Create rep pool config with all necessary config when no rep pool config is specified
        else:
            kw["config"]["rep_pool_config"] = {
                "pool": "rep_pool_" + rep_mirror1.random_string(),
                "image": "rep_image_" + rep_mirror1.random_string(),
                "size": "1G",
                "io_total": "1G",
                "mode": "pool",
            }

        rep_mirror1.initial_mirror_config(
            rep_mirror2,
            poolname=kw["config"]["rep_pool_config"]["pool"],
            imagename=kw["config"]["rep_pool_config"]["image"],
            imagesize=kw["config"]["rep_pool_config"]["size"],
            io_total=kw["config"]["rep_pool_config"]["io_total"],
            mode=kw["config"]["rep_pool_config"]["mode"],
            mirrormode=kw["config"]["rep_pool_config"].get("mirrormode", ""),
            **kw,
        )

        kw["config"]["ec-pool-k-m"] = ec_pool_k_m
        rbd_obj.update(
            {"rep_rbdmirror": {"mirror1": rep_mirror1, "mirror2": rep_mirror2}}
        )

    # Create ec pool object only when rep-pool-only is not specified or set to false
    if not kw.get("config").get("rep-pool-only"):
        if not kw.get("config").get("ec-pool-k-m"):
            kw["config"]["ec-pool-k-m"] = "go_with_default"

        ec_mirror1, ec_mirror2 = [
            RbdMirror(cluster, kw.get("config", {}))
            for cluster in kw.get("ceph_cluster_dict").values()
        ]

        # Create ec pool config with all necessary config when some ec pool config is specified
        if kw.get("config").get("ec_pool_config"):
            kw["config"]["ec_pool_config"]["pool"] = kw["config"]["ec_pool_config"].get(
                "pool", "ec_img_pool_" + ec_mirror1.random_string()
            )
            kw["config"]["ec_pool_config"]["image"] = kw["config"][
                "ec_pool_config"
            ].get("image", "ec_image" + ec_mirror1.random_string())
            kw["config"]["ec_pool_config"]["size"] = kw["config"]["ec_pool_config"].get(
                "size", "1G"
            )
            kw["config"]["ec_pool_config"]["io_total"] = kw["config"][
                "ec_pool_config"
            ].get("io_total", "1G")
            kw["config"]["ec_pool_config"]["mode"] = kw["config"]["ec_pool_config"].get(
                "mode", "pool"
            )
        # Create ec pool config with all necessary config when no ec pool config is specified
        else:
            kw["config"]["ec_pool_config"] = {
                "pool": "ec_img_pool_" + ec_mirror1.random_string(),
                "image": "ec_image" + ec_mirror1.random_string(),
                "size": "1G",
                "io_total": "1G",
                "mode": "pool",
            }
        ec_mirror1.initial_mirror_config(
            ec_mirror2,
            poolname=kw["config"]["ec_pool_config"]["pool"],
            imagename=kw["config"]["ec_pool_config"]["image"],
            imagesize=kw["config"]["ec_pool_config"]["size"],
            io_total=kw["config"]["ec_pool_config"]["io_total"],
            mode=kw["config"]["ec_pool_config"]["mode"],
            mirrormode=kw["config"]["ec_pool_config"].get("mirrormode", ""),
            **kw,
        )

        rbd_obj.update({"ec_rbdmirror": {"mirror1": ec_mirror1, "mirror2": ec_mirror2}})

    return rbd_obj
