import ast
import datetime
import json
import random
import string
import time

from ceph.ceph import CommandFailed
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
            self.datapool = "rbd_datapool"
            self.ec_profile = "rbd_ec_profile"
            self.set_ec_profile(profile=self.ec_profile)

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

    # Retrieve required details from json output
    def value(self, key, dictionary):
        return str(list(self.find(key, dictionary))[0])

    # Finding required details from json output
    def find(self, key, dictionary):
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

    # Enable, Start or Stop Rbd Mirror Daemon
    def mirror_daemon(self, enable=None, start=None, stop=None, restart=None):

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
        self.exec_cmd(ceph_args=False, cmd="yum install -y rbd-mirror")

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

        self.mirror_daemon(enable=True, start=True)

    def config_mirror(self, peer_cluster, **kw):
        poolname = kw.get("poolname")
        primary_cluster = "master"
        secondary_cluster = "slave"
        mode = kw.get("mode")

        self.enable_mirroring("pool", poolname, mode=mode)
        peer_cluster.enable_mirroring("pool", poolname, mode=mode)

        if self.ceph_version >= 4:
            self.bootstrap_peers(poolname=poolname, cluster_name=primary_cluster)
            self.copy_file(
                file_name="/root/bootstrap_token_master",
                src=self.ceph_client,
                dest=peer_cluster.ceph_client,
            )
            peer_cluster.import_bootstrap(
                poolname=poolname, cluster_name=secondary_cluster
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

        self.wait_for_status(poolname=poolname, health_pattern="OK")
        peer_cluster.wait_for_status(poolname=poolname, health_pattern="OK")

    # Wait for required status
    def wait_for_status(self, **kw):
        tout = datetime.timedelta(seconds=600)
        starttime = datetime.datetime.now()
        # Waiting for cluster to be aware of new event
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
                        "Images in {} pool in {} cluster {}: {}".format(
                            kw.get("poolname"),
                            self.cluster_name,
                            state_pattern,
                            num_image,
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
            master_pos = out.find("master_position")
            mirror_pos = out.find("mirror_position")
            entries_behind = out.find("entries")
            pos = [
                out[master_pos : mirror_pos - 2],
                out[mirror_pos : entries_behind - 2],
                out[entries_behind:],
            ]
            if "master" in pattern:
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

    def mirror_snapshot_schedule_add(self, poolname, **kwargs):
        """
        This will schedule snapshot based on below kwargs
        Args:
            poolname:
            **kwargs:
                imagename : if image name is specified schedule will be done per image if not will done globally
                interval : Positional argument need to be specified
                starttime : optional if specified will be configured
        Returns:
        """
        cmd1 = f"rbd mirror snapshot schedule add --pool {poolname}"
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
            poolname:
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

    def mirror_snapshot_schedule_remove(self, poolname, **kwargs):
        """
        This will remove the mirror snapshot schedule based on below kwargs
        Args:
            poolname:
            **kwargs:
                imagename : if imagename is specified schedule will be done per image if not will done globally
                interval : optional if specified will be configured
                starttime : optional if specified will be configured
        Returns:
        """
        cmd1 = f"rbd mirror snapshot schedule remove --pool {poolname}"
        if kwargs.get("imagename"):
            cmd1 += f" --image {kwargs.get('imagename')}"
        cmd1 += f" {kwargs.get('interval')}" if kwargs.get("interval") else ""
        cmd1 += f" {kwargs.get('starttime')}" if kwargs.get("starttime") else ""
        self.exec_cmd(cmd=cmd1)

    # Check data consistency
    def check_data(self, peercluster, imagespec):
        self.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        peercluster.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        if self.get_mirror_mode(imagespec) != "snapshot":
            peercluster.wait_for_replay_complete(imagespec)
        export_path = "/home/cephuser/image.export"
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
        cmd = "rbd create -s {} {} --image-feature exclusive-lock,journaling".format(
            kw.get("size", "2G"), kw.get("imagespec")
        )

        if kw.get("datapool", self.datapool):
            cmd = cmd + " --data-pool {}".format(kw.get("datapool", self.datapool))
        self.exec_cmd(cmd=cmd)

    def export_image(self, **kw):
        self.exec_cmd(
            cmd="rbd export {} {}".format(kw.get("imagespec"), kw.get("path")),
            long_running=True,
        )

    # Enable Pool or Image Mirroring
    def enable_mirroring(self, *args, **kw):
        self.exec_cmd(
            cmd="rbd mirror {} enable {} {}".format(
                args[0], args[1], kw.get("mode", "")
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
        cmd += " --site-name master"
        cmd += f" {kw['poolname']} > /root/bootstrap_token_master"
        return self.exec_cmd(cmd=cmd)

    def import_bootstrap(self, **kw):
        """Import bootstrap token that is created on primary from scecondary cluster"""
        if "one-way" in kw.get("way", ""):
            cmd = "rbd mirror pool peer bootstrap import"
            cmd += " --site-name slave --direction rx-only"
            cmd += f" {kw['poolname']} /root/bootstrap_token_master"
            return self.exec_cmd(cmd=cmd)
        else:
            cmd = "rbd mirror pool peer bootstrap import"
            cmd += " --site-name slave"
            cmd += f" {kw['poolname']} /root/bootstrap_token_master"
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

    # Demote Image
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
            cmd=f"systemctl list-units --all | grep {service_name} | awk {{'print $1'}}",
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
                return 1
            else:
                raise CommandFailed

        return 0

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
