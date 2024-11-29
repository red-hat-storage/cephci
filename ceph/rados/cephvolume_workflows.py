"""
Test Module to perform specific functionalities of ceph-volume.
 - ceph-volume lvm list [OSD_ID | DEVICE_PATH | VOLUME_GROUP/LOGICAL_VOLUME]
 - ceph-volume lvm zap [--destroy] [--osd-id OSD_ID | --osd-fsid OSD_FSID | DEVICE_PATH]
 - ceph-volume --help
 - ceph-volume lvm --help
 - ceph-volume lvm prepare --bluestore [--block.db --block.wal] --data path
 - ceph-volume lvm activate [--bluestore OSD_ID OSD_FSID] [--all]
 - ceph-volume lvm deactivate OSD_ID
 - ceph-volume lvm create --bluestore --data VOLUME_GROUP/LOGICAL_VOLUME
 - ceph-volume lvm batch --bluestore PATH_TO_DEVICE [PATH_TO_DEVICE]
 - ceph-volume lvm migrate --osd-id OSD_ID --osd-fsid OSD_UUID
    --from [ data | db | wal ] --target VOLUME_GROUP_NAME/LOGICAL_VOLUME_NAME
 - ceph-volume lvm new-db --osd-id OSD_ID --osd-fsid OSD_FSID --target VOLUME_GROUP_NAME/LOGICAL_VOLUME_NAME
 - ceph-volume lvm new-wal --osd-id OSD_ID --osd-fsid OSD_FSID --target VOLUME_GROUP_NAME/LOGICAL_VOLUME_NAME
"""

import json

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class CephVolumeWorkflows:
    """
    Contains various functions to verify ceph-volume commands
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run ceph-volume commands
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.client = node.cluster.get_nodes(role="client")[0]

    def run_cv_command(
        self,
        cmd: str,
        host,
        timeout: int = 300,
    ) -> str:
        """
        Runs ceph-volume commands within cephadm shell
        Args:
            cmd: command that needs to be run
            host: (ceph.utils.CephVMNode): CephVMNode
            timeout: Maximum time allowed for execution.
        Returns:
            output of respective ceph-volume command in string format
        """
        _cmd = f"cephadm {cmd}"
        try:
            out, err = host.exec_command(sudo=True, cmd=_cmd, timeout=timeout)
        except Exception as er:
            log.error(f"Exception hit while command execution. {er}")
            raise
        return str(out)

    def help(self, host):
        """Module to run help command with ceph-volume to display usage
         Args:
            host: (ceph.utils.CephVMNode): CephVMNode
        Returns:
            Output of ceph-volume usage
        Example:
            volumeobject.help(osd_host)
        """
        _cmd = "ceph-volume --help"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_help(self, host):
        """Module to run help command with ceph-volume lvm to display usage
         Args:
            host: (ceph.utils.CephVMNode): CephVMNode
        Returns:
            Output of ceph-volume lvm usage
        Example:
            volumeobject.lvm_help(osd_host)
        """
        _cmd = "ceph-volume lvm --help"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_list(self, host, osd_id=None, device_path=None):
        """Module to list logical volumes and devices
        associated with a Ceph cluster
        Args:
            [optional] osd_id: ID of the OSD to list
            [optional] device_path: path of the OSD device to list
            host: (ceph.utils.CephVMNode): CephVMNode
        Returns:
            Returns the output of ceph-volume lvm list
        Example:
            usage without any arguements:
                volumeobject.lvm_list(host=osd_host)
            usage with osd_id:
                volumeobject.lvm_list(osd_id="6",host=osd_host)
            usage with device:
                volumeobject.lvm_list(device_path="/dev/vdb", host=osd_host)
        """
        # Extracting the ceush map from the cluster
        _cmd = "ceph-volume lvm list"
        if osd_id:
            _cmd = f"{_cmd} {osd_id}"
        elif device_path:
            _cmd = f"{_cmd} {device_path}"

        _cmd = f"{_cmd} --format json"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_zap(
        self,
        host,
        destroy=None,
        osd_id=None,
        osd_fsid=None,
        device=None,
        logical_volume=None,
    ):
        """Module to remove all data and file systems from a logical volume or partition.
        Optionally, you can use the --destroy flag for complete removal of a logical volume,
        partition, or the physical device.
        Args:
            [optional] destroy: If destroy is true --destory flag will be added to the command for
            complete removal of logical volume, volume group and physical volume
            [optional] osd_id: ID of the OSD to zap
            [optional] osd_fsid: FSID of the OSD to zap
            [optional] device: Device path of the OSD to zap
            [optional] logical_volume: `volume_group/logical_volume` of the logical_volume to zap
        Returns:
            Returns the output of ceph-volume lvm zap
        Example:
            usage with device path:
                volumeobject.lvm_zap(device="/dev/vdb", destory=False, host=osd_host)
            usage with osd-id:
                volumeobject.lvm_zap(osd_id="6", destroy=True, host=osd_host)
            usage with osd-fsid:
                volumeobject.lvm_zap(osd_fsid="5cf86425-bd5b-4526-9961-a7d2286ab973", destroy=True, host=osd_host)
            usage with logical_volume:
                *Note: volume_group has to be passed along with logical_volume in format volume_group/logical_volume.
                volumeobject.lvm_zap(logical_volume="ceph-7fe80d7f-893e-43fa-82e0-29f3bb417c94/osd-block-82474a14-a79a-4288-8727-8def64745c6d",
                host=osd_host)
        """
        _cmd = "ceph-volume lvm zap"
        if device:
            _cmd = f"{_cmd} {device}"

        if osd_id:
            _cmd = f"{_cmd} --osd-id {osd_id}"

        if osd_fsid:
            _cmd = f"{_cmd} --osd-fsid {osd_fsid}"

        if destroy:
            _cmd = f"{_cmd} --destroy"

        if logical_volume:
            _cmd = f"{_cmd} {logical_volume}"

        log.info(f"Proceeding to zap using command: {_cmd}")
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_prepare(self, device: str, host):
        """Module to prepare volumes, partitions or physical devices.
        The prepare subcommand prepares an OSD back-end object store
        and consumes logical volumes (LV) for both the OSD data and journal.
        Args:
            destory
        Returns:
            Returns the output of ceph-volume lvm prepare
        Example:
            volumeobject.lvm_prepare("/dev/vdb", osd_host)
        """
        _cmd = f"ceph-volume lvm prepare --bluestore --data {device}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_activate(self, osd_id: int, osd_fsid: str, all: bool, host):
        """Module to activate OSD. The activation process for a Ceph OSD enables
        a systemd unit at boot time, which allows the correct OSD identifier and
        its UUID to be enabled and mounted.
        Args:
            osd_id: ID of OSD to activate
            osd_fsid: FSID of OSD to activate
            all: activate all OSD that are prepared for activation by passing --all flag
        Returns:
            Returns the output of ceph-volume lvm activate
        Example:
            volumeobject.activate(osd_id="1",osd_fsid="7ce687d9-07e7-4f8f-a34e-d1b0efb89921", osd_host)
            or
            volumeobject.lvm_activate(all=True, osd_host)
        """
        _cmd = "ceph-volume lvm activate"
        if all:
            _cmd = f"{_cmd} --all"
        else:
            _cmd = f"{_cmd} --bluestore {osd_id} {osd_fsid}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_deactivate(self, osd_id: int, host):
        """Module to remove the volume groups and the logical volume
        Args:
            osd_id: ID of the OSD to deactivate
        Returns:
            Returns the output of ceph-volume lvm deactivate
        Example:
            volumeobject.lvm_deactivate(osd_id="1", osd_host)
        """
        _cmd = f"ceph-volume lvm deactivate {osd_id}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_batch(self, devices: list, host):
        """Module to automate the creation of multiple OSDs
        Args:
            devices: list of devices for OSD creation
        Returns:
            Returns the output of ceph-volume lvm batch
        """
        _cmd = "ceph-volume lvm batch --bluestore"
        for device in devices:
            _cmd = f"{_cmd} {device}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_migrate(
        self, osd_id: int, osd_fsid: str, host, from_flag_list: list, target: str
    ):
        """Module to migrate the BlueStore file system (BlueFS) data (RocksDB data)
        from the source volume to the target volume.
        Args:
            from: arguement for --from flag. list can contain "data" "db" "wal"
            osd_id: ID of the source OSD for data migration
            osd_fsid: FSID of the source OSD for data migration
            target: target device/logical volume for data migration
        Returns:
            Returns the output of ceph-volume lvm migrate
        """
        _cmd = f"ceph-volume lvm migrate --osd-id {osd_id} --osd-fsid {osd_fsid} --from"
        for device in from_flag_list:
            _cmd = f"{_cmd} {device}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_create(self, path: str, host):
        """Module to call the prepare subcommand, and then calls the activate subcommand.
        Args:
            path: path can be device path (/dev/vdb) or logical volume (volume_group/logical_volume)
        Returns:
            Returns the output of ceph-volume lvm create
        """
        _cmd = f"ceph-volume lvm create --bluestore --data {path}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_new_db(
        self, host, osd_id: str, osd_fsid: str, logical_volume: str, volume_group: str
    ):
        """Module to attach the logical volume to OSD as DB device
        Args:
            osd_id: ID of the OSD to add new DB device
            osd_fsid: FSID of the OSD to add new DB device
            logical_volume: logical_volume to be added as DB device
            volume_group: volume_group of the logical volume
        Returns:
            Returns the output of ceph-volume lvm new-db
        Usage:
            volumeobject.lvm_new_db(host=osd_host, osd-id="1",
              osd-fsid="7ce687d9-07e7-4f8f-a34e-d1b0efb89921, "volumegroup/logicalvolume)
        """
        _cmd = f"ceph-volume lvm new-db \
            --osd-id {osd_id} --osd-fsid {osd_fsid} --target {volume_group}/{logical_volume}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def lvm_new_wal(
        self, host, osd_id: str, osd_fsid: str, logical_volume: str, volume_group: str
    ):
        """Module to attach the logical volume to OSD as WAL device
        Args:
            osd_id: ID of the OSD to add new WAL device
            osd_fsid: FSID of the OSD to add new WAL device
            logical_volume: logical_volume to be added as WAL device
            volume_group: volume_group of the logical volume
        Returns:
            Returns the output of ceph-volume lvm new-wal
        Usage:
            volumeobject.lvm_new_wal(host=osd_host, osd-id="1",
              osd-fsid="7ce687d9-07e7-4f8f-a34e-d1b0efb89921, "volumegroup/logicalvolume)
        """
        _cmd = f"ceph-volume lvm new-wal --osd-id {osd_id} \
            --osd-fsid {osd_fsid} --target {volume_group}/{logical_volume}"
        return self.run_cv_command(cmd=_cmd, host=host)

    def osd_id_or_device_exists_in_lvm_list(
        self, osd_host, osd_id, device_path
    ) -> bool:
        """Module to check if OSD ID or device path entry exists in `ceph-volume lvm list` command output
        Args:
            volumeobject: CephVolumeWorkflows object
            osd_host: Host to perform check
            osd_id: OSD ID, example: 0
            device_path: device path, example: /dev/vdb
        Usage:
            check_osd_id_or_device_exists_in_lvm_list(volumeobject, osd_host, osd_id="0", device_path="/dev/vdb")
        Returns:
            Fail -> False
            Pass -> True
        """

        out = self.lvm_list(host=osd_host)

        try:
            ceph_volume_lvm_list = json.loads(out)
        except json.JSONDecodeError as e:
            log.error(f"Error decoding ceph-volume lvm list command {e}")
            raise Exception(
                "Error Deserialising output of `ceph-volume lvm list` into python object"
            )

        cephvolume_lvm_list_osd_ids = [osd_id for osd_id in ceph_volume_lvm_list]
        cephvolume_lvm_list_devices = [
            ceph_volume_lvm_list[osd_id][0]["devices"][0]
            for osd_id in ceph_volume_lvm_list
        ]

        log.info(
            f"OSD ID's in `ceph-volume lvm list` output: {cephvolume_lvm_list_osd_ids}\n"
            f"Devices in `ceph-volume lvm list` output: {cephvolume_lvm_list_devices}"
        )

        if (
            osd_id in cephvolume_lvm_list_osd_ids
            or device_path in cephvolume_lvm_list_devices
        ):
            log.info(
                f"`ceph-volume lvm list` output contains device path: {device_path}\n"
                f"`ceph-volume lvm list` output contains OSD ID: {ceph_volume_lvm_list}"
            )
            return True

        log.info(
            f"`ceph-volume lvm list` output does not contains device path: {device_path}\n"
            f"`ceph-volume lvm list` output does not contains OSD ID: {ceph_volume_lvm_list}"
        )
        return False

    def filesystem_exists_in_logical_volume(
        self, osd_host, osd_id, osd_lv_name, osd_vg_name
    ):
        """Module to check if Filesystem exists on logical volume.
        Uses command `lsblk --fs /dev/osd_volume_group_name/osd_logical_volume_name --json`.
        Args:
            osd_host: CephNode object of OSD host
            osd_id: ID of OSD to check
            osd_lv_name: name of logical volume associated with osd_id
            osd_vg_name: name of physical volume associated with osd_id
        Returns:
            True -> File system exists
            False -> File system does not exist
        Usage:
            volumeobject.filesystem_exists_in_logical_volume(osd_host=osd_host, osd_id=osd_id)
        """
        lvm_list = self.lvm_list(host=osd_host, osd_id=osd_id)
        try:
            lvm_list = json.loads(lvm_list)
        except json.JSONDecodeError as e:
            raise Exception(
                f"Error Deserialising output of `ceph-volume lvm list` into python object: {e}"
            )

        log.info(
            "Successfully retrieved logical volume and volume group "
            f"name for OSD {osd_id} on host {osd_host.hostname}\n"
            f"Logical volume name: {osd_lv_name}\n"
            f"Volume group name: {osd_vg_name}\n"
            "Proceeding to check if file system exists on the OSD device"
        )

        cmd = f"lsblk --fs /dev/{osd_vg_name}/{osd_lv_name} --json"
        out, _ = osd_host.exec_command(
            sudo=True,
            cmd=cmd,
        )

        try:
            out = json.loads(out)
        except json.JSONDecodeError as e:
            raise Exception(
                f"Error Deserialising output of `{cmd}` into python object: {e}"
            )

        file_system = out["blockdevices"][0]["fstype"]

        log.info(
            f"Filesystem on OSD {osd_id} on host {osd_host.hostname}: {file_system}"
        )

        if file_system:
            return True

        return False

    def get_osd_lvm_resource_name(self, osd_host, osd_id: str, resource: str) -> str:
        """Module to retrieve name of logical volume/volume group associated with OSD.
        Uses command `ceph-volume lvm list` to fetch name of logical volume/volume group.
        Args:
            osd_host: CephNode object of OSD host
            osd_id: ID of the OSD to retrieve name of logical volume/volume group
            resource: lv/vg
        Returns:
            If "lv" is passed as resource -> name of logical volume associated with OSD on host osd_host
            If "vg" is passed as resource -> name of volume group associated with OSD on host osd_host
        Usage:
            Name of logical volume:
                volumeobject.get_osd_lvm_resource_name(osd_host=osd_host, osd_id=osd_id, resource="lv")
            Name of the volume group:
                volumeobject.get_osd_lvm_resource_name(osd_host=osd_host, osd_id=osd_id, resource="vg")
        """
        lvm_list = self.lvm_list(host=osd_host, osd_id=osd_id)
        try:
            lvm_list = json.loads(lvm_list)
        except json.JSONDecodeError as e:
            raise Exception(
                f"Error Deserialising output of `ceph-volume lvm list` into python object: {e}"
            )

        return lvm_list[str(osd_id)][0][f"{resource}_name"]
