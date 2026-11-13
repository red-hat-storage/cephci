"""Exercise Ceph-volume metadata and a cephadm-managed OSD on unused disks."""

import json
import sys
import time
from contextlib import contextmanager
from shlex import quote
from uuid import UUID, uuid4

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import reboot_node
from tests.ceph_volume.utils import ceph_volume_shell, get_available_devices
from utility.log import Log

log = Log(__name__)


def command(node, cmd):
    """Run a checked command without depending on CLI wrapper flag defaults."""
    out, _ = node.exec_command(cmd=cmd, sudo=True, check_ec=True, timeout=300)
    return out


def ceph(node, args):
    return command(node, f"cephadm shell -- ceph {args}")


@contextmanager
def monitor_setting(node, name):
    """Temporarily permit a fixture operation and restore the prior config entry."""
    entries = json.loads(ceph(node, "config dump --format json"))
    previous = [
        entry
        for entry in entries
        if entry.get("section") == "mon"
        and entry.get("name") == name
        and not entry.get("mask")
    ]
    try:
        ceph(node, f"config set mon {name} true")
        yield
    finally:
        if previous:
            ceph(node, f"config set mon {name} {quote(str(previous[0]['value']))}")
        else:
            ceph(node, f"config rm mon {name}")


def volume(node, args, mounts=()):
    return command(node, f"{ceph_volume_shell(mounts=mounts)} ceph-volume {args}")


def wait_for(check, description, timeout=300):
    """Fail explicitly when the expected state never becomes observable."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except Exception as error:
            last_error = type(error).__name__
        time.sleep(5)
    if last_error:
        log.error(f"Last polling error while waiting for {description}: {last_error}")
    raise OperationFailedError(f"Timed out waiting for {description}")


def inventory_device(node, path, available):
    """Validate the documented single-device inventory and eligibility state."""
    record = json.loads(volume(node, f"inventory {quote(path)} --format json"))
    if isinstance(record, list) and len(record) == 1:
        record = record[0]
    if not isinstance(record, dict) or record.get("path") != path:
        raise OperationFailedError(f"Incorrect inventory record for {path}")
    if record.get("available") is not available:
        raise OperationFailedError(f"Unexpected availability for {path}")
    if not isinstance(record.get("rejected_reasons"), list):
        raise OperationFailedError(f"Missing rejection reasons for {path}")
    if available == bool(record["rejected_reasons"]):
        raise OperationFailedError(f"Inconsistent rejection reasons for {path}")
    if record.get("sys_api", {}).get("size", 0) <= 0:
        raise OperationFailedError(f"Missing device size for {path}")
    return record


class Lifecycle:
    """Own only disks that were unused at entry and resources created by this case."""

    def __init__(self, node, config):
        self.node = node
        self.config = config
        self.name = f"cephci-volume-{uuid4().hex[:10]}"
        self.directory = f"/tmp/{self.name}"
        self.disks = []
        self.osd_id = None
        self.osd_fsid = None
        self.deployed = False
        self.pool_created = False
        self.rule_created = False
        self.crush_created = False
        self.original_ids = set(json.loads(ceph(node, "osd ls --format json")))
        self.cluster_fsid = ceph(node, "fsid").strip()

    def metadata(self, selector):
        selector = quote(str(selector)) if selector != "" else ""
        return json.loads(volume(self.node, f"lvm list {selector} --format json"))

    def validate_metadata(self, expected, encrypted):
        mapping = self.metadata(self.disks[0])
        if len(mapping) != 1:
            raise OperationFailedError("Expected exactly one OSD on the test data disk")
        osd_id, entries = next(iter(mapping.items()))
        if int(osd_id) in self.original_ids:
            raise OperationFailedError("The test selected a pre-existing OSD")
        self.osd_id = osd_id
        device_entries = entries
        mapping = self.metadata(osd_id)
        entries = mapping.get(osd_id, [])
        if any(entry not in entries for entry in device_entries):
            raise OperationFailedError("Device and OSD-ID list filters disagree")
        blocks = [entry for entry in entries if entry.get("type") == "block"]
        if len(blocks) != 1:
            raise OperationFailedError("Expected exactly one data LV")
        tags = blocks[0].get("tags", {})
        fsid = tags.get("ceph.osd_fsid", "")
        UUID(fsid)
        if self.osd_fsid and fsid != self.osd_fsid:
            raise OperationFailedError("OSD FSID changed during the lifecycle")
        self.osd_fsid = fsid
        if set(entry.get("type") for entry in entries) != set(expected):
            raise OperationFailedError("Unexpected data/DB/WAL layout")
        for role, device in expected.items():
            matches = [entry for entry in entries if entry.get("type") == role]
            if len(matches) != 1:
                raise OperationFailedError(f"Expected exactly one {role} LV")
            entry = matches[0]
            tags = entry.get("tags", {})
            if device.startswith("/dev/") and device in self.disks:
                correct_device = set(entry.get("devices", [])) == {device}
            else:
                correct_device = entry.get("lv_path") == device
            if not correct_device:
                raise OperationFailedError(f"Incorrect {role} device mapping")
            if (
                str(tags.get("ceph.osd_id")) != osd_id
                or tags.get("ceph.osd_fsid") != fsid
                or tags.get("ceph.cluster_fsid") != self.cluster_fsid
                or str(tags.get("ceph.encrypted")) != str(int(encrypted))
            ):
                raise OperationFailedError(
                    f"Incorrect OSD identity/encryption tags for {role}"
                )
            if encrypted and str(tags.get("ceph.encrypted")) not in (
                "1",
                "true",
                "True",
            ):
                raise OperationFailedError(
                    f"Encrypted {role} LV is missing the ceph.encrypted tag"
                )
        by_lv = self.metadata(blocks[0]["lv_path"])
        if by_lv != {osd_id: blocks}:
            raise OperationFailedError("LV filter does not return the expected data LV")
        pretty = volume(self.node, f"lvm list {osd_id}")
        if fsid not in pretty:
            raise OperationFailedError("Pretty LVM list does not identify the test OSD")
        inventory_device(self.node, self.disks[0], False)
        return mapping

    def up(self):
        rows = json.loads(ceph(self.node, "osd dump --format json"))["osds"]
        return any(
            str(row["osd"]) == self.osd_id
            and row.get("uuid") == self.osd_fsid
            and row["up"] == row["in"] == 1
            for row in rows
        )

    def check_io(self, write=False):
        shell = f"cephadm shell --mount {self.directory}:/cephci --"
        if write:
            command(self.node, f"head -c 1048576 /dev/urandom > {self.directory}/input")
            command(self.node, f"{shell} rados -p {self.name} put proof /cephci/input")
        placement = json.loads(
            ceph(self.node, f"osd map {self.name} proof --format json")
        )
        if placement.get("acting") != [int(self.osd_id)]:
            raise OperationFailedError("The I/O object is not placed on the test OSD")
        command(self.node, f"{shell} rados -p {self.name} get proof /cephci/output")
        command(self.node, f"cmp {self.directory}/input {self.directory}/output")

    def run(self):
        layout = self.config.get("layout", "data")
        roles = {
            "data": [],
            "data-db": ["db"],
            "data-wal": ["wal"],
            "data-db-wal": ["db", "wal"],
        }
        if layout not in roles:
            raise ConfigError(f"Unsupported layout: {layout}")
        mode = self.config.get("operation", "prepare")
        if mode not in ("prepare", "create", "batch"):
            raise ConfigError("operation must be prepare, create or batch")
        precreated = self.config.get("precreated_lvs", False)
        encrypted = self.config.get("encrypted", False)
        count = 1 if precreated else 1 + len(roles[layout])
        self.disks = get_available_devices(self.node)[:count]
        if len(self.disks) != count:
            raise ConfigError(f"This layout requires {count} unused disks")
        for disk in self.disks:
            inventory_device(self.node, disk, True)
        command(self.node, f"install -d -m 700 {self.directory}")
        # Create the bootstrap keyring directly in the private mounted directory.
        command(
            self.node,
            f"cephadm shell --mount {self.directory}:/cephci -- ceph auth get client.bootstrap-osd -o /cephci/keyring",
        )
        command(self.node, f"chmod 600 {self.directory}/keyring")
        mounts = (f"{self.directory}/keyring:/var/lib/ceph/bootstrap-osd/ceph.keyring",)
        expected = {"block": self.disks[0]}
        expected.update(zip(roles[layout], self.disks[1:]))
        if precreated:
            command(self.node, f"vgcreate {self.name} {quote(self.disks[0])}")
            expected = {}
            for role in ["block", *roles[layout]]:
                size = "8G" if role == "block" else "2G"
                command(self.node, f"lvcreate -y -L {size} -n {role} {self.name}")
                expected[role] = f"/dev/{self.name}/{role}"
        args = f"lvm {mode} --bluestore --data {quote(expected['block'])}"
        for role in roles[layout]:
            args += f" --block.{role} {quote(expected[role])}"
        if encrypted:
            args += " --dmcrypt"
        if mode == "create":
            args += " --no-systemd"
        if mode == "batch":
            if precreated or layout != "data" or encrypted:
                raise ConfigError(
                    "The batch case requires one unencrypted raw data disk"
                )
            args = f"lvm batch --no-auto --no-systemd {quote(self.disks[0])}"
            before = self.metadata("")
            before_lvs = command(
                self.node,
                "lvs --devicesfile '' --reportformat json -o lv_uuid,lv_size,devices",
            )
            report = json.loads(
                volume(self.node, f"{args} --report --format json", mounts)
            )
            if (
                not isinstance(report, list)
                or len(report) != 1
                or report[0].get("data") != self.disks[0]
            ):
                raise OperationFailedError(
                    "Batch report does not describe the requested OSD"
                )
            pretty = volume(self.node, f"{args} --report", mounts)
            if self.disks[0] not in pretty:
                raise OperationFailedError("Batch pretty report omits the data device")
            if (
                self.metadata("") != before
                or command(
                    self.node,
                    "lvs --devicesfile '' --reportformat json -o lv_uuid,lv_size,devices",
                )
                != before_lvs
            ):
                raise OperationFailedError("Batch report modified LVM state")
            inventory_device(self.node, self.disks[0], True)
            args += " --yes"
        volume(self.node, args, mounts)
        self.validate_metadata(expected, encrypted)
        if mode == "batch":
            before = self.metadata(self.osd_id)
            volume(self.node, args, mounts)
            if self.metadata(self.disks[0]) != before:
                raise OperationFailedError(
                    "Repeated batch changed the OSD identity or layout"
                )
        log.info(
            f"Validated {mode} with {layout}, encrypted={encrypted}, precreated_lvs={precreated}"
        )
        # Repeated activation must preserve both the identity and the LV layout.
        before = self.metadata(self.osd_id)
        for _ in range(2):
            volume(
                self.node,
                f"lvm activate --no-systemd {self.osd_id} {self.osd_fsid}",
                mounts,
            )
        if self.metadata(self.osd_id) != before:
            raise OperationFailedError("Repeated activation changed the OSD metadata")
        volume(self.node, f"lvm deactivate {self.osd_id} {self.osd_fsid}")
        # An isolated CRUSH root prevents this disposable OSD from receiving
        # objects belonging to existing pools. Its own pool targets only this OSD.
        location = f"root={self.name} host={self.name}-host"
        ceph(
            self.node, f"config set osd.{self.osd_id} crush_location {quote(location)}"
        )
        self.crush_created = True
        ceph(self.node, f"osd crush add osd.{self.osd_id} 0.01 {location}")
        self.deployed = True
        ceph(self.node, f"cephadm osd activate {quote(self.node.hostname)}")
        wait_for(self.up, f"OSD {self.osd_id} up and in")
        deployed_fsid = command(
            self.node, f"cat /var/lib/ceph/{self.cluster_fsid}/osd.{self.osd_id}/fsid"
        ).strip()
        if deployed_fsid != self.osd_fsid:
            raise OperationFailedError(
                "The deployed daemon belongs to a different OSD FSID"
            )
        ceph(
            self.node, f"osd crush rule create-replicated {self.name} {self.name} host"
        )
        self.rule_created = True
        ceph(self.node, f"osd pool create {self.name} 1 1 replicated {self.name}")
        self.pool_created = True
        with monitor_setting(self.node, "mon_allow_pool_size_one"):
            ceph(self.node, f"osd pool set {self.name} size 1 --yes-i-really-mean-it")
        ceph(self.node, f"osd pool set {self.name} min_size 1")
        ceph(self.node, f"osd pool application enable {self.name} rados")
        self.check_io(write=True)
        if self.config.get("reboot", True):
            boot_id = command(self.node, "cat /proc/sys/kernel/random/boot_id").strip()
            if not reboot_node(self.node):
                raise OperationFailedError("Test host did not recover after reboot")
            if (
                command(self.node, "cat /proc/sys/kernel/random/boot_id").strip()
                == boot_id
            ):
                raise OperationFailedError(
                    "The host reconnected without completing a reboot"
                )
            wait_for(self.up, f"OSD {self.osd_id} recovery after reboot")
            self.validate_metadata(expected, encrypted)
            self.check_io()
        log.info(
            f"OSD {self.osd_id} served verified I/O with the expected device layout"
        )

    def cleanup(self):
        """Remove owned resources even when preparation or activation failed."""
        errors = []

        def attempt(action):
            try:
                action()
            except Exception as error:
                errors.append(type(error).__name__)
                log.error(f"Lifecycle cleanup failed: {type(error).__name__}")

        if self.pool_created:

            def remove_pool():
                with monitor_setting(self.node, "mon_allow_pool_delete"):
                    ceph(
                        self.node,
                        f"osd pool delete {self.name} {self.name} --yes-i-really-really-mean-it",
                    )

            attempt(remove_pool)
        # Discover a partially prepared OSD if the prepare command failed late.
        if self.disks and self.osd_id is None:

            def discover():
                mapping = self.metadata(self.disks[0])
                ids = [
                    value for value in mapping if int(value) not in self.original_ids
                ]
                if len(ids) == 1:
                    self.osd_id = ids[0]

            attempt(discover)
        if self.osd_id is not None:
            if self.deployed:
                attempt(lambda: ceph(self.node, f"orch daemon stop osd.{self.osd_id}"))
                attempt(
                    lambda: command(
                        self.node,
                        f"cephadm rm-daemon --fsid {self.cluster_fsid} "
                        f"--name osd.{self.osd_id} --force --force-delete-data",
                    )
                )
            if self.deployed:

                def removed():
                    daemons = json.loads(command(self.node, "cephadm ls"))
                    return not any(
                        daemon.get("fsid") == self.cluster_fsid
                        and daemon.get("name") == f"osd.{self.osd_id}"
                        for daemon in daemons
                    )

                attempt(lambda: wait_for(removed, "test daemon removal"))
                attempt(lambda: ceph(self.node, "orch ps --refresh --format json"))
                attempt(lambda: wait_for(lambda: not self.up(), "test OSD to stop"))
            attempt(
                lambda: ceph(
                    self.node, f"osd purge {self.osd_id} --yes-i-really-mean-it"
                )
            )
            attempt(
                lambda: ceph(self.node, f"config rm osd.{self.osd_id} crush_location")
            )
        for disk in self.disks:
            attempt(
                lambda disk=disk: volume(self.node, f"lvm zap {quote(disk)} --destroy")
            )
        if self.rule_created:
            attempt(lambda: ceph(self.node, f"osd crush rule rm {self.name}"))
        if self.crush_created:
            attempt(lambda: ceph(self.node, f"osd crush remove {self.name}-host"))
            attempt(lambda: ceph(self.node, f"osd crush remove {self.name}"))
        attempt(lambda: command(self.node, f"rm -rf -- {self.directory}"))
        for disk in self.disks:
            attempt(lambda disk=disk: inventory_device(self.node, disk, True))

            def no_metadata(disk=disk):
                if self.metadata(disk):
                    raise OperationFailedError("OSD metadata remains after zap")

            attempt(no_metadata)
        if errors:
            raise OperationFailedError(f"Lifecycle cleanup had {len(errors)} failures")


def run(ceph_cluster, **kw):
    """Validate a configured layout, real OSD I/O, recovery and device reuse."""
    node = ceph_cluster.get_nodes(role="installer")[0]
    case = Lifecycle(node, kw.get("config") or {})
    try:
        case.run()
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            case.cleanup()
        except Exception:
            if not failed:
                raise
            log.exception("Cleanup failed after a lifecycle failure")
    return 0
