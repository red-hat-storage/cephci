"""Regress IBMCEPH-13408 and IBMCEPH-16294 inventory crashes on hidden LVM LVs.

Create a disposable loop-backed thin pool (default) or RAID1 LV, verify its
hidden internal LVs, and require successful plain and JSON inventory scans.
"""

import json
import re
from collections.abc import Mapping
from uuid import uuid4

from cli.cephadm.cephadm import ceph_volume_shell
from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume.utils import parse_inventory_json
from utility.log import Log

log = Log(__name__)
LOOP_PATH = re.compile(r"/dev/loop[0-9]+")
SHELL = ceph_volume_shell(env=("CEPH_VOLUME_ALLOW_LOOP_DEVICES=1",))


def _execute(node, command):
    return node.exec_command(cmd=command, sudo=True, check_ec=True)


def _report_rows(output, section):
    """Reject incomplete LVM reports instead of treating them as absent devices."""
    try:
        report = json.loads(output)["report"]
        if not isinstance(report, list) or not report:
            raise ValueError("missing report entries")
        rows = []
        for item in report:
            records = item[section]
            if not isinstance(records, list) or not all(
                isinstance(record, dict) for record in records
            ):
                raise ValueError(f"invalid {section} records")
            rows.extend(records)
        return rows
    except (TypeError, ValueError, KeyError) as error:
        raise OperationFailedError(
            f"Invalid LVM {section} JSON report: {error}"
        ) from error


class _LoopLvmFixture:
    """Track ownership using unique filenames and verified loop backing files."""

    def __init__(self, node, fixture_type):
        self.node = node
        self.fixture_type = fixture_type
        self.namespace = uuid4().hex
        self.vg = f"cephci_inventory_{self.namespace}"
        self.images = []
        self.loops = []
        self.pv_attempted = False
        self.created_pvs = set()

    def _owned_loops(self):
        output, _ = _execute(self.node, "losetup --list --json --output NAME,BACK-FILE")
        try:
            records = json.loads(output)["loopdevices"]
            if not isinstance(records, list):
                raise ValueError("invalid loopdevices records")
            owned = {}
            for record in records:
                if (
                    not isinstance(record, dict)
                    or not isinstance(record.get("name"), str)
                    or not isinstance(record.get("back-file"), str)
                    or not record["name"].strip()
                    or not record["back-file"].strip()
                ):
                    raise ValueError("invalid loop device record")
                if record.get("back-file") in self.images:
                    loop = record.get("name")
                    if not isinstance(loop, str) or not LOOP_PATH.fullmatch(loop):
                        raise ValueError(f"invalid owned loop path: {loop!r}")
                    owned[loop] = record["back-file"]
            return owned
        except (TypeError, ValueError, KeyError) as error:
            raise OperationFailedError(
                f"Invalid loop ownership report: {error}"
            ) from error

    def create(self):
        count = 1 if self.fixture_type == "thin-pool" else 2
        for index in range(count):
            image = f"/var/tmp/cephci-inventory-{self.namespace}-{index}.img"
            # Exclusive creation makes cleanup ownership independent of dd success.
            _execute(self.node, f"(set -C; : > {image})")
            self.images.append(image)
            _execute(self.node, f"dd if=/dev/zero of={image} bs=1M count=1024")
            output, _ = _execute(self.node, f"losetup --find --show {image}")
            loop = output.strip()
            if not LOOP_PATH.fullmatch(loop):
                raise OperationFailedError(f"Invalid allocated loop device: {loop!r}")
            if self._owned_loops().get(loop) != image:
                raise OperationFailedError(
                    f"Allocated loop {loop} is not backed by owned image {image}"
                )
            self.loops.append(loop)
        self.pv_attempted = True
        _execute(self.node, f"pvcreate --yes {' '.join(self.loops)}")
        self.created_pvs = set(self.loops)
        _execute(self.node, f"vgcreate --yes {self.vg} {' '.join(self.loops)}")
        name = "thinpool" if self.fixture_type == "thin-pool" else "mirror_lv"
        mirrors = " --mirrors 1" if self.fixture_type == "raid1" else ""
        _execute(
            self.node,
            f"lvcreate --yes --size 900M --type {self.fixture_type}{mirrors} --name {name} {self.vg}",
        )
        output, _ = _execute(
            self.node,
            f"lvs --all --reportformat json --options vg_name,lv_name,segtype {self.vg}",
        )
        self._validate_lvs(output)

    def verify_container_visibility(self):
        """Require the reproducer inside the same shell context as inventory."""
        output, _ = _execute(
            self.node,
            f"{SHELL} lvs --all --reportformat json "
            f"--options vg_name,lv_name,segtype {self.vg}",
        )
        self._validate_lvs(output)

    def _validate_lvs(self, output):
        """Use identical host and container assertions for parent and hidden LVs."""
        name = "thinpool" if self.fixture_type == "thin-pool" else "mirror_lv"
        rows = _report_rows(output, "lv")
        rows = [row for row in rows if row.get("vg_name", "").strip() == self.vg]
        if not any(
            row.get("lv_name", "").strip() == name
            and row.get("segtype", "").strip() == self.fixture_type
            for row in rows
        ):
            raise OperationFailedError(
                f"Expected {self.fixture_type} LV was not created in {self.vg}"
            )
        hidden_names = (
            {"thinpool_tdata", "thinpool_tmeta"}
            if self.fixture_type == "thin-pool"
            else {
                f"mirror_lv_{kind}_{index}"
                for kind in ("rimage", "rmeta")
                for index in range(2)
            }
        )
        names = {row.get("lv_name", "").strip().strip("[]") for row in rows}
        if not hidden_names.issubset(names):
            raise OperationFailedError(
                f"Missing hidden {self.fixture_type} LVs in {self.vg}: {sorted(hidden_names - names)}"
            )

    def cleanup(self):
        """Remove only owned resources; retain dependents if safe removal fails."""
        errors = []
        if not self.images:
            return errors
        try:
            loops = self._owned_loops()
            pvs = {}
            if self.created_pvs - set(loops):
                raise OperationFailedError(
                    "Created PV loops missing from backing-file ownership report"
                )
            if loops and self.pv_attempted:
                output, _ = _execute(
                    self.node, "pvs --reportformat json --options pv_name,vg_name"
                )
                for row in _report_rows(output, "pv"):
                    if (
                        not isinstance(row.get("pv_name"), str)
                        or not row["pv_name"].strip()
                        or not isinstance(row.get("vg_name"), str)
                    ):
                        raise OperationFailedError("Invalid PV ownership record")
                    pv = row["pv_name"].strip()
                    vg = row["vg_name"].strip()
                    if vg == self.vg and pv not in loops:
                        raise OperationFailedError(
                            f"Refusing cleanup of owned VG {self.vg} with unowned member PV {pv}"
                        )
                    if pv not in loops:
                        continue
                    if vg and vg != self.vg:
                        raise OperationFailedError(
                            f"Refusing cleanup of {pv} in unowned VG {vg}"
                        )
                    pvs[pv] = vg
                missing_pvs = self.created_pvs - set(pvs)
                if missing_pvs:
                    raise OperationFailedError(
                        f"Created PVs missing from ownership report: {sorted(missing_pvs)}"
                    )
                # A failed pvcreate can initialize some devices before failing.
                # Independently inspect omitted devices before treating them as blank.
                for pv in set(self.loops) - set(pvs):
                    output, _ = _execute(self.node, f"wipefs --json --output TYPE {pv}")
                    signatures = json.loads(output)["signatures"]
                    if not isinstance(signatures, list) or signatures:
                        raise OperationFailedError(
                            f"Omitted PV {pv} has signatures or an ambiguous wipefs report"
                        )
                if self.vg in pvs.values():
                    output, _ = _execute(
                        self.node,
                        f"vgs --reportformat json --options vg_name,pv_count {self.vg}",
                    )
                    records = _report_rows(output, "vg")
                    vg_records = [
                        row for row in records if row.get("vg_name") == self.vg
                    ]
                    owned_count = sum(vg == self.vg for vg in pvs.values())
                    if len(vg_records) != 1 or str(
                        vg_records[0].get("pv_count", "")
                    ).strip() != str(owned_count):
                        raise OperationFailedError(
                            f"VG {self.vg} member count does not match owned PVs"
                        )
                    _execute(self.node, f"vgremove --force --yes {self.vg}")
        except Exception as error:
            return [f"Ownership/VG cleanup failed; retained {self.images}: {error}"]
        retained_images = set()
        for loop, image in loops.items():
            try:
                if loop in pvs:
                    _execute(self.node, f"pvremove --force --yes {loop}")
                _execute(self.node, f"losetup --detach {loop}")
            except Exception as error:
                retained_images.add(image)
                errors.append(f"Cleanup failed for {loop}; retained {image}: {error}")
        for image in self.images:
            if image in retained_images:
                continue
            try:
                _execute(self.node, f"rm -- {image}")
            except Exception as error:
                errors.append(f"Cleanup failed for {image}: {error}")
        return errors


def run(ceph_cluster, **kw):
    """Check inventory against a verified thin-pool or RAID1 reproducer."""
    config = kw.get("config", {})
    if not isinstance(config, Mapping):
        raise ConfigError("Inventory LVM fixture config must be a mapping")
    fixture_type = config.get("fixture_type", "thin-pool")
    if fixture_type not in ("thin-pool", "raid1"):
        raise ConfigError("fixture_type must be 'thin-pool' or 'raid1'")
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    if not osd_nodes:
        raise ConfigError("Inventory LVM regression requires an OSD host")
    installers = ceph_cluster.get_nodes(role="installer")
    node = next((node for node in installers if node in osd_nodes), osd_nodes[0])
    fixture = _LoopLvmFixture(node, fixture_type)
    primary_error = None
    try:
        fixture.create()
        fixture.verify_container_visibility()
        for output_format in ("plain", "json"):
            output, stderr = _execute(
                node, f"{SHELL} ceph-volume inventory --format {output_format}"
            )
            combined = f"{output}\n{stderr}".lower()
            if any(
                marker in combined
                for marker in (
                    "traceback (most recent call last)",
                    "segmentation fault",
                    "core dumped",
                )
            ):
                raise OperationFailedError(
                    f"Inventory crashed with {fixture_type}: {combined}"
                )
            if not output.strip():
                raise OperationFailedError(
                    f"Empty {output_format} inventory with {fixture_type}"
                )
            if output_format == "json":
                devices = parse_inventory_json(output, node.hostname)
                missing_loops = set(fixture.loops) - {
                    device["path"] for device in devices
                }
                if missing_loops:
                    raise OperationFailedError(
                        f"Inventory omitted fixture loops: {sorted(missing_loops)}"
                    )
    except Exception as error:
        primary_error = error
        raise
    finally:
        cleanup_errors = fixture.cleanup()
        if cleanup_errors:
            cleanup_error = OperationFailedError("; ".join(cleanup_errors))
            log.error(str(cleanup_error))
            if primary_error is not None:
                raise primary_error from cleanup_error
            raise cleanup_error
    return 0
