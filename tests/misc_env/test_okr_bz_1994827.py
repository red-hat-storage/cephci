import json
from datetime import datetime, timedelta
from time import sleep

from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)


class PGStateError(Exception):
    pass


def find_osd_by_id(osd_id, mon_node):
    """Find OSD by OSD.Id using 'ceph osd find {id}'

    Args:
        osd_id: osd id
        mon_node: mon_node to execute ceph commands

    Returns:
        osd_info
    """
    out, _ = mon_node.exec_command(
        cmd=f"ceph osd find {osd_id} --format json",
        sudo=True,
    )
    return json.loads(out)


def systemctl(node, op, unit):
    """Execute systemctl command operation(op) on specific service unit.

    Args:
        node: node to execute command
        op: systemctl supported action (ex., start|stop|enable|disable..,)
        unit: unit service-name

    Returns:
        out, err

    """
    cmd_ = f"systemctl {op} {unit}.service"
    out, err = node.exec_command(cmd=cmd_, sudo=True)
    log.info(out)
    return out, err


def down_osd_with_umount(osd_id, ceph_cluster):
    """Make OSD down by un-mounting OSD device path.

    - Find NODE where OSD resides
    - Stop OSD service using systemctl "sudo systemctl stop ceph-osd@{id}.service".
    - Disable Daemon "sudo systemctl disable ceph-osd{id}.service".
    - Find mount path using ceph OSD Id.
    - umount OSD device path.
    - Validate mount is been removed.

    Args:
        osd_id: OSD id
        ceph_cluster: ceph cluster object
    """
    mon_node = ceph_cluster.get_ceph_object("mon")
    osd_info = find_osd_by_id(osd_id, mon_node)

    # find OSD node
    osd_node = get_node_by_id(ceph_cluster, osd_info["host"])

    # Stop and disable OSD daemon
    systemctl(osd_node, "stop", f"ceph-osd@{osd_id}")
    systemctl(osd_node, "disable", f"ceph-osd@{osd_id}")

    # umount OSD device
    osd_node.exec_command(
        cmd=f"umount /var/lib/ceph/osd/ceph-{osd_id}",
        sudo=True,
    )
    osd_node.exec_command(
        cmd="mount | grep ceph",
        sudo=True,
    )


def wait_for_active_clean_pgs(mon_node, timeout=3600):
    """Wait for PGs to be in active+clean state.

    - Monitor OSD PG status.
    - Loop 1st step until all PGs comes to active+clean state.

    Args:
        mon_node: Monitor node to run ceph commands.
        timeout: timeout duration in seconds
    """
    end_time = datetime.now() + timedelta(seconds=timeout)

    while end_time > datetime.now():
        sleep(10)
        out, _ = mon_node.exec_command(
            cmd="ceph pg stat --format json",
            sudo=True,
        )
        out = json.loads(out.strip())
        pg_smry = out.get("pg_summary")
        log.info(f"PG STATS : {pg_smry}")

        active_clean_pgs = 0
        for pg_state in pg_smry.get("num_pg_by_state"):
            if pg_state["name"] == "active+clean":
                active_clean_pgs = pg_state["num"]
                break

        if pg_smry["num_pgs"] != active_clean_pgs:
            continue
        return
    else:
        raise PGStateError("Not all PGs are in active+clean state")


def run(ceph_cluster, **kw):
    """Down the OSD and monitor the PGs state to be active_clean.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    config = kw.get("config")
    osd_id = config.get("osd_id")
    down_osd_with_umount(osd_id, ceph_cluster)
    wait_for_active_clean_pgs(ceph_cluster.get_ceph_object("mon"))
    return 0
