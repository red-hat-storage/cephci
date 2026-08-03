"""
IBMCEPH-15679 — staggered upgrade must accept --daemon-types nvmeof.

Uses the cluster's current image and attempts:
  ceph orch upgrade start --image <current> --daemon-types nvmeof

Pass criteria: command is NOT rejected with "unexpected daemon type" / unsupported
nvmeof. If upgrade actually starts, stop it. Same-image / no-op outcomes are OK.
"""

import json
import re
import time

from ceph.ceph import Ceph
from utility.log import Log

LOG = Log(__name__)

_UNEXPECTED = re.compile(
    r"unexpected daemon type|not a valid daemon type|Viable daemon types",
    re.IGNORECASE,
)


def _current_image(installer):
    out, _ = installer.exec_command(
        cmd="ceph orch ps --daemon_type mgr --format json", sudo=True
    )
    rows = json.loads(out or "[]")
    for row in rows:
        img = row.get("container_image_id") or row.get("container_image_name")
        if row.get("container_image_name"):
            return row["container_image_name"]
        if img:
            return img
    out2, _ = installer.exec_command(
        cmd="ceph orch upgrade status --format json", sudo=True, check_ec=False
    )
    try:
        st = json.loads(out2 or "{}")
        if st.get("target_image"):
            return st["target_image"]
    except Exception:
        pass
    raise RuntimeError("Unable to determine current container image for upgrade probe")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    image = config.get("upgrade_image")
    try:
        if not image:
            image = _current_image(installer)
        LOG.info("Probing --daemon-types nvmeof with image=%s", image)

        cmd = f"ceph orch upgrade start --image {image} --daemon-types nvmeof"
        # Do not raise on non-zero — we classify the stderr/stdout ourselves
        out, err = installer.exec_command(cmd=cmd, sudo=True, check_ec=False)
        combined = f"{out or ''}\n{err or ''}"
        LOG.info("upgrade start output:\n%s", combined)

        if _UNEXPECTED.search(combined) and "nvmeof" in combined.lower():
            raise RuntimeError(
                "IBMCEPH-15679: nvmeof still rejected by --daemon-types: "
                + combined.strip()
            )

        # If an upgrade is in progress, stop it (probe only)
        time.sleep(int(config.get("post_start_settle", 5)))
        try:
            st_out, _ = installer.exec_command(
                cmd="ceph orch upgrade status --format json", sudo=True
            )
            st = json.loads(st_out or "{}")
            if st.get("in_progress") or st.get("status", {}).get("in_progress"):
                LOG.info("Stopping probe upgrade")
                installer.exec_command(cmd="ceph orch upgrade stop", sudo=True)
        except Exception as stop_err:
            LOG.warning("upgrade status/stop best-effort: %s", stop_err)

        LOG.info("--daemon-types nvmeof accepted (or non-rejection path)")
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
