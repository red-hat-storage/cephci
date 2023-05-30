import json

from ceph.utils import get_nodes_by_ids
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, UnexpectedStateError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test OSD memory Target scenario

    Args:
        host (str): Host on which the Memory target to be set
        value (int) : Value of memory target
    """
    config = kw["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(installer)

    # Set value of osd memory target autotune to False
    cephadm.ceph.config.set(
        key="osd_memory_target_autotune", value="false", daemon="osd"
    )
    log.info("OSD Memory autotuning is now set to False")

    # Set the memory target at the host level
    host = config.get("hosts")
    value = config.get("value")
    node = get_nodes_by_ids(ceph_cluster, host)
    hostname = node[0].shortname
    log.info("Setting the memory target on host")
    cephadm.ceph.config.set(
        key="osd_memory_target", value=value, daemon="osd/host:{hostname}"
    )
    log.info(f"Memory target is set on host '{hostname}' : '{value}'")

    # Verify that the option is set in ceph config dump
    kw = {"format": "json-pretty"}
    out = cephadm.ceph.config.dump(**kw)
    data = json.loads(out[0])
    found = False
    for data_config in data:
        if data_config["name"] == "osd_memory_target" and data_config["value"] == str(
            value
        ):
            found = True
            break
    if not found:
        raise ConfigError(
            "osd_memory_target not found or has an incorrect value in ceph config"
        )
    log.info("osd memory target is validated in ceph config")

    # Verify osd memory target for osd on the host matches the host level value
    out = cephadm.ceph._osd.tree(**kw)
    data = json.loads(out[0])
    for item in data["nodes"]:
        if item.get("type") == "host" and item.get("name") == hostname:
            osd_list = item["children"]
            osd_id = f"osd.{str(osd_list[0])}"
            out = cephadm.ceph.config.get(who="osd_id", key="osd_memory_target")
            if out[0].strip() not in str(value):
                raise UnexpectedStateError(
                    f"osd memory target for '{osd_id}' doesnot match host value '{value}' "
                )
            log.info(
                f"osd memory target for '{osd_id}' matches the host "
                f"level value : '{value}'"
            )
            return 0
