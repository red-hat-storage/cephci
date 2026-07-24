import json
from time import time

from utility.log import Log

logger = Log(__name__)


__script = """
import requests
import json

response = requests.get("https://{MGR_NODE}:{MGR_RESTFUL_PORT}/{RELATIVE_URI}",
                        auth=("{USER}", "{PASSWORD}"),
                        verify=False)

status_code = response.status_code
try: json_data = response.json()
except ValueError: json_data = json.text

print(json.dumps(dict([("status_code", status_code), ("json", json_data)])))
"""

ADD_BUCKET_CMD = "ceph osd crush add-bucket {RACK_NAME} rack"
MOVE_OSD_CRUSH_TO_RACK_CMD = "ceph osd crush move {NODE} rack={RACK_NAME}"
MOVE_OSD_RACK_TO_ROOT_CMD = "ceph osd crush move {RACK_NAME} root=default"
CREATE_OSD_CRUSH_RULE = (
    "ceph osd crush rule create-simple {RULE_NAME} default rack firstn"
)
CREATE_OSD_POOL_WITH_RULE = (
    "ceph osd pool create {POOL_RACK_NAME} 64 64 replicated {RULE_NAME}"
)


def exec_cmd_status(ceph_installer, commands):
    """
    Execute command
    Args:
        ceph_installer: installer object to exec cmd
        commands: list of commands to be executed
    Returns:
        Boolean
    """
    for cmd in commands:
        out, err = ceph_installer.exec_command(sudo=True, cmd=cmd)
        out, err = out.strip(), err.strip()
        logger.info("Command Response : {} {}".format(out, err))
    return True


def run(ceph_cluster, **kw):
    """
    ceph-mgr restful call for listing osds
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster object
        kw: test config arguments
    Returns:
        int: non-zero on failure, zero on pass
    """
    ceph_installer = ceph_cluster.get_ceph_object("installer")
    config = kw.get("config")

    # Get all OSD and MGR nodes
    osd_nodes = ceph_cluster.get_nodes("osd")
    mgr_nodes = ceph_cluster.get_nodes("mgr")
    logger.info("Get all OSD nodes : {}".format(osd_nodes))
    osd_node = osd_nodes[0]
    mgr_node = mgr_nodes[0]

    # enable restful service from MGR module with self-signed certificate
    cred = ceph_installer.enable_ceph_mgr_restful()

    # bz-1764919, steps from comment #5
    timestamp = int(time())
    rack = "rack_{}".format(timestamp)
    rule = "rule_{}".format(timestamp)
    pool = "pool_rack_{}".format(timestamp)

    commands = [
        ADD_BUCKET_CMD.format(RACK_NAME=rack),
        MOVE_OSD_CRUSH_TO_RACK_CMD.format(NODE=osd_node.hostname, RACK_NAME=rack),
        MOVE_OSD_RACK_TO_ROOT_CMD.format(RACK_NAME=rack),
        CREATE_OSD_CRUSH_RULE.format(RULE_NAME=rule),
        CREATE_OSD_POOL_WITH_RULE.format(POOL_RACK_NAME=pool, RULE_NAME=rule),
    ]

    exec_cmd_status(ceph_installer, commands)

    file_name = "/tmp/{}.py".format(timestamp)

    # Restful call to list OSD tree
    script = __script.format(
        MGR_NODE=mgr_node.hostname,
        MGR_RESTFUL_PORT=config.get("mgr_restful_port"),
        RELATIVE_URI=config.get("relative_uri"),
        USER=cred["user"],
        PASSWORD=cred["password"],
    )

    script_file = ceph_installer.remote_file(
        sudo=True, file_name=file_name, file_mode="w"
    )
    script_file.write(script)
    script_file.flush()

    out, err = ceph_installer.exec_command(
        cmd="python {SCRIPT_FILE}".format(SCRIPT_FILE=file_name)
    )
    out, err = out.strip(), err.strip()

    json_data = json.loads(out)
    logger.info("Status Code : {}".format(json_data.get("status_code")))

    if json_data.get("status_code") == 200:
        logger.info(json_data.get("json"))
        return 0
    logger.error(json_data.get("json"))
    return 1
