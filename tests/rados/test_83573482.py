import json
from time import sleep

from utility.log import Log

log = Log(__name__)

ENABLE_MSGR2 = "ceph mon enable-msgr2"
RESTART_MON_SERVICE = "systemctl restart ceph-mon@{HOSTNAME}.service"
CONFIG_CHECK = "ceph daemon mon.{HOSTNAME} config show"
CONFIG_CHECK_ALL = "ceph mon dump"


def exec_cmd_status(ceph_node, command, sudo=False):
    """
    Execute command
    Args:
        ceph_installer: installer object to exec cmd
        commands: list of commands to be executed
    Returns:
        Boolean
    """
    out, err = ceph_node.exec_command(sudo=sudo, cmd=command)
    out, err = out.strip(), err.strip()
    log.info("Command Response : {} {}".format(out, err))
    return out, err


def run(ceph_cluster, **kwargs):
    """
    CEPH-83573482 RADOS:
    Enabling encryption on wire feature which enables
    encryption of mon and client communication
    Steps:
        1. Have running cluster
        2. ceph mon enable-msgr2
        3. systemctl restart all ceph-mon@service
        4. ceph daemon mon.harvard config show and check for ms_bind_msr2
        5. ceph mon dump and check for v2 and port 3300

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
        kwargs: test args
    """
    log.info("Running CEPH-83573482")
    log.info(run.__doc__)

    try:
        mon_nodes = ceph_cluster.get_nodes("mon")

        # Enable MSGR2 on mon nodes
        out, err = exec_cmd_status(mon_nodes[0], command=ENABLE_MSGR2, sudo=True)
        assert not err, log.error("{} Command execution failed".format(ENABLE_MSGR2))

        # Restart mon service on mon-nodes
        for mon in mon_nodes:
            cmd = RESTART_MON_SERVICE.format(HOSTNAME=mon.hostname)
            out, err = exec_cmd_status(mon, command=cmd, sudo=True)
            assert not err, log.error("{} Command execution failed".format(cmd))
            sleep(5)

        for mon in mon_nodes:
            cmd = CONFIG_CHECK.format(HOSTNAME=mon.hostname)
            out, err = exec_cmd_status(mon, command=cmd, sudo=True)
            data = json.loads(out)
            assert data["ms_bind_msgr2"] == "true", log.error(
                "{} Command execution failed".format(cmd)
            )
            log.info("ms_bind MSGR Version2 is enabled")
        status, error = exec_cmd_status(
            mon_nodes[0], command=CONFIG_CHECK_ALL, sudo=True
        )

        for mon in mon_nodes:
            ip = mon.ip_address
            v2_check = "v2:{MON_IP}:3300".format(MON_IP=ip)
            v1_check = "v1:{MON_IP}:6789".format(MON_IP=ip)
            assert (
                v2_check in status and v1_check in status
            ), "MSGR Version2 is not enabled on {}".format(ip)
        log.info("MSGR version2 enabled successfully")
        return 0
    except AssertionError as err:
        log.error(err)
    return 1
