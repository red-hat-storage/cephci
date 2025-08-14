import json

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify BZ 2352523
    BZ is targeted to be fixed in 9.1
    """
    config = kw.get("config")
    installer = ceph_cluster.get_nodes("installer")
    _service = config.get("service")
    _config = config.get("config")
    _value = config.get("value")

    # get the daemon name for the service and add space
    daemons = CephAdm(installer).ceph.orch.ps(format="json")
    daemon_name = list(
        filter(
            lambda data: _service in data.get("daemon_name"),
            json.loads(daemons[installer[0].hostname][0]),
        )
    )[0].get("daemon_name")

    # add spaces to daemon name
    _daemon = f'"{daemon_name}  "'

    # set the config with spaces
    CephAdm(installer).ceph.config.set(daemon=_daemon, key=_config, value=_value)

    # check if the config is set
    kw = {"format": "json"}
    out = CephAdm(installer).ceph.config.dump(**kw)
    # get the dump for installer node where osd is present
    out = json.loads(out[installer[0].hostname][0])
    # validate the set cofnig command
    found = False
    for data_config in out:
        if data_config["section"] == _daemon.strip('"'):
            if data_config["name"] == str(_config) and data_config["value"] == "1/1":
                found = True
                break
    if not found:
        raise ConfigError("debug_ms not found or has an incorrect value in ceph config")
    log.info("debug_ms is validated in ceph config")

    # remove the config without passing the spaces to params
    CephAdm(installer).ceph.config.rm(who=str(daemon_name), option=str(_config))

    # config should not be removed as per issue observed in #BZ2352523
    kw = {"format": "json"}
    out = CephAdm(installer).ceph.config.dump(**kw)
    # get the dump for installer node where osd is present
    out = json.loads(out[installer[0].hostname][0])
    found = False
    for data_config in out:
        if data_config["section"] == _daemon.strip('"'):
            if data_config["name"] == str(_config) and data_config["value"] == "1/1":
                found = True
                break
    if not found:
        raise ConfigError(
            f"debug_ms value for {daemon_name} is removed from ceph config"
        )
    log.info(f"debug_ms value is not removed from ceph config for {daemon_name}")

    # remove the config again, but pass the daemon_name with spaces
    CephAdm(installer).ceph.config.rm(who=_daemon, option=str(_config))

    # Config should be removed now
    kw = {"format": "json"}
    out = CephAdm(installer).ceph.config.dump(**kw)
    # get the dump for installer node where osd is present
    out = json.loads(out[installer[0].hostname][0])
    found = False
    for data_config in out:
        if data_config["section"] == _daemon.strip('"'):
            if data_config["name"] == str(_config) and data_config["value"] == "1/1":
                found = True
                break
    if found:
        raise ConfigError(
            f"debug_ms value for {daemon_name} is not removed from ceph config"
        )
    log.info(
        f"debug_ms value is successfully removed from ceph config for {daemon_name}"
    )
    return 0
