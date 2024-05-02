from rest.endpoints.nvmeof.nvmeof import NVMEoF
from utility.log import Log

log = Log(__name__)


def get_info_nvmeof_gateway(**kw):
    """
    Verifies nvmeof gateway info
    Args:
        kw: kw args required to get gateway info
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)
    try:
        response = nvmeof_rest.get_gateway_info()
        log.info(f"SUCCESS: NVMeOF gateway info {response}")
        return 0
    except Exception as e:
        log.error(f"FAILURE: Getting nvmeof gateway info failed {str(e)}")
        return 1


def create_and_verify_subsystem(**kw):
    """
    Creates and verfies subsystem
    Args:
        kw: kw args required for creating and verifying subsytem
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)

    # 1. create a subsystem
    nqn = kw.get("nqn")
    if nqn is None:
        log.error("nqn is must for subsystem creation")
        return 1
    enable_ha = kw.get("enable_ha", True)
    max_namespaces = kw.get("max_namespaces", 256)
    try:
        log.info(f"creating a subsystem with nqn {nqn}")
        _ = nvmeof_rest.create_subsystem(
            nqn=nqn, enable_ha=enable_ha, max_namespaces=max_namespaces
        )
    except Exception as e:
        log.error(f"creating a subsystem with nqn {nqn} failed {str(e)}")
        return 1

    # 2. verify created subsystem using list GET call
    log.info("validating subsystem by list subsystem REST")
    try:
        list_ss_response = nvmeof_rest.list_subsystem()
        nqns_list = [nqn_info["nqn"] for nqn_info in list_ss_response]
        if nqn in nqns_list:
            log.info(f"SUCCESS: verification of subsystem {nqn} via list REST passed")
        else:
            log.error(f"FAILED verification of subsystem {nqn} failed by list")
            return 1
    except Exception as e:
        log.error(f"FAILED verification of subsystem {nqn} failed by list {str(e)}")
        return 1

    # 3. verify created subsystem using GET call
    log.info("validating subsystem by GET subsystem REST")
    try:
        _ = nvmeof_rest.get_subsystem(subsystem_nqn=nqn)
        log.info(f"SUCCESS: verification of subsystem {nqn} via GET REST passed")
        return 0
    except Exception as e:
        log.error(f"FAILED verification of subsystem {nqn} failed by GET {str(e)}")
        return 1
