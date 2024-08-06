from ceph.utils import get_nodes_by_ids
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


def add_and_verify_host(**kw):
    """
    Add and verfies Host
    Args:
        kw: kw args required for Adding  and verifying Host
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)

    # 1. Add a Host
    nqn = kw.get("nqn")
    host = kw.get("allow_host")
    if nqn is None:
        log.error("nqn is must for Adding Host")
        return 1
    try:
        log.info(f"Adding a Host with nqn {nqn}")
        _ = nvmeof_rest.add_host(nqn=nqn, host_nqn=host)
    except Exception as e:
        log.error(f"Adding a Host with nqn {nqn} failed {str(e)}")
        return 1

    # 2. verify added host using list GET call
    log.info("validating host by list host REST")
    if nqn is None:
        log.error("nqn is must for Adding Host")
        return 1
    try:
        _ = nvmeof_rest.list_host(subsystem_nqn=nqn)
        log.info(f"SUCCESS: verification of host {nqn} via GET REST passed")
        return 0
    except Exception as e:
        log.error(f"FAILED verification of host {nqn} failed by GET {str(e)}")
        return 1


def add_and_verify_listener(cluster, **kw):
    """
    Add and Verification of listener on each gateway node
    Args:
        kw: kw args required for Adding and Verifying Listener
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)
    nqn = kw.get("nqn")
    listeners = kw.get("listeners")
    for listener in get_nodes_by_ids(cluster, listeners):
        trsvcid = int(kw.get("listener_port", 4420))
        adrfam = 0
        if nqn is None:
            log.error("nqn is must for Adding Listener")
            return 1
        try:
            log.info(
                f"Adding a Listener with nqn {nqn} listener hosts {listener.hostname} and listener port {trsvcid}"
            )
            _ = nvmeof_rest.add_listener(
                nqn=nqn,
                host_name=listener.hostname,
                traddr=listener.ip_address,
                trsvcid=trsvcid,
                adrfam=adrfam,
            )
        except Exception as e:
            log.error(
                f"Adding a Listener with nqn {nqn} "
                f"listener hosts {listener.hostname} "
                f"and listener port {trsvcid} failed {str(e)}"
            )
            return 1
        # 2. verify added host using list GET call
        log.info("validating listeners by list listeners REST")
        if nqn is None or listener.hostname is None or trsvcid is None:
            log.error("nqn,gateway hostname and ip address is must for Adding Listener")
            return 1
        try:
            _ = nvmeof_rest.list_listener(subsystem_nqn=nqn)
            log.info(f"SUCCESS: verification of host {nqn} via GET REST passed")
            return 0
        except Exception as e:
            log.error(f"FAILED verification of host {nqn} failed by GET {str(e)}")
            return 1


def add_and_verify_namespace(**kw):
    """
    Add and Verification of namespace on  gateway node
    Args:
        kw: kw args required for Adding and Verifying Namespace
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)
    nqn = kw.get("nqn")
    rbd_image_name = kw.get("rbd_image_name")
    rbd_pool = kw.get("rbd_pool")
    create_image = kw.get("create_image")
    size = kw.get("size")
    block_size = kw.get("block_size")
    load_balancing_group = kw.get("load_balancing_group")
    if nqn is None or rbd_pool is None or rbd_image_name is None:
        log.error("nqn, rbd pool and rbd image name are  must for Adding Namespace")
        return 1
    try:
        log.info(
            f"Adding a Namespace with nqn {nqn} rbd pool {rbd_pool} and rbd image name {rbd_image_name}"
        )
        _ = nvmeof_rest.add_namespace(
            nqn=nqn,
            rbd_image_name=rbd_image_name,
            rbd_pool=rbd_pool,
            create_image=create_image,
            size=size,
            block_size=block_size,
            load_balancing_group=load_balancing_group,
        )
    except Exception as e:
        log.error(
            f"Adding a Namesapce with nqn {nqn} "
            f"rbd pool name  {rbd_pool} "
            f"and rbd image name  {rbd_image_name} failed {str(e)}"
        )
        return 1
    # 2. verify List  Namespace  using list GET call
    log.info("validating Namespace by list namespaces REST")
    if nqn is None or rbd_pool is None or rbd_image_name is None:
        log.error("nqn, rbd pool and rbd image name are must for Adding Namespace")
        return 1
    try:
        _ = nvmeof_rest.list_namespace(subsystem_nqn=nqn)
        log.info(
            f"SUCCESS: verification of namespace {nqn} {rbd_pool} {rbd_image_name} via GET REST passed"
        )
        return 0
    except Exception as e:
        log.error(
            f"FAILED verification of namespace {nqn} {rbd_pool} {rbd_image_name} failed by GET {str(e)}"
        )
        return 1


def cleanup(**kw):
    """
    All REST cleanup needed to this moment
    scope:
        (1) Delete namespace
        (2) Delete subsystem
    """
    _rest = kw.pop("rest", None)
    nvmeof_rest = NVMEoF(rest=_rest)
    nqn = kw.get("nqn")

    # 1. Delete all namespaces first
    try:
        namespaces = nvmeof_rest.list_namespace(subsystem_nqn=nqn)
        for _namespace in namespaces:
            _ = nvmeof_rest.delete_namespace(subsystem_nqn=nqn, nsid=_namespace["nsid"])
        namespaces_after_delete = nvmeof_rest.list_namespace(subsystem_nqn=nqn)
        if len(namespaces_after_delete) != 0:
            log.error(f"FAILED Deleting of namespace for susbsystem {nqn} failed ")
            return 1
    except Exception as e:
        log.error(
            f"FAILED Deleting or Listing of namespace for susbsystem {nqn} failed  {str(e)}"
        )
        return 1

    # 2. Now Delete the subsystem
    try:
        _ = nvmeof_rest.delete_subsystem(subsystem_nqn=nqn)
        log.info("=======CLEANUP SUCCESS=======")
        return 0
    except Exception as e:
        log.error(f"FAILED: subsystem delete failed {str(e)}")
