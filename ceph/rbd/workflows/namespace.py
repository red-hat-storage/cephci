from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def create_namespace_and_verify(**kw):
    """
    Creates a namespace in a given pool
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool in which namespace should be stored, default pool is rbd
                namespace(str): namespace to created in the pool
                See rbd help create for more supported keys
    """
    rbd = Rbd(kw["client"])

    # verify if the pool exists, if pool is not passed then consider default pool name i.e rbd
    pool_name = kw.get("pool-name", "rbd")
    log.info(f"verifying if pool {pool_name} exists")
    pool_init_kw = {}
    pool_init_kw["pool-name"] = pool_name
    (_, pool_stats_err) = rbd.pool.stats(**pool_init_kw)

    # if pool does not exists, create a pool
    if pool_stats_err:
        log.error(
            f"The pool where namespace is suppose to be created does not exist, creating the pool {pool_name}"
        )
        # TBD: Currently to use initial_rb_config methods to create a pool we need add an ability to not create image
        return 1

    # once successfully pool gets created, create a namespace
    namespace_name = kw.get("namespace", None)
    log.info(f"namespace {namespace_name} in {pool_name} is getting created")
    ns_create_kw = {}
    if namespace_name is None:
        log.error("namespace name is a must for create namespace method")
        return 1
    _pool_name = kw.get("pool-name", None)
    ns_create_kw["namespace"] = namespace_name
    ns_create_kw["pool-name"] = _pool_name
    (_, ns_create_err) = rbd.namespace.create(**ns_create_kw)
    if not ns_create_err:
        log.info(
            f"SUCCESS: Namespace {namespace_name} got created in pool {pool_name} "
        )
    else:
        log.error(
            f"FAIL: Namespace {namespace_name} creation failed in pool {pool_name} with error {ns_create_err}"
        )
        return 1

    # verify created namespace using namespace ls command
    log.info(
        f"created namespace {namespace_name} in pool {pool_name} is getting verified"
    )
    ns_verify_kw = {}
    ns_verify_kw["pool-name"] = _pool_name
    ns_ls_out = rbd.namespace.list(**ns_verify_kw)
    if namespace_name in ns_ls_out[0]:
        log.info(f"namespace {namespace_name} creation successfully verified")
        return 0
