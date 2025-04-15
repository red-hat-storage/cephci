from ceph.rbd.utils import getdict, random_string
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
    else:
        log.error(f"namespace {namespace_name} creation verified failed")
        return 1


def remove_namespace_and_verify(**kw):
    """
    Removes a namespace in a given pool
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool in which namespace should be removed, default pool is rbd
                namespace(str): namespace to removed in the pool
                See rbd help create for more supported keys
    """
    rbd = Rbd(kw["client"])

    # verify if the pool exists, if pool is not passed then consider default pool name i.e rbd
    pool_name = kw.get("pool-name", "rbd")
    log.info(f"verifying if pool {pool_name} exists")
    pool_init_kw = {}
    pool_init_kw["pool-name"] = pool_name
    (_, pool_stats_err) = rbd.pool.stats(**pool_init_kw)

    # if pool does not exists,  exit
    if pool_stats_err:
        log.error(
            f"The pool {pool_name} where namespace is suppose to be delete does not exist"
        )
        return 1

    # remove a namespace
    namespace_name = kw.get("namespace", None)
    log.info(f"namespace {namespace_name} in {pool_name} is getting deleted")
    ns_delete_kw = {}
    if namespace_name is None:
        log.error("namespace name is a must for delete namespace method")
        return 1
    _pool_name = kw.get("pool-name", None)
    ns_delete_kw["namespace"] = namespace_name
    ns_delete_kw["pool-name"] = _pool_name
    (_, ns_delete_err) = rbd.namespace.remove(**ns_delete_kw)
    if not ns_delete_err:
        log.info(
            f"SUCCESS: Namespace {namespace_name} got deleted in pool {pool_name} "
        )
    else:
        log.error(
            f"FAIL: Namespace {namespace_name} deletion failed in pool {pool_name} with error {ns_delete_err}"
        )
        return 1

    # verify deleted namespace using namespace ls command
    log.info(
        f"Verifying the deleted namespace {namespace_name} in pool {pool_name} using namespace ls command"
    )
    ns_verify_kw = {}
    ns_verify_kw["pool-name"] = _pool_name
    ns_ls_out = rbd.namespace.list(**ns_verify_kw)
    if namespace_name not in ns_ls_out[0]:
        log.info(f"namespace {namespace_name} deletion successfully verified")
        return 0
    else:
        log.error(f"namespace {namespace_name} deletion verified failed")
        return 1


def create_namespace_in_pool(rbd, client, pool_type, **kw):
    """
    Creates a namespace in a pool for namespace level mirroring
        Args:
        rbd (module): The rbd object
        client (object): client object.
        pool_type (str): The type of pool where namespace is being created
        kw (dict): A dictionary of keyword arguments.

    Returns:
        int: 0 if the namespace was created successfully, otherwise a non-zero value.
    """
    namespace_type = (
        kw.get("config").get(pool_type, {}).get("namespace_mirror_type", "")
    )
    rbd_config = kw.get("config", {}).get(pool_type, {})
    multi_pool_config = getdict(rbd_config)
    namespace = "namespace_" + random_string(len=3)
    size = kw["config"][pool_type]["size"]

    for pool, pool_config in multi_pool_config.items():
        multi_image_config = getdict(pool_config)
        if (
            kw.get("is_secondary", False) is False
            and namespace_type == "non-default_to_non-default"
        ):
            rc = create_namespace_and_verify(
                **{"pool-name": pool, "namespace": namespace, "client": client}
            )
            if rc != 0:
                return rc
            kw["config"][pool_type].get(pool, {}).update({"namespace": namespace})
            for imagename in multi_image_config.keys():
                imagespec = f"{pool}/{namespace}/{imagename}"
                out, err = rbd.create(**{"image-spec": imagespec, "size": size})
                if err:
                    log.error("Failed to create image {}", err)
                    return 1
        elif (
            kw.get("is_secondary", False) is True
            and namespace_type == "non-default_to_non-default"
        ):
            rc = create_namespace_and_verify(
                **{"pool-name": pool, "namespace": namespace, "client": client}
            )
            if rc != 0:
                return rc
            kw["config"][pool_type].get(pool, {}).update(
                {"remote_namespace": namespace}
            )
        elif (
            kw.get("is_secondary", False) is False
            and namespace_type == "non-default_to_default"
        ):

            rc = create_namespace_and_verify(
                **{"pool-name": pool, "namespace": namespace, "client": client}
            )
            if rc != 0:
                return rc
            kw["config"][pool_type].get(pool, {}).update({"namespace": namespace})
            kw["config"][pool_type].get(pool, {}).update({"remote_namespace": ""})
            for imagename in multi_image_config.keys():
                imagespec = f"{pool}/{namespace}/{imagename}"
                out, err = rbd.create(**{"image-spec": imagespec, "size": size})
                if err:
                    log.error("Failed to create image {}", err)
                    return 1
        elif (
            kw.get("is_secondary", False) is True
            and namespace_type == "default_to_non-default"
        ):
            rc = create_namespace_and_verify(
                **{"pool-name": pool, "namespace": namespace, "client": client}
            )
            if rc != 0:
                return rc
            kw["config"][pool_type].get(pool, {}).update({"namespace": ""})
            kw["config"][pool_type].get(pool, {}).update(
                {"remote_namespace": namespace}
            )
        elif (
            kw.get("is_secondary", False) is False
            and namespace_type == "default_to_non-default"
        ):
            for imagename in multi_image_config.keys():
                imagespec = f"{pool}/{imagename}"
                out, err = rbd.create(**{"image-spec": imagespec, "size": size})
                if err:
                    log.error("Failed to create image {}", err)
                    return 1


def enable_namespace_mirroring(primary_config, secondary_config, pool, **pool_config):
    """
    Enables namespace level mirroring
        Args:
        primary config : primary cluster config object
        Secondary config : secondary cluster config object
        pool : pool name
        pool config: object having all the pool configuration

    Returns:
        int: 0 if the namespace level mirroring is enabled successfully, otherwise a non-zero value.
    """
    rbd_primary = primary_config.get("rbd")
    rbd_secondary = secondary_config.get("rbd")

    if pool_config["remote_namespace"]:
        prim_remote_namespace = pool_config["remote_namespace"]
        seco_pool_spec = f"{pool}/{pool_config['remote_namespace']}"
    else:
        prim_remote_namespace = "''"
        seco_pool_spec = pool
    if pool_config["namespace"]:
        prim_pool_spec = f"{pool}/{pool_config['namespace']}"
        seco_remote_namespace = pool_config["namespace"]
    else:
        prim_pool_spec = pool
        seco_remote_namespace = "''"

    enable_primary_namespace_config = {
        "pool-spec": prim_pool_spec,
        "mode": pool_config["mode"],
        "remote-namespace": prim_remote_namespace,
    }
    out, err = rbd_primary.mirror.pool.enable(**enable_primary_namespace_config)
    if err:
        log.error(
            "Failed to enable namespace level mirroring on primary cluster {}", err
        )
        return 1

    enable_secondary_namespace_config = {
        "pool-spec": seco_pool_spec,
        "mode": pool_config["mode"],
        "remote-namespace": seco_remote_namespace,
    }
    out, err = rbd_secondary.mirror.pool.enable(**enable_secondary_namespace_config)
    if err:
        log.error(
            "Failed to enable namespace level mirroring on seconday cluster{}", err
        )
        return 1
    return 0
