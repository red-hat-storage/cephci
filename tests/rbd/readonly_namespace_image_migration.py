"""Module to execute Live migration scenario namespace from client-X
having read-only permission to images.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test cases covered :
CEPH-83573341 - Set read only for client-x on namespace and migrate the images and
verify migration is not allowed

Test Case Flow :
1. Create pool.
2. Create a namespace within the pool
3. Give readonly access to the namespace
4. Create images within the namespace
5. Migrate the created images to other pool
6. verify that image migration is not allowed as image is read-only for client-x.
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import (
    Rbd,
    client_config_read_only,
    initial_rbd_config,
    verify_namespace_exist,
)
from utility.log import Log

log = Log(__name__)


def image_migrate_and_verify(
    rbd,
    src_pool,
    dest_pool,
    namespace,
    image,
):
    """Migrate the image from source to destination and verify migration

    Args:
        src_pool: source pool name
        dest_pool: destination pool name
        namespace: name of namespace
    """
    src_spec1 = src_pool + "/" + namespace + "/" + image
    dest_spec1 = dest_pool + "/" + image
    client_id = "client.1"
    mgr_role = "profile rbd-read-only"
    osd_role = "profile rbd-read-only"
    if client_config_read_only(
        rbd,
        client_id,
        src_pool,
        namespace=namespace,
        mgr_role=mgr_role,
        osd_role=osd_role,
    ):
        log.error("verify for command syntax")
        return 1
    rbd.create_image(
        pool_name=src_pool,
        image_name=image,
        size="5G",
        namespace=namespace,
    )
    rbd.image_exists(
        pool_name=src_pool,
        image_name=image,
        namespace=namespace,
    )
    out, err = rbd.migration_prepare(src_spec1, dest_spec1, client_id=client_id)
    if "Migration: prepare: failed to open image" in err:
        log.error(f"{err}")
        log.info(
            "Image migration for client-x having read-only permission to images "
            "from namespace is successfully blocked"
        )
        return 0
    else:
        log.error(
            "Image migration is allowed for client-x having read-only permission "
            "to images in namespace"
        )
        return 1


def run(**kw):
    """verify for image migration when Set read only permission to client-x on namespace
    and migrate the images.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """

    for key, value in kw["config"].items():
        if key == "source":
            kw["config"].update(value)
            initial_rbd_config(**kw)
            for key in kw["config"]["source"]:
                kw["config"].pop(key)
            kw["config"].pop("ec-pool-k-m")
        elif key == "destination":
            kw["config"].update(value)
            initial_rbd_config(**kw)
            for key in kw["config"]["destination"]:
                kw["config"].pop(key)
            kw["config"].pop("ec-pool-k-m")

    rbd = Rbd(**kw)
    src_pools = []
    src_pool1 = kw["config"]["source"]["rep_pool_config"]["pool"]
    src_pools.append(src_pool1)
    src_pool2 = kw["config"]["source"]["ec_pool_config"]["pool"]
    src_pools.append(src_pool2)
    dest_pool1 = kw["config"]["destination"]["rep_pool_config"]["pool"]
    dest_pool2 = kw["config"]["destination"]["ec_pool_config"]["pool"]
    namespace = kw["config"].get("namespace", "testnamespace")
    flag = 0
    try:
        # create namesapce for rep_pool and ec_pool
        for pool in src_pools:
            rbd.create_namespace(pool, namespace)
            verify_namespace_exist(rbd, pool, namespace)

        # backup the client.1 keyring file
        cmd = (
            "cp /etc/ceph/ceph.client.1.keyring /etc/ceph/ceph.client.1.keyring.backup"
        )
        rbd.exec_cmd(cmd=cmd)

        log.info("running test case on Replication pool")
        if image_migrate_and_verify(
            rbd,
            src_pool=src_pool1,
            dest_pool=dest_pool1,
            namespace=namespace,
            image="rbd_rep_image",
        ):
            flag = 1

        log.info("Running test case on EC pool")
        if image_migrate_and_verify(
            rbd,
            src_pool=src_pool2,
            dest_pool=dest_pool2,
            namespace=namespace,
            image="rbd_ec_image",
        ):
            flag = 1

        if flag:
            log.error("Test case failed")

        return flag

    except RbdBaseException as error:
        log.error(error.message)

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(
                pools=[
                    src_pool1,
                    src_pool2,
                    dest_pool1,
                    dest_pool2,
                    kw["config"]["source"]["ec_pool_config"]["data_pool"],
                    kw["config"]["destination"]["ec_pool_config"]["data_pool"],
                ]
            )
        rbd.exec_cmd(cmd="rm -rf /etc/ceph/ceph.client.1.keyring")
        rbd.exec_cmd(
            cmd="mv /etc/ceph/ceph.client.1.keyring.backup /etc/ceph/ceph.client.1.keyring"
        )
        rbd.exec_cmd(cmd="ceph auth del client.1")
        rbd.exec_cmd(cmd="ceph auth add client.1 -i /etc/ceph/ceph.client.1.keyring")
