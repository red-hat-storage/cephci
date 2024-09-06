"""
Test Module to perform specific functionalities of ceph-objectstore-tool.
ceph-objectstore-tool --data-path path to osd [--op list ]
Possible obj operations:
    (get|set)-bytes [file]
    set-(attr|omap) [file]
    (get|rm)-attr|omap)
    get-omaphdr
    set-omaphdr [file]
    list-attrs
    list-omap
    remove|removeall
    dump
    set-size
    clear-data-digest
    remove-clone-metadata
ceph-objectstore-tool --data-path path to osd [ --op list $obj_ID]
"""

import json
import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83581811
    Test to perform +ve workflows for the ceph-objectstore-tool utility
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)

    def get_bench_obj(_osd_id):
        out = objectstore_obj.list_objects(osd_id=_osd_id)
        obj_list = [json.loads(x) for x in out.split()]
        found = False
        for obj in obj_list:
            object_id = obj[1]["oid"]
            if "benchmark" in object_id:
                pg_id = obj[0]
                obj = json.dumps(obj[1])
                return pg_id, obj
        if not found:
            raise Exception(
                f"No obj with OMAP data found in the list of objs for OSD: {osd_id}"
            )

    def get_omap_obj(_osd_id):
        out = objectstore_obj.list_objects(osd_id=_osd_id)
        obj_list = [json.loads(x) for x in out.split()]
        # find an object with OMAP data
        found = False
        for obj in obj_list:
            object_id = obj[1]["oid"]
            if "omap_obj" in object_id:
                pg_id = obj[0]
                obj = json.dumps(obj[1])
                return pg_id, obj
        if not found:
            raise Exception(
                f"No obj with OMAP data found in the list of objs for OSD: {osd_id}"
            )

    all_ops = [
        "print_usage",
        "list_objects",
        "remove_object",
        "fixing_lost_objects",
        "manipulate_object_content",
        "list_omap",
        "manipulate_omap_header",
        "manipulate_omap_key",
        "remove_omap_key",
        "list_attributes",
        "manipulate_object_attribute",
        "remove_obj_attribute",
    ]
    operations = config.get("operations", all_ops)
    try:
        osd_list = rados_obj.get_active_osd_list()
        log.info(f"List of OSDs: \n{osd_list}")

        log.info("Create a data pool with default config")
        assert rados_obj.create_pool(pool_name="cot-pool")

        log.info("Write data to the pool using rados bench, 100 objects")
        assert rados_obj.bench_write(
            pool_name="cot-pool",
            rados_write_duration=200,
            max_objs=200,
            verify_stats=False,
        )

        log.info(
            "Write OMAP entries to the pool using rados bench, 100 objects with 50 omap entries each"
        )
        assert pool_obj.fill_omap_entries(
            pool_name="cot-pool", obj_start=0, obj_end=100, num_keys_obj=50
        )

        for operation in operations:
            if operation == "print_usage":
                # Execute ceph-objectstore-tool --help
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ----------------------------------"
                    f"\n Printing COT Usage/help for OSD {osd_id}"
                    f"\n ----------------------------------"
                )
                try:
                    out = objectstore_obj.help(osd_id=osd_id)
                except Exception as e:
                    log.info(e)

            if operation == "list_objects":
                # Execute ceph-objectstore-tool --data-path <osd_path> --op list
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ------------------------------------------"
                    f"\n Identify all objects within an OSD: {osd_id}"
                    f"\n ------------------------------------------"
                )
                out = objectstore_obj.list_objects(osd_id=osd_id)
                obj_list = [json.loads(x) for x in out.split()]
                log.info(f"List of objects in OSD {osd_id}: \n\n {obj_list}")
                assert 'oid":"benchmark_data_' in out

                # Execute ceph-objectstore-tool --data-path <osd_path> --op list $OBJECT_ID
                log.info(
                    f"\n -------------------------------------------"
                    f"\n Identify the placement group (PG) that an object belongs to for OSD {osd_id}"
                    f"\n -------------------------------------------"
                )
                # sample object -
                # ["5.1f",{"oid":"lc.0","key":"","snapid":-2,"hash":641746943,"max":0,"pool":5,"namespace":"lc","max":0}]
                pg_id, obj = get_bench_obj(_osd_id=osd_id)
                object_id = json.loads(obj)["oid"]
                out = objectstore_obj.get_pg_from_object(
                    osd_id=osd_id, obj_id=object_id
                )
                fetched_obj = json.loads(out)
                log.info(fetched_obj)
                _pg = fetched_obj[0]
                log.info(f"PG for ObjectID {object_id}: {_pg} | Expected value {pg_id}")
                assert _pg == pg_id

                # Execute ceph-objectstore-tool --data-path <osd_path> --pgid $PG_ID --op list
                log.info(
                    f"\n ----------------------------------"
                    f"\n Identify all objects within a placement group {pg_id} for OSD.{osd_id}"
                    f"\n ----------------------------------"
                )
                out = objectstore_obj.list_objects(osd_id=osd_id, pgid=pg_id)
                log.info(f"List of objects in OSD.{osd_id} and PG {pg_id}: \n {out}")
                assert f'["{pg_id}",{{"oid":' in out

            """ Removal of an object as documented here
            https://docs.ceph.com/en/latest/man/8/ceph-objectstore-tool/#removing-an-object does not work
            for Pacific builds.
            BZ - 2262909 | RHCS 5.3
            """
            if operation == "remove_object" and (not rhbuild.startswith("5")):
                # Execute ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT remove
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ------------------------------------"
                    f"\n Remove an object within an OSD: {osd_id}"
                    f"\n ------------------------------------"
                )
                pg_id, obj = get_bench_obj(_osd_id=osd_id)
                obj_list = objectstore_obj.list_objects(
                    osd_id=osd_id, pgid=pg_id
                ).strip()
                log.info(f"List of objects before removal: {obj_list}")
                out = objectstore_obj.remove_object(osd_id=osd_id, obj=obj, pgid=pg_id)
                assert "remove" in out
                _obj_list = objectstore_obj.list_objects(
                    osd_id=osd_id, pgid=pg_id
                ).strip()
                log.info(f"List of objects post removal: \n {_obj_list}")
                assert str(obj) not in _obj_list

            if operation == "fixing_lost_objects":
                # Execute ceph-objectstore-tool --data-path <osd_path> --op fix-lost
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n -----------------------------------"
                    f"\n Fix all lost objects for OSD: {osd_id}"
                    f"\n -----------------------------------"
                )
                out = objectstore_obj.fix_lost_object(osd_id=osd_id)
                log.info(out)

                # Execute ceph-objectstore-tool --data-path <osd_path> --pgid $PG_ID --op fix-lost
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ---------------------------------"
                    f"\n Fix all the lost objects within a specified placement group: for OSD.{osd_id}"
                    f"\n ---------------------------------"
                )
                pg_id = rados_obj.get_pgid(osd_primary=osd_id)[0]
                out = objectstore_obj.fix_lost_object(osd_id=osd_id, pgid=pg_id)
                log.info(out)

                """
                Error: Can't specify both --op and object command syntax
                BZ-2263374 | Pacific and Quincy
                """
                if rhbuild.startswith("7"):
                    # Execute ceph-objectstore-tool --data-path <osd_path> --op fix-lost $OBJECT_ID
                    osd_id = random.choice(osd_list)
                    log.info(
                        f"\n --------------------"
                        f"\n Fix a lost object by its identifier for OSD: {osd_id}"
                        f"\n --------------------"
                    )
                    pg_id, obj = get_bench_obj(_osd_id=osd_id)
                    obj_id = json.loads(obj)["oid"]
                    out = objectstore_obj.fix_lost_object(osd_id=osd_id, obj_id=obj_id)
                    log.info(out)

            if operation == "manipulate_object_content":
                # Find the object by listing the objects of the OSD or placement group.
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ------------------------------------------------------"
                    f"\n Manipulate object content in OSD {osd_id}"
                    f"\n ------------------------------------------------------"
                )
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                pg_id, obj = get_bench_obj(_osd_id=osd_id)

                # Before setting the bytes on the object, make a backup and a working copy of the object.
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-bytes > out_file
                log.info(f"Fetch object bytes for obj: {obj} in PG: {pg_id}")
                objectstore_obj.get_bytes(
                    osd_id=osd_id, obj=obj, pgid=pg_id, out_file="/tmp/obj_work"
                )

                # Edit the working copy object's data
                log.info("Modify bytes fetched for the object")
                edit_cmd = "echo 'random data' >> /tmp/obj_work"
                out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

                # Set the bytes of the object
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-bytes < modified_file
                log.info(f"Inject modified object bytes for obj {obj}")
                out = objectstore_obj.set_bytes(
                    osd_id=osd_id, obj=obj, pgid=pg_id, in_file="/tmp/obj_work"
                )
                log.info(out)

                # verify manipulation of object data
                log.info(f"Fetch object bytes for obj {obj} post injection")
                objectstore_obj.get_bytes(
                    osd_id=osd_id, obj=obj, pgid=pg_id, out_file="/tmp/mod_obj"
                )

                try:
                    mod_obj_data, _ = osd_host.exec_command(
                        sudo=True, cmd="cat /tmp/mod_obj"
                    )
                    log.info(mod_obj_data)
                    assert "random data" in mod_obj_data
                    log.info(
                        f"Additional text found in modified bytes fetched for object {obj}"
                    )
                except UnicodeDecodeError:
                    log.info(
                        "Object content was not in utf-8 encoding, attempting to fix"
                    )
                    mod_obj_data = mod_obj_data.decode("utf-8", "ignore")

            if operation == "list_omap":
                # Use the ceph-objectstore-tool to list the contents of the object map (OMAP).
                # The output is a list of keys.
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------"
                    f"\n List the object map for OSD: {osd_id}"
                    f"\n --------------------"
                )
                pg_id, obj = get_omap_obj(_osd_id=osd_id)
                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")

                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-omap
                log.info(f"Fetch object map for obj {obj}")
                out = objectstore_obj.list_omap(osd_id=osd_id, pgid=pg_id, obj=obj)
                log.info(f"List of omap entries for obj {obj}: \n {out}")
                assert "key_" in out

            if operation == "manipulate_omap_header":
                # The ceph-objectstore-tool utility will output the object map (OMAP) header
                # with the values associated with the object’s keys.
                osd_id = random.choice(osd_list)
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                pg_id, obj = get_omap_obj(_osd_id=osd_id)
                log.info(
                    f"\n ------------------------------------------------------"
                    f"\n Manipulate OMAP header for object {obj} in OSD {osd_id} and PG: {pg_id}"
                    f"\n ------------------------------------------------------"
                )

                # Get the object map header
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omaphdr > out_file
                log.info(f"Fetch OMAP header for obj {obj}")
                objectstore_obj.get_omap_header(
                    osd_id=osd_id, obj=obj, pgid=pg_id, out_file="/tmp/omaphdr_org"
                )

                # Edit the working copy of object's OMAP header
                log.info(f"Modify OMAP header fetched for the object {obj}")
                edit_cmd = "echo 'random omap header data' >> /tmp/omaphdr_org"
                out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

                # Set the modified object map header
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omaphdr < in_file
                out = objectstore_obj.set_omap_header(
                    osd_id=osd_id, obj=obj, pgid=pg_id, in_file="/tmp/omaphdr_org"
                )
                log.info(out)

                # verify manipulation of OMAP header data
                log.info(f"Fetch OMAP header for obj {obj} post modification")
                objectstore_obj.get_omap_header(
                    osd_id=osd_id, obj=obj, pgid=pg_id, out_file="/tmp/omaphdr_mod"
                )

                mod_omap_header, _ = osd_host.exec_command(
                    sudo=True, cmd="cat /tmp/omaphdr_mod"
                )
                assert "random omap header" in mod_omap_header
                log.info(
                    f"Additional text found in modified OMAP header fetched for object {obj}"
                )

            if operation == "manipulate_omap_key":
                # Use the ceph-objectstore-tool utility to change the object map (OMAP) key.
                # You need to provide the data path, the placement group identifier (PG ID),
                # the object, and the key in the OMAP.
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------"
                    f"\n Manipulation of object map key for OSD: {osd_id}"
                    f"\n --------------------"
                )
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                pg_id, obj = get_omap_obj(_osd_id=osd_id)

                # Get the object map key list
                out = objectstore_obj.list_omap(osd_id=osd_id, pgid=pg_id, obj=obj)
                omap_key = out.split()[-1]

                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")
                log.info(f"Chosen OMAP key: {omap_key}")

                # fetch the omap key value
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omap $KEY > out_file
                value = objectstore_obj.get_omap(
                    osd_id=osd_id, obj=obj, pgid=pg_id, key=omap_key
                )
                log.info(f"Value for OMAP key {omap_key} in object {obj}: {value}")

                # Modify OMAP key's value
                log.info(f"Modify OMAP key for the object {obj}")
                mod_value = "value_1111"
                edit_cmd = f"echo '{mod_value}' > /tmp/omap_value"
                out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

                # Set the object map value for an omap key
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omap $KEY < in_file
                out = objectstore_obj.set_omap(
                    osd_id=osd_id,
                    obj=obj,
                    pgid=pg_id,
                    key=omap_key,
                    in_file="/tmp/omap_value",
                )
                log.info(out)

                # verify manipulation of OMAP key value
                log.info(f"Fetch OMAP key's value for obj {obj} post modification")
                value = objectstore_obj.get_omap(
                    osd_id=osd_id, obj=obj, pgid=pg_id, key=omap_key
                )
                assert mod_value in value
                log.info(
                    f"Modified value({value}) for OMAP key({omap_key}) verified for object {obj}"
                )

            if operation == "remove_omap_key":
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------"
                    f"\n Remove an Object map key for OSD: {osd_id}"
                    f"\n --------------------"
                )
                pg_id, obj = get_omap_obj(_osd_id=osd_id)
                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")

                # Get the object map key list
                out = objectstore_obj.list_omap(osd_id=osd_id, pgid=pg_id, obj=obj)
                omap_key = out.split()[-1]
                log.info(f"Chosen omap key: {omap_key}")

                # remove the object map key
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-omap $KEY
                out = objectstore_obj.remove_omap(
                    osd_id=osd_id, pgid=pg_id, obj=obj, key=omap_key
                )
                log.info(out)

                # verify removal of omap key
                out = objectstore_obj.list_omap(osd_id=osd_id, pgid=pg_id, obj=obj)
                log.info(f"List of omap entries post deletion: {out}")
                assert omap_key not in out

            if operation == "list_attributes":
                # Use the ceph-objectstore-tool utility to list an object’s attributes.
                # The output provides you with the object’s keys and values.
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------------------"
                    f"\n List attributes of an object within OSD: {osd_id}"
                    f"\n --------------------------------"
                )
                pg_id, obj = get_bench_obj(_osd_id=osd_id)
                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")

                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-attrs
                log.info(f"List attributes for obj {obj}")
                out = objectstore_obj.list_attributes(
                    osd_id=osd_id, pgid=pg_id, obj=obj
                )
                log.info(f"List of attributes for obj {obj}: \n {out}")

            if operation == "manipulate_object_attribute":
                # Use the ceph-objectstore-tool utility to change an object’s attribute.
                # To manipulate the object’s attributes you need the data and journal paths,
                # the placement group identifier (PG ID), the object,
                # and the key in the object’s attribute.
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n -----------------------------------"
                    f"\n Manipulate object attribute for OSD: {osd_id}"
                    f"\n ------------------------------------"
                )
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                pg_id, obj = get_bench_obj(_osd_id=osd_id)
                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")
                log.info(f"List attributes for obj {obj}")
                out = objectstore_obj.list_attributes(
                    osd_id=osd_id, pgid=pg_id, obj=obj
                )
                attr_list = out.split()
                obj_attr = attr_list[-1]
                log.info(f"List of attributes for obj {obj}: \n {attr_list}")

                log.info(f"Chosen attribute for object {obj}: {obj_attr}")

                # fetch the value of object's attribute
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-attr $KEY > out_file
                value = objectstore_obj.get_attribute(
                    osd_id=osd_id, obj=obj, pgid=pg_id, attr=obj_attr
                )
                log.info(
                    f"Value for object attribute {obj_attr} in object {obj}: {value}"
                )

                # Modify object attribute's value
                log.info(f"Modify {obj_attr} object attribute's value for {obj}")
                mod_value = "Base64:AwIdAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAA="
                edit_cmd = f"echo '{mod_value}' > /tmp/attr_value"
                out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

                # Set the object attribute's value
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-attr $KEY < in_file
                out = objectstore_obj.set_attribute(
                    osd_id=osd_id,
                    obj=obj,
                    pgid=pg_id,
                    attr=obj_attr,
                    in_file="/tmp/attr_value",
                )
                log.info(out)

                # verify manipulation of object attributes value
                value = objectstore_obj.get_attribute(
                    osd_id=osd_id, obj=obj, pgid=pg_id, attr=obj_attr
                )
                log.info(
                    f"Modified value for object attribute {obj_attr} in object {obj}: {value}"
                )
                assert mod_value in value
                log.info(
                    f"Modified value({value}) for object attribute({obj_attr}) verified for object {obj}"
                )

            if operation == "remove_obj_attribute":
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n ----------------------------------"
                    f"\n Remove an Object attribute for OSD: {osd_id}"
                    f"\n ----------------------------------"
                )
                pg_id, obj = get_bench_obj(_osd_id=osd_id)
                log.info(f"PG: {pg_id}")
                log.info(f"Chosen object: {obj}")
                log.info(f"List attributes for obj {obj}")
                out = objectstore_obj.list_attributes(
                    osd_id=osd_id, pgid=pg_id, obj=obj
                )
                attr_list = out.split()
                obj_attr = attr_list[-1]
                log.info(f"List of attributes for obj {obj}: \n {attr_list}")
                log.info(f"Chosen attribute for object {obj}: {obj_attr}")

                # remove the object attribute entry
                # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-attr $KEY
                out = objectstore_obj.remove_attribute(
                    osd_id=osd_id, pgid=pg_id, obj=obj, attr=obj_attr
                )
                log.info(out)

                # verify removal of object attribute
                out = objectstore_obj.list_attributes(
                    osd_id=osd_id, pgid=pg_id, obj=obj
                )
                log.info(f"List of object attributes post deletion: {out}")
                assert obj_attr not in out

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info("\n\n\n*********** Execution of finally block starts ***********\n\n")
        rados_obj.delete_pool(pool="cot-pool")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed verification of Ceph-objectstore-Tool commands.")
    return 0
