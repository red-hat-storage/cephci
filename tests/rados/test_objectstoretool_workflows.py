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

import datetime
import json
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)
STOPPED_OSDS = []


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
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    bench_obj_size_kb = 4096

    def get_bench_obj(_osd_id, cot_obj=objectstore_obj):
        cot_obj.list_objects(osd_id=_osd_id, file_redirect=True)
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=_osd_id)
        out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/cot_stdout")
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
                "No rados bench object found in the list of objs for OSD: "
                + str(osd_id)
            )

    def get_omap_obj(_osd_id, cot_obj=objectstore_obj):
        cot_obj.list_objects(osd_id=_osd_id, file_redirect=True)
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=_osd_id)
        out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/cot_stdout")
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
                "No obj with OMAP data found in the list of objs for OSD: "
                + str(osd_id)
            )

    all_ops = [
        "print_usage",
        "list_objects",
        "list_pgs",
        "remove_object",
        "dump",
        "fixing_lost_objects",
        "export",
        "meta-list",
        "get-osdmap",
        "get-superblock",
        "get-inc-osdmap",
        "remove_obj_attribute",
        "list_attributes",
        "manipulate_object_attribute",
        "manipulate_object_content",
        "list_omap",
        "manipulate_omap_header",
        "manipulate_omap_key",
        "remove_omap_key",
    ]
    readonly_ops = [
        "list_objects",
        "list_pgs",
        "list_get_omap",
        "list_get_attributes",
        "get_omap_header",
        "get_bytes",
        "dump",
        "export",
        "meta-list",
        "get-osdmap",
        "get-superblock",
        "get-inc-osdmap",
    ]

    def pick_random_osd():
        """
        Method to choose an OSD from UP OSDs and stop it
        Returns:
            OSD ID
        """
        osd_list = rados_obj.get_osd_list(status="up")
        osd_id = random.choice(osd_list)
        rados_obj.change_osd_state(action="stop", target=osd_id)
        STOPPED_OSDS.append(osd_id)
        return osd_id

    def execute_ops(op, cot_obj, _osd_id):
        """
        Method to execute an operation on desired OSD
        Args:
            op: operation to perform
            cot_obj: library object to use
            _osd_id: OSD ID on which operation will be performed
        Return:
            None
        """
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=_osd_id)
        if op == "print_usage":
            # Execute ceph-objectstore-tool --help
            intro = (
                f"\n ----------------------------------"
                f"\n Printing COT Usage/help for OSD {_osd_id}"
                f"\n ----------------------------------"
            )
            log.info(intro)
            try:
                out = cot_obj.help(osd_id=_osd_id)
            except Exception as e:
                log.info(e)

        if op == "list_objects":
            # Execute ceph-objectstore-tool --data-path <osd_path> --op list
            intro = (
                f"\n ------------------------------------------"
                f"\n Identify all objects within an OSD: {_osd_id}"
                f"\n ------------------------------------------"
            )
            log.info(intro)
            cot_obj.list_objects(osd_id=_osd_id, file_redirect=True)
            out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/cot_stdout")
            obj_list = [json.loads(x) for x in out.split()]
            log.info("Last 50 objects in OSD %s: \n\n %s" % (_osd_id, obj_list[-50:]))
            assert 'oid":"benchmark_data_' in out

            # Execute ceph-objectstore-tool --data-path <osd_path> --op list $OBJECT_ID
            intro = (
                f"\n -------------------------------------------"
                f"\n Identify the placement group (PG) that an object belongs to for OSD {_osd_id}"
                f"\n -------------------------------------------"
            )
            log.info(intro)
            # sample object -
            # ["5.1f",{"oid":"lc.0","key":"","snapid":-2,"hash":641746943,"max":0,"pool":5,"namespace":"lc","max":0}]
            object_id = json.loads(bench_obj)["oid"]
            out = cot_obj.get_pg_from_object(osd_id=_osd_id, obj_id=object_id)
            fetched_obj = json.loads(out)
            log.info(fetched_obj)
            _pg = fetched_obj[0]
            log.info(
                "PG for ObjectID %s: %s | Expected value %s"
                % (object_id, _pg, bench_pg)
            )
            assert _pg == bench_pg

            # Execute ceph-objectstore-tool --data-path <osd_path> --pgid $PG_ID --op list
            intro = (
                f"\n ----------------------------------"
                f"\n Identify all objects within a placement group {bench_pg} for OSD.{_osd_id}"
                f"\n ----------------------------------"
            )
            log.info(intro)
            cot_obj.list_objects(osd_id=_osd_id, pgid=bench_pg, file_redirect=True)
            out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/cot_stdout")
            obj_list = [json.loads(x) for x in out.split()]
            log.info(
                "List of last 50 objects in OSD.%s and PG %s: \n %s"
                % (_osd_id, bench_pg, obj_list[-50:])
            )
            assert f'["{bench_pg}",{{"oid":' in out

        if op == "list_pgs":
            # Execute ceph-objectstore-tool --data-path <osd_path> --op list-pgs
            intro = (
                f"\n ------------------------------------------"
                f"\n Identify all PGs within an OSD: {_osd_id}"
                f"\n ------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.list_pgs(osd_id=_osd_id)
            pg_list = out.split()
            log.info("List of PGs in OSD %s: \n\n %s" % (_osd_id, pg_list))
            return pg_list

        if op == "remove_object" and (not rhbuild.startswith("5")):
            """Removal of an object as documented here
            https://docs.ceph.com/en/latest/man/8/ceph-objectstore-tool/#removing-an-object does not work
            for Pacific builds.
            BZ - 2262909 | RHCS 5.3"""
            # Execute ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT remove
            intro = (
                f"\n ------------------------------------"
                f"\n Remove an object within an OSD: {_osd_id}"
                f"\n ------------------------------------"
            )
            log.info(intro)
            pg_id, obj = get_bench_obj(_osd_id=_osd_id)
            init_list = cot_obj.list_objects(osd_id=_osd_id, pgid=pg_id).strip()
            log.info("List of objects before removal: " + str(init_list))
            out = cot_obj.remove_object(osd_id=_osd_id, pgid=pg_id, obj=obj)
            assert "remove" in out
            rm_list = cot_obj.list_objects(osd_id=_osd_id, pgid=pg_id).strip()
            log.info("List of objects post removal: \n " + str(rm_list))
            assert str(obj) not in rm_list

        if op == "fixing_lost_objects":
            # Execute ceph-objectstore-tool --data-path <osd_path> --op fix-lost
            intro = (
                f"\n -----------------------------------"
                f"\n Fix all lost objects for OSD: {_osd_id}"
                f"\n -----------------------------------"
            )
            log.info(intro)
            out = cot_obj.fix_lost_object(osd_id=_osd_id)
            log.info(out)

            # Execute ceph-objectstore-tool --data-path <osd_path> --pgid $PG_ID --op fix-lost
            intro = (
                f"\n ---------------------------------"
                f"\n Fix all the lost objects within a specified placement group: for OSD.{_osd_id}"
                f"\n ---------------------------------"
            )
            log.info(intro)
            out = cot_obj.fix_lost_object(osd_id=_osd_id, pgid=bench_pg)
            log.info(out)

            """
            Error: Can't specify both --op and object command syntax
            BZ-2263374 | Pacific and Quincy
            """
            if rhbuild.startswith("7"):
                # Execute ceph-objectstore-tool --data-path <osd_path> --op fix-lost $OBJECT_ID
                log.info(
                    f"\n --------------------"
                    f"\n Fix a lost object by its identifier for OSD: {_osd_id}"
                    f"\n --------------------"
                )
                obj_id = json.loads(bench_obj)["oid"]
                out = cot_obj.fix_lost_object(osd_id=_osd_id, obj_id=obj_id)
                log.info(out)

        if op == "manipulate_object_content":
            # Find the object by listing the objects of the OSD or placement group.
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Manipulate object content in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            pg_id, _obj = get_bench_obj(_osd_id=_osd_id)

            # Before setting the bytes on the object, make a backup and a working copy of the object.
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-bytes > out_file
            log.info("Fetch object bytes for obj: %s in PG: %s" % (_obj, pg_id))
            cot_obj.get_bytes(
                osd_id=_osd_id, obj=_obj, pgid=pg_id, out_file="/tmp/obj_work"
            )

            # Edit the working copy object's data
            log.info("Modify bytes fetched for the object " + str(_obj))
            edit_cmd = "echo 'random data' >> /tmp/obj_work"
            out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

            # Set the bytes of the object
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-bytes < modified_file
            log.info("Inject modified object bytes for obj " + str(_obj))
            out = cot_obj.set_bytes(
                osd_id=_osd_id, obj=_obj, pgid=pg_id, in_file="/tmp/obj_work"
            )
            log.info(out)

            # verify manipulation of object data
            log.info("Fetch object bytes for obj %s post injection" % _obj)
            cot_obj.get_bytes(
                osd_id=_osd_id, obj=_obj, pgid=pg_id, out_file="/tmp/mod_obj"
            )

            try:
                mod_obj_data, _ = osd_host.exec_command(
                    sudo=True, cmd="cat /tmp/mod_obj"
                )
                log.info(mod_obj_data)
                assert "random data" in mod_obj_data
                log.info(
                    "Additional text found in modified bytes fetched for object "
                    + str(_obj)
                )
            except UnicodeDecodeError:
                log.info("Object content was not in utf-8 encoding, attempting to fix")
                mod_obj_data = mod_obj_data.decode("utf-8", "ignore")

        if op == "list_omap":
            # Use the ceph-objectstore-tool to list the contents of the object map (OMAP).
            # The output is a list of keys.
            intro = (
                f"\n --------------------"
                f"\n List the object map for OSD: {_osd_id}"
                f"\n --------------------"
            )
            log.info(intro)
            log.info("PG: " + str(omap_pg))
            log.info("Chosen object: " + str(omap_obj))

            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-omap
            log.info("Fetch object map for obj " + str(omap_obj))
            out = cot_obj.list_omap(osd_id=_osd_id, pgid=omap_pg, obj=omap_obj)
            log.info("List of omap entries for obj %s: \n %s" % (omap_obj, out))
            assert "key_" in out
            return out

        if op == "list_get_omap":
            key_list = execute_ops(op="list_omap", cot_obj=cot_obj, _osd_id=_osd_id)

            # Use the ceph-objectstore-tool utility to retrieve the object map (OMAP) key.
            # You need to provide the data path, the placement group identifier (PG ID),
            # the object, and the key in the OMAP.
            intro = (
                f"\n --------------------"
                f"\n Fetching the object map key for OSD: {_osd_id}"
                f"\n --------------------"
            )
            log.info(intro)
            omap_key = key_list.split()[-1]
            log.info("PG: " + str(omap_pg))
            log.info("Chosen object: " + str(omap_obj))
            log.info("Chosen OMAP key: " + str(omap_key))

            # fetch the omap key value
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omap $KEY > out_file
            value = cot_obj.get_omap(
                osd_id=_osd_id, obj=omap_obj, pgid=omap_pg, key=omap_key
            )
            log.info(
                "Value for OMAP key %s in object %s: %s" % (omap_key, omap_obj, value)
            )
            return omap_key

        if op == "manipulate_omap_key":
            # Use the ceph-objectstore-tool utility to change the object map (OMAP) key.
            # You need to provide the data path, the placement group identifier (PG ID),
            # the object, and the key in the OMAP.
            omap_key = execute_ops(op="list_get_omap", cot_obj=cot_obj, _osd_id=_osd_id)
            intro = (
                f"\n ----------------------------------------"
                f"\n Manipulate the omap key {omap_key} for object {omap_obj} for OSD: {_osd_id}"
                f"\n ----------------------------------------"
            )
            log.info(intro)

            # Modify OMAP key's value
            log.info("Modify OMAP key for the object %s " % omap_obj)
            mod_value = "value_1111"
            edit_cmd = f"echo '{mod_value}' > /tmp/omap_value"
            out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

            # Set the object map value for an omap key
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omap $KEY < in_file
            out = cot_obj.set_omap(
                osd_id=_osd_id,
                obj=omap_obj,
                pgid=omap_pg,
                key=omap_key,
                in_file="/tmp/omap_value",
            )
            log.info(out)

            # verify manipulation of OMAP key value
            log.info("Fetch OMAP key's value for obj %s post modification" % omap_pg)
            value = cot_obj.get_omap(
                osd_id=_osd_id, obj=omap_obj, pgid=omap_pg, key=omap_key
            )
            assert mod_value in value
            log.info(
                "Modified value(%s) for OMAP key(%s) verified for object %s"
                % (value, omap_key, omap_pg)
            )

        if op == "remove_omap_key":
            intro = (
                f"\n --------------------"
                f"\n Remove an Object map key for OSD: {_osd_id}"
                f"\n --------------------"
            )
            log.info(intro)
            key_list = execute_ops(op="list_omap", cot_obj=cot_obj, _osd_id=_osd_id)
            omap_key = key_list.split()[-1]
            log.info("Chosen omap key: " + omap_key)

            # remove the object map key
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-omap $KEY
            out = cot_obj.remove_omap(
                osd_id=_osd_id, pgid=omap_pg, obj=omap_obj, key=omap_key
            )
            log.info(out)

            # verify removal of omap key
            out = cot_obj.list_omap(osd_id=_osd_id, pgid=omap_pg, obj=omap_obj)
            log.info("List of omap entries post deletion: " + str(out))
            assert omap_key not in out

        if op == "get_omap_header":
            # The ceph-objectstore-tool utility will output the object map (OMAP) header
            # with the values associated with the object’s keys.
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch OMAP header for object {omap_obj} in OSD {_osd_id} and PG: {omap_pg}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)

            # Get the object map header
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omaphdr > out_file
            log.info("Fetch OMAP header for obj " + str(omap_obj))
            cot_obj.get_omap_header(
                osd_id=_osd_id,
                obj=omap_obj,
                pgid=omap_pg,
                out_file="/tmp/omaphdr_org",
            )

            # log the object's OMAP header
            log.info("OMAP header fetched for the object " + str(omap_obj))
            out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/omaphdr_org")
            log.info(out)

        if op == "manipulate_omap_header":
            # Edit the working copy of object's OMAP header
            log.info("Modify OMAP header fetched for the object " + str(omap_obj))
            edit_cmd = "echo 'random omap header data' >> /tmp/omaphdr_org"
            out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

            # Set the modified object map header
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omaphdr < in_file
            out = cot_obj.set_omap_header(
                osd_id=_osd_id, obj=omap_obj, pgid=omap_pg, in_file="/tmp/omaphdr_org"
            )
            log.info(out)

            # verify manipulation of OMAP header data
            log.info("Fetch OMAP header for obj %s post modification" % omap_obj)
            cot_obj.get_omap_header(
                osd_id=_osd_id, obj=omap_obj, pgid=omap_pg, out_file="/tmp/omaphdr_mod"
            )

            mod_omap_header, _ = osd_host.exec_command(
                sudo=True, cmd="cat /tmp/omaphdr_mod"
            )
            assert "random omap header" in mod_omap_header
            log.info(
                "Additional text found in modified OMAP header fetched for object "
                + str(omap_obj)
            )

        if op == "list_attributes":
            # Use the ceph-objectstore-tool utility to list an object’s attributes.
            # The output provides you with the object’s keys and values.
            intro = (
                f"\n --------------------------------"
                f"\n List attributes of an object within OSD: {_osd_id}"
                f"\n --------------------------------"
            )
            log.info(intro)
            log.info("PG: " + str(bench_pg))
            log.info("Chosen object: " + str(bench_obj))

            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-attrs
            log.info("List attributes for obj " + str(bench_obj))
            out = cot_obj.list_attributes(osd_id=_osd_id, pgid=bench_pg, obj=bench_obj)
            attr_list = out.split()
            log.info("List of attributes for obj %s: \n %s" % (bench_obj, attr_list))
            return attr_list

        if op == "list_get_attributes":
            attr_list = execute_ops(
                op="list_attributes", cot_obj=cot_obj, _osd_id=_osd_id
            )

            obj_attr = attr_list[-1]
            log.info("Chosen attribute for object %s: %s" % (bench_obj, obj_attr))

            # Use the ceph-objectstore-tool utility to fetch an object’s attribute.
            # To fetch the object’s attributes you need the data and journal paths,
            # the placement group identifier (PG ID), the object,
            # and the key in the object’s attribute.
            # fetch the value of object's attribute
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-attr $KEY > out_file
            intro = (
                f"\n -----------------------------------"
                f"\n Fetch object attribute for OSD: {_osd_id}"
                f"\n ------------------------------------"
            )
            log.info(intro)
            value = cot_obj.get_attribute(
                osd_id=_osd_id, obj=bench_obj, pgid=bench_pg, attr=obj_attr
            )
            log.info(
                "Value for object attribute %s in object %s: %s"
                % (obj_attr, bench_obj, value)
            )
            return obj_attr

        if op == "manipulate_object_attribute":
            # Use the ceph-objectstore-tool utility to change an object’s attribute.
            # To manipulate the object’s attributes you need the data and journal paths,
            # the placement group identifier (PG ID), the object,
            # and the key in the object’s attribute.

            intro = (
                f"\n -----------------------------------"
                f"\n Manipulate object attribute for OSD: {_osd_id}"
                f"\n ------------------------------------"
            )
            log.info(intro)
            obj_attr = execute_ops(
                op="list_get_attributes", cot_obj=cot_obj, _osd_id=_osd_id
            )

            # Modify object attribute's value
            log.info(
                "Modify %s object attribute's value for %s" % (obj_attr, bench_obj)
            )
            mod_value = "Base64:AwIdAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            edit_cmd = f"echo '{mod_value}' > /tmp/attr_value"
            out, _ = osd_host.exec_command(sudo=True, cmd=edit_cmd)

            # Set the object attribute's value
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-attr $KEY < in_file
            out = cot_obj.set_attribute(
                osd_id=_osd_id,
                obj=bench_obj,
                pgid=bench_pg,
                attr=obj_attr,
                in_file="/tmp/attr_value",
            )
            log.info(out)

            # verify manipulation of object attributes value
            value = cot_obj.get_attribute(
                osd_id=_osd_id, obj=bench_obj, pgid=bench_pg, attr=obj_attr
            )
            log.info(
                "Modified value for object attribute %s in object %s: %s"
                % (obj_attr, bench_obj, value)
            )
            assert mod_value in value
            log.info(
                "Modified value %s for object attribute %s verified for object %s"
                % (value, obj_attr, bench_obj)
            )

        if op == "remove_obj_attribute":
            intro = (
                f"\n ----------------------------------"
                f"\n Remove an Object attribute for OSD: {_osd_id}"
                f"\n ----------------------------------"
            )
            log.info(intro)
            obj_attr = execute_ops(
                op="list_get_attributes", cot_obj=cot_obj, _osd_id=_osd_id
            )

            # remove the object attribute entry
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-attr $KEY
            out = cot_obj.remove_attribute(
                osd_id=_osd_id, pgid=bench_pg, obj=bench_obj, attr=obj_attr
            )
            log.info(out)

            # verify removal of object attribute
            out = cot_obj.list_attributes(osd_id=_osd_id, pgid=bench_pg, obj=bench_obj)
            log.info("List of object attributes post deletion: " + str(out))
            assert obj_attr not in out

        if op == "get_bytes":
            # Find the object by listing the objects of the OSD or placement group.
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch object content in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)

            # Before setting the bytes on the object, make a backup and a working copy of the object.
            # ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-bytes > out_file
            log.info("Fetch object bytes for obj: %s in PG: %s" % (bench_obj, bench_pg))
            cot_obj.get_bytes(
                osd_id=_osd_id,
                obj=bench_obj,
                pgid=bench_pg,
                out_file="/tmp/obj_work",
            )

            # log the working copy object's data
            log.info("Logging bytes fetched for the object")
            out, _ = osd_host.exec_command(sudo=True, cmd="cat /tmp/obj_work")
            log.info(out)

        if op == "dump":
            # Obtain the object dump for objects of the OSD or placement group.
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch object dump for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.fetch_object_dump(
                osd_id=_osd_id, pgid=bench_pg, obj=bench_obj
            )
            log.info(out)
            obj_dump = json.loads(out)
            obj_dump_stats = obj_dump["stat"]
            obj_dump_extents = obj_dump["onode"]["extents"]
            log.info("Object dump stats: \n " + str(obj_dump_stats))
            log.info("Object dump extents: \n " + str(obj_dump_extents))

        if op == "export":
            # log.warning(
            #     "Automation framework is currently incapable of handling "
            #     "the o/p of COT export, skipping execution"
            # )
            # log.warning(
            #     "Tracker: https://issues.redhat.com/browse/RHCEPHQE-20426"
            # )
            # export the content of a PG from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Export the content of a PG in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            log.info("Get the list of PGs in OSD: " + str(_osd_id))
            _pg_list = execute_ops(op="list_pgs", cot_obj=cot_obj, _osd_id=_osd_id)
            _pg_id = random.choice(
                [pgid for pgid in _pg_list if pgid.startswith(("1", "2", "3"))]
            )
            log.info("PG for which data will be exported: " + str(_pg_id))
            out = cot_obj.export(osd_id=_osd_id, pgid=_pg_id, out_file="/mnt/pg_export")
            log.info(out)
            assert "Exporting" in out and "Export successful" in out

        if op == "meta-list":
            # get the meta-list of an object from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch the meta-list for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.get_meta_list(
                osd_id=_osd_id, pgid=bench_pg, obj_name=bench_obj
            )
            log.info(out)

        if op == "get-osdmap":
            # get the osd-map of an object from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch the osd-map for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.get_osdmap(osd_id=_osd_id, pgid=bench_pg, obj_name=bench_obj)
            log.info(out)

        if op == "inc-get-osdmap":
            # get the inc-osd-map of an object from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch the inc-osd-map for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            # pg_id, obj = get_bench_obj(_osd_id)
            out = cot_obj.get_inc_osdmap(
                osd_id=_osd_id, pgid=bench_pg, obj_name=bench_obj
            )
            log.info(out)

        if op == "get-superblock":
            # get the objectstore superblock of an object from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch the objectstore superblock for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.get_superblock(osd_id=_osd_id)
            log.info(out)

        if op == "get-inc-osdmap":
            # get the objectstore inc-osdmap of an object from an OSD
            intro = (
                f"\n ------------------------------------------------------"
                f"\n Fetch the objectstore inc-osdmap for an object in OSD {_osd_id}"
                f"\n ------------------------------------------------------"
            )
            log.info(intro)
            out = cot_obj.get_inc_osdmap(
                osd_id=_osd_id, pgid=bench_pg, obj_name=bench_obj
            )
            log.info(out)

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        if config.get("bluestore-enospc"):

            log.info(
                "\n\n Execution begins for COT Bluestore ENOSPC scenarios ************ \n\n"
            )

            # create a data pool with single pg
            _pool_name = "cot-enospc"
            assert rados_obj.create_pool(pool_name=_pool_name, pg_num=1, pg_num_max=1)
            log.info("Pool %s with single PG created successfully" % _pool_name)

            # retrieving the size of each osd part of acting set for the pool
            acting_set = rados_obj.get_pg_acting_set(pool_name=_pool_name)
            # osd_sizes = {}
            # for osd_id in acting_set:
            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter=f"osd.{acting_set[0]}"
            )
            #    osd_sizes[osd_id] = osd_df_stats["nodes"][0]["kb"]
            primary_osd_size = osd_df_stats["nodes"][0]["kb"]

            log.info(
                "Write OMAP entries to the pool using librados, 200 objects with 5 omap entries each"
            )
            assert pool_obj.fill_omap_entries(
                pool_name=_pool_name, obj_start=0, obj_end=200, num_keys_obj=5
            )
            time.sleep(30)

            # set nearfull, backfill-full and full-ratio to 100%
            # set noout and norebalance flags
            cmds = [
                "ceph osd set-full-ratio 1",
                "ceph osd set-backfillfull-ratio 1",
                "ceph osd set-nearfull-ratio 1",
                "ceph osd set noout",
                "ceph osd set norebalance",
            ]

            [cephadm.shell(args=[cmd]) for cmd in cmds]

            # determine the number of objects to be written to the pool
            # to achieve ENOPSC state
            objs_enospc = int(primary_osd_size / bench_obj_size_kb * 0.9)

            # perform rados bench to fill OSDs till 90%
            rados_obj.bench_write(
                pool_name=_pool_name,
                rados_write_duration=600,
                max_objs=objs_enospc,
                byte_size=f"{bench_obj_size_kb}KB",
                verify_stats=False,
                check_ec=False,
            )
            time.sleep(30)

            # calculate the number of 3KB objects needed to utilize 15% of the OSD
            objs_enospc = int(primary_osd_size / 3 * 0.15)
            init_time, _ = rados_obj.client.exec_command(
                cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
            )

            osd_down = False
            for _ in range(3):
                # perform rados bench to trigger ENOSPC warning
                rados_obj.bench_write(
                    pool_name=_pool_name,
                    rados_write_duration=600,
                    max_objs=objs_enospc,
                    byte_size="3072",
                    verify_stats=False,
                    check_ec=False,
                )
                time.sleep(30)

                # log the cluster and pool fill %
                cluster_fill = (
                    int(rados_obj.get_cephdf_stats()["stats"]["total_used_raw_ratio"])
                    * 100
                )
                pool_fill = (
                    int(
                        rados_obj.get_cephdf_stats(pool_name=_pool_name)["stats"][
                            "percent_used"
                        ]
                    )
                    * 100
                )

                log.info("Cluster fill percentage: %d" % cluster_fill)
                log.info("Pool %s fill percentage: %d" % (_pool_name, pool_fill))

                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
                # wait for 300 secs to let OSDs crash
                while datetime.datetime.now() < timeout_time:
                    for _osd_id in acting_set:
                        if "down" == rados_obj.fetch_osd_status(_osd_id):
                            log.info("OSD %s is down, as expected" % _osd_id)
                            osd_down = True
                            break  # exit for-loop if even a single OSD is down
                    else:
                        log.warning(
                            "OSDs %s are up and running, re-check after 30 secs"
                            % acting_set
                        )
                        time.sleep(30)
                    if osd_down:
                        break  # exit while-loop if even a single OSD is down
                else:
                    log.warning(
                        "None of the OSDs went down with ENOPSC, rerun rados bench"
                    )
                    time.sleep(10)

                if osd_down:
                    down_osds = rados_obj.get_osd_list(status="down")
                    log.info("Down OSDs on the cluster: %s" % down_osds)
                    break  # exit outer for-loop if even a single OSD is down
            else:
                log.error("Could not generate ENOSPC on OSDs after 3 attempts")
                raise Exception("Could not generate ENOSPC on OSDs after 3 attempts")

            end_time, _ = rados_obj.client.exec_command(
                cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
            )
            # check for entries of "ceph_abort_msg" and "bluefs enospc" in OSD log
            log_lines = rados_obj.get_journalctl_log(
                start_time=init_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=_osd_id,
            )

            for line in log_lines.splitlines():
                if "bluefs enospc" in line:
                    log.info("Expected BlueFS ENOSPC log entry found:\n %s" % line)
                    break
            else:
                err_msg = (
                    "Expected BluesFS ENOSPC log entries not found for OSD %s" % _osd_id
                )
                log.error(err_msg)
                raise Exception(err_msg)

            omap_pg, omap_obj = get_omap_obj(_osd_id, cot_obj=objectstore_obj)
            bench_pg, bench_obj = get_bench_obj(_osd_id, cot_obj=objectstore_obj)

            # execute COT commands that are now feasible in read-only mode
            for operation in readonly_ops:
                if operation == "list_get_attributes":
                    log.info(
                        "Skipping execution for 'get attribute' cmd during ENOSPC. "
                        "Bug - https://bugzilla.redhat.com/show_bug.cgi?id=2404644"
                    )
                    continue
                log.info("Next operation will be performed for ENOSPC OSD %s" % _osd_id)
                execute_ops(op=operation, cot_obj=objectstore_obj, _osd_id=_osd_id)

            log.info(
                "ceph-objectstore-tool read-only functionalities verified successfully"
            )
        else:
            log.info(
                "\n\n Execution begins for COT scenarios on healthy OSD ************ \n\n"
            )
            osd_list = rados_obj.get_osd_list(status="up")
            log.info("List of OSDs: \n " + str(osd_list))

            pool_cfg = config.get("pool_config", {})
            pool_type = pool_cfg.get("pool_type", "replicated")
            if pool_type == "erasure":
                log.info("Create an EC data pool with default config")
                _pool_name = pool_cfg.get("pool_name", "cot-ec-pool")
                default_ec_pool_config = {
                    "pool_name": _pool_name,
                    "profile_name": "ec_profile_cot",
                    "k": 4,
                    "m": 2,
                    "erasure_code_use_overwrites": "true",
                    "enable_fast_ec_features": "true",
                    "pg_num": 128,
                    "pg_num_min": 128,
                }
                ec_pool_config = config.get("pool_config", default_ec_pool_config)
                assert rados_obj.create_erasure_pool(**ec_pool_config)
            else:
                log.info("Create a replicated data pool with default config")
                _pool_name = "cot-repli-pool"
                assert rados_obj.create_pool(
                    pool_name=_pool_name, pg_num=128, pg_num_min=128
                )

            log.info("Write data to the pool using rados bench, 1000 objects")
            assert rados_obj.bench_write(
                pool_name=_pool_name,
                rados_write_duration=200,
                max_objs=1000,
                verify_stats=False,
            )

            if pool_type != "erasure":
                log.info(
                    "Write OMAP entries to the pool using librados, 200 objects with 5 omap entries each"
                )
                assert pool_obj.fill_omap_entries(
                    pool_name=_pool_name, obj_start=0, obj_end=200, num_keys_obj=5
                )

            # choose a random osd and stop it
            osd_id = pick_random_osd()

            if pool_type != "erasure":
                omap_pg, omap_obj = get_omap_obj(osd_id, cot_obj=objectstore_obj)
            bench_pg, bench_obj = get_bench_obj(osd_id, cot_obj=objectstore_obj)

            for operation in all_ops:
                if "omap" in operation and pool_type == "erasure":
                    log.info(
                        "Operation %s is not valid for EC pools, moving on to next ops",
                        operation,
                    )
                    continue
                op_osd = osd_id
                if operation in ["manipulate_object_content", "remove_object"]:
                    op_osd = pick_random_osd()
                log.info(
                    "\n\n Next operation will be performed for DOWN OSD %s" % op_osd
                )
                execute_ops(op=operation, cot_obj=objectstore_obj, _osd_id=op_osd)

            log.info("ceph-objectstore-tool functionalities verified successfully")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info("\n\n\n*********** Execution of finally block starts ***********\n\n")
        # start stopped OSD
        if not config.get("bluestore-enospc"):
            for osd_id in STOPPED_OSDS:
                log.debug("Starting stopped OSD %s" % osd_id)
                rados_obj.change_osd_state(action="start", target=osd_id)

        # reset nearfull, backfill-full and full-ratio to 100%
        # reset noout and norebalance flags
        cmds = [
            "ceph osd set-full-ratio 0.95",
            "ceph osd set-backfillfull-ratio 0.8",
            "ceph osd set-nearfull-ratio 0.75",
            "ceph osd unset noout",
            "ceph osd unset norebalance",
        ]
        [cephadm.shell(args=[cmd]) for cmd in cmds]

        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ) and not config.get("bluestore-enospc"):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed verification of Ceph-objectstore-Tool commands.")
    return 0
