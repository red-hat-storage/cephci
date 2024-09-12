import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-11264 - Move data and meta_data pool to different root level bucket in OSD tree.

    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 2 buckets with root1 and root2 and move them to root level
    2. Move OSDs of 1 host to root1
    3. Move osds of other host to root2
    4. Collect Crush map
    5. Convert Crush map into text
    6. Edit Crush map and add root1 as destination in replicated rule
    7. Set crush map
    8. Mount and write data to the cephfs
    9. Validate the data going to root1 bucket
    10. Change the crush map and make it to  use root2
    11. Data should be migrated to root2 bucket osds

    Cleanup:
    Umount and remove data
    set back the crush to older values
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        log.info("Install ceph-base as we require while generating crush map")
        client1.exec_command(
            sudo=True, cmd="yum install ceph-base -y --nogpgcheck", check_ec=False
        )
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
            if not fs_util.wait_for_mds_process(client1, fs_name):
                raise CommandFailed(f"Failed to start MDS deamons for {fs_name}")

        out, rc = client1.exec_command(sudo=True, cmd="ceph osd tree -f json")
        osd_tree = json.loads(out)
        root_children_ids = [
            node["children"] for node in osd_tree["nodes"] if node["name"] == "default"
        ][0]
        log.info(root_children_ids)
        children_dict = {}
        for node in osd_tree["nodes"]:
            if node["id"] in root_children_ids:
                children_ids = [child_id for child_id in node["children"]]
                children_dict[node["name"]] = children_ids
        log.info(children_dict)
        log.info("Create backup crush map for reverting after")
        client1.exec_command(
            sudo=True, cmd="ceph osd getcrushmap > crush.map_initial.bin"
        )

        log.info("Create root1 and root2 buckets")
        client1.exec_command(sudo=True, cmd="ceph osd crush add-bucket root1 root")
        client1.exec_command(sudo=True, cmd="ceph osd crush add-bucket root2 root")

        client1.exec_command(sudo=True, cmd="ceph osd crush move root1 root=default")
        client1.exec_command(sudo=True, cmd="ceph osd crush move root2 root=default")

        out, rc = client1.exec_command(sudo=True, cmd="ceph osd tree")
        log.info(f"\n{out}")

        log.info("move the osds to root1 and root2")
        root1_osd_list = []
        root2_osd_list = []
        for node in osd_tree["nodes"]:
            if node["id"] == root_children_ids[0]:
                osd_list = children_dict[node["name"]]
                for osd in osd_list:
                    root1_osd_list.append(osd)
                    client1.exec_command(
                        sudo=True, cmd=f"ceph osd crush move osd.{osd} root=root1"
                    )
            if node["id"] == root_children_ids[1]:
                osd_list = children_dict[node["name"]]
                for osd in osd_list:
                    root2_osd_list.append(osd)
                    client1.exec_command(
                        sudo=True, cmd=f"ceph osd crush move osd.{osd} root=root2"
                    )

        out, rc = client1.exec_command(sudo=True, cmd="ceph osd tree")
        log.info(f"\n{out}")

        log.info("Get the Crush map")
        client1.exec_command(
            sudo=True, cmd="ceph osd getcrushmap > crush.map_root1_root_2.bin"
        )

        client1.exec_command(
            sudo=True,
            cmd="crushtool -d crush.map_root1_root_2.bin -o crush.map_root1.txt",
        )

        client1.exec_command(
            sudo=True, cmd="cp crush.map_root1.txt crush.map_root1_bkp.txt"
        )

        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/step take default/step take root1/' crush.map_root1.txt",
        )
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/step chooseleaf firstn 0 type host/step chooseleaf firstn 0 type osd/' crush.map_root1.txt",
        )

        log.info("Apply the crush rule on the ceph")
        client1.exec_command(
            sudo=True, cmd="crushtool -c crush.map_root1.txt -o crush_root1.map.bin"
        )
        client1.exec_command(
            sudo=True, cmd="ceph osd setcrushmap -i crush_root1.map.bin"
        )

        df_utilization_root1_initial, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root1 -f json"
        )
        df_utilization_root1_initial = json.loads(df_utilization_root1_initial)

        df_utilization_root2_initial, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root2 -f json"
        )
        df_utilization_root2_initial = json.loads(df_utilization_root2_initial)
        log.info("Initial df values for root1 and root2")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root1")
        log.info(f"\n{out}")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root2")
        log.info(f"\n{out}")
        log.info("mount the ceph and add IOs")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {fs_name}",
            fstab=True,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={fuse_mounting_dir_1}/test_1.txt bs=1G count=1",
            long_running=True,
        )
        df_utilization_root1_step_1, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root1 -f json"
        )
        df_utilization_root1_step_1 = json.loads(df_utilization_root1_step_1)

        df_utilization_root2_step_1, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root2 -f json"
        )
        df_utilization_root2_step_1 = json.loads(df_utilization_root2_step_1)
        log.info("Step 1 df values for root1 and root2")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root1")
        log.info(f"\n{out}")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root2")
        log.info(f"\n{out}")

        if (
            df_utilization_root1_initial["summary"]["total_kb_used"]
            < df_utilization_root1_step_1["summary"]["total_kb_used"]
        ):
            log.info(
                f"Utilization has been increased after filling data before: "
                f"{df_utilization_root1_initial['summary']['total_kb_used']}"
                f"\n {df_utilization_root1_step_1['summary']['total_kb_used']}"
            )
        else:
            raise CommandFailed(
                (
                    f"Utilization has not increased after filling data before: "
                    f"{df_utilization_root1_initial['summary']['total_kb_used']}"
                    f"\n {df_utilization_root1_step_1['summary']['total_kb_used']}"
                )
            )

        if (
            df_utilization_root2_initial["summary"]["total_kb_used"]
            >= df_utilization_root2_step_1["summary"]["total_kb_used"]
        ):
            raise CommandFailed(
                (
                    f"Utilization has increased after filling data in root2 : "
                    f"{df_utilization_root2_initial['summary']['total_kb_used']}"
                    f"\n {df_utilization_root2_step_1['summary']['total_kb_used']}"
                )
            )
        log.info("Change crush rule to root2")
        client1.exec_command(
            sudo=True, cmd="cp crush.map_root1.txt crush.map_root2.txt"
        )
        client1.exec_command(
            sudo=True,
            cmd="sed -i 's/step take root1/step take root2/' crush.map_root2.txt",
        )
        client1.exec_command(
            sudo=True, cmd="crushtool -c crush.map_root2.txt -o crush_root2.map.bin"
        )
        client1.exec_command(
            sudo=True, cmd="ceph osd setcrushmap -i crush_root2.map.bin"
        )

        time.sleep(120)
        df_utilization_root1_step_2, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root1 -f json"
        )
        df_utilization_root1_step_2 = json.loads(df_utilization_root1_step_2)

        df_utilization_root2_step_2, rc = client1.exec_command(
            sudo=True, cmd="ceph osd df root2 -f json"
        )
        df_utilization_root2_step_2 = json.loads(df_utilization_root2_step_2)
        log.info("Step 2 df values for root1 and root2")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root1")
        log.info(f"\n{out}")
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd df root2")
        log.info(f"\n{out}")
        if (
            df_utilization_root1_step_1["summary"]["total_kb_used"]
            < df_utilization_root1_step_2["summary"]["total_kb_used"]
        ):
            raise CommandFailed(
                f"Data not migrated to the root2 from root1 \n before :"
                f"{df_utilization_root1_step_1['summary']['total_kb_used']}"
                f"\n after:{df_utilization_root1_step_2['summary']['total_kb_used']}"
            )
        log.info(
            f"data has been migrated from root1 \n before: "
            f"{df_utilization_root1_step_1['summary']['total_kb_used']}"
            f"\n After :{df_utilization_root1_step_2['summary']['total_kb_used']}"
        )

        if (
            df_utilization_root2_step_2["summary"]["total_kb_used"]
            < df_utilization_root2_step_1["summary"]["total_kb_used"]
        ):
            raise CommandFailed(
                f"Data migrated to different root than root2 \nbefore : "
                f"{df_utilization_root2_step_1['summary']['total_kb_used']}, "
                f"After it is :{df_utilization_root2_step_2['summary']['total_kb_used']}"
            )
        log.info(
            f"data has been migrated to root2 \n before: "
            f"{df_utilization_root2_step_1['summary']['total_kb_used']}"
            f"\n After :{df_utilization_root2_step_2['summary']['total_kb_used']}"
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        client1.exec_command(
            sudo=True, cmd="ceph osd setcrushmap -i crush.map_initial.bin"
        )
        out, rc = client1.exec_command(sudo=True, cmd="ceph osd tree")
        log.info(f"\n{out}")
