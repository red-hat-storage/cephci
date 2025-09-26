import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_declarative,
    deploy_smb_service_imperative,
    smb_cifs_mount,
    smb_cleanup,
    smbclient_check_shares,
)

from ceph.ceph_admin import CephAdmin
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def compare_io_limit_values(aio_rate_limit_conf, io_perf_data):
    try:
        log.info(f"qos rate limit values: {aio_rate_limit_conf}")
        log.info(f"io_perf_data: {io_perf_data}")
        metrics_to_check = [
            "read_iops_limit",
            "write_iops_limit",
            "read_bw_limit",
            "write_bw_limit",
        ]
        for share, aio_rate_limit_values in aio_rate_limit_conf.items():
            io_perf_vals = io_perf_data.get(share, {})
            for metric in metrics_to_check:
                io_perf_val = io_perf_vals.get(metric, 0)
                aio_rate_limit_value = aio_rate_limit_values.get(metric, 0)
                if io_perf_val > aio_rate_limit_value:
                    log.error("io values greater than qos set value")
                    return 1
        return 0
    except Exception as e:
        raise ConfigError(f"Fail to compare limit vlaue, Error {e}")


def run_fio_for_share(
    client, share, rw, rwmixread, bs, ioengine, iodepth, size, runtime, numjobs
):
    try:
        fio_perf_data = {}
        # Run FIO
        cmd = f"""fio --name=smb_test_{share} \
        --directory=/mnt/{share} \
        --rw={rw} --rwmixread={rwmixread} --bs={bs} --ioengine={ioengine} --iodepth={iodepth} \
        --size={size} \
        --runtime={runtime} --time_based --numjobs={numjobs} --group_reporting \
        --output-format=json --output=fio_output_{share}.json"""
        client.exec_command(
            sudo=True,
            cmd=cmd,
        )

        # fetch iops value
        cmd = f"""jq -n --slurpfile f fio_output_{share}.json '
        {{
        "{share}": {{
            "read_iops_limit": ($f[0].jobs[0].read.iops),
            "write_iops_limit": ($f[0].jobs[0].write.iops),
            "read_bw_limit": ($f[0].jobs[0].read.bw * 1024 | floor),
            "write_bw_limit": ($f[0].jobs[0].write.bw * 1024 | floor)
        }}
        }}'"""
        out = client.exec_command(
            sudo=True,
            cmd=cmd,
        )[0]
        out = json.loads(out)
        for share, vals in out.items():
            fio_perf_data[share] = {
                k: int(round(v)) if "iops" in k else v for k, v in vals.items()
            }
        return fio_perf_data
    except Exception as e:
        raise OperationFailedError(f"Fail to run fio per share, Error {e}")


def run_fio_parallel(
    client, smb_shares, rw, rwmixread, bs, ioengine, iodepth, size, runtime, numjobs
):
    try:
        fio_perf_data = {}
        with ThreadPoolExecutor(max_workers=len(smb_shares)) as executor:
            future_to_share = {
                executor.submit(
                    run_fio_for_share,
                    client,
                    share,
                    rw,
                    rwmixread,
                    bs,
                    ioengine,
                    iodepth,
                    size,
                    runtime,
                    numjobs,
                ): share
                for share in smb_shares
            }
            for future in as_completed(future_to_share):
                share_data = future.result()
                fio_perf_data.update(share_data)
        return fio_perf_data
    except Exception as e:
        raise OperationFailedError(f"Fail to run parallel fio, Error {e}")


def verify_qos_with_smb_show(installer, aio_rate_limit_conf, smb_cluster_id):
    try:
        # Fetch qos values from smb show
        out = CephAdm(installer).ceph.smb.show("ceph.smb.share")
        data = json.loads(out)
        smb_show_qos_info = {}
        for res in data.get("resources", []):
            share_id = res.get("share_id")
            qos = res.get("cephfs", {}).get("qos", {})
            smb_show_qos_info[share_id] = qos
        log.info(f"ceph smb show qos values {smb_show_qos_info}")

        # Validate smb show value
        for share, actual_qos in smb_show_qos_info.items():
            expected_qos = aio_rate_limit_conf.get(share)
            if (not actual_qos and expected_qos) or (actual_qos and not expected_qos):
                log.error(
                    f"QoS mismatch for share '{share}': actual {actual_qos}, expected {expected_qos}"
                )
                continue
            mismatches = {
                k: (actual_qos[k], expected_qos[k])
                for k in actual_qos
                if expected_qos.get(k) != actual_qos[k]
            }
            if mismatches:
                log.error(f"QoS value mismatches for share '{share}': {mismatches}")
    except Exception as e:
        raise ConfigError(f"Fail to verify qos value with smb show, Error {e}")


def verify_qos_with_testparm(aio_rate_limit_conf, smb_cluster_id, smb_nodes):
    try:
        # Fetch qos values from testparm
        cmd = f"cephadm ls --no-detail | jq -r 'map(select(.name | startswith(\"smb.{smb_cluster_id}\")))[-1].name'"
        out = smb_nodes[0].exec_command(sudo=True, cmd=cmd)[0].strip()
        cmd = f'cephadm enter -n {out} -- bash -c "testparm -s"'
        out = smb_nodes[0].exec_command(sudo=True, cmd=cmd)
        out = out[0] + "\n" + out[1]
        testparm_shares_qos = {}
        current_share = None
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("[") and line.endswith("]") and line != "[global]":
                current_share = line.strip("[]")
                testparm_shares_qos[current_share] = {}
            elif current_share and line.startswith("aio_ratelimit:"):
                key, val = line.split("=", 1)
                key = key.split(":", 1)[1].strip()
                val = int(val.strip())
                testparm_shares_qos[current_share][key] = val
        log.info(f"testparm qos values {testparm_shares_qos}")

        # Validate testparm qos value
        for share, actual_qos in testparm_shares_qos.items():
            expected_qos = aio_rate_limit_conf.get(share)
            if (not actual_qos and expected_qos) or (actual_qos and not expected_qos):
                log.error(
                    f"QoS mismatch for share '{share}': actual {actual_qos}, expected {expected_qos}"
                )
                continue
            mismatches = {
                k: (actual_qos[k], expected_qos[k])
                for k in actual_qos
                if expected_qos.get(k) != actual_qos[k]
            }
            if mismatches:
                log.error(f"QoS value mismatches for share '{share}': {mismatches}")
    except Exception as e:
        raise ConfigError(f"Fail to verify qos value with testpram, Error {e}")


def verify_qos_values(installer, aio_rate_limit_conf, smb_cluster_id, smb_nodes):

    # Verify qos value with ceph smb show command
    verify_qos_with_smb_show(installer, aio_rate_limit_conf, smb_cluster_id)

    # Verify qos value with conatiner testparm
    verify_qos_with_testparm(aio_rate_limit_conf, smb_cluster_id, smb_nodes)


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")

    # Get cephadm obj
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    # Get cephfs volume
    cephfs_vol = config.get("cephfs_volume", "cephfs")

    # Get smb subvloume group
    smb_subvol_group = config.get("smb_subvolume_group", "smb")

    # Get smb subvloumes
    smb_subvols = config.get("smb_subvolumes", ["sv1"])

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get smb cluster id
    smb_cluster_id = config.get("smb_cluster_id", "smb1")

    # Get auth_mode
    auth_mode = config.get("auth_mode", "user")

    # Get domain_realm
    domain_realm = config.get("domain_realm", None)

    # Get custom_dns
    custom_dns = config.get("custom_dns", None)

    # Get smb user name
    smb_user_name = config.get("smb_user_name", "user1")

    # Get smb user password
    smb_user_password = config.get("smb_user_password", "passwd")

    # Get smb shares
    smb_shares = config.get("smb_shares", ["share1"])

    # Get smb path
    path = config.get("path", "/")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Check ctdb clustering
    clustering = config.get("clustering", "default")

    # Check aio rate limit conf
    aio_rate_limit_conf = config.get("aio_rate_limit_conf")

    # Check mount type
    mount_type = config.get("mount_type", "cifs")

    # Check io tool
    io_tool = config.get("io_tool", "fio")

    # Check deployment method
    deployment_method = config.get("deployment_method", "imperative")

    # Check deployment method
    deployment_method = config.get("deployment_method", "imperative")

    # Check Public Addrs
    public_addrs = config.get("public_addrs", None)

    # Check Smb Port
    smb_port = config.get("smb_port", "445")

    try:
        # deploy smb services
        if deployment_method == "imperative":
            deploy_smb_service_imperative(
                installer,
                cephfs_vol,
                smb_subvol_group,
                smb_subvols,
                smb_subvolume_mode,
                smb_cluster_id,
                auth_mode,
                smb_user_name,
                smb_user_password,
                smb_shares,
                path,
                domain_realm,
                custom_dns,
                clustering,
            )

        if deployment_method == "declarative":
            # Get spec file type
            file_type = config.get("file_type")
            # Get smb spec
            smb_spec = config.get("spec")
            # Get smb spec file mount path
            file_mount = config.get("file_mount", "/tmp")
            # Get smb service value from spec file
            smb_shares = []
            smb_subvols = []
            for spec in smb_spec:
                if spec["resource_type"] == "ceph.smb.cluster":
                    smb_cluster_id = spec["cluster_id"]
                    auth_mode = spec["auth_mode"]
                    if "domain_settings" in spec:
                        domain_realm = spec["domain_settings"]["realm"]
                    else:
                        domain_realm = None
                    if "clustering" in spec:
                        clustering = spec["clustering"]
                    else:
                        clustering = "default"
                    if "public_addrs" in spec:
                        public_addrs = [
                            public_addrs["address"].split("/")[0]
                            for public_addrs in spec["public_addrs"]
                        ]
                    else:
                        public_addrs = None
                    if "custom_ports" in spec:
                        smb_port = spec["custom_ports"]["smb"]
                    else:
                        smb_port = "445"
                elif spec["resource_type"] == "ceph.smb.usersgroups":
                    smb_user_name = spec["values"]["users"][0]["name"]
                    smb_user_password = spec["values"]["users"][0]["password"]
                elif spec["resource_type"] == "ceph.smb.join.auth":
                    smb_user_name = spec["auth"]["username"]
                    smb_user_password = spec["auth"]["password"]
                elif spec["resource_type"] == "ceph.smb.share":
                    cephfs_vol = spec["cephfs"]["volume"]
                    smb_subvol_group = spec["cephfs"]["subvolumegroup"]
                    smb_subvols.append(spec["cephfs"]["subvolume"])
                    smb_shares.append(spec["share_id"])
            # deploy smb services
            deploy_smb_service_declarative(
                installer,
                cephfs_vol,
                smb_subvol_group,
                smb_subvols,
                smb_cluster_id,
                smb_subvolume_mode,
                file_type,
                smb_spec,
                file_mount,
            )

        # Verify ctdb clustering
        if clustering != "never":
            # check samba clustermeta in rados
            if not check_rados_clustermeta(cephadm, smb_cluster_id, smb_nodes):
                log.error("rados clustermeta for samba not found")
                return 1
            # Verify CTDB health
            if not check_ctdb_health(smb_nodes, smb_cluster_id):
                log.error("ctdb health error")
                return 1

        # Check smb share using smbclient
        smbclient_check_shares(
            smb_nodes,
            client,
            smb_shares,
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
            smb_port,
            public_addrs,
        )

        # Update qos value
        if deployment_method == "imperative":
            for smb_share in smb_shares:
                if smb_share in aio_rate_limit_conf:
                    CephAdm(installer).ceph.smb.share.update_cephfs_qos(
                        smb_cluster_id,
                        smb_share,
                        read_iops_limit=aio_rate_limit_conf[smb_share][
                            "read_iops_limit"
                        ],
                        write_iops_limit=aio_rate_limit_conf[smb_share][
                            "write_iops_limit"
                        ],
                        read_bw_limit=aio_rate_limit_conf[smb_share]["read_bw_limit"],
                        write_bw_limit=aio_rate_limit_conf[smb_share]["write_bw_limit"],
                        read_delay_max=aio_rate_limit_conf[smb_share]["read_delay_max"],
                        write_delay_max=aio_rate_limit_conf[smb_share][
                            "write_delay_max"
                        ],
                    )

        # Verify qos values updated as expected
        verify_qos_values(installer, aio_rate_limit_conf, smb_cluster_id, smb_nodes)

        # Mount smb shares and run io
        if mount_type == "cifs":
            # Mount shares
            for smb_share in smb_shares:
                cifs_mount_point = f"/mnt/{smb_share}"
                smb_cifs_mount(
                    smb_nodes[0],
                    client,
                    smb_share,
                    smb_user_name,
                    smb_user_password,
                    auth_mode,
                    domain_realm,
                    cifs_mount_point,
                )
            if io_tool == "fio":
                # Run FIO
                rw = config.get("fio")["rw"]
                rwmixread = config.get("fio")["rwmixread"]
                bs = config.get("fio")["bs"]
                ioengine = config.get("fio")["ioengine"]
                iodepth = config.get("fio")["iodepth"]
                size = config.get("fio")["size"]
                runtime = config.get("fio")["runtime"]
                numjobs = config.get("fio")["numjobs"]
                fio_perf_data = run_fio_parallel(
                    client,
                    smb_shares,
                    rw,
                    rwmixread,
                    bs,
                    ioengine,
                    iodepth,
                    size,
                    runtime,
                    numjobs,
                )

                # Compare IOs values after setting qos limits
                compare_io_limit_values(aio_rate_limit_conf, fio_perf_data)

                # Cleaup fio report
                client.exec_command(
                    sudo=True,
                    cmd="rm -rf /root/fio_output_share*",
                )

            # share cleanup
            for smb_share in smb_shares:
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf /mnt/{smb_share}/*",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"umount /mnt/{smb_share}/",
                )
            client.exec_command(
                sudo=True,
                cmd="rm -rf /mnt/*",
            )

    except Exception as e:
        log.error(f"Failed to deploy samba with auth_mode 'user' : {e}")
        return 1
    finally:
        smb_cleanup(installer, smb_shares, smb_cluster_id)
    return 0
