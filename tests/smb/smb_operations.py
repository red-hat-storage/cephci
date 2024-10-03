import json
import re
import tempfile
from threading import Thread
from time import sleep

import yaml

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, OperationFailedError
from cli.ops.host import host_maintenance_enter, host_maintenance_exit
from cli.utilities.utils import (
    get_ip_from_node,
    get_service_id,
    kill_process,
    reboot_node,
    set_service_state,
)
from utility.log import Log

log = Log(__name__)


def deploy_smb_service_imperative(
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
    clustering="default",
    earmark=None,
):
    """Deploy smb services
    Args:
        installer (obj): Installer node obj
        cephfs_vol (str): CephFS volume
        smb_subvol_group (str): Smb subvloume group
        smb_subvols (str): Smb subvloumes
        smb_subvolume_mode (str): Smb subvolume mode
        smb_cluster_id (str): Smb cluster id
        auth_mode (str): Smb auth mode (user or active-directory)
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        smb_shares (list): Smb shares list
        path (str): Smb path
        domain_realm (str): Smb AD domain relam
        custom_dns (str): Smb AD custom dms
    """
    try:
        # Create volume and smb subvolume
        create_vol_smb_subvol(
            installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
            earmark,
        )

        # Enable smb module
        enable_smb_module(installer, smb_cluster_id)
        sleep(30)

        # Create smb cluster with auth_mode
        create_smb_cluster(
            installer,
            smb_cluster_id,
            auth_mode,
            domain_realm,
            smb_user_name,
            smb_user_password,
            custom_dns,
            clustering,
        )

        # Check smb cluster
        check_smb_cluster(installer, smb_cluster_id)

        # Create smb share
        create_smb_share(
            installer,
            smb_shares,
            smb_cluster_id,
            cephfs_vol,
            path,
            smb_subvol_group,
            smb_subvols,
        )

        # Check smb service
        verify_smb_service(installer, service_name="smb")
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to deploy smb cluster {smb_cluster_id}, Error {e}"
        )


def deploy_smb_service_declarative(
    installer,
    cephfs_vol,
    smb_subvol_group,
    smb_subvols,
    smb_cluster_id,
    smb_subvolume_mode,
    file_type,
    smb_spec,
    file_mount,
    earmark=None,
):
    """Deploy smb services using spec file
    Args:
        installer (obj): Installer node obj
        cephfs_vol (str): CephFS volume
        smb_subvol_group (str): Smb subvloume group
        smb_subvols (str): Smb subvloumes
        smb_cluster_id (str): Smb cluster id
        smb_subvolume_mode (str): Smb subvolume mode
        file_type (str): spec file type (yaml or json)
        smb_spec (dir): smb spec details
    """
    try:
        # Create volume and smb subvolume
        create_vol_smb_subvol(
            installer,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
            earmark,
        )

        # Enable smb module
        enable_smb_module(installer, smb_cluster_id)
        sleep(30)

        # Create smb spec file
        smb_spec_file = generate_smb_spec(installer, file_type, smb_spec)

        # Apply smb spec file
        apply_smb_spec(installer, smb_spec_file, file_mount)

        # Check smb service
        verify_smb_service(installer, service_name="smb")
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to deploy smb cluster {smb_cluster_id}, Error {e}"
        )


def smbclient_check_shares(
    smb_nodes,
    client,
    smb_shares,
    smb_user_name,
    smb_user_password,
    auth_mode,
    domain_realm,
    public_addrs=None,
):
    """Smb share check using smbclients
    Args:
        smb_nodes (list): Smb nodes obj
        client (obj): Client object
        smb_shares (list): Smb shares list
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        auth_mode (str): Smb auth mode (user or active-directory)
        domain_realm (str): Smb AD domain relam
    """
    try:
        if public_addrs:
            for public_addr in public_addrs:
                for smb_share in smb_shares:
                    if auth_mode == "active-directory":
                        cmd = (
                            f"smbclient -U '{domain_realm.split('.')[0].upper()}\\"
                            f"{smb_user_name}%{smb_user_password}' "
                            f"//{public_addr.split('/')[0]}/{smb_share} -c ls"
                        )
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                    elif auth_mode == "user":
                        cmd = (
                            f"smbclient -U {smb_user_name}%{smb_user_password}"
                            f" //{public_addr.split('/')[0]}/{smb_share} -c ls"
                        )
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                    sleep(1)
        else:
            for smb_node in smb_nodes:
                for smb_share in smb_shares:
                    if auth_mode == "active-directory":
                        cmd = (
                            f"smbclient -U '{domain_realm.split('.')[0].upper()}\\"
                            f"{smb_user_name}%{smb_user_password}' "
                            f"//{smb_node.ip_address}/{smb_share} -c ls"
                        )
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                    elif auth_mode == "user":
                        cmd = (
                            f"smbclient -U {smb_user_name}%{smb_user_password}"
                            f" //{smb_node.ip_address}/{smb_share} -c ls"
                        )
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                    sleep(1)
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to access smb share {smb_share}, Error {e}"
        )


def smb_cleanup(installer, smb_shares, smb_cluster_id):
    """Smb service cleanup
    Args:
        installer (obj): Installer node obj
        smb_shares (list): Smb shares list
        smb_cluster_id (str): Smb cluster id
    """
    try:
        # Remove smb shares
        remove_smb_share(installer, smb_shares, smb_cluster_id)
        # Remove smb cluster
        remove_smb_cluster(installer, smb_cluster_id)
        sleep(9)
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to cleanup smb cluster {smb_cluster_id}, Error {e}"
        )


def create_vol_smb_subvol(
    installer,
    cephfs_vol,
    smb_subvol_group,
    smb_subvols,
    smb_subvolume_mode,
    earmark=None,
):
    """Create cephfs volume and smb sub volume
    Args:
        installer (obj): Installer node obj
        cephfs_vol (str): CephFS volume
        smb_subvol_group (str): Smb subvloume group
        smb_subvols (str): Smb subvloumes
        smb_subvolume_mode (str): Smb subvolume mode
    """
    try:
        if earmark:
            CephAdm(installer).ceph.fs.volume.create(cephfs_vol)
            CephAdm(installer).ceph.fs.sub_volume_group.create(
                cephfs_vol, smb_subvol_group
            )
            for subvol in smb_subvols:
                CephAdm(installer).ceph.fs.sub_volume.create(
                    cephfs_vol,
                    subvol,
                    group_name=smb_subvol_group,
                    mode=smb_subvolume_mode,
                    earmark=earmark,
                )
        else:
            CephAdm(installer).ceph.fs.volume.create(cephfs_vol)
            CephAdm(installer).ceph.fs.sub_volume_group.create(
                cephfs_vol, smb_subvol_group
            )
            for subvol in smb_subvols:
                CephAdm(installer).ceph.fs.sub_volume.create(
                    cephfs_vol,
                    subvol,
                    group_name=smb_subvol_group,
                    mode=smb_subvolume_mode,
                )
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to create volume and smb subvolume, Error {e}"
        )


def enable_smb_module(installer, smb_cluster_id):
    """Enable smb mgr module
    Args:
        installer (obj): Installer node obj
    """
    # Enable mgr smb module
    CephAdm(installer).ceph.mgr.module.enable("smb")

    # check mgr module is enabled
    timeout, interval = 150, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(installer).ceph.mgr.module.ls()
        module_status = {
            module.split()[0]: " ".join(module.split()[1:])
            for module in out.strip().split("\n")[1:]
        }
        if module_status["smb"] == "on":
            return True
    if w.expired:
        raise CephadmOpsExecutionError(f"Smb cluster {smb_cluster_id} not available")


def create_smb_cluster(
    installer,
    smb_cluster_id,
    auth_mode,
    domain_realm,
    smb_user_name,
    smb_user_password,
    custom_dns,
    clustering,
):
    """Create smb cluster
    Args:
        installer (obj): Installer node obj
        smb_cluster_id (str): Smb cluster id
        auth_mode (str): Smb auth mode (user or active-directory)
        domain_realm (str): Smb AD domain relam
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        custom_dns (str): Smb AD custom dms
    """
    try:
        if auth_mode == "user":
            CephAdm(installer).ceph.smb.cluster.create(
                smb_cluster_id,
                auth_mode,
                define_user_pass=f"{smb_user_name}%{smb_user_password}",
                placement="label:smb",
                clustering=clustering,
            )
        elif auth_mode == "active-directory":
            CephAdm(installer).ceph.smb.cluster.create(
                smb_cluster_id,
                auth_mode,
                domain_realm=domain_realm,
                domain_join_user_pass=f"{smb_user_name}%{smb_user_password}",
                custom_dns=custom_dns,
                placement="label:smb",
                clustering=clustering,
            )
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to create smb cluster, Error {e}")


def check_smb_cluster(installer, smb_cluster_id):
    """Check smb cluster
    Args:
        installer (obj): Installer node obj
        smb_cluster_id (str): Smb cluster id
    """
    timeout, interval = 150, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(installer).ceph.smb.cluster.ls()
        if smb_cluster_id in out:
            return True
    if w.expired:
        raise CephadmOpsExecutionError(f"Smb cluster {smb_cluster_id} not available")


def create_smb_share(
    installer,
    smb_shares,
    smb_cluster_id,
    cephfs_vol,
    path,
    smb_subvol_group,
    smb_subvols,
):
    """Create smb share
    Args:
        installer (obj): Installer node obj
        smb_shares (list): Smb shares list
        smb_cluster_id (str): Smb cluster id
        cephfs_vol (str): CephFS volume
        path (str): Smb path
        smb_subvol_group (str): Smb subvloume group
        smb_subvols (str): Smb subvloumes
    """
    try:
        for i in range(0, len(smb_shares)):
            CephAdm(installer).ceph.smb.share.create(
                smb_cluster_id,
                smb_shares[i],
                cephfs_vol,
                path,
                subvolume=f"{smb_subvol_group}/{smb_subvols[i]}",
            )
        # Check share count
        out = json.loads(CephAdm(installer).ceph.smb.share.ls(smb_cluster_id))
        if len(smb_shares) != len(out):
            log.error("Samba shares list count not matching")
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to create smb shares, Error {e}")


def verify_smb_service(node, service_name):
    """
    Verifies service is up
    Args:
        node (str): monitor node object
        service_name (str): service name
    return: (bool)
    """
    # Get service status
    out = json.loads(
        CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
    )[0]
    # Check smb service is up
    timeout, interval = 90, 30
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = json.loads(
            CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
        )[0]
        if out["service_type"] == "smb" and out["status"]["running"] > 0:
            return True
    if w.expired:
        raise CephadmOpsExecutionError(f"Service {service_name} is not deployed")


def remove_smb_share(installer, smb_shares, smb_cluster_id):
    """Remove smb shares
    Args:
        installer (obj): Installer node obj
        smb_shares (list): Smb shares list
        smb_cluster_id (str): Smb cluster id
    """
    for smb_share in smb_shares:
        try:
            CephAdm(installer).ceph.smb.share.rm(smb_cluster_id, smb_share)
        except Exception as e:
            raise CephadmOpsExecutionError(
                f"Fail to delete smb share {smb_share}, Error {e}"
            )


def remove_smb_cluster(installer, smb_cluster_id):
    """Remove smb cluster
    Args:
        installer (obj): Installer node obj
        smb_cluster_id (str): Smb cluster id
    """
    try:
        CephAdm(installer).ceph.smb.cluster.rm(smb_cluster_id)
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to remove smb cluster {smb_cluster_id}, Error {e}"
        )


def generate_smb_spec(installer, file_type, smb_spec):
    """Generate smb spec file
    Args:
        installer (obj): Installer node obj
        file_type (str): Smb spec file type (yaml or json)
        smb_spec (dir): Smb spec details
    """
    # Create temporory file path
    temp_file = tempfile.NamedTemporaryFile(suffix=f".{file_type}")

    try:
        # Create temporary file and dump data
        with installer.remote_file(
            sudo=True, file_name=temp_file.name, file_mode="w"
        ) as _f:
            # Generate config
            if file_type == "yaml":
                yaml.dump(smb_spec, _f, default_flow_style=False, sort_keys=False)
            elif file_type == "json":
                json.dump(smb_spec, _f)

        _f.flush()

        return temp_file.name
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to create smb spec file, Error {e}")


def apply_smb_spec(installer, smb_spec_file, file_mount):
    """Apply smb spec file
    Args:
        installer (obj): Installer node obj
        smb_spec_file (str): Smb spec file name
        file_mount (str): Smb spec file mount point
    """
    try:
        CephAdm(installer, mount=file_mount).ceph.smb.apply.apply(smb_spec_file)
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to apply smb spec file, Error {e}")


def get_samba_pid_and_memory(samba_servers):
    """get samba pid and memory consumption(RSS)
    Args:
        smb_server (obj): Smb server
    Returns:
        smb_server_info(dic): {"samba server1": ["PID","RSS(MB)"], "samba server2": ["PID","RSS(MB)"]}
    """
    samba_server_info = {}
    if not isinstance(samba_servers, list):
        samba_servers = [samba_servers]

    for samba_server in samba_servers:
        try:
            pid = samba_server.exec_command(sudo=True, cmd="pgrep -o smbd")[0].strip()
            rss = samba_server.exec_command(sudo=True, cmd=f"ps -p {pid} -o rss=")[
                0
            ].strip()
            samba_server_info[samba_server.hostname] = [pid, rss]
        except Exception:
            raise CephadmOpsExecutionError(
                f"failed get samba process ID and rss for {samba_server}"
            )
    return samba_server_info


def smb_cifs_mount(
    smb_node,
    client,
    smb_share,
    smb_user_name,
    smb_user_password,
    auth_mode,
    domain_realm,
    cifs_mount_point,
    public_addrs=None,
):
    """Smb cifs mount
    Args:
        smb_node (obj): Smb node obj
        client (obj): Client object
        smb_share (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        auth_mode (str): Smb auth mode (user or active-directory)
        domain_realm (str): Smb AD domain relam
        cifs_mount_point (str): Smb cifs mount point
        public_addrs(str): public addrs ip
    """
    try:
        # Create cifs mount dir
        cmd = f"mkdir {cifs_mount_point}"
        client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        if public_addrs:
            # Mount smb share using cifs
            if auth_mode == "user":
                cmd = (
                    f"mount.cifs //{public_addrs[0]}/{smb_share} {cifs_mount_point}"
                    f" -o username={smb_user_name},password={smb_user_password}"
                )
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
            elif auth_mode == "active-directory":
                cmd = (
                    f"mount.cifs //{public_addrs[0]}/{smb_share} {cifs_mount_point}"
                    f" -o username={smb_user_name},password={smb_user_password}"
                    f",domian={domain_realm.split('.')[0].upper()}"
                )
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )

        else:
            # Mount smb share using cifs
            if auth_mode == "user":
                cmd = (
                    f"mount.cifs //{smb_node.ip_address}/{smb_share} {cifs_mount_point}"
                    f" -o username={smb_user_name},password={smb_user_password}"
                )
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
            elif auth_mode == "active-directory":
                cmd = (
                    f"mount.cifs //{smb_node.ip_address}/{smb_share} {cifs_mount_point}"
                    f" -o username={smb_user_name},password={smb_user_password}"
                    f",domian={domain_realm.split('.')[0].upper()}"
                )
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to mount smb share {smb_share} using cifs, Error {e}"
        )


def generate_apply_smb_spec(installer, file_type, smb_spec, file_mount):
    """Generate and apply smb spec
    Args:
        installer (obj): Installer node obj
        file_type (str): spec file type (yaml or json)
        smb_spec (dir): smb spec details
    """
    try:
        # Create smb spec file
        smb_spec_file = generate_smb_spec(installer, file_type, smb_spec)

        # Apply smb spec file
        apply_smb_spec(installer, smb_spec_file, file_mount)

        # Check smb service
        verify_smb_service(installer, service_name="smb")
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to generate and apply smb spec, Error {e}"
        )


def win_mount(
    clients,
    mount_point,
    samba_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    public_addrs=None,
):
    """Window mount
    Args:
        clients (obj): window clients node obj
        mount_point (str): mount point
        samba_server (obj): Smb server node obj
        smb_share (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        public_addrs(str): public addrs ip
    """
    try:
        if public_addrs:
            for client in clients:
                cmd = (
                    f"net use {mount_point} \\\\{public_addrs[0]}\\{smb_share}"
                    f" /user:{smb_user_name} {smb_user_password} /persistent:yes"
                )
                client.exec_command(cmd=cmd)
        else:
            for client in clients:
                cmd = (
                    f"net use {mount_point} \\\\{samba_server}\\{smb_share}"
                    f" /user:{smb_user_name} {smb_user_password} /persistent:yes"
                )
                client.exec_command(cmd=cmd)
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to mount, Error {e}")


def clients_cleanup(
    clients,
    mount_point,
    samba_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    windows_client=False,
):
    """Clients cleanup
    Args:
        clients (obj): window clients node obj
        mount_point (str): mount point
        samba_server (obj): Smb server node obj
        smb_share (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        windows_client (bool): trur or false
    """
    try:
        if windows_client:
            for client in clients:
                client.exec_command(
                    cmd=f"rd /s /q {mount_point}\\",
                )
                client.exec_command(
                    cmd=f"net use {mount_point} /delete",
                )
        else:
            for client in clients:
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf {mount_point}",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"umount {mount_point}",
                )
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to cleanup clients, Error {e}")


def check_rados_clustermeta(cephadm, smb_cluster_id, smb_nodes):
    """Check rados clustermeta for samba cluster
    Args:
        cephadm (obj): cephadm obj
        smb_cluster_id (str): samba cluster id
        smb_nodes (list): samba server nodes obj list
    """
    try:
        # Get rados clustermeta for smb cluster
        cmd = f"rados --pool=.smb -N {smb_cluster_id} get cluster.meta.json /dev/stdout"
        out = json.loads(cephadm.shell([cmd])[0])

        # Get samba server nodes ip
        smb_nodes_ips = [smb_node.ip_address for smb_node in smb_nodes]

        # check rados clustermeta consist of all the sambe server IP
        smb_clustering = any(
            node["node"] in smb_nodes_ips and node["state"] == "ready"
            for node in out["nodes"]
        )
        return smb_clustering
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to get rados clustermeta for samba cluster, Error {e}"
        )


def check_ctdb_health(smb_nodes, smb_cluster_id):
    """Check rctdb health
    Args:
        smb_nodes (list): samba server nodes obj list
        smb_cluster_id (str): samba cluster id
    """
    try:
        # Get samba service
        cmd = f"cephadm ls --no-detail | jq -r 'map(select(.name | startswith(\"smb.{smb_cluster_id}\")))[-1].name'"
        out = smb_nodes[0].exec_command(sudo=True, cmd=cmd)[0].strip()

        # Get ctdb status
        sleep(30)
        cmd = f"cephadm enter -n {out} ctdb status"
        ctdb_status = smb_nodes[0].exec_command(sudo=True, cmd=cmd)[0]

        # Get number of nodes and status
        nodes = re.search(r"Number of nodes:(\d+)", ctdb_status).group(1)
        status = all(
            re.search(r"pnn:\d+ \S+ *OK", line)
            for line in ctdb_status.splitlines()
            if line.startswith("pnn")
        )
        log.info(f"ctdb status: nodes: {nodes},status: {status}")

        # Validate ctdb health
        if int(nodes) == len(smb_nodes) and status:
            return True
        return False
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to check ctdb health, Error {e}")


def smb_failover(smb_nodes, public_addr, failover_type="reboot"):
    """Perform failover
    Args:
        smb_nodes (list): samba server nodes obj list
        public_addrs (str): public addrs ip
        failover_type (str): failover type
    """
    try:
        # Get failover node
        for smb_node in smb_nodes:
            assigned_ips = get_ip_from_node(smb_node)
            if any(ip in assigned_ips for ip in public_addr):
                failover_node = smb_node
                log.info(f"Failover node {failover_node.hostname}")
                break

        # Perfrom failover
        if failover_type == "reboot":
            failover = Thread(target=reboot_node, args=(failover_node,))
        elif failover_type == "restart_service":
            service_id = get_service_id(failover_node, "ctdbd")[0].strip()
            failover = Thread(
                target=set_service_state, args=(failover_node, service_id, "restart")
            )
        elif failover_type == "stop_service":
            service_id = get_service_id(failover_node, "ctdbd")[0].strip()
            failover = Thread(
                target=set_service_state, args=(failover_node, service_id, "stop")
            )
        elif failover_type == "kill_service":
            pid = failover_node.exec_command(sudo=True, cmd="pgrep -o ctdbd")[0].strip()
            failover = Thread(target=kill_process, args=(failover_node, pid))
        elif failover_type == "maintenance_mode":
            timeout = 300
            interval = 10
            force = True
            yes_i_really_mean_it = True
            failover = Thread(
                target=host_maintenance_enter,
                args=(
                    failover_node,
                    failover_node.hostname,
                    timeout,
                    interval,
                    force,
                    yes_i_really_mean_it,
                ),
            )

        # Start failover
        failover.start()

        # Check public adress assigned to other samba server
        flag = False
        for w in WaitUntil(timeout=120, interval=5):
            for smb_node in smb_nodes:
                if smb_node != failover_node:
                    assigned_ips = get_ip_from_node(smb_node)
                    if any(ip in assigned_ips for ip in public_addr):
                        flag = True
                        log.info(
                            f"Failover success, public_addr reassigned to {smb_node.hostname}"
                        )
            if flag:
                break
        if w.expired:
            raise OperationFailedError(
                "The failover process failed and public address is not assigned to the available samba servers"
            )

        # Exit from maintenance_mode after failover type is maintenance_mode
        if failover_type == "maintenance_mode":
            host_maintenance_exit(failover_node, failover_node.hostname)

        # Wait for the failover to completed
        failover.join()
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to perfrom samba failover, Error {e}")


def reconnect_share(
    clients,
    mount_point,
    samba_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    auth_mode,
    domain_realm,
    public_addrs=False,
    windows_client=False,
):
    """Reconnect samba share
    Args:
        clients (list): List of client object
        mount_point (str): mount point
        samba_server (obj): Smb server node obj
        smb_share (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        auth_mode (str): Smb auth mode (user or active-directory)
        domain_realm (str): Smb AD domain relam
        public_addrs(str): public addrs ip
        windows_client(bool): True|False
    """
    try:
        # cleanup
        clients_cleanup(
            clients,
            mount_point,
            samba_server,
            smb_share,
            smb_user_name,
            smb_user_password,
            windows_client,
        )
        # Mount samba share
        if windows_client:
            win_mount(
                clients,
                mount_point,
                samba_server.ip_address,
                smb_share,
                smb_user_name,
                smb_user_password,
                public_addrs,
            )
        else:
            for client in clients:
                smb_cifs_mount(
                    samba_server,
                    client,
                    smb_share,
                    smb_user_name,
                    smb_user_password,
                    auth_mode,
                    domain_realm,
                    mount_point,
                    public_addrs,
                )
    except Exception as e:
        raise CephadmOpsExecutionError(f"Fail to reconnect samba server, Error {e}")


def remove_sub_vol_group(installer, cephfs_vol, smb_subvol_group, smb_subvols):
    """Remove sub volume and sub volume group
    Args:
        installer (obj): Installer node obj
        cephfs_vol (str): CephFS volume
        smb_subvol_group (str): Subvloume group
        smb_subvols [list]: Subvloumes
    """
    try:
        # Remove subvloumes
        for smb_subvol in smb_subvols:
            CephAdm(installer).ceph.fs.sub_volume.rm(
                cephfs_vol, smb_subvol, smb_subvol_group
            )
        # Remove sub volume group
        CephAdm(installer).ceph.fs.sub_volume_group.rm(cephfs_vol, smb_subvol_group)
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to remove sub volume and sub volume group, Error {e}"
        )
