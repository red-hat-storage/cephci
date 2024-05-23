import json
import tempfile
from time import sleep

import yaml

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError
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
            installer, cephfs_vol, smb_subvol_group, smb_subvols, smb_subvolume_mode
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
            installer, cephfs_vol, smb_subvol_group, smb_subvols, smb_subvolume_mode
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
    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to cleanup smb cluster {smb_cluster_id}, Error {e}"
        )


def create_vol_smb_subvol(
    installer, cephfs_vol, smb_subvol_group, smb_subvols, smb_subvolume_mode
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
        CephAdm(installer).ceph.fs.volume.create(cephfs_vol)
        CephAdm(installer).ceph.fs.sub_volume_group.create(cephfs_vol, smb_subvol_group)
        for subvol in smb_subvols:
            CephAdm(installer).ceph.fs.sub_volume.create(
                cephfs_vol, subvol, group_name=smb_subvol_group, mode=smb_subvolume_mode
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
            )
        elif auth_mode == "active-directory":
            CephAdm(installer).ceph.smb.cluster.create(
                smb_cluster_id,
                auth_mode,
                domain_realm=domain_realm,
                domain_join_user_pass=f"{smb_user_name}%{smb_user_password}",
                custom_dns=custom_dns,
                placement="label:smb",
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
