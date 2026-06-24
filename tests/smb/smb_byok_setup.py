from smb_operations import create_vol_smb_subvol

from cli.exceptions import ConfigError, OperationFailedError
from tests.smb.smb_byok_tools import (
    deploy_samba_service_with_byok,
    ensure_fresh_gklm_kmip_client,
    get_enctag,
    load_gklm_config,
    setup_gklm_infrastructure,
    wait_for_gklm_server_restart,
)
from utility.gklm_client.gklm_client import build_gklm_client
from utility.log import Log
from utility.utils import generate_self_signed_certificate, get_cephci_config

log = Log(__name__)

KMIP_PORT = 5696


def generate_self_signed_certificate_for_smb_node(installer_node):
    """Generate self signed certificates for samba node
    Args:
        installer_node (obj): samba server node installer node obj
    """
    subject = {
        "common_name": installer_node.hostname,
        "ip_address": installer_node.ip_address,
    }
    key, cert, ca = generate_self_signed_certificate(subject=subject)

    key_file = installer_node.remote_file(
        sudo=True, file_name="byok_key.key", file_mode="w+"
    )
    key_file.write(key)
    key_file.flush()
    cert_file = installer_node.remote_file(
        sudo=True, file_name="byok_cert.crt", file_mode="w+"
    )
    cert_file.write(cert)
    cert_file.flush()
    ca_file = installer_node.remote_file(
        sudo=True, file_name="byok_ca.ca", file_mode="w+"
    )
    ca_file.write(ca)
    ca_file.flush()
    return key, cert, ca


def check_keybridge_service(smb_node):
    """Check if grpc keybridge service is running
    Args:
        smb_node (obj): samba installer server node obj
    """
    # Get keybridge service
    cmd = "systemctl list-units --type=service | grep keybridge"
    out = smb_node.exec_command(sudo=True, cmd=cmd)[0].strip()
    log.info(out)

    if "keybridge" in out:
        log.info(f"BYOK keybridge service has been loaded: {out}")
    else:
        raise OperationFailedError(f"BYOK keybridge service has not been loaded {out}")

    if "keybridge" in out and "active" in out and "running" in out:
        log.info(
            f"BYOK keybridge service is running activetly: {out}, BYOK smb cluster is created successfully"
        )
    else:
        raise OperationFailedError(
            f"BYOK keybridge service has been loaded but failed {out}"
        )


def run(ceph_cluster, **kw):
    """Deploy samba with auth_mode 'user' using imperative style(CLI Commands)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test
    """
    # Get config
    config = kw.get("config")
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    # Load GKLM configuration
    gklm_params = load_gklm_config(custom_data, config, cephci_data)
    gklm_ip = gklm_params["gklm_ip"]
    gklm_user = gklm_params["gklm_user"]
    gklm_hostname = gklm_params["gklm_hostname"]

    gkml_client_name = "SMBBYOK1"
    gklm_cert_alias = "smb"

    # Check mandatory parameter file_type
    if not config.get("file_type"):
        raise ConfigError("Mandatory config 'file_type' not provided")

    # Get spec file type
    file_type = config.get("file_type")

    # Check mandatory parameter spec
    if not config.get("spec"):
        raise ConfigError("Mandatory config 'spec' not provided")

    # Get smb spec file mount path
    file_mount = config.get("file_mount", "/tmp")

    # Get smb subvolume mode
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")

    # Get smb spec
    smb_spec = config.get("spec")
    log.info("smb_spec_log: {} ".format(smb_spec))

    # Get smb service value from spec file
    smb_subvols = []
    for spec in smb_spec:
        if spec["resource_type"] == "ceph.smb.cluster":
            smb_cluster_id = spec["cluster_id"]
        elif spec["resource_type"] == "ceph.smb.share":
            cephfs_vol = spec["cephfs"]["volume"]
            smb_subvol_group = spec["cephfs"]["subvolumegroup"]
            smb_subvols.append(spec["cephfs"]["subvolume"])

    # Get installer node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Generate self signed certificates crt, cacrt, key
    key, cert, _ = generate_self_signed_certificate_for_smb_node(installer_node)

    try:
        log.info("Step 1: Setting up GKLM infrastructure")
        setup_gklm_infrastructure(
            smb_nodes=smb_nodes,
            gklm_ip=gklm_ip,
            gklm_hostname=gklm_hostname,
        )

        log.info("Step 2: Initializing GKLM REST client")
        gklm_rest_client = build_gklm_client(gklm_params, verify=False)

        log.info(
            "Step 2b: Remove any existing GKLM KMIP client %r (and legacy cert2), "
            "then create a new empty client",
            gkml_client_name,
        )
        ensure_fresh_gklm_kmip_client(
            gklm_rest_client,
            gkml_client_name,
            legacy_cert_aliases=("smb",),
        )

        log.info("Step 3: generating certificates and keys, and CA_cert from GKLM")
        # GKLM 5.x lists TLS system certs under GET /system/certificates, not GET /certificates.
        sys_cert_details = None
        for entry in gklm_rest_client.certificates.list_system_certificates():
            a = (
                entry.get("alias")
                or entry.get("Alias")
                or entry.get("certAlias")
                or entry.get("name")
            )
            if a and str(a) == gklm_hostname:
                sys_cert_details = entry
                break

        legacy_cert_details = None
        if not sys_cert_details:
            for entry in gklm_rest_client.certificates.list_certificates():
                a = (
                    entry.get("alias")
                    or entry.get("Alias")
                    or entry.get("certAlias")
                    or entry.get("name")
                )
                if a and str(a) == gklm_hostname:
                    legacy_cert_details = entry
                    break

        needs_restart = False

        if not sys_cert_details and not legacy_cert_details:
            log.info(
                "No system certificate with alias %s; creating self-signed system cert",
                gklm_hostname,
            )
            gklm_rest_client.certificates.create_system_certificate(
                {
                    "type": "Self-signed",
                    "alias": gklm_hostname,
                    "cn": gklm_hostname,
                    "validity": "3650",
                    "algorithm": "RSA",
                    "usageSubtype": "KEYSERVING_TLS",
                }
            )
            needs_restart = True
        else:
            cert_to_check = sys_cert_details or legacy_cert_details
            usage = str(cert_to_check.get("usage", "")) + str(
                cert_to_check.get("usageSubtype", "")
            )

            if "KEYSERVING" not in usage.upper():
                log.info(
                    "System certificate %s exists but is not KEYSERVING. Updating to add KEYSERVING_TLS.",
                    gklm_hostname,
                )
                gklm_rest_client.certificates.update_system_certificate(
                    alias=gklm_hostname,
                    add_usage_subtype="KEYSERVING_TLS",
                )
                needs_restart = True
            else:
                log.info(
                    "System certificate %s exists and is already KEYSERVING. No restart needed.",
                    gklm_hostname,
                )

        if needs_restart:
            log.info("Restarting GKLM server to apply system certificate changes.")
            gklm_rest_client.server.restart_server()
            wait_for_gklm_server_restart(
                gklm_rest_client=gklm_rest_client,
                timeout=500,
                check_interval=10,
                initial_wait=10,
            )
        ca_cert = gklm_rest_client.certificates.get_system_certificate(
            cert_name=gklm_hostname
        )
        all_clients = gklm_rest_client.clients.list_clients()
        if gkml_client_name.upper() not in [x.get("clientName") for x in all_clients]:
            log.info(f"Not Found client name {gkml_client_name}, creating ")
            gklm_rest_client.clients.create_client(gkml_client_name)

        enctag = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        # Create volume and smb subvolume
        create_vol_smb_subvol(
            installer_node,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_subvolume_mode,
        )

        log.info("Step 5: Deploying SMB cluster with BYOK")
        deploy_samba_service_with_byok(
            installer=installer_node,
            smb_cluster_id=smb_cluster_id,
            kmip_hosts=gklm_ip,
            kmip_port=KMIP_PORT,
            key=key,
            cert=cert,
            ca_cert=ca_cert,
            smb_spec=smb_spec,
            fscrypt_key_name=enctag,
            file_type=file_type,
            file_mount=file_mount,
        )

        # Check grpc keybridge service
        check_keybridge_service(installer_node)

    except Exception as e:
        log.error(f"Failed to deploy samba with BYOK: {e}")
        return 1
    return 0
