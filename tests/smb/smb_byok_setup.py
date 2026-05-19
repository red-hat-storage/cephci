import yaml
from smb_operations import deploy_smb_service_declarative, smbclient_check_shares

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log
from utility.utils import generate_self_signed_certificate

log = Log(__name__)

CA = """-----BEGIN CERTIFICATE-----
MIIC+DCCAeCgAwIBAgIHNI+wFxpITzANBgkqhkiG9w0BAQsFADArMSkwJwYDVQQDEyBjZXBoLWdr
bG0teGUxamF3LW5vZGUxLWluc3RhbGxlcjAeFw0yNjAxMTMxMjA2MzJaFw0yOTAxMTIxMjA2MzJa
MCsxKTAnBgNVBAMTIGNlcGgtZ2tsbS14ZTFqYXctbm9kZTEtaW5zdGFsbGVyMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnpkFcTBzt20tMVBpePo30RtWAw3r3nuu8FVU8FIbwIudJASH
EBJtQdFzM7NUziwD3RpT8LdjssDsub+quylhyXsDaqEX/yuP1x9w6yYfJFxT5j2TGL3zFVm6vlXD
NkUqe+MUBrFPnQHvuxOcGkeKptXUqrtPBQRQ9B8jGTCqxainAtbvtpetnniiCr6B5qml60dWgT7j
UE5tEJ+5yrSbEDu5Z/+Rqb97M+EcMoVdpgCMNok33R9jMP6Y/XcVwlfSX7XEM3MMGxFJitBr10py
N0H+aXgVP8vBYT629Fk8xUdDroYovjR9oC74T3jlMANh1jl5Q0lR0c/8gJxQ/ip1ZwIDAQABoyEw
HzAdBgNVHQ4EFgQU3igRGjdvzDl9Riw49fj2QE1/+/AwDQYJKoZIhvcNAQELBQADggEBAAOb1WB8
0OTg/9MqPqeJTbMnX9/cNCH5C11uA7LRUcP/C4/nWBrf6wYfHJxQ+VilLGOMSKEJ/MzJYQoz1joX
Jo9molEkvBQGopLiYQx2ZeKSNRtIXwAXm6eSvPSncwFTehniRD0EaKbajhkH8vbodk3T+WuTLGzz
WCQeZD4L73AVj89lCdmMEb9e9MccJE+NfoKYws7LgSKgoh36Y1JaZ5F9sa3RI8S3fHJodchLkaaT
RcUTctv/NjKGgDwSNF7cXuc9F+Z8DKb+vidpDV+1EXhLR0DlWWianTTXZ42q8TYEvzJpTo/LknuI
NfrG3Uh1p3VnFNXdSCruvbOPUsgf66M=
-----END CERTIFICATE-----"""

FSCRYPT_KEYNAME = "KEY-74e01cb-2acf8553-8b45-414a-96ed-1066d9994b86"

cert_tls_credential_id = "cert1"
key_tls_credential_id = "key1"
cacert_tls_credential_id = "cacert1"

class LiteralString(str):
    pass

def literal_presenter(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(LiteralString, literal_presenter)

def create_grpc_spec_with_literal_strings(cert, key, ca, cert_tls_id, key_tls_id, cacert_tls_id):
    byok_spec_tls_credential_dict = [
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": cert_tls_id,
            "credential_type": "cert",
            "value": LiteralString(cert.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": key_tls_id,
            "credential_type": "cert",
            "value": LiteralString(key.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": cacert_tls_id,
            "credential_type": "ca-cert",
            "value": LiteralString(ca.rstrip("\n")),
        },
    ]
    return byok_spec_tls_credential_dict

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
    # Get grpc remotectl service
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
            if spec["cephfs"]["fscrypt_key"]["scope"] == "kmip":
                spec["cephfs"]["fscrypt_key"]["name"] = FSCRYPT_KEYNAME


    # Get installer node
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    # Get smb nodes
    smb_nodes = ceph_cluster.get_nodes("smb")

    # Get client node
    client = ceph_cluster.get_nodes(role="client")[0]

    # Generate self signed certificates crt, cacrt, key
    key, cert, _ = generate_self_signed_certificate_for_smb_node(installer_node)

    byok_spec_tls_credential_dict = create_grpc_spec_with_literal_strings(cert, key, CA, cert_tls_credential_id, key_tls_credential_id,
                                          cacert_tls_credential_id)
    smb_spec.extend(byok_spec_tls_credential_dict)
    log.info("smb_spec: {}".format(smb_spec))

    try:
        # deploy smb services
        deploy_smb_service_declarative(
            installer_node,
            cephfs_vol,
            smb_subvol_group,
            smb_subvols,
            smb_cluster_id,
            smb_subvolume_mode,
            file_type,
            smb_spec,
            file_mount,
        )

        # Check grpc keybridge service
        check_keybridge_service(installer_node)

    except Exception as e:
        log.error(f"Failed to deploy samba with BYOK: {e}")
        return 1
    return 0
