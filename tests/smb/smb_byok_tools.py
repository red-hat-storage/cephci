import time

import requests
import yaml

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import CephadmOpsExecutionError, ConfigError
from tests.smb.smb_operations import (
    apply_smb_spec,
    enable_smb_module,
    generate_smb_spec,
    verify_smb_service,
)
from utility.log import Log

log = Log(__name__)


class LiteralString(str):
    pass

def literal_presenter(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(LiteralString, literal_presenter)


def create_byok_spec_tls_credentials_dict(cert, key, ca):
    byok_spec_tls_credential_dict = [
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": "cert1",
            "credential_type": "cert",
            "value": LiteralString(cert.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": "key1",
            "credential_type": "cert",
            "value": LiteralString(key.rstrip("\n")),
        },
        {
            "resource_type": "ceph.smb.tls.credential",
            "tls_credential_id": "cacert1",
            "credential_type": "ca-cert",
            "value": LiteralString(ca.rstrip("\n")),
        },
    ]
    return byok_spec_tls_credential_dict


def remove_gklm_kmip_client_and_legacy_certs(
    gklm_rest_client,
    client_name: str,
    legacy_cert_aliases=("smb",),
):
    """
    Best-effort removal of a generic KMIP client and stale client cert aliases.

    Deletes managed objects for ``client_name``, deletes the client, then tries to
    delete each legacy certificate alias (e.g. ``cert2`` from older test runs).
    """
    try:
        for obj in gklm_rest_client.objects.list_client_objects(client_name):
            oid = obj.get("uuid") or obj.get("id")
            if not oid:
                continue
            try:
                gklm_rest_client.objects.delete_object(oid)
            except Exception as e:
                log.warning("Could not delete GKLM object %s: %s", oid, e)
    except Exception as e:
        log.debug("list_client_objects for %r: %s", client_name, e)

    try:
        gklm_rest_client.clients.delete_client(client_name)
        log.info("Removed GKLM generic KMIP client %r (if it existed).", client_name)
    except Exception as e:
        log.debug("delete_client %r: %s", client_name, e)

    for alias in legacy_cert_aliases:
        if not alias:
            continue
        try:
            gklm_rest_client.certificates.delete_certificate(alias)
            log.info("Removed legacy GKLM client certificate alias %r.", alias)
        except Exception as e:
            log.debug("delete_certificate %r: %s", alias, e)


def ensure_fresh_gklm_kmip_client(
    gklm_rest_client,
    client_name: str,
    legacy_cert_aliases=("smb",),
):
    """
    Drop any existing GKLM state for ``client_name`` (client + keys + legacy certs),
    then register a new empty generic KMIP client with the same name.

    Call this right after ``build_gklm_client(...)`` when tests must not reuse a
    client or symmetric keys left from a failed prior run.
    """
    remove_gklm_kmip_client_and_legacy_certs(
        gklm_rest_client, client_name, legacy_cert_aliases
    )
    try:
        names = {
            str(x.get("clientName") or "").upper()
            for x in gklm_rest_client.clients.list_clients()
        }
    except Exception as e:
        log.warning("Could not list GKLM clients after cleanup: %s", e)
        names = set()
    if client_name.upper() not in names:
        log.info("Creating new GKLM generic KMIP client %r", client_name)
        gklm_rest_client.clients.create_client(client_name)
    else:
        log.warning(
            "GKLM client %r still exists after removal; skipping create_client",
            client_name,
        )
    try:
        gklm_rest_client.login()
    except Exception as e:
        log.debug("GKLM re-login after ensure_fresh_gklm_kmip_client: %s", e)


def get_enctag(
    gklm_client,
    gkml_client_name,
    gklm_cert_alias,
    gklm_user,
    cert,
    created_client_data=None,
):
    """
    Initialize a GKLM client, assign a certificate, and create a symmetric key object for encryption/decryption.

    Args:
        gklm_client: Instance of the GKLM REST client.
        created_client_ Dictionary containing the newly created GKLM client data.
        gkml_client_name: Name of the GKLM client entity created.
        gklm_cert_alias: Alias under which the certificate will be stored in GKLM.
        gklm_user: User to be assigned access to the symmetric key.
        cert: Certificate (PEM format) to be associated with the client.

    Returns:
        str: The UUID of the newly created symmetric key object (enctag).

    Raises:
        Exception: If any step fails, logs the error and re-raises.
    """
    try:

        all_certs = [
            x.get("alias", None) for x in gklm_client.certificates.list_certificates()
        ]
        if gklm_cert_alias not in all_certs:
            log.info(f"Certificate alias '{gklm_cert_alias}' not found in GKLM")
            log.info(
                f"Assigning certificate '{gklm_cert_alias}' to GKLM client '{gkml_client_name}'"
            )
            gklm_client.clients.assign_users_to_generic_kmip_client(
                client_name=gkml_client_name, users=[gklm_user]
            )
            assign_cert_data = gklm_client.clients.assign_client_certificate(
                client_name=gkml_client_name, cert_pem=cert, alias=gklm_cert_alias
            )
            log.info(
                f"Successfully created GKLM client "
                f"{created_client_data if created_client_data is not None else gkml_client_name} "
                f"and assigned certificate {assign_cert_data}"
            )

        log.info(
            f"Creating symmetric key object for client '{gkml_client_name}', alias prefix 'SMB'"
        )
        if len(gklm_client.objects.list_client_objects(gkml_client_name)) < 2:
            symmetric_data = gklm_client.objects.create_symmetric_key_object(
                number_of_objects=1,
                client_name=gkml_client_name,
                alias_prefix_name="SMB",
                cryptoUsageMask="Encrypt,Decrypt",
            )

            enctag = symmetric_data["id"]
            log.info(
                f"Created symmetric key object with UUID {enctag} for client '{gkml_client_name}'"
            )
        else:
            symmetric_data = gklm_client.objects.list_client_objects(gkml_client_name)[
                0
            ]
            log.info(
                f"Reusing existing symmetric key object with UUID {symmetric_data['uuid']} "
                f"for client '{gkml_client_name}'"
            )
            enctag = symmetric_data["uuid"]

        log.info(f"Using encryption tag {enctag} for client '{gkml_client_name}'")

        return enctag

    except Exception as e:
        log.error(
            f"Failed to initialize GKLM client '{gkml_client_name}' or assign certificate '{gklm_cert_alias}': {e}"
        )
        raise

def deploy_samba_service_with_byok(
    installer,
    smb_cluster_id,
    kmip_hosts,
    kmip_port,
    key,
    cert,
    ca_cert,
    smb_spec,
    fscrypt_key_name,
    file_type,
    file_mount,
):
    """
    Deploy samba service with BYOK (Bring Your Own Key) KMIP configuration,
    using the provided certificates and key material.

    Args:
        installer: Samba server Node where Ceph orchestrator commands are run.
        smb_cluster_id: New SMB service ID.
        kmip_hosts: KMIP server IP.
        kmip_port: KMIP server Port.
        key: BYOK private key (PEM format).
        cert: BYOK Client certificate (PEM format).
        ca_cert: CA certificate (PEM format).
        smb_spec: SMB yaml spec
        fscrypt_key_name: The UUID of the symmetric key object
        file_type (str): spec file type (yaml or json)
        file_mount (str): yaml file mount

    Raises:
        Exception: If SMB service creation fails, logs the error and re-raises.
    """
    log.info(
        f"Creating SMB Ganesha service '{smb_cluster_id}' with BYOK on node {installer.hostname}"
    )
    keybridge_dict = {
        "keybridge": {
            "scopes": [
                {
                    "name": "kmip",
                    "kmip_hosts": [kmip_hosts],
                    "kmip_port": kmip_port,
                    "kmip_cert": {"source_type": "resource", "ref": "cert1"},
                    "kmip_key": {"source_type": "resource", "ref": "key1"},
                    "kmip_ca_cert": {"source_type": "resource", "ref": "cacert1"},
                }
            ]
        }
    }

    byok_spec_tls_credentials_dict = create_byok_spec_tls_credentials_dict(
        cert, key, ca_cert
    )
    smb_spec.extend(byok_spec_tls_credentials_dict)
    log.info("smb_spec: {}".format(smb_spec))

    for spec in smb_spec:
        if spec["resource_type"] == "ceph.smb.cluster":
            spec.extend(keybridge_dict)
        elif spec["resource_type"] == "ceph.smb.share":
            if spec["cephfs"]["fscrypt_key"]["scope"] == "kmip":
                spec["cephfs"]["fscrypt_key"]["name"] = fscrypt_key_name

    log.info("SMB service spec: {}".format(smb_spec))

    try:
        # Enable smb module
        enable_smb_module(installer, smb_cluster_id)
        time.sleep(30)

        # Create smb spec file
        smb_spec_file = generate_smb_spec(installer, file_type, smb_spec)

        # Apply smb spec file
        apply_smb_spec(installer, smb_spec_file, file_mount)

        # Check smb service
        verify_smb_service(installer, service_name="smb")
        log.info("SMB BYOK service creation successful")

    except Exception as e:
        raise CephadmOpsExecutionError(
            f"Fail to deploy smb cluster {smb_cluster_id}, Error {e}"
        )


def setup_gklm_infrastructure(smb_nodes, gklm_ip, gklm_hostname):
    """
    Ensure GKLM hostname resolution on all SMB nodes by updating `/etc/hosts`.

    This helper updates `/etc/hosts` on each node in `nfs_nodes` to associate
    `gklm_ip` with `gklm_hostname`. Existing entries that match the hostname or
    the IP are removed before the new line is appended to avoid duplicates.

    Args:
        smb_nodes (iterable): Iterable of node objects. Each node is expected to
            have `hostname` and can be passed to `Ceph(node).execute`.
        gklm_ip (str): IP address of the GKLM server to add to `/etc/hosts`.
        gklm_hostname (str): Hostname of the GKLM server to add to `/etc/hosts`.

    Raises:
        Exception: Any exception raised by `Ceph(node).execute` will propagate
            to the caller (for example SSH/command execution failures).
    """
    for node in smb_nodes:
        log.info(
            f"Updating /etc/hosts on SMB node {node.hostname} with GKLM server {gklm_hostname} at {gklm_ip}"
        )
        cmd = (
            rf"gklm_ip={gklm_ip}; gklm_hostname={gklm_hostname}; "
            rf'sudo sed -i -e "/$gklm_hostname\>/d" -e "/^$gklm_ip\>/d" /etc/hosts && '
            rf'echo "$gklm_ip $gklm_hostname" | sudo tee -a /etc/hosts'
        )
        out = Ceph(node).execute(sudo=True, cmd=cmd)
        log.info(
            f"Updated /etc/hosts on {node.hostname} with GKLM entry. Result: {out}"
        )


def clean_up_gklm(gklm_rest_client, gkml_client_name, gklm_cert_alias):
    """
    Clean up GKLM test resources:
    - Deletes all symmetric key objects associated with the client.
    - Deletes the certificate using the provided alias.
    - Deletes the GKLM client.

    Args:
        gklm_rest_client: Instance of the GKLM REST client.
        gkml_client_name (str): Name of the GKLM client to delete.
        gklm_cert_alias (str): Certificate alias to delete.
    """
    log.info("Starting GKLM resource cleanup.")

    # Step 1: Delete symmetric keys associated with the client
    log.info("Retrieving symmetric key objects for client '%s'.", gkml_client_name)
    ids = []
    try:
        objects = gklm_rest_client.objects.list_client_objects(gkml_client_name)
        ids = [obj["uuid"] for obj in objects if obj.get("uuid")]
        log.info("Found %d symmetric key object(s) to delete.", len(ids))
    except Exception as e:
        log.warning(
            "Could not list symmetric keys for client '%s' (client may be absent): %s",
            gkml_client_name,
            e,
        )

    for key_id in ids:
        try:
            log.debug("Deleting symmetric key object: %s", key_id)
            gklm_rest_client.objects.delete_object(key_id)
        except Exception as e:
            log.warning("Failed to delete symmetric key '%s': %s", key_id, str(e))
    if ids:
        log.info("Symmetric key objects deleted (or attempted).")
    else:
        log.info("No symmetric keys found to delete.")

    # Step 2: Delete the certificate associated with the client
    log.info("Deleting GKLM certificate alias: '%s'", gklm_cert_alias)
    try:
        gklm_rest_client.certificates.delete_certificate(gklm_cert_alias)
        log.info("Certificate alias '%s' deleted successfully.", gklm_cert_alias)
    except Exception as e:
        log.warning(
            "Failed to delete certificate alias '%s': %s", gklm_cert_alias, str(e)
        )

    # Step 3: Delete the GKLM client itself
    log.info("Deleting GKLM client: '%s'", gkml_client_name)
    try:
        gklm_rest_client.clients.delete_client(gkml_client_name)
        log.info("Client '%s' deleted successfully.", gkml_client_name)
    except Exception as e:
        log.warning("Failed to delete client '%s': %s", gkml_client_name, str(e))

    log.info("GKLM resource cleanup completed.")


def load_gklm_config(custom_data, config, cephci_data):
    """
    Load GKLM parameters in this order of precedence:
      1. ``~/.cephci.yaml`` (or cephci equivalent) ``gklm_config`` for the runtime cloud
      2. Overrides from ``--custom-config-file`` YAML
      3. Overrides from explicit ``--custom-config`` entries
      4. Raises ConfigError if required keys are still missing

    ``gklm_config`` lookup: uses ``cloud-type`` from the test/config (normally set by
    ``run.py`` from ``--cloud``). ``baremetal`` maps to ``openstack``. When
    ``cloud-type`` is missing or empty, ``openstack`` is assumed. If that section is
    empty, falls back to the first populated entry among ``openstack``, ``ibmc``.

    Optional key ``gklm_rest_prefix`` (e.g. ``/GKLM/rest/v1`` for GKLM 5.x) selects the
    REST API root; when omitted, ``/SKLM/rest/v1`` is used for backward compatibility.

    Args:
        custom_data (dict): {'custom-config': [], 'custom-config-file': None}
        config (dict): CLI config, contains 'cloud-type'
        cephci_data (dict): global cephci config, contains 'gklm_config' section

    Returns:
        dict: GKLM connection parameters

    Raises:
        ConfigError: If GKLM data is missing after all lookup methods
    """
    merged = {}

    cloud_type = config.get("cloud-type")
    if cloud_type == "baremetal":
        lookup_type = "openstack"
    elif not cloud_type:
        lookup_type = "openstack"
    else:
        lookup_type = cloud_type

    gklm_master_raw = cephci_data.get("gklm_config")
    if not isinstance(gklm_master_raw, dict):
        gklm_master_raw = {}

    cloud_gklm = gklm_master_raw.get(lookup_type) if lookup_type else {}
    if isinstance(cloud_gklm, dict):
        cloud_gklm = {k: v for k, v in cloud_gklm.items() if v not in ("", None)}
    else:
        cloud_gklm = {}

    if not cloud_gklm:
        for fk in ("openstack", "ibmc"):
            block = gklm_master_raw.get(fk)
            if isinstance(block, dict) and any(
                v not in ("", None) for v in block.values()
            ):
                cloud_gklm = dict(block)
                log.info(
                    "Loaded GKLM config from cephci_data gklm_config[%r] "
                    "(no usable entry for cloud-type %r)",
                    fk,
                    cloud_type if cloud_type else lookup_type,
                )
                break
    elif cloud_gklm:
        log.info("Loaded GKLM config from cephci_data for cloud '%s'", lookup_type)

    if cloud_gklm:
        merged.update(cloud_gklm)
    elif gklm_master_raw:
        log.debug(
            "cephci_data defines gklm_config but no usable block for '%s' "
            "(tried fallback openstack/ibmc); supply --custom-config-file or fill ~/.cephci.yaml",
            lookup_type,
        )

    # Override from YAML file if provided (``gklm:`` stanza or top-level GKLM keys)
    yaml_file = custom_data.get("custom-config-file")
    if yaml_file:
        try:
            with open(yaml_file) as f:
                raw = yaml.safe_load(f)
            data = {}
            if isinstance(raw, dict):
                inner = raw.get("gklm")
                if isinstance(inner, dict):
                    data = inner
                else:
                    gklm_yaml_keys = {
                        "gklm_ip",
                        "gklm_user",
                        "gklm_password",
                        "gklm_node_username",
                        "gklm_node_password",
                        "gklm_hostname",
                        "gklm_rest_prefix",
                    }
                    for k in gklm_yaml_keys:
                        if k in raw and raw[k] not in ("", None):
                            data[k] = raw[k]
            if data:
                merged.update(data)
                log.info("Loaded GKLM config from file '%s': %s", yaml_file, data)
            else:
                log.warning(
                    "GKLM section empty or missing under 'gklm:' in '%s'",
                    yaml_file,
                )
        except Exception as e:
            log.error("Failed to load GKLM config file '%s': %s", yaml_file, e)
            raise ConfigError(f"Unable to parse custom-config-file: {e}")

    # Override from --custom-config list
    for item in custom_data.get("custom-config", []):
        try:
            key, val = item.split("=", 1)
        except ValueError:
            log.warning("Skipping invalid custom-config entry: '%s'", item)
            continue

        if key in {
            "gklm_ip",
            "gklm_user",
            "gklm_password",
            "gklm_node_username",
            "gklm_node_password",
            "gklm_hostname",
            "gklm_rest_prefix",
        }:
            merged[key] = val
            log.info("Overrode GKLM config '%s' via custom-config: %s", key, val)
        else:
            log.warning("Unknown GKLM config key in custom-config: '%s'", key)

    # 5. Validate that all required keys are present
    required = [
        "gklm_ip",
        "gklm_user",
        "gklm_password",
        "gklm_node_username",
        "gklm_node_password",
        "gklm_hostname",
    ]
    missing = [k for k in required if not merged.get(k)]
    if missing:
        raise ConfigError(
            f"GKLM configuration incomplete. Missing keys: {missing}. "
            "Provide via cephci_data, custom-config-file, or --custom-config."
        )

    log.info(
        "Final GKLM configuration keys: %s",
        [k for k in required if k in merged]
        + (["gklm_rest_prefix"] if merged.get("gklm_rest_prefix") else []),
    )
    return merged


def wait_for_gklm_server_restart(
    gklm_rest_client,
    timeout: int = 300,
    check_interval: int = 5,
    initial_wait: int = 10,
) -> bool:
    """
    Wait for GKLM server to restart and become healthy.

    This method waits for the server to go down (health check fails) and then
    come back up (health check succeeds).

    Args:
        timeout: Maximum time to wait for server restart in seconds (default: 300)
        check_interval: Interval between health checks in seconds (default: 5)
        initial_wait: Initial wait time before starting checks in seconds (default: 10)

    Returns:
        True if server restarted successfully, False if timeout occurred

    Raises:
        RuntimeError: If server health check fails unexpectedly
    """
    log.info(
        "Waiting for GKLM server to restart with timeout of {} seconds".format(timeout)
    )

    # Initial wait to allow server shutdown
    log.info("Waiting {} seconds for server to initiate shutdown".format(initial_wait))
    time.sleep(initial_wait)

    # Use WaitUntil for clean retry logic
    waiter = WaitUntil(timeout=timeout, interval=check_interval)
    for attempt in waiter:
        try:
            gklm_rest_client.login()  # Re-authenticate if needed
            is_healthy = gklm_rest_client.server.health_check()

            if is_healthy == '{"overall":true}':
                log.info("Server has restarted successfully and is healthy")
                return True
            else:
                log.debug("Server is still down, waiting for restart")
                log.info(
                    f"Attempt {attempt}: Server is still down, waiting for restart"
                )

        except requests.exceptions.RequestException as e:
            log.debug(
                "Connection error while waiting for server startup: {}".format(str(e))
            )

    # If we exit the loop, timeout occurred
    if waiter.expired:
        log.error("Server restart timed out after {} seconds".format(timeout))
        return False
