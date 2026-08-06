"""
Test module for the ceph_secrets MGR module.

Validates the full lifecycle of the ``ceph_secrets`` module:

1. Enable the ``ceph_secrets`` MGR module
2. Set a secret via ``ceph secret set``
3. Get secret metadata via ``ceph secret get`` (without reveal)
4. Get secret metadata with the raw value via ``ceph secret get --reveal``
5. Get raw secret value via ``ceph secret get-value``
6. List secrets, with optional namespace/scope/target filters
7. Remove the secret via ``ceph secret rm`` and verify idempotency
8. Disable the ``ceph_secrets`` MGR module

Test configuration example (suite YAML)::

    tests:
      - test:
          name: Ceph Secrets Module - lifecycle
          module: test_ceph_secrets_module.py
          polarion-id: CEPH-XXXXX
          config:
            namespace: cephadm
            scope: service
            target: my-test-svc
            secret_name: api_token
            secret_data: "s3cr3t-v4lu3"
          desc: Verify ceph_secrets module enable, CRUD operations, and disable
"""

import json
import tempfile

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)

MODULE_NAME = "ceph_secrets"


def run(ceph_cluster, **kw):
    """Execute the ceph_secrets module lifecycle test.

    Args:
        ceph_cluster: Ceph cluster object
        kw (dict): Test configuration key/value pairs

    Returns:
        0 on success, 1 on failure
    """
    log.info("Starting ceph_secrets module lifecycle test")
    config = kw.get("config", {})

    installer = ceph_cluster.get_nodes(role="installer")[0]

    namespace = config.get("namespace", "cephadm")
    scope = config.get("scope", "service")
    target = config.get("target", "test-service")
    secret_name = config.get("secret_name", "api_token")
    secret_data = config.get("secret_data", "test-secret-value-12345")

    # Canonical CLI path: <namespace>/<scope>/<target>/<name>
    secret_path = f"{namespace}/{scope}/{target}/{secret_name}"

    # Temp file written on the installer host; mounted into cephadm shell for secret set
    tmp = tempfile.NamedTemporaryFile(delete=False)
    secret_file = tmp.name
    tmp.close()

    try:
        # ----------------------------------------------------------
        # Step 1: Enable the ceph_secrets module
        # ----------------------------------------------------------
        log.info(f"Step 1: Enable MGR module '{MODULE_NAME}'")
        if CephAdm(installer).ceph.mgr.module.enable(MODULE_NAME, force=True):
            raise OperationFailedError(f"Failed to enable {MODULE_NAME} module")

        out = CephAdm(installer).ceph.mgr.module.ls()
        if MODULE_NAME not in out:
            raise OperationFailedError(
                f"'{MODULE_NAME}' not listed under enabled_modules after enable"
            )
        log.info(f"Module '{MODULE_NAME}' is enabled")

        # ----------------------------------------------------------
        # Step 2: Write secret data to a file on the installer and
        #         pass it into cephadm shell via --mount
        # ----------------------------------------------------------
        log.info(f"Step 2: Set secret at path '{secret_path}'")
        with installer.remote_file(
            sudo=True, file_name=secret_file, file_mode="w"
        ) as f:
            f.write(secret_data)

        raw = CephAdm(
            installer, src_mount=secret_file, mount=secret_file
        ).ceph.secret.set(secret_path, secret_file)
        result = json.loads(raw)
        version = (result.get("metadata") or {}).get("version")
        if version is None:
            raise OperationFailedError(
                f"secret set response missing 'metadata.version'. Got: {result}"
            )
        log.info(f"Secret stored at version {version}")

        # ----------------------------------------------------------
        # Step 3: Get metadata without reveal
        # ----------------------------------------------------------
        log.info("Step 3: Get secret metadata (no reveal)")
        raw = CephAdm(installer).ceph.secret.get(secret_path, reveal=False)
        result = json.loads(raw)
        if "metadata" not in result:
            raise OperationFailedError(
                f"Expected 'metadata' key in get response. Got: {result}"
            )
        if "data" in result:
            raise OperationFailedError(
                f"Unexpected 'data' key in get response when reveal=False. Got: {result}"
            )
        log.info("Metadata returned correctly without 'data' field")

        # ----------------------------------------------------------
        # Step 4: Get metadata with reveal
        # ----------------------------------------------------------
        log.info("Step 4: Get secret metadata with --reveal")
        raw = CephAdm(installer).ceph.secret.get(secret_path, reveal=True)
        result = json.loads(raw)
        if "data" not in result:
            raise OperationFailedError(
                f"Expected 'data' key in get --reveal response. Got: {result}"
            )
        if result["data"] != secret_data:
            raise OperationFailedError(
                f"Revealed data mismatch. Expected: '{secret_data}', Got: '{result['data']}'"
            )
        log.info("Revealed data matches the stored secret")

        # ----------------------------------------------------------
        # Step 5: Get raw value via get-value
        # ----------------------------------------------------------
        log.info("Step 5: Get raw secret value via get-value")
        raw_value = CephAdm(installer).ceph.secret.get_value(secret_path)
        if raw_value != secret_data:
            raise OperationFailedError(
                f"get-value mismatch. Expected: '{secret_data}', Got: '{raw_value}'"
            )
        log.info("get-value returned the correct raw secret")

        # ----------------------------------------------------------
        # Step 6: List secrets filtered by namespace / scope / target
        # ----------------------------------------------------------
        log.info(
            f"Step 6: List secrets (namespace={namespace}, scope={scope}, "
            f"sec_target={target})"
        )
        raw = CephAdm(installer).ceph.secret.ls(
            namespace=namespace, scope=scope, sec_target=target
        )
        ls_result = json.loads(raw)
        if secret_path not in ls_result:
            raise OperationFailedError(
                f"Expected '{secret_path}' in ls output. Got keys: {list(ls_result.keys())}"
            )
        log.info(f"Secret '{secret_path}' present in ls output")

        # ----------------------------------------------------------
        # Step 7a: Remove the secret
        # ----------------------------------------------------------
        log.info("Step 7a: Remove secret")
        raw = CephAdm(installer).ceph.secret.rm(secret_path)
        rm_result = json.loads(raw)
        if rm_result.get("status") != "removed":
            raise OperationFailedError(
                f"Expected status 'removed' after rm. Got: {rm_result}"
            )
        log.info("Secret removed successfully")

        # ----------------------------------------------------------
        # Step 7b: Remove again – idempotency check
        # ----------------------------------------------------------
        log.info("Step 7b: Remove same secret again (idempotency check)")
        raw = CephAdm(installer).ceph.secret.rm(secret_path)
        rm_again = json.loads(raw)
        if rm_again.get("status") != "not_found":
            raise OperationFailedError(
                f"Expected status 'not_found' on idempotent rm. Got: {rm_again}"
            )
        log.info("Idempotent rm returned 'not_found' as expected")

        # ----------------------------------------------------------
        # Step 8: Disable the ceph_secrets module
        # ----------------------------------------------------------
        log.info(f"Step 8: Disable MGR module '{MODULE_NAME}'")
        if CephAdm(installer).ceph.mgr.module.disable(MODULE_NAME):
            raise OperationFailedError(f"Failed to disable {MODULE_NAME} module")
        log.info(f"Module '{MODULE_NAME}' disabled successfully")

    except Exception as exc:
        log.error(f"ceph_secrets lifecycle test failed: {exc}")
        return 1

    finally:
        installer.exec_command(sudo=True, cmd=f"rm -f {secret_file}", check_ec=False)

    log.info("ceph_secrets module lifecycle test PASSED")
    return 0
