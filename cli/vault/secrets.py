import json
from copy import deepcopy


class Secrets(object):
    """Vault secrets engine management via REST API.

    Handles enabling secrets engines and general read/write operations.
    """

    def __init__(self, parent):
        self.parent = parent
        self.transit = self.Transit(parent=self)

    def enable(self, **kw):
        """Enable a secrets engine at a given path.

        Args:
            kw(dict): Key/value pairs for enabling a secrets engine.
                Supported keys:
                    path(str): Mount path for the engine (e.g., "transit", "kv")
                    type(str): Engine type (e.g., "transit", "kv")
                    options(dict): Engine-specific options (e.g., {"version": "2"})
        """
        kw_copy = deepcopy(kw)
        path = kw_copy.pop("path", "")
        engine_type = kw_copy.pop("type", "")
        options = kw_copy.pop("options", {})
        payload = {"type": engine_type}
        if options:
            payload["options"] = options
        data = json.dumps(payload)
        cmd = (
            f"curl -s -X POST"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/sys/mounts/{path}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def write(self, **kw):
        """Write data to a Vault path.

        Args:
            kw(dict): Key/value pairs for the write operation.
                Supported keys:
                    path(str): Vault path to write to
                    data(dict): Data payload to write
        """
        kw_copy = deepcopy(kw)
        path = kw_copy.pop("path", "")
        payload = kw_copy.pop("data", {})
        data = json.dumps(payload) if payload else "{}"
        cmd = (
            f"curl -s -X POST"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/{path}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def read(self, **kw):
        """Read data from a Vault path.

        Args:
            kw(dict): Key/value pairs for the read operation.
                Supported keys:
                    path(str): Vault path to read from
        """
        kw_copy = deepcopy(kw)
        path = kw_copy.pop("path", "")
        cmd = (
            f"curl -s"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" {self.parent.vault_url}/v1/{path}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    class Transit(object):
        """Transit secrets engine operations."""

        def __init__(self, parent):
            self.parent = parent

        def create_key(self, **kw):
            """Create a named encryption key in the Transit engine.

            Args:
                kw(dict): Key/value pairs for key creation.
                    Supported keys:
                        key-name(str): Name of the encryption key
                        type(str): Key type (default: aes256-gcm96)
            """
            kw_copy = deepcopy(kw)
            key_name = kw_copy.pop("key-name", "")
            payload = {}
            key_type = kw_copy.pop("type", "")
            if key_type:
                payload["type"] = key_type
            data = json.dumps(payload) if payload else "{}"
            cmd = (
                f"curl -s -X POST"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" -H 'Content-Type: application/json'"
                f" -d '{data}'"
                f" {self.parent.parent.vault_url}/v1/transit/keys/{key_name}"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)

        def configure_key(self, **kw):
            """Configure an existing Transit encryption key.

            Args:
                kw(dict): Key/value pairs for key configuration.
                    Supported keys:
                        key-name(str): Name of the encryption key
                        auto-rotate-period(str): Auto rotation period (e.g., "24h")
                        min-decryption-version(int): Minimum decryption version
                        min-encryption-version(int): Minimum encryption version
                        deletion-allowed(bool): Allow key deletion
            """
            kw_copy = deepcopy(kw)
            key_name = kw_copy.pop("key-name", "")
            payload = {}
            auto_rotate = kw_copy.pop("auto-rotate-period", "")
            if auto_rotate:
                payload["auto_rotate_period"] = auto_rotate
            min_dec = kw_copy.pop("min-decryption-version", "")
            if min_dec:
                payload["min_decryption_version"] = min_dec
            min_enc = kw_copy.pop("min-encryption-version", "")
            if min_enc:
                payload["min_encryption_version"] = min_enc
            deletion = kw_copy.pop("deletion-allowed", "")
            if deletion:
                payload["deletion_allowed"] = deletion
            data = json.dumps(payload)
            cmd = (
                f"curl -s -X POST"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" -H 'Content-Type: application/json'"
                f" -d '{data}'"
                f" {self.parent.parent.vault_url}"
                f"/v1/transit/keys/{key_name}/config"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)

        def read_key(self, **kw):
            """Read a Transit encryption key's details.

            Args:
                kw(dict): Key/value pairs for reading the key.
                    Supported keys:
                        key-name(str): Name of the encryption key
            """
            kw_copy = deepcopy(kw)
            key_name = kw_copy.pop("key-name", "")
            cmd = (
                f"curl -s"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" {self.parent.parent.vault_url}/v1/transit/keys/{key_name}"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)
