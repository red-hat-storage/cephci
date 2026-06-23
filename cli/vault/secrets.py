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
        path = kw.get("path", "")
        engine_type = kw.get("type", "")
        options = kw.get("options", {})
        payload = {"type": engine_type}
        if options:
            payload["options"] = options
        return self.parent._request("POST", f"/v1/sys/mounts/{path}", data=payload)

    def write(self, **kw):
        """Write data to a Vault path.

        Args:
            kw(dict): Key/value pairs for the write operation.
                Supported keys:
                    path(str): Vault path to write to
                    data(dict): Data payload to write
        """
        path = kw.get("path", "")
        payload = kw.get("data", {})
        return self.parent._request("POST", f"/v1/{path}", data=payload or {})

    def read(self, **kw):
        """Read data from a Vault path.

        Args:
            kw(dict): Key/value pairs for the read operation.
                Supported keys:
                    path(str): Vault path to read from
        """
        path = kw.get("path", "")
        return self.parent._request("GET", f"/v1/{path}")

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
            key_name = kw.get("key-name", "")
            payload = {}
            key_type = kw.get("type", "")
            if key_type:
                payload["type"] = key_type
            return self.parent.parent._request(
                "POST", f"/v1/transit/keys/{key_name}", data=payload or {}
            )

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
            key_name = kw.get("key-name", "")
            payload = {}
            auto_rotate = kw.get("auto-rotate-period", "")
            if auto_rotate:
                payload["auto_rotate_period"] = auto_rotate
            min_dec = kw.get("min-decryption-version", "")
            if min_dec:
                payload["min_decryption_version"] = min_dec
            min_enc = kw.get("min-encryption-version", "")
            if min_enc:
                payload["min_encryption_version"] = min_enc
            deletion = kw.get("deletion-allowed", "")
            if deletion:
                payload["deletion_allowed"] = deletion
            return self.parent.parent._request(
                "POST", f"/v1/transit/keys/{key_name}/config", data=payload
            )

        def read_key(self, **kw):
            """Read a Transit encryption key's details.

            Args:
                kw(dict): Key/value pairs for reading the key.
                    Supported keys:
                        key-name(str): Name of the encryption key
            """
            key_name = kw.get("key-name", "")
            return self.parent.parent._request("GET", f"/v1/transit/keys/{key_name}")
