from cli import Cli


class Secret(Cli):
    """CLI interface for the ceph_secrets MGR module commands (``ceph secret ...``)."""

    def __init__(self, nodes, base_cmd):
        super(Secret, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} secret"

    # ------------------------------------------------------------------
    # ceph secret set <path> -i <file>
    # ------------------------------------------------------------------
    def set(self, path, infile):
        """Create or update a secret from a file.

        Args:
            path (str): Secret path, e.g. ``cephadm/service/my-svc/api_token``
            infile (str): Path to the file whose contents become the secret data

        Returns:
            Command output string
        """
        cmd = f"{self.base_cmd} set {path} -i {infile}"
        out = self.execute(sudo=True, check_ec=True, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    # ------------------------------------------------------------------
    # ceph secret get <path> [--reveal] [--format json|yaml]
    # ------------------------------------------------------------------
    def get(self, path, reveal=False, format="json"):
        """Retrieve metadata (and optionally data) for a secret.

        Args:
            path (str): Secret path
            reveal (bool): Include the stored data in the response
            format (str): Output format – ``json`` (default) or ``yaml``

        Returns:
            Command output string
        """
        cmd = f"{self.base_cmd} get {path} --format {format}"
        if reveal:
            cmd += " --reveal"
        out = self.execute(sudo=True, check_ec=True, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    # ------------------------------------------------------------------
    # ceph secret get-value <path>
    # ------------------------------------------------------------------
    def get_value(self, path):
        """Return the raw secret data string with no JSON envelope.

        Args:
            path (str): Secret path

        Returns:
            Raw secret value string
        """
        cmd = f"{self.base_cmd} get-value {path}"
        out = self.execute(sudo=True, check_ec=True, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    # ------------------------------------------------------------------
    # ceph secret ls [--namespace <ns>] [--scope <scope>]
    #                [--sec_target <target>] [--reveal] [--show_internals]
    #                [--format json|yaml]
    # ------------------------------------------------------------------
    def ls(
        self,
        namespace=None,
        scope=None,
        sec_target=None,
        reveal=False,
        show_internals=False,
        format="json",
    ):
        """List secrets, optionally filtered by namespace, scope, and/or target.

        Args:
            namespace (str): Filter by namespace (optional)
            scope (str): Filter by scope – ``global``, ``service``, ``host``,
                         or ``custom`` (optional)
            sec_target (str): Filter by target within the scope (optional)
            reveal (bool): Include stored data in each record
            show_internals (bool): Include the ``policy`` object in each record
            format (str): Output format – ``json`` (default) or ``yaml``

        Returns:
            Command output string
        """
        cmd = f"{self.base_cmd} ls --format {format}"
        if namespace:
            cmd += f" --namespace {namespace}"
        if scope:
            cmd += f" --scope {scope}"
        if sec_target:
            cmd += f" --sec_target {sec_target}"
        if reveal:
            cmd += " --reveal"
        if show_internals:
            cmd += " --show_internals"
        out = self.execute(sudo=True, check_ec=True, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    # ------------------------------------------------------------------
    # ceph secret rm <path>
    # ------------------------------------------------------------------
    def rm(self, path):
        """Remove a secret (idempotent – succeeds even if the secret is absent).

        Args:
            path (str): Secret path

        Returns:
            Command output string, e.g. ``{"status": "removed"}``
        """
        cmd = f"{self.base_cmd} rm {path}"
        out = self.execute(sudo=True, check_ec=True, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
