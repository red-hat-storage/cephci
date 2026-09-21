"""
CLI interface for `ceph nvme-gw` monitor commands.

These commands run on a node with ceph admin (e.g. installer) and control
NVMe-oF gateway availability, location, and disaster state for stretch clusters.
They are separate from the gateway-side `ceph nvmeof` entity management CLI.

All commands are run via `cephadm shell --` so `ceph` is available (same as
other ceph CLI usage in cephci, e.g. `cephadm shell -- ceph nvmeof gateway info`).

All commands use normal exit-code checking (check_ec=True by default where applicable).
"""

from cli import Cli

CEPHADM_SHELL_PREFIX = "cephadm shell -- "


class NvmeGw(Cli):
    """CLI for `ceph nvme-gw` (gateway monitor/control commands)."""

    def __init__(self, nodes, base_cmd):
        super(NvmeGw, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} nvme-gw"

    def execute(self, cmd, **kwargs):
        """Run command via cephadm shell so `ceph` is available on the node."""
        wrapped_cmd = f"{CEPHADM_SHELL_PREFIX}{cmd}"
        return super(NvmeGw, self).execute(cmd=wrapped_cmd, **kwargs)

    def show(self, pool, group, **kwargs):
        """
        Show gateway group state (gateways, ANA states, locations).

        Args:
            pool: NVMe-oF pool name (e.g. rbd pool used by the service).
            group: Gateway group name.
            **kwargs: Optional format (e.g. format="json") for output.

        Returns:
            Command output string (or parsed if format=json).
        """
        cmd = f"{self.base_cmd} show {pool} {group}"
        if kwargs.get("format"):
            cmd += f" --format {kwargs.get('format')}"
        out = self.execute(sudo=True, check_ec=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def enable(self, gw_id, pool, group, **kwargs):
        """
        Mark a gateway as available (admin control).

        Args:
            gw_id: Gateway ID (e.g. daemon name or id from show).
            pool: NVMe-oF pool name.
            group: Gateway group name.

        Returns:
            Command output string.
        """
        cmd = f"{self.base_cmd} enable {gw_id} {pool} {group}"
        out = self.execute(sudo=True, check_ec=kwargs.get("check_ec", True), cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disable(self, gw_id, pool, group, **kwargs):
        """
        Mark a gateway as unavailable (admin control).

        Args:
            gw_id: Gateway ID.
            pool: NVMe-oF pool name.
            group: Gateway group name.

        Returns:
            Command output string.
        """
        cmd = f"{self.base_cmd} disable {gw_id} {pool} {group}"
        out = self.execute(sudo=True, check_ec=kwargs.get("check_ec", True), cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_location(self, gw_id, pool, group, location, **kwargs):
        """
        Set the location attribute for a gateway (stretch cluster).
        To unset location, pass location="" so the command includes "" as the fourth argument.

        Args:
            gw_id: Gateway ID.
            pool: NVMe-oF pool name.
            group: Gateway group name.
            location: User-defined location string (e.g. site/datacenter name). Use "" to unset.

        Returns:
            Command output string.
        """
        location_arg = '""' if location == "" else location
        cmd = f"{self.base_cmd} set-location {gw_id} {pool} {group} {location_arg}"
        check_ec = kwargs.get("check_ec", True)
        try:
            out = self.execute(sudo=True, check_ec=check_ec, cmd=cmd)
        except Exception as exc:
            msg = str(exc).lower()
            if "already set" in msg or "eexist" in msg:
                return str(exc)
            raise
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disaster_set(self, pool, group, location, **kwargs):
        """
        Mark a location as in disaster state.
        Failbacks to gateways in this location are not allowed until cleared.

        Args:
            pool: NVMe-oF pool name.
            group: Gateway group name.
            location: Location to mark as in disaster.

        Returns:
            Command output string.
        """
        cmd = f"{self.base_cmd} disaster-set {pool} {group} {location}"
        check_ec = kwargs.get("check_ec", True)
        try:
            out = self.execute(sudo=True, check_ec=check_ec, cmd=cmd)
        except Exception as exc:
            msg = str(exc).lower()
            if "already" in msg or "eexist" in msg:
                return str(exc)
            raise
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disaster_clear(self, pool, group, location, **kwargs):
        """
        Clear disaster state for a location.
        Triggers failback workflow for gateways in that location.

        Args:
            pool: NVMe-oF pool name.
            group: Gateway group name.
            location: Location to mark as recovered.

        Returns:
            Command output string.
        """
        cmd = f"{self.base_cmd} disaster-clear {pool} {group} {location}"
        check_ec = kwargs.get("check_ec", True)
        try:
            out = self.execute(sudo=True, check_ec=check_ec, cmd=cmd)
        except Exception as extra:
            msg = str(extra).lower()
            if "not in disaster" in msg or "already" in msg or "enoent" in msg:
                return str(extra)
            raise
        if isinstance(out, tuple):
            return out[0].strip()
        return out
