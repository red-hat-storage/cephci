import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Qos(Cli):

    def __init__(self, nodes, base_cmd):
        super(Qos, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} qos"

    def get(self, nfs_name, export, **kwargs):
        """
        nfs_name = nfs cluster name
        export = export
        """
        cmd = f"{self.base_cmd} get {nfs_name} {export} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            out = out[0].strip()
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            return out

    def enable(
        self, nfs_name: str, export: str, operation="bandwidth_control", **kwargs
    ):
        """
        nfs_name = nfs cluster name
        export = export
        operation = "bandwidth_control"
        max_export_read_bw : required  - pass in kwargs
        max_export_read_bw : required  - pass in kwargs
        """
        if "max_export_write_bw" not in kwargs or "max_export_read_bw" not in kwargs:
            raise Exception(
                'For "PerShare" - kwargs "max_export_write_bw" and "max_export_read_bw" required'
                + ', example - max_export_write_bw = "10MB"'
            )

        cmd = f"{self.base_cmd} enable {operation} {nfs_name} {export} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disable(self, cluster_id, export, operation="bandwidth_control", **kwargs):
        """
        nfs_name = nfs cluster name
        export = export
        operation = "bandwidth_control"
        """
        cmd = f"{self.base_cmd} disable {operation} {cluster_id} {export} {build_cmd_from_args(**kwargs)}"
        f" {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
