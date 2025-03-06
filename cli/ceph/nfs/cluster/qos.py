import json

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Qos(Cli):

    def __init__(self, nodes, base_cmd):
        super(Qos, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} qos"

    def get(self, cluster_id: str, **kwargs):
        """
        cluster_id = cluster name
        """
        cmd = f"{self.base_cmd} get {cluster_id} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            out = out[0].strip()
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            return out

    def enable(
        self, cluster_id: str, qos_type: str, operation="bandwidth_control", **kwargs
    ):
        """
        nfs_name = nfs cluster name
        export = export
        operation = "bandwidth_control"
        max_export_read_bw : required  - pass in kwargs
        max_export_read_bw : required  - pass in kwargs
        """
        if qos_type == "PerShare" and (
            "max_export_write_bw" not in kwargs or "max_export_read_bw" not in kwargs
        ):
            raise Exception(
                'For "PerShare" - kwargs "max_export_write_bw" and "max_export_read_bw" required'
                + ', example - max_export_write_bw = "10MB"'
            )
        cmd = f"{self.base_cmd} enable {operation} {cluster_id} {qos_type} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disable(self, cluster_id, operation="bandwidth_control", **kwargs):
        """
        cluster_id = cluster name
        qos_type = PerShare | PerClient | PerShare-PerClient
        operation="bandwidth_control"
        """
        cmd = f"{self.base_cmd} disable {operation} {cluster_id}"
        f" {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
