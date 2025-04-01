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
        self,
        qos_type: str,
        nfs_name: str,
        export: str,
        operation: str = "bandwidth_control",
        **kwargs,
    ) -> str:
        """Enable QoS controls for an NFS cluster.

        Args:
            cluster_id: NFS cluster identifier
            qos_type: QoS type (PerShare/PerClient/PerShare-PerClient)
            operation: Operation type (default: bandwidth_control)
            **kwargs: Required parameters based on qos_type:
                - PerShare: Either (max_export_write_bw + max_export_read_bw)
                            OR max_export_combined_bw
                - PerClient: Either (max_client_write_bw + max_client_read_bw)
                             OR max_client_combined_bw
                - PerShare-PerClient: Combination of PerShare AND PerClient requirements

        Returns:
            Command execution output

        Raises:
            ValueError: For missing/invalid parameters or qos_type
        """
        # Validate qos_type
        params = []

        # Handle Export (PerShare) parameters
        if qos_type in ["PerShare", "PerShare_PerClient"]:
            if "max_export_combined_bw" in kwargs:
                params.append(
                    f"--max_export_combined_bw {kwargs['max_export_combined_bw']}"
                )
            else:
                params.extend(
                    [
                        f"--max_export_write_bw {kwargs['max_export_write_bw']}",
                        f"--max_export_read_bw {kwargs['max_export_read_bw']}",
                    ]
                )

        # Handle Client (PerClient) parameters
        if qos_type in ["PerShare_PerClient"]:
            if "max_client_combined_bw" in kwargs:
                params.append(
                    f"--max_client_combined_bw {kwargs['max_client_combined_bw']}"
                )
            else:
                params.extend(
                    [
                        f"--max_client_write_bw {kwargs['max_client_write_bw']}",
                        f"--max_client_read_bw {kwargs['max_client_read_bw']}",
                    ]
                )
        cmd = (
            f"{self.base_cmd} enable {operation} {nfs_name} {export} "
            f"{' '.join(params)}"
        )
        result = self.execute(sudo=True, cmd=cmd)

        return result[0].strip() if isinstance(result, tuple) else result

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
