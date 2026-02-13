import json

from cli import Cli
from cli.exceptions import OperationFailedError
from cli.utilities.utils import build_cmd_from_args

from .qos import Qos


class Cluster_qos(Cli):

    def __init__(self, nodes: list, base_cmd: str) -> None:
        super().__init__(nodes)
        self.base_cmd = "%s cluster_qos" % base_cmd
        self.qos = Qos(nodes, base_cmd)

    def disable(self, cluster_id: str, **kwargs):
        cmd = " ".join(
            [self.base_cmd, cluster_id, "disable", build_cmd_from_args(**kwargs)]
        )
        out = self.execute(sudo=True, cmd=cmd)
        processed_out = out[0].strip() if isinstance(out, tuple) else out

        # Validation
        self._validate_qos(cluster_id, expected_state=False)
        return processed_out

    def enable(self, cluster_id: str, **kwargs):
        cmd = " ".join(
            [self.base_cmd, cluster_id, "enable", build_cmd_from_args(**kwargs)]
        )
        out = self.execute(sudo=True, cmd=cmd)
        processed_out = out[0].strip() if isinstance(out, tuple) else out

        # Validation
        self._validate_qos(cluster_id, expected_state=True)
        return processed_out

    def _validate_qos(self, cluster_id, expected_state):
        """Validate if cluster_qos is enabled/disabled."""
        out = self.qos.get(cluster_id, format="json")
        try:
            data = json.loads(out)
            actual_state = data.get("enable_cluster_qos")
            if actual_state != expected_state:
                raise OperationFailedError(
                    f"Cluster QoS validation failed. Expected enable_cluster_qos={expected_state}, "
                    f"but got {actual_state}. Data: {data}"
                )
        except json.JSONDecodeError:
            raise OperationFailedError(f"Failed to parse QoS get output: {out}")
