from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Qos(Cli):

    def __init__(self, nodes: list, base_cmd: str) -> None:
        super().__init__(nodes)
        self.base_cmd = "%s qos" % base_cmd

    def get(self, cluster_id: str, **kwargs):
        cmd = " ".join(
            [self.base_cmd, "get", cluster_id, build_cmd_from_args(**kwargs)]
        )
        out = self.execute(sudo=True, cmd=cmd)
        processed_out = out[0].strip() if isinstance(out, tuple) else out
        return processed_out

    def enable_per_share(
        self,
        qos_type: str,
        cluster_id: str,
        max_export_combined_bw=None,
        max_export_write_bw=None,
        max_export_read_bw=None,
    ) -> str:
        params = self._build_per_share_params(
            max_export_combined_bw, max_export_write_bw, max_export_read_bw
        )
        return self._execute_qos_cmd(
            "enable", "bandwidth_control", cluster_id, params, qos_type=qos_type
        )

    def enable_per_client(
        self,
        cluster_id: str,
        qos_type: str,
        max_client_combined_bw: str = None,
        max_client_write_bw: str = None,
        max_client_read_bw: str = None,
    ) -> str:
        params = self._build_client_params(
            max_client_combined_bw, max_client_write_bw, max_client_read_bw
        )
        return self._execute_qos_cmd(
            "enable", "bandwidth_control", cluster_id, params, qos_type=qos_type
        )

    def enable_per_share_per_client(
        self,
        cluster_id: str,
        qos_type: str,
        max_export_combined_bw: str = None,
        max_export_write_bw: str = None,
        max_export_read_bw: str = None,
        max_client_combined_bw: str = None,
        max_client_write_bw: str = None,
        max_client_read_bw: str = None,
    ) -> str:
        share_params = self._build_per_share_params(
            max_export_combined_bw, max_export_write_bw, max_export_read_bw
        )
        client_params = self._build_client_params(
            max_client_combined_bw, max_client_write_bw, max_client_read_bw
        )
        return self._execute_qos_cmd(
            "enable",
            "bandwidth_control",
            cluster_id,
            share_params + client_params,
            qos_type=qos_type,
        )

    def disable(
        self,
        cluster_id: str,
        qos_type: str = None,
        operation: str = "bandwidth_control",
        **kwargs
    ) -> str:
        cmd_parts = [self.base_cmd, "disable", operation, cluster_id]

        if qos_type:
            cmd_parts.append(qos_type)

        cmd_parts.append(build_cmd_from_args(**kwargs))
        cmd = " ".join(cmd_parts)

        result = self.execute(sudo=True, cmd=cmd)
        return result[0].strip() if isinstance(result, tuple) else result

    def _build_per_share_params(
        self,
        combined_bw: str,
        write_bw: str,
        read_bw: str,
    ) -> list:
        if combined_bw:
            return ["--combined-rw-bw-ctrl --max_export_combined_bw %s" % combined_bw]
        if write_bw and read_bw:
            return [
                "--max_export_write_bw %s" % write_bw,
                "--max_export_read_bw %s" % read_bw,
            ]
        raise ValueError(
            "Per-share QoS requires either combined bandwidth or both read/write limits"
        )

    def _build_client_params(
        self,
        combined_bw: str,
        write_bw: str,
        read_bw: str,
    ) -> list:
        if combined_bw:
            return ["--combined-rw-bw-ctrl --max_client_combined_bw %s" % combined_bw]
        if write_bw and read_bw:
            return [
                "--max_client_write_bw %s" % write_bw,
                "--max_client_read_bw %s" % read_bw,
            ]
        raise ValueError(
            "Per_client QoS requires either combined bandwidth or both read/write limits"
        )

    def _execute_qos_cmd(
        self, action: str, operation: str, cluster_id: str, params: list, qos_type: str
    ) -> str:
        # Ensure --combined-rw-bw-ctrl is not duplicated
        if len([x for x in params if x.startswith("--combined")]) > 1:
            params[-1] = [x for x in params if x.startswith("--combined")][-1].replace(
                "--combined-rw-bw-ctrl ", ""
            )

        cmd = " ".join(
            [self.base_cmd, action, operation, cluster_id, qos_type, " ".join(params)]
        )

        result = self.execute(sudo=True, cmd=cmd)
        return result[0].strip() if isinstance(result, tuple) else result
