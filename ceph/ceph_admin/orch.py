"""
Module that interacts with the orchestrator CLI.

Provide the interfaces to ceph orch and in turn manage the orchestration engine.
"""
from datetime import datetime, timedelta
from json import loads
from time import sleep

from ceph.ceph import ResourceNotFoundError
from utility.log import Log

from .ceph import CephCLI
from .common import config_dict_to_string
from .helper import GenerateServiceSpec
from .ls import LSMixin
from .pause import PauseMixin
from .ps import PSMixin
from .reconfig import ReconfigMixin
from .redeploy import RedeployMixin
from .remove import RemoveMixin
from .restart import RestartMixin
from .resume import ResumeMixin
from .start import StartMixin
from .stop import StopMixin
from .upgrade import UpgradeMixin

LOG = Log(__name__)


class Orch(
    LSMixin,
    PSMixin,
    ReconfigMixin,
    RedeployMixin,
    RemoveMixin,
    RestartMixin,
    StartMixin,
    StopMixin,
    UpgradeMixin,
    PauseMixin,
    ResumeMixin,
    CephCLI,
):
    """Represent ceph orch command."""

    direct_calls = ["ls", "ps"]

    def get_hosts_by_label(self, label: str):
        """
        Fetch host object by label attached to it.

        Args:
            label (Str): name of the label

        Returns:
            hosts (List)
        """
        out, _ = self.shell(args=["ceph", "orch", "host", "ls", "--format=json"])
        return [node for node in loads(out) if label in node.get("labels")]

    def check_service_exists(
        self,
        service_name: str = None,
        service_type: str = None,
        timeout: int = 1800,
        interval: int = 20,
    ) -> bool:
        """
        Verify the provided service is running for the given list of ids.

        Args:
            service_name (Str): The name of the service to be checked.
            service_type (Str): The type of the service to be checked.
            timeout (Int):  In seconds, the maximum allowed time (default=300)
            interval (int): In seconds, the polling interval time (default=5)

        Returns:
            Boolean: True if the service and the list of daemons are running else False.

        """
        end_time = datetime.now() + timedelta(seconds=timeout)
        check_status_dict = {
            "base_cmd_args": {"format": "json"},
            "args": {"refresh": True},
        }

        if service_name:
            check_status_dict["args"]["service_name"] = service_name

        if service_type:
            check_status_dict["args"]["service_type"] = service_type

        _retries = 3  # cross-verification retries
        _count = 0
        while end_time > datetime.now():
            sleep(interval)
            out, err = self.ls(check_status_dict)
            out = loads(out)[0]
            running = out["status"]["running"]
            count = out["status"]["size"]

            LOG.info(
                f"{running}/{count} {service_name if service_name else service_type} up... retrying"
            )

            if count + running < 1:
                continue

            if count == running and _count == count:
                if _retries < 1:
                    return True
                _retries -= 1

            if _count != count:
                _count = count
                _retries = 3

        # Identify the failure
        out, err = self.ls(check_status_dict)
        out = loads(out)
        LOG.error(
            f"{service_name if service_name else service_type} failed with \n{out[0].get('events')}"
        )

        return False

    def get_role_service(self, service_name: str) -> str:
        """
        Get service info by name.

        Args:
            service_name (Str): service name

        Returns:
            service (Dict)

        Raises:
            ResourceNotFound: when no resource with the provided is matched.
        """
        out, _ = self.ls()

        for svc in loads(out):
            if service_name in svc.get("service_name"):
                return svc

        raise ResourceNotFoundError(f"No service names matched {service_name}")

    def check_service(
        self, service_name: str, timeout: int = 300, interval: int = 5, exist=True
    ) -> bool:
        """
        check service existence based on the exist parameter.
        if exist is set, then validate its presence. otherwise, for its removal.

        Args:
            service_name (Str): service name
            timeout (Int): timeout in seconds
            interval (Int): interval in seconds
            exist (Bool): exists or not

        Returns:
            Boolean

        """
        end_time = datetime.now() + timedelta(seconds=timeout)

        while end_time > datetime.now():
            sleep(interval)
            out, err = self.ls({"base_cmd_args": {"format": "json"}})
            out = loads(out)
            service = [d for d in out if d.get("service_name") == service_name]

            if service_name not in service and not exist:
                return True
            elif service_name in service and exist:
                return True
            LOG.info("[%s] check for existence: %s, retrying" % (service_name, exist))

        return False

    def apply_spec(self, config) -> None:
        """
        Execute the apply_spec method using the object's service name and provided input.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            config:
                command: apply_spec
                service: orch
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                specs:
                 - service_type: host
                   attach_ip_address: true
                   labels: apply-all-labels
                   nodes:
                     - node2
                     - node3

            base_cmd_args   - key/value pairs to set for base command
            specs           - service specifications.

        """
        base_cmd = ["ceph", "orch"]

        if config.get("base_cmd_args"):
            base_cmd_args_str = config_dict_to_string(config.get("base_cmd_args"))
            base_cmd.append(base_cmd_args_str)

        base_cmd.append("apply -i")
        specs = config["specs"]

        spec_cls = GenerateServiceSpec(
            node=self.installer, cluster=self.cluster, specs=specs
        )
        spec_filename = spec_cls.create_spec_file()
        base_cmd.append(spec_filename)

        out, err = self.shell(
            args=base_cmd,
            base_cmd_args={"mount": "/tmp:/tmp"},
        )

        LOG.info(f"apply-spec command response :\n{out}")
        # todo: add verification part

        # validate services
        validate_spec_services = config.get("validate-spec-services")
        if validate_spec_services:
            self.validate_spec_services(specs=specs)
            LOG.info("Validation of service created using a spec file is completed")

    def op(self, op, config):
        """
        Execute the command ceph orch <start|stop|restart|reconfigure|redeploy> <service>.

        Args:
            config (Dict): command and service are passed from the test case.
            op (Str): operation parameters.

        Returns:
          output (Str), error (Str)  returned by the command.

        Example::

            Testing ceph orch restart mon
            op: restart|start|stop|reconfigure|redeploy

            config:
              command: restart
              service: mon
            ...

            config:
                command: start
                base_cmd_args:
                    verbose: true
                pos_args:
                    - service_name

        """

        base_cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))
        base_cmd.append(op)

        base_cmd.extend(config.get("pos_args"))

        return self.shell(args=base_cmd)

    def status(self, config) -> bool:
        """Execute the command ceph orch status <args>.

        Args:
            config (Dict): The key/value pairs passed from the test case.

        Returns:
            output, error   returned by the command.

        Example::

            Testing ceph orch status

            config:
                command: status
                base_cmd_args:
                    verbose: true
                    format: json | json-pretty | xml | xml-pretty | plain | yaml
                args:
                    detail: true

        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("status")

        if config and config.get("args"):
            args = config.get("args")
            if args["detail"]:
                cmd.append("--detail")

        return self.shell(args=cmd)

    def verify_status(self, op) -> None:
        """Verify the status of the orchestrator for the operation specified.

        Args:
            op (str): pause/resume based on whether the pause or resume status to be checked
        """
        config = {"command": "status", "base_cmd_args": {"format": "json"}}
        out, _ = self.status(config)
        status = loads(out)

        if op == "pause" and status["paused"]:
            LOG.info("The orch operations are paused")
            return True
        elif op == "resume" and not loads(out)["paused"]:
            LOG.info("The orch operations are resumed")
            LOG.info("The orch operations are resumed")
            return True
        return False

    def validate_spec_services(self, specs) -> None:
        LOG.info("Validating spec services")
        for spec in specs:
            svc_type = spec["service_type"]
            svc_id = spec.get("service_id")
            if svc_id:
                self.check_service_exists(
                    service_name=f"{svc_type}.{spec['service_id']}"
                )
            else:
                self.check_service_exists(service_type=spec["service_type"])
        return False
