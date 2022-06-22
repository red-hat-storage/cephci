import logging

from src.utilities.utils import check_and_raise_fabfile_command_failure

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExecuteCommandMixin:
    def __init__(self):
        pass

    def must_pass(self, **kw):
        """
        This method is used to check whether command is successfully executed or not ,else raises an exception.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              function(str): Function in which the command needs to be executed.
              kw_args(Dict): arguments sent to the function in the form of dictionary.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        step_output = kw.get("step_output")
        function = kw.get("function")
        kw_args = kw.get("kw_args", dict())
        kw_args["env_config"] = kw.get("env_config")
        kw_args["cluster_name"] = kw.get("cluster_name")
        logger.info(f"Running function {function}")
        out = function(kw=kw_args)
        if out is None:
            step_output["result"] = None
            step_output["status"] = "pass"
            return None
        step_output["result"] = list(out.values())[0]
        if check_and_raise_fabfile_command_failure(
            output=out, parallel=kw_args["env_config"].get("parallel"), fail=True
        ):
            step_output["status"] = "pass"
        else:
            status = False
            for ip, result in out.items():
                skip_strings = ["already"]
                if result.__dict__.get("failed"):
                    for s in skip_strings:
                        if s in str(result):
                            status = True
                            break
            if status:
                step_output["status"] = "pass"
            else:
                step_output["status"] = "fail"
        logger.info(f"Step output is {step_output}")
        return out

    def must_fail(self, **kw):
        """
        This method is used to check whether command is successfully failed or not ,else raises an exception.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              function(str): Function in which the command needs to be executed.
              kw_args(Dict): arguments sent to the function in the form of dictionary.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        step_output = kw.get("step_output")
        function = kw.get("function")
        kw_args = kw.get("kw_args")
        kw_args["env_config"] = kw.get("env_config")
        kw_args["cluster_name"] = kw.get("cluster_name")
        logger.info(f"Running function {function}")
        out = function(kw=kw_args)
        if out is None:
            step_output["result"] = None
            step_output["status"] = "pass"
            return None
        if check_and_raise_fabfile_command_failure(
            output=out, parallel=kw_args["env_config"].get("parallel"), fail=False
        ):
            step_output["status"] = "pass"
        else:
            step_output["status"] = "fail"
        return out
