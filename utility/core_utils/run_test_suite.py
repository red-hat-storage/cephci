import asyncio
import builtins
from cgi import test
import datetime
from distutils.command.config import config
import importlib
import logging
import os
import sys
from keyword import iskeyword
from ceph.utils import cleanup_ceph_nodes, cleanup_ibmc_ceph_nodes
from run import create_nodes

from utility.core_utils.cli import CLI
from utility.core_utils.runner import Runner
from utility.core_utils.execute_command import ExecuteCommandMixin
from utility.core_utils.loader import LoaderMixin
from src.utilities.metadata import Metadata
from utility.core_utils.parallel_executor import ParallelExecutor
from utility.utils import configure_logger, create_unique_test_name

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

execute_command_mixin = ExecuteCommandMixin()
METHOD_MAP = dict(
    {
        "must_pass": execute_command_mixin.must_pass,
        "must_fail": execute_command_mixin.must_fail,
    }
)


class RunTestSuite:
    """
    This module provides CLI interface to run the test suite.
    """

    def __init__(self, config, ceph_cluster_dict, rp_logger, test_names, run_dir) -> None:
        sys.path.append(os.path.abspath("utility/core_utils"))
        self.ceph_cluster_dict = ceph_cluster_dict
        self.rp_logger = rp_logger
        self.test_names = test_names
        self.run_dir = run_dir
        self.config = config
        # self.ceph = ceph
        # self.ceph_node = ceph.ceph_node
        # self.ceph_role = ceph.ceph_role
        self.cli_obj = CLI()
        self.suite_config = LoaderMixin().load_file(config, kind="suites")
        self.parallel_executor = ParallelExecutor()
        self.output = {}
        self.redefine_suite(self.suite_config)
        self.check_cluster_name(self.suite_config)
        # Metadata(suite_output=self.output)
        self.fail = False

    def check_cluster_name(self, suite):
        tests = suite.get("tests")
        for cluster_tests in tests:
            if not cluster_tests.get("runs_on"):
                raise Exception()

    def flatten_config(self, config):
        for i in range(len(config)):
            test = config[i].get("test")
            if test.get("suite"):
                test_popped = config.pop(i).get("test")
                suite_file = test_popped.get("suite")
                suite = LoaderMixin().load_file(suite_file, kind="suites")
                suite_tests = self.redefine_suite(suite)
                suite_tests.reverse()
                for suite_test in suite_tests:
                    config.insert(i, suite_test)
        return config

    def redefine_suite(self, suite):
        tests = suite.get("tests")
        config = []
        for cluster_tests in tests:
            config = cluster_tests.get("config")
            config = self.flatten_config(config)
            cluster_tests["config"] = config
        return config

    async def redefine_args(self, args, cluster_name):
        """
        This method is used to load the data of other test into args.
        Args:
          args(dict): arguments of the method.

        Returns:
          args(dict): arguments after loading output of tests data.
        """
        if isinstance(args, dict):
            if args.get("load_input_result"):
                val = args.get("load_input_result")
                ids = val.split(".")
                replace_with = self.output
                if len(ids) == 2:
                    replace_with = replace_with.get(cluster_name)
                while True:
                    replace_with_temp = replace_with
                    i = 0
                    while replace_with_temp is not None and i < len(ids):
                        replace_with_temp = replace_with_temp.get(ids[i])
                        i = i + 1
                    if (
                        i >= len(ids)
                        and replace_with_temp is not None
                        and replace_with_temp.get("status") is not None
                    ):
                        return replace_with_temp.get("result")
                    await asyncio.sleep(5)
            for key, val in args.items():
                args[key] = await self.redefine_args(val, cluster_name)
            return args
        elif isinstance(args, list):
            for i in range(len(args)):
                args[i] = await self.redefine_args(args[i], cluster_name)
            return args
        else:
            return args

    def redefine_service(self, method):
        """
        This method is used to rename the inbuilt method by concatenating underscore.
        Args:
          method(str): Takes inbuilt method name as input.

        Returns:
          method(str): Modified method name.

        Example: changes the inbuilt keyword 'import' to 'import_'.
        """
        if "-" in str(method):
            method = str(method).replace("-", "_")
        if self.check_for_builtins(method):
            method = method + "_"
        return method

    def check_for_builtins(self, service):
        """
        This method is used to check whether the service is a inbuilt function.
        If it is an inbuilt function it is modified.
        Args:
          service(str): name of the method

        Returns:
          None
        """
        if iskeyword(service) or service in dir(builtins):
            return True
        else:
            for key in builtins.__dict__.keys():
                if service in dir(builtins.__dict__[key]):
                    return True
            return False

    async def run_step(self, test, must_present_key, cluster_name, test_output):
        """
        This method is used to run series of steps as a part of cluster creation or deletion.
        Args:
          test(dict): Consists of series of steps that need to performed as apart of one test.
          must_present_key(str): a key [must_pass | must_fail ]
          cluster_name(str) : Name of cluster
        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        test = test.get("test")
        logger.info(f"Running test {test.get('name')} in {cluster_name}")
        global_mod_file_name = test.get("module")
        steps = test.get("steps")
        for i in range(len(steps)):
            if self.fail:
                break
            step = steps[i]
            test_output[f"step{i}"] = {"status": None, "result": None}
            roles = step.get("role")
            component = step.get("component")
            cls = step.get("class")
            method = step.get("method")
            method = self.redefine_service(method)
            args = step.get("args", {})
            args = await self.redefine_args(args, cluster_name)
            mod_file_name = step.get("module") or global_mod_file_name
            mod_file_name = os.path.splitext(mod_file_name)[0]
            mod_file = importlib.import_module(mod_file_name, package=__package__)
            module_method = mod_file.get_module(
                obj=self.cli_obj, component=component, module=cls, service=method
            )
            must_method = METHOD_MAP.get(must_present_key)
            runner = Runner(
                cluster_name=cluster_name,
                roles=roles,
                method=module_method,
                must_method=must_method,
                kwargs=args,
                ceph=self.ceph,
                parallel=True,
                step_output=test_output[f"step{i}"],
            )
            await asyncio.sleep(3)
            if step.get("retry"):
                utils_file_name = os.path.splitext("utils.py")[0]
                utils = importlib.import_module(utils_file_name, package=__package__)
                kw = {"runner": runner, "args": step.get("retry").get("args")}
                utils.retry(kw)
            else:
                runner.run()
            if test_output[f"step{i}"].get("status") == "fail":
                self.fail = True
                break
        logger.info(f"Completed running test {test.get('name')} of {cluster_name}")
        return 0

    def get_must_key(self, test_config):
        """
        This method is used to check whether test has to pass or fail or raise an exception mandatorily.
        Args:
          test_config(Dict): dictionary consists of test configuration in key-value pair.
        Returns:
          must_present_key(key): a key [must_pass | must_fail ]
        """
        for must_present_key in METHOD_MAP.keys():
            if test_config.get(must_present_key):
                return must_present_key

    def run_tests(self):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None

        Returns:
          None
        """
        self.parallel_executor.run_until_complete(self.run_tests_async)
        return self.output

    async def run_tests_async(self):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None

        Returns:
          None
        """
        tasks = []
        logger.info("Running test suite")
        logger.info(f"Running test {self.config}")
        start = datetime.datetime.now()
        clusters_parallel_execution = self.suite_config.get("parallel", False)
        for cluster_test_conf in self.suite_config.get("tests"):
            if self.fail:
                break
            if clusters_parallel_execution:
                tasks.append((self.run_tests_on_cluster, cluster_test_conf))
            else:
                await self.run_tests_on_cluster(cluster_test_conf)
        if clusters_parallel_execution:
            await self.parallel_executor.run(tasks)
        elapsed = datetime.datetime.now() - start

    async def run_tests_on_cluster(self, cluster_test_conf):
        """
        This method is used to run list of tests in cluster present in a yaml file.
        Args:
          cluster_test_conf(Dict): dictionary consists of test configuration in key-value pair.

        Returns:
          None
        """
        cluster_name = cluster_test_conf.get("runs_on")
        cluster_output = self.output[cluster_name] = dict()
        logger.info(f"Running tests on {cluster_name}")

        config = cluster_test_conf.get("config")
        tests_parallel_execution = cluster_test_conf.get("parallel")
        self.must_present_key = self.get_must_key(self.suite_config)
        tasks = []
        for i in range(len(config)):
            if self.fail:
                if self.rp_logger:
                    self.rp_logger.finish_test_item(status="FAILED")
                break
            test_name = config[i].get("test").get("name").replace(".", "")
            unique_test_name = create_unique_test_name(f"running test {test_name}", self.test_names)
            config[i].test["log-link"] = configure_logger(unique_test_name, self.run_dir)
            if config[i].get("test").get("log-link"):
                print("Test logfile location: {log_url}".format(log_url=config[i].get("test").get("log-link")["log-link"]))
            if self.rp_logger:
                self.test_names.append(unique_test_name)
                self.rp_logger.start_test_item(name=unique_test_name, description=test_name, item_type="STEP")
            test_output = cluster_output[test_name] = dict()
            if tests_parallel_execution:
                tasks.append(
                    (
                        self.run_step,
                        config[i],
                        self.must_present_key,
                        cluster_name,
                        test_output,
                    )
                )
            else:
                await self.run_step(
                    config[i], self.must_present_key, cluster_name, test_output
                )
        if tests_parallel_execution:
            await self.parallel_executor.run(tasks)
        self.rp_logger.finish_test_item(status="PASSED")
