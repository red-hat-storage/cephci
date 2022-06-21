import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("startup.log", mode="a")
file_handler.setLevel(logging.ERROR)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class Runner:
    """Purpose of this module is to provide environment information such as clustername, hosts, nodename, roles etc
    to run ceph cli commands.
    """

    def __init__(
        self,
        cluster_name,
        roles,
        method,
        must_method,
        kwargs,
        ceph,
        parallel,
        step_output,
    ):
        self.step_output = step_output
        self.ceph = ceph
        self.ceph_node = self.ceph.ceph_node
        self.ceph_role = self.ceph.ceph_role
        self.method = method
        self.kwargs = kwargs
        self.roles = roles
        self.parallel = parallel
        if not self.roles:
            self.roles = ["client"]
        self.must_method = must_method
        self.cluster_name = cluster_name
        self.hosts = self.ceph_role.get_ips_by_roles_list(self.roles, self.cluster_name)
        # installer = ceph_cluster_dict[cluster].get_nodes(role="installer")[0]
        self.env_config = {}
        self.cluster_name = cluster_name
        self.set_environment_for_run()

    def set_environment_for_run(self):
        """
        Sets environment variables to run a command through fabfile.
        Args:
            None

        Returns:
            None
        """
        self.env_config = {
            "hosts": self.hosts,
            "username": "root",
            "password": "passwd",
            "parallel": self.parallel,
        }

    def run(self):
        """
        Runs a command in must methods[must_pass | must_fail | must_raise].
        Args:
            None

        Returns:
            Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        out = self.must_method(
            function=self.method,
            kw_args=self.kwargs,
            env_config=self.env_config,
            cluster_name=self.cluster_name,
            step_output=self.step_output,
        )
        return out
