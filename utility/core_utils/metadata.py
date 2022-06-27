class Metadata:
    _instance = None
    cluster_name = None
    ceph = None
    method = None
    roles = None
    must_present_key = None
    must_method = None
    kwargs = None
    env_config = {
        "hosts": None,
        "username": None,
        "password": None,
        "parallel": False,
        "use_ssh_config": True,
        "use_shell": False,
        "timeout": 300,
        "sudo": False,
    }
    suite_output = {}

    def __new__(
        cls,
        cluster_name=None,
        ceph=None,
        method=None,
        roles=None,
        must_present_key=None,
        kwargs=None,
        parallel=None,
        use_ssh_config=None,
        use_shell=None,
        timeout=None,
        sudo=None,
        hosts=None,
        must_method=None,
        suite_output=None,
    ):
        if not Metadata._instance:
            Metadata._instance = cls
        if cluster_name:
            Metadata._instance.cluster_name = cluster_name
        if ceph:
            Metadata._instance.ceph = ceph
        if method:
            Metadata._instance.method = method
        if roles:
            Metadata._instance.roles = roles
        if kwargs:
            Metadata._instance.kwargs = kwargs
        if must_present_key:
            Metadata._instance.must_present_key = must_present_key
        if parallel:
            Metadata._instance.env_config["parallel"] = parallel
        if use_ssh_config:
            Metadata._instance.env_config["use_ssh_config"] = use_ssh_config
        if use_shell:
            Metadata._instance.env_config["use_shell"] = use_shell
        if timeout:
            Metadata._instance.env_config["timeout"] = timeout
        if sudo:
            Metadata._instance.env_config["sudo"] = sudo
        if hosts:
            Metadata._instance.env_config["hosts"] = hosts
        if must_method:
            Metadata._instance.must_method = must_method
        if suite_output:
            Metadata._instance.suite_output = suite_output
        return Metadata._instance
