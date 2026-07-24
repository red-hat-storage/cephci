from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Namespace(Cli):
    def __init__(self, nodes, base_cmd):
        super(Namespace, self).__init__(nodes)
        self.base_cmd = base_cmd + " namespace"

    def create(self, **kw):
        """
        Creates a namespace in a given pool, if pool is not given then create in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool into which namespace should be stored,default pool is rbd
                namespace(str): namespace to created in the pool
                See rbd help create for more supported keys
        """
        pool_name = kw.get("pool-name", None)
        namespace_name = kw.get("namespace", None)
        ns_create_kw = {}
        ns_create_kw["namespace"] = namespace_name
        if pool_name is not None:
            ns_create_kw["pool"] = pool_name
        cmd = f"{self.base_cmd} create {build_cmd_from_args(**ns_create_kw)}"

        return self.execute_as_sudo(cmd=cmd)

    def list(self, **kw):
        """
        lists all namespace in a given pool, if pool is not given then list in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool into which namespace should be stored
                format(str): json format output of listing namespace
                See rbd help create for more supported keys
        """
        pool_name = kw.get("pool-name", None)
        format = kw.get("format", None)
        namespace_ls_kw = {}
        if pool_name is not None:
            namespace_ls_kw["pool"] = pool_name
        if format is not None:
            namespace_ls_kw["format"] = format
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**namespace_ls_kw)}"

        return self.execute_as_sudo(cmd=cmd)

    def remove(self, **kw):
        """
        Removes a namespace in a given pool, if pool is not given then remove in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool into which namespace should be deleted
                namespace(str): namespace to delete in the pool
                See rbd help create for more supported keys
        """
        pool_name = kw.get("pool-name", None)
        namespace_name = kw.get("namespace", None)
        ns_remove_kw = {}
        ns_remove_kw["namespace"] = namespace_name
        if pool_name is not None:
            ns_remove_kw["pool"] = pool_name
        cmd = f"{self.base_cmd} rm {build_cmd_from_args(**ns_remove_kw)}"

        return self.execute_as_sudo(cmd=cmd)
