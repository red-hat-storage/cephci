import json

from cli import Cli
from utility.log import Log

log = Log(__name__)


class Migration(Cli):
    def __init__(self, nodes, base_cmd):
        super(Migration, self).__init__(nodes)
        self.base_cmd = base_cmd + " migration"

    def prepare(self, **kw):
        """
        Prepare the live migration of image from source to destination,
        Args:
        kw(dict): Key/value pairs that needs to be provided to the client node.
            Example::
            Supported keys:
                source_spec : source image spec SOURCE_POOL_NAME/SOURCE_IMAGE_NAME
                    or json formatted string for streamed imports
                dest_spec: Target image spec TARGET_POOL_NAME/TARGET_IMAGE_NAME
        """
        log.info("Starting prepare Live migration of image")
        source_spec = kw.get("source_spec", None)
        dest_spec = kw.get("dest_spec", None)
        src_spec = json.dumps(source_spec)
        if "{" not in src_spec:
            cmd = f"{self.base_cmd} prepare {source_spec} {dest_spec}"
        else:
            cmd = f"echo '{src_spec}' | {self.base_cmd} prepare --import-only --source-spec-path - {dest_spec}"
        return self.execute_as_sudo(cmd=cmd)

    def action(self, **kw):
        """
        Execute/commit the live migration as per the provided action,
        Args:
        kw(dict): Key/value pairs that needs to be provided to the client node.
            Example::
            Supported keys:
                action: execute or commit
                dest_spec: Target image spec in format of TARGET_POOL_NAME/TARGET_IMAGE_NAME
        """
        action = kw.get("action", None)
        dest_spec = kw.get("dest_spec", None)
        cluster_name = kw.get("cluster_name", "ceph")
        log.info(f"Starting the {action} migration process")
        cmd = f"{self.base_cmd} {action} {dest_spec} --cluster {cluster_name}"
        return self.execute_as_sudo(cmd=cmd, long_running=True)

    def prepare_import(self, **kw):
        """
        Prepare the live migration of image from one ceph cluster to another,
        Args:
        kw(dict): Key/value pairs that needs to be provided to the client node.
            Example::
            Supported keys:
                source_spec_path : json formatted string for streamed imports
                dest_spec: Target image spec TARGET_POOL_NAME/TARGET_IMAGE_NAME

        """
        log.info("Starting prepare Live migration of image to external ceph cluster")
        source_spec_path = kw.get("source_spec_path", None)
        dest_spec = kw.get("dest_spec", None)
        cluster_name = kw.get("cluster_name", None)
        cmd = (
            f"{self.base_cmd} prepare --import-only --source-spec-path {source_spec_path} "
            f"{dest_spec} --cluster {cluster_name}"
        )
        return self.execute_as_sudo(cmd=cmd, long_running=True)
