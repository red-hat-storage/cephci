"""Module to hold methods of rbd mirror pool peer bootstrap

This module has all subcommands of the command -
rbd mirror pool peer bootsrap
"""

from utility.log import Log

log = Log(__name__)


class Bootstrap:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " bootstrap"
        self.node = node

    def create(self, args):
        """Wrapper for pool peer bootstrap create.

        Create a peer bootstrap token to import in a remote cluster.

        Args:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same

        Returns:
            out: Output after execution of command
                 Out would contain the key.
            err: Error after execution of command
        """
        cmd = self.base_cmd + f' create {args["pool_name"]}'
        if args.get("site_name", None):
            cmd = cmd + f" --site-name {args['site_name']}"

        return self.node.exec_command(cmd=cmd)

    def key_import(self, args):
        """Wrapper for pool peer bootstrap import.

        Import a peer bootstrap token created from a remote cluster

        Args:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same
            file_path: The file name in which the key is stored
        """

        cmd = self.base_cmd + f' import {args["poolname"]} {args["filepath"]}'
        if args.get("direction", None):
            cmd = cmd + f" --direction {args['direction']}"

        if args.get("site_name", None):
            cmd = cmd + f" --site-name {args['site_name']}"

        cmd = cmd + f" --token-path {args['file_path']}"
        return self.node.exec_command(cmd=cmd)
