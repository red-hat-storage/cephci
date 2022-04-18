"""Module to hold methods of rbd mirror pool peer bootstrap

This module has all subcommands of the command -
rbd mirror pool peer bootsrap
"""

from cephV2.rbd.mirror.peer import Peer
from utility.log import Log

log = Log(__name__)


class Bootstrap(Peer):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Bootstrap, self).__init__(nodes=nodes)

    def create(self, args):
        """Wrapper for pool peer bootstrap create.

        Create a peer bootstrap token to import in a remote cluster.

        Args:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same
            file_path: Provide the file name if key needs to be stored in
                       a file instead of returning.

        Returns:
            out: Output after execution of command
                 Out would contain the key.
            err: Error after execution of command
        """
        cmd = f'rbd mirror pool peer bootstrap create {args["pool_name"]}'
        if args.get("site_name", None):
            cmd = cmd + f" --site-name {args['site_name']}"
        if args.get("file_path", None):
            cmd = cmd + f" > {args['file_path']}"
        return self.exec_cmd(cmd)

    def key_import(self, args):
        """Wrapper for pool peer bootstrap import.

        Import a peer bootstrap token created from a remote cluster

        Args:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same
            file_path: The file name in which the key is stored
        """

        cmd = f'rbd mirror pool peer bootstrap import {args["poolname"]} {args["filepath"]}'
        if args.get("direction", None):
            cmd = cmd + f" --direction {args['direction']}"

        if args.get("site_name", None):
            cmd = cmd + f" --site-name {args['site_name']}"

        cmd = cmd + f" --token-path {args['file_path']}"
        self.exec_cmd(cmd)
