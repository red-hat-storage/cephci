"""Wrappers for subcommands of rbd mirror image.

This module has wrapper for all subcommands of the command -
rbd mirror image
"""

from tkinter import _ImageSpec
from cephV2.rbd.mirror.mirror import Mirror
from utility.log import Log

log = Log(__name__)


class Image(Mirror):
    def __init__(self, nodes):
        self.nodes = nodes
        self.base_cmd = "rbd mirror image"
        super(Image, self).__init__(nodes=nodes)

    def demote(self, args):
        """Wrapper for rbd mirror image demote.
        
        Usage:
        - args:
            imagespec: poolname/[namespace]/imagename

        Returns:
            out: Output after execution of command
                 Out would contain the key.
            err: Error after execution of command
        """
        cmd = self.base_cmd + " demote " + args["imagespec"]
        return self.exec_cmd(cmd)

    def disable(self, args):
        """Wrapper for rbd mirror image disable.
        
        Usage:
        - args:
            imagespec: poolname/[namespace]/imagename

        Returns:
            out: Output after execution of command
                 Out would contain the key.
            err: Error after execution of command
        """
        cmd = self.base_cmd + " disable " + args["imagespec"]
        return self.exec_cmd(cmd)

    def enable(self, args):
        """Wrapper for rbd mirror image demote.
        
        Usage:
        - args:
            imagespec: poolname/[namespace]/imagename
            mode: journal or snapshot

        Returns:
            out: Output after execution of command
                 Out would contain the key.
            err: Error after execution of command
        """
        cmd = self.base_cmd + " enable " + args["imagespec"]
        return self.exec_cmd(cmd)

    def promote(self, args):
        pass

    def resync(self, args):
        pass

    def snapshot(self, args):
        pass

    def status(self, args):
        pass