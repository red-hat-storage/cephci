from ceph.cephV2.osd import Osd
from utility.log import Log

log = Log(__name__)


class Pool(Osd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Pool, self).__init__(nodes=nodes)

    def create(self, args):
        """CLI wrapper for 'ceph osd pol create command

        Args:
            args: disctionary object with key pool_name and the
                  name of the pool to be created as its value.

        Returns:
            out: output after execution of command
            err: error after execution of command
        """
        cmd = f'ceph osd pool create {args["pool_name"]}'
        return self.exec_cmd(cmd)
