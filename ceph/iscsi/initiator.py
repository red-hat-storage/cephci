class Iscsi_Initiator:

    def __init__(self, node):
        """
        iSCSI Initiator node
        - Node
        - IP
        - Platform
        - OS
        - Pre-requisites
            - iscsi-initiator-utils
            - multipath
            - config update
        - iscsiadm
            - discovery
            - node
            -
        - Devices management
            - listing of devices
        - FIO

        Args:
            node: CephNode object

        - Node
            - IP
            - Initiator CLIs
        """
        self.node = node
        self.cli = None
