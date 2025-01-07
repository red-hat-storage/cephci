from ceph.ceph_admin.common import config_dict_to_string


class ISCSIConfigs:
    """iSCSI Configuration attributes."""

    INITIATOR_IQN = "InitiatorName={IQN}"
    ISCSI_IQN_PATH = "/etc/iscsi/initiatorname.iscsi"
    MPATH_CONFIG_FILE_PATH = "/etc/multipath.conf"
    ISCSID_CONFIG_FILE_PATH = "/etc/iscsi/iscsid.conf"
    ISCSI_PACKAGES = "iscsi-initiator-utils device-mapper-multipath"
    MPATH_CONFIG = """
devices {
    device {
            vendor                 "LIO-ORG"
            product                "TCMU device"
            hardware_handler       "1 alua"
            path_grouping_policy   "failover"
            path_selector          "queue-length 0"
            failback               60
            path_checker           tur
            prio                   alua
            prio_args              exclusive_pref_bit
            fast_io_fail_tmo       25
            no_path_retry          queue
    }
}
"""


class ISCSIInitiator(ISCSIConfigs):

    def __init__(self, node, iqn):
        """iSCSI Initiator module.

        Args:
            node: Ceph Node object
            iqn: Initiator's iSCSI Qualified Name
        """
        self.node = node
        self._iqn = iqn
        self.iscsiadm = self.ISCSIadmin(self)
        self.multipath = self.Multipath(self)
        self.configure()

    @property
    def iqn(self):
        return self._iqn

    @iqn.setter
    def iqn(self, value):
        self._iqn = value

    def configure(self, multipath=True):
        """Configure iSCSI initiator."""
        # Install iscsi utils
        configure_cmds = [f"dnf install -y {self.ISCSI_PACKAGES}"]
        for cmd in configure_cmds:
            self.node.exec_command(cmd=cmd, sudo=True)

        f = self.node.remote_file(
            sudo=True, file_name=self.ISCSI_IQN_PATH, file_mode="w"
        )
        f.write(self.INITIATOR_IQN.format(IQN=self.iqn))
        f.flush()
        f.close()

        # Update multipath
        if multipath:
            self.node.exec_command(
                cmd="mpathconf --enable --with_multipathd y", sudo=True
            )
            f = self.node.remote_file(
                sudo=True, file_name=self.MPATH_CONFIG_FILE_PATH, file_mode="w"
            )
            f.write(self.MPATH_CONFIG)
            f.flush()
            f.close()
            self.node.exec_command(cmd="systemctl reload multipathd", sudo=True)

    class ISCSIadmin:
        """iscsiadm CLI module."""

        def __init__(self, parent):
            self.parent = parent

        def exec_iscsiadm_cli(self, **kwargs):
            cmd = "iscsiadm"
            if kwargs.get("mode"):
                cmd += f" --mode {kwargs['mode']} "
            cmd += config_dict_to_string(kwargs.get("cmd_args"))
            return self.parent.node.exec_command(cmd=cmd, sudo=True)

        def discover(self, **kwargs):
            """Discover targets."""
            args = {"mode": "discovery"}
            args["cmd_args"] = kwargs
            return self.exec_iscsiadm_cli(**args)

        def node(self, **kwargs):
            args = {"mode": "node"}
            args["cmd_args"] = kwargs
            return self.exec_iscsiadm_cli(**args)

        def version(self):
            args = {"cmd_args": {"version": True}}
            return self.exec_iscsiadm_cli(**args)

    class Multipath:
        """Multipath CLI module."""

        def __init__(self, parent):
            self.parent = parent

        def exec_multipath_cli(self, option, **kwargs):
            cmd = "multipath "
            cmd += config_dict_to_string(kwargs)
            return self.parent.node.exec_command(cmd=cmd, sudo=True)

        def list_by_level(self, level=2):
            _args = {"v": level}
            return self.exec_multipath_cli("-ll", **_args)
