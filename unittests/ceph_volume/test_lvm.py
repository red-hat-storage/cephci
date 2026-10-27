"""Check ceph-volume wrapper arguments against the supported CLI interface."""

from unittest.mock import Mock

import pytest

from cli.ceph.ceph_volume.lvm import Lvm


@pytest.mark.parametrize("bluestore", [True, False])
def test_deactivate_does_not_pass_unsupported_objectstore_flag(bluestore):
    node = Mock()
    node.exec_command.return_value = ("deactivated", "")
    lvm = Lvm(node, "cephadm shell ceph-volume")

    assert (
        lvm.deactivate("0", "osd-fsid", bluestore=bluestore, check_ec=True)
        == "deactivated\n"
    )

    node.exec_command.assert_called_once_with(
        cmd="cephadm shell ceph-volume lvm deactivate 0 osd-fsid",
        sudo=True,
        long_running=False,
        check_ec=True,
        timeout=3600,
    )
