import unittest

import mock
from mock import Mock

from ceph.ceph_admin.alert_manager import AlertManager


class AlertManagerTest(unittest.TestCase):
    def setUp(self):
        self._alertmanager = AlertManager(Mock())

    @mock.patch("ceph.ceph_admin.alert_manager.ApplyMixin.apply")
    def test_apply(self, mock_apply):
        config = {
            "command": "apply",
            "service": "osd",
            "args": {"all-available-devices": True},
            "verify": False,
            "base_cmd_args": {"verbose": True},
            "pos_args": ["node1", "dev/vdb", "dev"],
        }
        mock_apply.return_value = None
        self._alertmanager.apply(config)
        self.assertEqual(mock_apply.call_count, 1)


if __name__ == "__main__":
    unittest.main()
