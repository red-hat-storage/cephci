import mock
import pytest

from ceph.ceph_admin.add import AddMixin


class MockAddMixinTest(AddMixin):
    def __init__(self):
        self.cluster = None

    def shell(self, args):
        return 1, 0


class TestAddMixin:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self._add = MockAddMixinTest()

    @mock.patch("ceph.ceph_admin.add.get_node_by_id")
    @mock.patch("ceph.ceph_admin.add.config_dict_to_string")
    def test_add(self, config_mock, get_node_mock):
        config = {
            "command": "apply",
            "service": "osd",
            "args": {"all-available-devices": True},
            "verify": False,
            "base_cmd_args": {"verbose": True},
            "pos_args": ["node1", "dev/vdb", "dev"],
        }
        config_mock.return_value = [
            "ceph",
            "orch",
            "daemon",
            "add",
            "osd",
            "--verbose",
            "--all-available-devices",
        ]
        obj = mock.Mock()
        obj.shortname = "host"
        get_node_mock.return_value = obj
        self._add.add(config)
        assert config_mock.call_count == 2
        assert get_node_mock.call_count == 1

    @mock.patch("ceph.ceph_admin.add.get_node_by_id")
    @mock.patch("ceph.ceph_admin.add.config_dict_to_string")
    def test_add_witout_osd_service(self, config_mock, get_node_mock):
        config = {
            "command": "apply",
            "service": "rgw",
            "args": {"all-available-devices": True},
            "verify": False,
            "base_cmd_args": {"verbose": True},
            "pos_args": ["node1", "dev/vdb", "dev"],
        }
        config_mock.return_value = ["ceph", "orch", "daemon", "add", "osd", "--verbose"]
        obj = mock.Mock()
        obj.shortname = "host"
        get_node_mock.return_value = obj
        self._add.add(config)
        assert config_mock.call_count == 2
        assert get_node_mock.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__])
