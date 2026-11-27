from tests.nvmeof.test_ceph_nvmeof_tlspsk_basic import namespace_owner_args
from tests.nvmeof.test_ceph_nvmeof_tlspsk_ns_masking import namespace_devices_visible


class _Gateway:
    def __init__(self, ana_group_id):
        self.ana_group_id = ana_group_id


def test_namespace_is_pinned_to_the_connected_gateway():
    assert namespace_owner_args(_Gateway(1)) == {"load-balancing-group": 1}


def test_missing_ana_group_leaves_placement_to_the_gateway():
    assert namespace_owner_args(_Gateway(None)) == {}


def test_nvme_list_device_without_nqn_is_visible():
    payload = {
        "Devices": [
            {
                "NameSpace": 1,
                "DevicePath": "/dev/nvme0n1",
                "ModelNumber": "Ceph bdev Controller",
            }
        ]
    }
    assert namespace_devices_visible(payload) is True


def test_empty_nvme_list_is_not_visible():
    assert namespace_devices_visible('{"Devices":[]}') is False
