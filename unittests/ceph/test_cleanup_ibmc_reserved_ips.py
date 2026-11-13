from unittest.mock import MagicMock

from ceph.utils import cleanup_ibmc_reserved_ips

PREFIX = "ci-ab12c"


def _resp(result=None, status=204):
    resp = MagicMock()
    resp.get_result.return_value = result
    resp.get_status_code.return_value = status
    return resp


def _client(reserved_ips, delete_status=204):
    client = MagicMock()
    client.list_subnets.return_value = _resp(
        {
            "subnets": [
                {"id": "s1", "vpc": {"name": "vpc-a"}},
                {"id": "s2", "vpc": {"name": "vpc-b"}},
            ]
        }
    )
    client.list_subnet_reserved_ips.return_value = _resp({"reserved_ips": reserved_ips})
    client.delete_subnet_reserved_ip.return_value = _resp(status=delete_status)
    return client


def _rip(rip_id, name, **kwargs):
    return {"id": rip_id, "name": name, "address": f"10.0.0.{rip_id}", **kwargs}


def test_deletes_only_matching_unattached_ips_in_vpc():
    client = _client(
        [
            # names derived by the pipeline: <cluster-name>-<instances-name>-...
            _rip("1", f"ceph-pri-{PREFIX}-ingress-vip"),
            _rip("2", f"ceph1-{PREFIX}-ingress-vip"),
            _rip("3", PREFIX),
            _rip("4", "ceph-pri-ci-zz99z-ingress-vip"),  # another run
            _rip("5", f"ceph-sec-{PREFIX}-vip", target={"id": "nic"}),  # attached
            _rip("6", "random-name"),
        ]
    )

    assert cleanup_ibmc_reserved_ips(client, "vpc-a", PREFIX) is True

    deleted = [c.kwargs["id"] for c in client.delete_subnet_reserved_ip.call_args_list]
    assert deleted == ["1", "2", "3"]
    # subnet of another VPC is never listed
    client.list_subnet_reserved_ips.assert_called_once_with(subnet_id="s1")


def test_returns_false_when_delete_fails():
    client = _client([_rip("1", f"{PREFIX}-ingress-vip")], delete_status=500)

    assert cleanup_ibmc_reserved_ips(client, "vpc-a", PREFIX) is False


def test_nothing_matched_is_success():
    client = _client([_rip("1", "random-name")])

    assert cleanup_ibmc_reserved_ips(client, "vpc-a", PREFIX) is True
    client.delete_subnet_reserved_ip.assert_not_called()


def test_empty_pattern_is_rejected():
    client = _client([_rip("1", "random-name")])

    assert cleanup_ibmc_reserved_ips(client, "vpc-a", "") is False
    client.list_subnets.assert_not_called()


def test_follows_pagination():
    client = _client([])
    client.list_subnet_reserved_ips.side_effect = [
        _resp(
            {
                "reserved_ips": [_rip("1", PREFIX)],
                "next": {"href": "https://x/reserved_ips?start=abc&limit=1"},
            }
        ),
        _resp({"reserved_ips": [_rip("2", f"{PREFIX}-vip")]}),
    ]

    assert cleanup_ibmc_reserved_ips(client, "vpc-a", PREFIX) is True

    assert client.list_subnet_reserved_ips.call_args_list[1].kwargs == {
        "subnet_id": "s1",
        "start": "abc",
    }
    assert client.delete_subnet_reserved_ip.call_count == 2
