from ceph.ceph_admin.common import config_dict_to_string


def test_wildcard_host_nqn_is_quoted_for_the_remote_shell():
    rendered = config_dict_to_string(
        {"host_nqn": "*", "gw_group": "group-c", "server_address": "10.0.66.8"}
    )
    assert rendered == (
        " --host_nqn '*' --gw_group group-c --server_address 10.0.66.8"
    )


def test_plain_values_stay_unquoted():
    rendered = config_dict_to_string(
        {"nqn": "nqn.2016-06.io.spdk:tlspsk-kc", "listener_port": 4420}
    )
    assert rendered == " --nqn nqn.2016-06.io.spdk:tlspsk-kc --listener_port 4420"


def test_repr_wildcard_is_not_quoted_again():
    """repr('*') is already the shell token '*' and must stay that way."""
    rendered = config_dict_to_string({"host_nqn": repr("*")})
    assert rendered == " --host_nqn '*'"


def test_value_with_spaces_is_quoted_once():
    rendered = config_dict_to_string({"name": "host one"})
    assert rendered == " --name 'host one'"
