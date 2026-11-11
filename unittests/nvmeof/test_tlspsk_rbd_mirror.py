"""Sync is replay caught up on the secondary, and the namespace is that image."""

from tests.nvmeof.test_ceph_nvmeof_tlspsk_rbd_mirror import (
    demoted_mirror_state,
    entries_behind_primary,
    namespace_add_args,
    secondary_caught_up,
)

REPLAYING = {
    "state": "up+replaying",
    "description": (
        'replaying, {"bytes_per_second":0.0,"entries_behind_primary":0,'
        '"entries_behind_per_second":0.0}'
    ),
}


def test_entries_behind_primary_reads_the_journal_count():
    assert entries_behind_primary(REPLAYING) == 0
    behind = {
        "description": 'replaying, {"entries_behind_primary":4}',
    }
    assert entries_behind_primary(behind) == 4
    assert entries_behind_primary({"description": "local image is primary"}) is None


def test_up_stopped_is_not_caught_up():
    assert secondary_caught_up({"state": "up+stopped", "description": ""}) is False
    assert secondary_caught_up({}) is False
    assert secondary_caught_up(REPLAYING) is True
    still_behind = {
        "state": "up+replaying",
        "description": '{"entries_behind_primary":3}',
    }
    assert secondary_caught_up(still_behind) is False


def test_namespace_exports_the_mirrored_image():
    args = namespace_add_args("nqn.2016-06.io.spdk:tlspsk-mirror", "rbd", "nvmeof-tlspsk-mirror")
    assert args["rbd-image"] == "nvmeof-tlspsk-mirror"
    assert args["rbd-pool"] == "rbd"
    assert "rbd-create-image" not in args


def test_demoted_state_follows_the_release():
    assert demoted_mirror_state("9.2-rhel-10") == "up+unknown"
    assert demoted_mirror_state("2.5") == "up+stopped"
