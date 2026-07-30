import conditional_logging.conditional_logging_utils as clu
import pytest


def test_build_conditional_log_block_any_policy():
    block = clu.build_conditional_log_block(
        match_policy="ANY",
        global_level="EVENT",
        conditional_components={"FSAL": "FULL_DEBUG", "NFS_V4": "MID_DEBUG"},
        exports=[1, 2, 77],
        clients=["10.0.0.5", "192.168.1.0/24", "*"],
    )
    assert "Match_Policy = ANY;" in block
    assert "FSAL = FULL_DEBUG;" in block
    assert "NFS_V4 = MID_DEBUG;" in block
    assert "Exports = 1, 2, 77;" in block
    assert "Clients = 10.0.0.5, 192.168.1.0/24, *;" in block
    assert "FSAL = INFO;" in block
    assert "NFS_V4 = INFO;" in block
    assert block.strip().startswith("LOG {")


def test_build_baseline_log_block_has_no_conditional():
    block = clu.build_baseline_log_block(
        global_level="EVENT",
        components={"FSAL": "INFO", "NFS_V4": "INFO"},
    )
    assert "Default_Log_Level = EVENT;" in block
    assert "FSAL = INFO;" in block
    assert "Conditional" not in block
    assert "Match_Policy" not in block


def test_verify_baseline_no_conditional_debug():
    ok, _ = clu.verify_baseline_no_conditional_debug(
        "28/07/2026 10:00:00 : epoch :NFS_V4 :EVENT :op\n"
    )
    assert ok
    ok, detail = clu.verify_baseline_no_conditional_debug(
        "28/07/2026 10:00:00 : epoch :FSAL :F_DBG :ceph_get_posix_acl\n"
    )
    assert not ok
    assert "fsal_f_dbg" in detail


def test_verify_matching_client_conditional_debug():
    matched = (
        "28/07/2026 10:00:00 : epoch :FSAL :F_DBG :ceph_get_posix_acl\n"
        "28/07/2026 10:00:01 : epoch :EXPORT :M_DBG :op\n"
    )
    ok, detail = clu.verify_matching_client_conditional_debug(matched)
    assert ok, detail
    ok, _ = clu.verify_matching_client_conditional_debug(
        "28/07/2026 10:00:00 : epoch :FSAL :F_DBG :only\n"
    )
    assert not ok


def test_verify_match_policy_in_log():
    ok, detail = clu.verify_match_policy_in_log(
        "Conditional logging match policy changed to (MATCH_ALL)\n",
        expected_policy="MATCH_ALL",
    )
    assert ok, detail
    ok, _ = clu.verify_match_policy_in_log(
        "unrelated log line only\n", expected_policy="MATCH_ALL"
    )
    assert not ok


def test_build_conditional_log_block_match_all_normalized():
    block = clu.build_conditional_log_block(
        match_policy="MATCH_ALL", exports=[5], clients=["*"]
    )
    assert "Match_Policy = ALL;" in block


def test_replace_log_block_appends_when_missing():
    template = "%NFS_CORE_PART%\n"
    new_block = "LOG { Default_Log_Level = EVENT; }\n"
    merged = clu.replace_log_block_in_template(template, new_block)
    assert "LOG {" in merged
    assert "%NFS_CORE_PART%" in merged


def test_replace_log_block_replaces_existing():
    template = "LOG { Default_Log_Level = INFO; }\n%NFS_CORE_PART%\n"
    new_block = "LOG { Default_Log_Level = EVENT; Match_Policy = ANY; }\n"
    merged = clu.replace_log_block_in_template(template, new_block)
    assert merged.count("LOG {") == 1
    assert "Match_Policy = ANY" in merged


def test_parse_ganesha_mgr_show_output():
    sample = """
Conditional Logging Configuration
==================================
Clients (2):
10.0.0.1
192.168.1.0/24
Exports (1):
Export ID: 77
Match Policy : MATCH_ANY
Component Log Levels:
ALL : EVENT
FSAL : FULL_DEBUG
NFS_V4 : MID_DEBUG
"""
    parsed = clu.parse_ganesha_mgr_show_output(sample)
    assert "10.0.0.1" in parsed["clients"]
    assert 77 in parsed["exports"]
    assert parsed["match_policy"] == "MATCH_ANY"
    assert parsed["components"]["FSAL"] == "FULL_DEBUG"


def test_verify_conditional_verbosity_matched_higher():
    matched = "FSAL :FULL_DEBUG :sample\nNFS_V4 :MID_DEBUG :op\n"
    unmatched = "FSAL :EVENT :sample\nNFS_V4 :EVENT :op\n"
    ok, detail = clu.verify_conditional_verbosity(matched, unmatched)
    assert ok, detail


def test_verify_conditional_verbosity_fails_when_not_higher():
    matched = "FSAL :EVENT :sample\n"
    unmatched = "FSAL :FULL_DEBUG :sample\nNFS_V4 :FULL_DEBUG\n"
    ok, _ = clu.verify_conditional_verbosity(matched, unmatched)
    assert not ok


def test_verify_no_elevated_debug():
    ok, _ = clu.verify_no_elevated_debug("FSAL :EVENT :only\n")
    assert ok
    ok, _ = clu.verify_no_elevated_debug("FSAL :FULL_DEBUG :bad\n")
    assert not ok


def test_malformed_log_block_cases_graceful_set():
    cases = clu.malformed_log_block_cases(export_id=7, client="10.0.66.98")
    assert len(cases) == 5
    by_name = {c["name"]: c for c in cases}
    assert "case1_invalid_match_policy" in by_name
    assert "INVALID_POLICY" in by_name["case1_invalid_match_policy"]["log_block"]
    assert by_name["case1_invalid_match_policy"]["require_warn"] is True
    assert "INVALID_COMPONENT" in by_name["case2_unknown_component"]["log_block"]
    assert "SUPER_DEBUG" in by_name["case3_invalid_log_level"]["log_block"]
    assert ",," in by_name["case4b_empty_trailing_commas"]["log_block"]
    assert "Conditional" not in by_name["case6_missing_conditional_block"]["log_block"]
    assert by_name["case6_missing_conditional_block"]["verify_no_elevate"] is True
    assert "10.0.66.98" in by_name["case1_invalid_match_policy"]["log_block"]
    assert "Exports = 7" in by_name["case1_invalid_match_policy"]["log_block"]


def test_log_contains_fatal_and_any():
    assert clu.log_contains_fatal("something FATAL happened")
    assert not clu.log_contains_fatal("WARN: Unknown token")
    assert clu.log_contains_any("Unknown token (INVALID_POLICY)", ["INVALID_POLICY"])
    assert not clu.log_contains_any("ok", ["INVALID_POLICY"])


def test_export_id_from_entry_variants():
    assert clu.export_id_from_entry({"export_id": 42}) == 42
    assert clu.export_id_from_entry({"id": 7}) == 7
    assert clu.export_id_from_entry({"Export_Id": 3}) == 3
    assert clu.export_id_from_entry({}) is None


@pytest.mark.parametrize(
    "policy_in,policy_out",
    [
        ("any", "ANY"),
        ("MATCH_ANY", "ANY"),
        ("all", "ALL"),
        ("MATCH_ALL", "ALL"),
    ],
)
def test_match_policy_normalization(policy_in, policy_out):
    block = clu.build_conditional_log_block(
        match_policy=policy_in, exports=[1], clients=["*"]
    )
    assert f"Match_Policy = {policy_out};" in block
