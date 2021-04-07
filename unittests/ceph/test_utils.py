import pytest

from ceph.utils import get_node_by_id


class FakeCephNode(object):
    # Fake ceph.ceph.CephNode object
    def __init__(self, shortname):
        self.shortname = shortname


class FakeCeph(object):
    # Fake ceph.ceph.Ceph object
    def __init__(self):
        self.nodes = []

    def get_nodes(self):
        return self.nodes


class TestGetNodeByID(object):
    @pytest.fixture
    def cluster(self):
        return FakeCeph()

    def test_simple(self, cluster):
        osd1 = FakeCephNode("osd1")
        osd2 = FakeCephNode("osd2")
        cluster.nodes = [osd1, osd2]
        assert get_node_by_id(cluster, "osd1") == osd1
        assert get_node_by_id(cluster, "osd2") == osd2

    def test_no_match(self, cluster):
        osd1 = FakeCephNode("osd1")
        osd2 = FakeCephNode("osd2")
        cluster.nodes = [osd1, osd2]
        assert get_node_by_id(cluster, "bogushost") is None

    @pytest.mark.xfail
    def test_substring_does_not_match(self, cluster):
        osd1 = FakeCephNode("osd1")
        osd2 = FakeCephNode("osd2")
        cluster.nodes = [osd1, osd2]
        assert get_node_by_id(cluster, "osd") is None
