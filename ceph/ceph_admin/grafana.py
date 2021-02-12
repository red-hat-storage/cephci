"""
Module to deploy Grafana service and individual daemon(s).

this module deploy Grafana service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class Grafana(ApplyMixin, Orch):
    SERVICE_NAME = "grafana"
