"""
Module to deploy Prometheus service and individual daemon(s).

this module deploy Prometheus service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class Prometheus(ApplyMixin, Orch):
    SERVICE_NAME = "prometheus"
