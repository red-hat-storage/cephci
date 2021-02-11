"""
Module to deploy Alert-Manager service and individual daemon(s).

this module deploy Alert-Manager service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class AlertManager(ApplyMixin, Orch):
    SERVICE_NAME = "alertmanager"
