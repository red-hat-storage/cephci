"""
Module to deploy MGR service and individual daemon(s).

this module deploy MGR service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class Mgr(ApplyMixin, Orch):

    SERVICE_NAME = "mgr"
