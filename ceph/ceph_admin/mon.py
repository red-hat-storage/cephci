"""
Module to deploy MON service and individual daemon(s).

this module deploy MON service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class Mon(ApplyMixin, Orch):

    SERVICE_NAME = "mon"
