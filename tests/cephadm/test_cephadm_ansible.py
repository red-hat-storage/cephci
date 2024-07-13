"""Test module which enables ceph-ansible.

Playbook supported
- cephadm-preflight.yaml
- cephadm-purge-cluster.yaml
- cephadm-clients.yaml
"""

from ceph.ceph_admin.cephadm_ansible import CephadmAnsible
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs) -> int:
    """Module to execute cephadm-ansible playbboks

    Return the status of the test execution run with the provided keyword arguments.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: cephadm-ansible preflight playbook
            desc: execute cephadm-preflight playbook
            config:
                playbook: cephadm-preflight.yaml
                extra-vars:
                    ceph_origin: rhcs
                extra-args:
                    limit: osds
    """
    config = kwargs.get("config")
    playbook = config["playbook"]

    cephadm_ansible = CephadmAnsible(cluster=ceph_cluster)
    cephadm_ansible.execute_playbook(
        playbook=playbook,
        extra_vars=config.get("extra-vars"),
        extra_args=config.get("extra-args"),
    )
    return 0
