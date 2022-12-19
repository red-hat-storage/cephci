from os.path import basename

from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError, ConfigNotFoundError
from cli.utilities.utils import put_cephadm_ansible_playbook
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs):
    """Module to execute cephadm-ansible wrapper playbooks

    Example:
        - test:
            name: Set 'osd' config using cephadm-ansible module 'ceph_config'
            desc: Execute 'playbooks/set-osd-config.yml' playbook
            polarion-id: CEPH-83575214
            module: test_cephadm_ansible_wrapper_set_osd_config.py
            config:
              ansible_wrapper:
                module: "ceph_config"
                playbook: playbooks/set-osd-config.yml

    """
    installer = ceph_cluster.get_ceph_object("installer")

    config = kwargs.get("config")
    aw = config.get("ansible_wrapper")
    if not aw:
        raise ConfigNotFoundError("Mandatory parameter 'ansible_wrapper' not found")

    playbook = aw.get("playbook")
    if not playbook:
        raise ConfigNotFoundError("Mandatory resource 'playbook' not found")

    put_cephadm_ansible_playbook(installer, playbook)
    Ansible(installer).run_playbook(playbook=basename(playbook))

    if not CephAdm(installer).ceph.status():
        raise CephadmOpsExecutionError("Failed to perform action via cephadm ansible")

    return 0
