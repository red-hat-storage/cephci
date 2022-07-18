import yaml

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Add rbd_mirror daemon to provided cluster.

    Adds rbdmirror configs to rbdmirrors.yaml and initites playbook
    to add rbd mirror daemons.
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """

    ceph_installer = ceph_cluster.get_ceph_object("installer")
    config = kw.get("config")

    write_configs_to_rbdmirrors_yaml(ceph_installer, config)

    file_name = "site.yml"

    if ceph_cluster.containerized:
        file_name = "site-container.yml"

    cmd = (
        "cd /usr/share/ceph-ansible; ANSIBLE_STDOUT_CALLBACK=debug;"
        f"ansible-playbook -vvvv -i hosts {file_name} --limit rbdmirrors"
    )
    rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    return rc


def write_configs_to_rbdmirrors_yaml(ceph_installer, config):
    """Write contents to rbdmirrors.yaml file."""

    rbdmirrors_yaml = yaml.dump(config.get("ansi_config"), default_flow_style=False)

    log.info("global vars {}".format(rbdmirrors_yaml))
    gvars_file = ceph_installer.remote_file(
        sudo=True,
        file_name="/usr/share/ceph-ansible/group_vars/rbdmirrors.yml",
        file_mode="w",
    )
    gvars_file.write(rbdmirrors_yaml)
    gvars_file.flush()
