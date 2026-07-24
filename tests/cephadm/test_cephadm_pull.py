from ceph.ceph_admin import CephAdmin
from utility.log import Log
from utility.utils import fetch_image_tag

log = Log(__name__)


class CephadmpullFailure(Exception):
    pass


def run(ceph_cluster, **kw):
    """
    Verify ceph version using cephadm pull option
    Args:
        ceph_cluster: Ceph cluster object
        **kw :     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful.
    """

    config = kw.get("config")
    instance = CephAdmin(cluster=ceph_cluster, **config)
    # Geting latest image using cephadm pull command
    cmd = "cephadm pull"
    instance.installer.exec_command(cmd, sudo=True)
    cmd = 'podman images --format "table {{.Tag}}" | grep ceph'
    result = instance.installer.exec_command(cmd, sudo=True)
    cephadm_pull_image = result[0].rstrip()
    log.info(f"Image pulled by command cephadm pull : '{cephadm_pull_image}'")
    # Geting latest ceph image from recipe file"
    rhbuild = config.get("rhbuild").split("-")
    recipe_file_image = fetch_image_tag(rhbuild[0])
    log.info(f"Latest Image available in recipe file : '{cephadm_pull_image}'")
    if cephadm_pull_image != recipe_file_image:
        raise CephadmpullFailure("cephadm pull verification unsuccessful")
    log.info("cephadm pull verification Successful")
    return 0
