"""switches non-containerized ceph daemon to containerized ceph daemon"""
import yaml

from utility.log import Log

log = Log(__name__)


def ceph_repository_type_cdn(ansible_dir, installer_node):
    """
    Fetches container image information from all.yml.sample

    Args:
         ansible_dir         ansible directory on installer node
         installer_node      installer node to fetch all.yml.sample

    Returns:
        docker_registry, docker_image, docker_image_tag
    """
    out, err = installer_node.exec_command(
        sudo=True,
        cmd="cat {ansible_dir}/group_vars/all.yml.sample".format(
            ansible_dir=ansible_dir
        ),
    )
    sample_conf = yaml.safe_load(out)
    docker_registry = sample_conf.get("ceph_docker_registry")
    docker_image = sample_conf.get("ceph_docker_image")
    docker_image_tag = sample_conf.get("ceph_docker_image_tag")
    return docker_registry, docker_image, docker_image_tag


def run(**kw):

    log.info("Running exec test")
    ceph_nodes = kw.get("ceph_nodes")
    config = kw.get("config")
    build = config.get("rhbuild")
    docker_registry = config.get("ceph_docker_registry")
    docker_image = config.get("ceph_docker_image")
    docker_image_tag = config.get("ceph_docker_image_tag")
    installer_node = None
    ansible_dir = "/usr/share/ceph-ansible"
    playbook = "switch-from-non-containerized-to-containerized-ceph-daemons.yml"
    for cnode in ceph_nodes:
        if cnode.role == "installer":
            installer_node = cnode
    log.info("Get all.yml content")
    out, err = installer_node.exec_command(
        sudo=True,
        cmd="cat {ansible_dir}/group_vars/all.yml".format(ansible_dir=ansible_dir),
    )
    conf = yaml.safe_load(out)
    log.info(f"Previous all.yml file content:{conf}")

    if conf.get("ceph_repository_type") == "cdn":
        docker_registry, docker_image, docker_image_tag = ceph_repository_type_cdn(
            ansible_dir, installer_node
        )

    conf.update(
        [
            ("ceph_docker_registry", docker_registry),
            ("ceph_docker_image", docker_image),
            ("ceph_docker_image_tag", docker_image_tag),
            ("containerized_deployment", True),
        ]
    )
    log.info(f"Modified all.yml file content :{conf}")
    conf_yaml_file = yaml.dump(conf)
    destination_file = installer_node.remote_file(
        sudo=True,
        file_name=f"{ansible_dir}/group_vars/all.yml",
        file_mode="w",
    )
    destination_file.write(conf_yaml_file)
    destination_file.flush()

    if build.startswith("3"):
        installer_node.exec_command(
            sudo=True,
            cmd="cd {ansible_dir}; cp {ansible_dir}/infrastructure-playbooks/{playbook} .".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
        )
        rc = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv {playbook}"
            " -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    else:
        rc = installer_node.exec_command(
            cmd="cd {ansible_dir};ansible-playbook -vvvv"
            " infrastructure-playbooks/{playbook} -e ireallymeanit=yes -i hosts".format(
                ansible_dir=ansible_dir, playbook=playbook
            ),
            long_running=True,
        )

    if rc == 0:
        log.info(
            "ansible-playbook switch-from-non-containerized-to-containerized-ceph-daemons.yml successful"
        )
        return 0

    log.info(
        "ansible-playbook switch-from-non-containerized-to-containerized-ceph-daemons.yml failed"
    )
    return 1
