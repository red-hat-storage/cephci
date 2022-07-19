import re

import yaml

from utility.log import Log
from utility.utils import setup_cluster_access

log = Log(__name__)

DIR = {
    "v2": {
        "script": "/ceph-qe-scripts/rgw/v2/tests/nfs_ganesha/",
        "lib": "/ceph-qe-scripts/rgw/v2/lib/",
        "config": "/ceph-qe-scripts/rgw/v2/tests/nfs_ganesha/config/",
    },
}


def run(ceph_cluster, **kw):
    rgw_ceph_object = ceph_cluster.get_ceph_object("rgw")
    rgw_node = rgw_ceph_object.node
    rgw_node_host = rgw_node.shortname
    test_folder = "rgw-tests"
    test_folder_path = f"~/{test_folder}"
    git_url = "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    git_clone = f"git clone {git_url} -b master"
    rgw_node.exec_command(
        cmd=f"sudo rm -rf {test_folder}"
        + f" && mkdir {test_folder}"
        + f" && cd {test_folder}"
        + f" && {git_clone}"
    )

    config = kw.get("config")
    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_version = config.get("test-version", "v2")
    script_dir = DIR[test_version]["script"]
    config_dir = DIR[test_version]["config"]
    timeout = config.get("timeout", 300)

    # Clone the repository once for the entire test suite
    pip_cmd = "venv/bin/pip"
    python_cmd = "venv/bin/python"
    out, err = rgw_node.exec_command(cmd="ls -l venv", check_ec=False)

    if not out:
        rgw_node.exec_command(
            cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
        )
        rgw_node.exec_command(cmd="python3 -m venv venv")
        rgw_node.exec_command(cmd=f"{pip_cmd} install --upgrade pip")

        rgw_node.exec_command(
            cmd=f"{pip_cmd} install "
            + f"-r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
        )

        if ceph_cluster.rhcs_version.version[0] == 5:
            setup_cluster_access(ceph_cluster, rgw_node)
            rgw_node.exec_command(
                sudo=True, cmd="yum install -y ceph-common ceph-radosgw --nogpgcheck"
            )
            rgw_node.exec_command(
                sudo=True, cmd="yum install -y ceph-common --nogpgcheck"
            )
        if ceph_cluster.rhcs_version.version[0] in [3, 4]:
            if ceph_cluster.containerized:
                # install ceph-radosgw on the host hosting the container
                rgw_node.exec_command(
                    sudo=True,
                    cmd="yum install -y ceph-common ceph-radosgw --nogpgcheck",
                )
                rgw_node.exec_command(
                    sudo=True, cmd="yum install -y ceph-common --nogpgcheck"
                )
    # Mount point ops
    mount_dir = config.get("mount-dir", "/mnt/ganesha/")

    checkdir_cmd = f"""[ -d '{mount_dir}' ] && [ ! -L '{mount_dir}' ] && echo 'Directory {mount_dir} exists.'
    || echo 'Error: Directory {mount_dir} exists but point to $(readlink -f {mount_dir}).'"""
    out, err = rgw_node.exec_command(sudo=True, cmd=checkdir_cmd, check_ec=False)

    rgw_node.exec_command(cmd=f"mkdir {mount_dir}", check_ec=False, sudo=True)
    nfs_version = config.get("nfs-version", "4")
    # Mount cmd: mount -t nfs -o nfsvers=<nfs version>,noauto,soft,sync,proto=tcp `hostname -s`:/ /mnt/ganesha/
    mount_cmd = f"mount -t nfs -o nfsvers={nfs_version},noauto,soft,sync,proto=tcp {rgw_node_host}:/ {mount_dir}"
    rgw_node.exec_command(cmd=mount_cmd, check_ec=False, sudo=True)
    log.info("nfs ganesha mounted successfully on the mountpoint")
    # To parse the nfs-ganesha configuration file : /etc/ganesha/ganesha.conf
    ganesha_conf_out, err = rgw_node.exec_command(
        cmd="cat /etc/ganesha/ganesha.conf", check_ec=False, sudo=True
    )

    def clean(x):
        return re.sub("[^A-Za-z0-9]+", "", x)

    ganesha_conf = ganesha_conf_out.split("\n")
    for content in ganesha_conf:
        if "Access_Key_Id" in content:
            access_key = clean(content.split("=")[1])
        if "Secret_Access_Key" in content:
            secret_key = clean(content.split("=")[1])
        if "User_Id" in content:
            rgw_user_id = clean(content.split("=")[1])

    rgw_user_config = dict(
        user_id=rgw_user_id,
        access_key=access_key,
        secret_key=secret_key,
        rgw_hostname=rgw_node_host,  # short hostname of rgw to populate under rgw_user.yaml
        ganesha_config_exists=True,
        already_mounted=True,
        cleanup=True,
        do_unmount=True,
        nfs_version=nfs_version,
        nfs_mnt_point=mount_dir,
        Pseudo="cephobject",
    )

    rgw_user_config_fname = (
        "rgw_user.yaml"  # Destination: ceph-qe-scripts/rgw/v2/tests/nfs_ganesha/config/
    )
    local_file = (
        "/home/cephuser/rgw-tests/ceph-qe-scripts/rgw/v2/tests/nfs_ganesha/config/"
        + rgw_user_config_fname
    )
    log.info("creating rgw_user.yaml : %s" % rgw_user_config)
    local_conf_file = rgw_node.remote_file(file_name=local_file, file_mode="w")
    local_conf_file.write(yaml.dump(rgw_user_config, default_flow_style=False))
    log.info("rgw_user.yaml file written")

    test_config = {"config": config.get("test-config", {})}
    if test_config["config"]:
        log.info("creating custom config")
        f_name = test_folder + config_dir + config_file_name
        remote_fp = rgw_node.remote_file(file_name=f_name, file_mode="w")
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    out, err = rgw_node.exec_command(
        cmd=f"sudo {python_cmd} "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name
        + " -r "
        + local_file,
        timeout=timeout,
    )
    log.info(out)
    log.error(err)

    return 0
