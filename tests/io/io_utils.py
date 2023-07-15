import traceback
from datetime import datetime, timedelta
from time import sleep

from yaml import safe_dump

from utility.log import Log
from utility.utils import get_smallfile_config

log = Log(__name__)


def smallfile_io(client, **kwargs):
    """
    Runs the Small file IOs on the mounted direcoty
    Arguments:
        mounting_dir
        small_config - this includes how many iterations it should run
    """
    mounting_dir = kwargs.get("mounting_dir")
    small_config = kwargs.get("small_config")
    ops = kwargs.get("operation", ["create", "delete"])
    iter = small_config.get("iterations", 1)
    if kwargs.get("docker_compose"):
        install_docker_compose(client)
        service_list = []
        for op in ops:
            for i in range(0, iter):
                service_list.append(
                    {
                        "name": f"{kwargs.get('compose_file')}_smallfile_{i}",
                        "command": f"git clone https://github.com/distributed-system-analysis/smallfile.git;"
                        f"mkdir -p /app_test_io/dir_{client.node.hostname}_{i};python3 smallfile/smallfile_cli.py "
                        f"--operation {op} "
                        f"--threads {int(small_config.get('threads'))} --file-size {small_config.get('file_size')} "
                        f"--files {small_config.get('files')} --top "
                        f"/app_test_io/dir_{client.node.hostname}_{i}",
                        "volumes": [f"{mounting_dir}:/app_test_io"],
                    }
                )
        compose_dir = generate_compose(
            client, service_list, compose_file=kwargs.get("compose_file")
        )
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"cd {compose_dir} ; podman-compose -f {kwargs.get('compose_file')} config --services",
        )
        services = out.strip().split("\n")
        total_services = len(services) - 1 if len(services) != 1 else 1
        BATCH_SIZE = kwargs.get("batch_size", 10)
        service_prefix = "fs_compose.yaml_smallfile_"
        failed_containers = []
        for i in range(0, total_services, BATCH_SIZE):
            # Construct the docker-compose command for this batch
            services = " ".join(
                [
                    service_prefix + str(j)
                    for j in range(i, min(i + BATCH_SIZE, total_services))
                ]
            )
            log.info(f"Starting services for batch {i + 1}...")
            log.info(
                f"cd {compose_dir};podman-compose -f {kwargs.get('compose_file')} up -d {services}"
            )
            client.exec_command(
                sudo=True,
                cmd=f"cd {compose_dir};podman-compose -f {kwargs.get('compose_file')} up -d {services}",
                long_running=True,
            )
            while True:
                # Check container status
                sleep(120)
                out, rc = client.exec_command(
                    sudo=True, cmd="podman ps -q --format '{{.Names}} {{.Status}}'"
                )
                log.info(out)
                if not out:
                    log.info("All the Containers have been Exited")
                    break
                sleep(30)
        check_command = (
            "podman ps -a --format '{{.Names}} {{.Status}}' | grep 'Exited (1)'"
        )
        out, rc = client.exec_command(sudo=True, cmd=check_command, check_ec=False)
        failed_containers += out.strip().split("\n")
        if failed_containers:
            for service in failed_containers:
                log.info(f"Failed Service : {service}")
    else:
        for op in ops:
            for i in range(0, iter):
                client.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/dir_{i}")
                client.exec_command(
                    sudo=True,
                    cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation {op} "
                    f"--threads {int(small_config.get('threads'))} --file-size {small_config.get('file_size')} "
                    f"--files {small_config.get('files')} --top "
                    f"{mounting_dir}/dir_{i}",
                    long_running=True,
                )
    return


def start_io(io_obj, mounting_dir, **kwargs):
    """
    This method runs IOs and decides what kind IOs should run on the mounted Directory
    """
    try:
        run_start_time = datetime.now()
        log.info("Adding container permissions for the mounted folders")
        io_obj.client.exec_command(
            sudo=True,
            cmd=f"semanage fcontext -a -t container_file_t '{mounting_dir[:-1]}(/.*)?'",
        )
        io_obj.client.exec_command(sudo=True, cmd=f"restorecon -R -v {mounting_dir}")
        stats = {"total_iterations": 0}
        if io_obj.timeout == -1:
            if io_obj.io_tool == "smallfile":
                small_config = get_smallfile_config(
                    io_obj.client, io_obj.fill_data, io_obj.pool
                )
                smallfile_io(
                    io_obj.client,
                    operation=["create"],
                    small_config=small_config,
                    mounting_dir=mounting_dir,
                    docker_compose=kwargs.get("docker_compose", True),
                    compose_file=kwargs.get("compose_file"),
                    batch_size=kwargs.get("batch_size", 10),
                )
            return 0
        elif io_obj.timeout:
            stop = datetime.now() + timedelta(seconds=io_obj.timeout)
        else:
            stop = 0
        while True:
            if stop and datetime.now() > stop:
                log.info("Timed out")
                break
            if io_obj.io_tool == "smallfile":
                smallfile_io(io_obj.client, operation=["create", "delete"])
        stats["total_iterations"] += 1
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
    finally:
        run_end_time = datetime.now()
        duration = divmod((run_end_time - run_start_time).total_seconds(), 60)
        log.info(
            "---------------------------------------------------------------------"
        )
        log.info(f"Test Summary for IOs ran on {mounting_dir}")
        log.info(
            "---------------------------------------------------------------------"
        )
        log.info(f"Total Duration: {int(duration[0])} mins, {int(duration[1])} secs")
        log.info(
            "---------------------------------------------------------------------"
        )


def install_docker_compose(client):
    """
    Install docker conpose on the client node
    """
    out, rc = client.exec_command(
        sudo=True, cmd="podman-compose version", check_ec=False
    )
    if rc:
        cmd_list = [
            "yum install podman-docker -y",
            "curl -SL "
            "https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 "
            "-o /usr/local/bin/docker-compose",
            "chmod +x /usr/local/bin/docker-compose",
            "ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose",
            "systemctl enable --now podman.socket",
            "systemctl status podman.socket",
            "pip3 install --upgrade pip",
            "pip3 install podman-compose",
            "ln -sf /usr/local/bin/podman-compose /usr/bin/podman-compose",
        ]

        for cmd in cmd_list:
            client.exec_command(sudo=True, cmd=cmd, long_running=True)

        log.info("Validate docker compose installtion ")
        out, rc = client.exec_command(sudo=True, cmd="podman-compose version")
    log.info(f"docker compose Versions : {out}")


def generate_compose(client, service_list, **kwargs):
    """
    Generate docker compose files
    """
    compose_file_dict = {"version": 2}
    compose_file_dict["services"] = {}
    for service in service_list:
        command = []
        command.append("sh")
        command.append("-c")
        command.append(service["command"])
        compose_file_dict["services"][service.get("name")] = {
            "user": "root:root",
            "image": "registry.redhat.io/rhel8/python-36",
            "command": command,
            "volumes": service["volumes"],
        }
    dir = kwargs.get("compose-dir", "/home/cephuser/docker")
    client.exec_command(
        cmd=f"mkdir -p {dir};touch {dir}/{kwargs.get('compose_file')}; chmod +777 {dir}/{kwargs.get('compose_file')}"
    )
    docker_compose = client.remote_file(
        sudo=True, file_name=f"{dir}/{kwargs.get('compose_file')}", file_mode="w"
    )
    safe_dump(compose_file_dict, docker_compose)
    docker_compose.flush()
    return dir


def remove_allcontainers():
    # todo
    pass


def collect_container_logs():
    # Todo
    pass
