import io
import random
import threading
import time
from multiprocessing import Process

from smb_client import SMBClient

from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


load_files = []
connections = []
status = {"connection": 0, "write": 0, "read": 0, "delete": 0, "client_error": 0}


def smb_load(
    installer,
    client,
    smb_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    load_test_config,
):
    """smb load testing
    Args:
        installer (obj): Installer node obj
        client (obj): Client object
        smb_server (obj): Smb server
        smb_shares (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        load_test_config (list): Smb load test parameter
    """
    # make client connection and create load directory
    make_load_dir(
        smb_server, smb_share, smb_user_name, smb_user_password, load_test_config
    )

    # start load process
    processes = []
    for process in range(1, load_test_config["total_load_process"] + 1):
        make_load_dir(
            smb_server,
            smb_share,
            smb_user_name,
            smb_user_password,
            load_test_config,
            process,
        )
        process = Process(
            target=start_smb_load_test,
            args=(
                installer,
                smb_server,
                smb_share,
                smb_user_name,
                smb_user_password,
                load_test_config,
                process,
            ),
        )
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    # cleanup
    cleanup(
        client,
        smb_server,
        smb_share,
        smb_user_name,
        smb_user_password,
        load_test_config,
    )


def make_load_dir(
    smb_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    load_test_config,
    process=None,
):
    """create load dir using smbclient
    Args:
        smb_server (obj): Smb server
        smb_shares (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        load_test_config (list): Smb load test parameter
        process (int): process id
    """
    try:
        load_dir = load_test_config["load_dir"]
        smbclient = SMBClient(
            smb_server.ip_address,
            smb_share,
            smb_user_name,
            smb_user_password,
            load_test_config["port"],
        )
        dirs = smbclient.listdir()
        if load_dir not in dirs:
            smbclient.mkdir(f"/{load_dir}")
        if process:
            load_process_dir = f"{load_dir}/p{process}"
            dirs = smbclient.listdir(f"/{load_dir}")
            if load_process_dir not in dirs:
                smbclient.mkdir(f"/{load_process_dir}")
        smbclient.disconnect()
    except Exception as e:
        raise OperationFailedError(f"Fail to create load dir, Error {e}")


def start_smb_load_test(
    installer,
    smb_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    load_test_config,
    process,
):
    """start smb load test
    Args:
        installer (obj): Installer node obj
        smb_server (obj): Smb server
        smb_shares (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        load_test_config (list): Smb load test parameter
        process (int): process id
    """
    try:
        # delay start by 10 seconds to give sufficient time to setup threads/processes.
        start_time = time.time() + 10
        stop_time = start_time + load_test_config["load_runtime"]
        load_operations = []
        thread_per_load_process = load_test_config["thread_per_load_process"]
        total_load_process = load_test_config["total_load_process"]
        status["connection"] = total_load_process * thread_per_load_process
        for _ in range(thread_per_load_process):
            smbclient = SMBClient(
                smb_server.ip_address,
                smb_share,
                smb_user_name,
                smb_user_password,
                load_test_config["port"],
            )
            load_operations.append(
                threading.Thread(
                    target=smb_load_ops,
                    args=(
                        installer,
                        smb_server,
                        smb_share,
                        smb_user_name,
                        smb_user_password,
                        load_test_config,
                        process,
                        smbclient,
                        start_time,
                        stop_time,
                    ),
                )
            )

        for load_operation in load_operations:
            try:
                load_operation.start()
            except RuntimeError:
                status["status"] += 1

        for Thread_operation in load_operations:
            Thread_operation.join()

    except Exception as e:
        raise OperationFailedError(f"Fail to start smb load test, Error {e}")


def smb_load_ops(
    installer,
    smb_server,
    smb_share,
    smb_user_name,
    smb_user_password,
    load_test_config,
    process,
    smbclient,
    start_time,
    stop_time,
):
    """smb load operations
    Args:
        installer (obj): Installer node obj
        smb_server (obj): Smb server
        smb_shares (str): Smb share
        smb_user_name (str): Smb username
        smb_user_password (str): Smb password
        load_test_config (list): Smb load test parameter
        process (int): process id
        smbclient (obj): Smb client obj
        start_time (int): operation start time
        stop_time (int): operation stop time

    """
    try:
        while time.time() < start_time:
            time.sleep(start_time - time.time())

        while time.time() < stop_time:
            if load_test_config["actions"]:
                for action in load_test_config["actions"]:
                    if action == "write":
                        write(installer, smbclient, load_test_config, process)
                        status["write"] += 1
                    elif action == "read":
                        read(smbclient, load_test_config, process)
                        status["read"] += 1
                    elif action == "delete":
                        delete(smbclient, load_test_config, process)
                        status["delete"] += 1
                time.sleep(1)
            total_Connections = status["connection"]
            total_write = status["write"]
            total_read = status["read"]
            total_delete = status["delete"]
            client_error = status["client_error"]
            log.info(
                f"Run status: total_Connections: {total_Connections},"
                f"total_write: {total_write}, total_read: {total_read}, "
                f"total_delete: {total_delete}, client_error: {client_error}"
            )
        smbclient.disconnect()
    except Exception as e:
        raise OperationFailedError(f"Fail to start smb load operations, Error {e}")


def write(installer, smbclient, load_test_config, process):
    """write operations
    Args:
        installer (obj): Installer node obj
        smbclient (obj): Smb client obj
        load_test_config (list): Smb load test parameter
        process (int): process id
    """
    try:
        load_file_size = load_test_config["load_file_size"] * 1024 * 1024
        load_file_content = b"a" * load_file_size
        load_file_obj = io.BytesIO(load_file_content)
        load_dir = load_test_config["load_dir"]
        load_file = f"/{load_dir}/p{process}/file" + str(random.randint(0, 1000))
        smbclient.write(load_file, load_file_obj)
        load_files.append(load_file)
    except Exception as e:
        raise OperationFailedError(f"write operation failed, Error {e}")


def read(smbclient, load_test_config, process):
    """read operations
    Args:
        smbclient (obj): Smb client obj
        load_test_config (list): Smb load test parameter
        process (int): process id
    """
    try:
        load_dir = load_test_config["load_dir"]
        load_files = smbclient.listdir(f"/{load_dir}/p{process}")
        for load_file in load_files:
            if load_file in load_files and load_file not in [".", ".."]:
                smbclient.read_text(f"/{load_dir}/p{process}/{load_file}")
    except Exception as e:
        raise OperationFailedError(f"read operation failed, Error {e}")


def delete(smbclient, load_test_config, process):
    """delete operations
    Args:
        smbclient (obj): Smb client obj
        load_test_config (list): Smb load test parameter
        process (int): process id
    """
    try:
        load_dir = load_test_config["load_dir"]
        files = smbclient.listdir(f"/{load_dir}/p{process}")
        for file in files:
            if file in load_files and file not in [".", ".."]:
                smbclient.unlink(f"/{load_dir}/p{process}/{file}")
                load_files.remove(f"/{load_dir}/p{process}/{file}")
    except Exception as e:
        raise OperationFailedError(f"delete operation failed, Error {e}")


def cleanup(
    client, smb_server, smb_share, smb_user_name, smb_user_password, load_test_config
):
    """clean up
    Args:
        client (obj): client obj
        load_test_config (list): Smb load test parameter
    """
    try:
        load_dir = load_test_config["load_dir"]
        cmd = (
            f"smbclient -U {smb_user_name}%{smb_user_password} "
            f"//{smb_server.ip_address}/{smb_share} -c 'deltree {load_dir}'"
        )
        time.sleep(3)
        client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        time.sleep(1)
    except Exception as e:
        raise OperationFailedError(f"cleanup operation failed, Error {e}")
