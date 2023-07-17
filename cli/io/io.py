from threading import Thread

from utility.log import Log

log = Log(__name__)


def linux_untar(clients, mountpoint, dirs=(".")):
    """
    Performs Linux untar on the given clients
    Args:
        clients (list): Client (s)
        mountpoint(str): Mount point where the volume is
                       mounted.
        dirs(tuple): A tuple of dirs where untar has to
                    started. (Default:('.'))
    """
    threads = []
    if not isinstance(clients, list):
        clients = [clients]

    for client in clients:
        # Download linux untar to root
        cmd = "wget https://cdn.kernel.org/pub/linux/kernel/" "v5.x/linux-5.4.54.tar.xz"
        client.exec_command(cmd=cmd, sudo=True)

        for directory in dirs:
            # copy linux tar to dir
            cmd = "cp /root/linux-5.4.54.tar.xz {}/{}".format(mountpoint, directory)
            client.exec_command(cmd=cmd, sudo=True)

            # Start linux untar
            cmd = "cd {}/{};tar -xvf linux-5.4.54.tar.xz".format(mountpoint, directory)
            untar = Thread(
                target=lambda: client.exec_command(cmd=cmd, sudo=True), args=()
            )
            untar.start()
            threads.append(untar)
    return threads
