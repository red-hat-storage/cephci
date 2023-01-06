import traceback
from datetime import datetime, timedelta

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


def start_io(io_obj, mounting_dir):
    """
    This method runs IOs and decides what kind IOs should run on the mounted Directory
    """
    try:
        run_start_time = datetime.now()
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
