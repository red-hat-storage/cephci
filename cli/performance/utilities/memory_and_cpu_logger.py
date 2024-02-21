"""
A tool to monitor and log memory consumption processes.
"""
from __future__ import print_function

import argparse
import csv
import os
import subprocess
from time import sleep


def run_command(cmd):
    """
    Run command using Popen and return output
    """
    ret = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
    )
    output = ret.stdout.read().decode("utf8").split("\n")[:-1]
    return output


def get_memory_and_cpu_consumption(proc_name):
    """
    Get the memory and cpu consumed by a given process
    """
    # The command gives an output as shown below:
    # [2020-08-07 09:34:48] 16422 0.0 9.99609
    #
    # Where,
    # [2020-08-07 09:34:48] is UTC timestamp.
    # 16422 is the process ID.
    # 0.0 is the CPU usage.
    # 9.99609 is memory consumption in MB.
    cmd = (
        "ps u -p `pgrep " + proc_name + "` | "
        "awk 'NR>1 && $11~/" + proc_name + "$/{print "
        'strftime("[%Y-%d-%m %H:%M:%S]", '
        "systime(), 1), $13,$2,$3,$6/1024}'"
    )
    memory_and_cpu_consumed = run_command(cmd)
    return memory_and_cpu_consumed


def main():
    """
    This tool monitors the memory and cpu utilisation of a given resource. The parameters for this tool are
    Usage:
        memory_and_cou_logger.py --process_name <Process Name>
                                 --interval <interval in seconds>
                                 --testname <name of the test>
        e.g: python memory_and_cpu_logger.py -p ceph-mgr -t Sample_Test_0 -i 10
    """
    # Setting up command line arguments
    parser = argparse.ArgumentParser(
        description="A tool to log memory usage of a given process"
    )
    parser.add_argument(
        "-p",
        "--process_name",
        type=str,
        dest="process_name",
        required=True,
        help="Name of process for which cpu and memory is to be logged",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        dest="interval",
        default=60,
        help="Time interval to wait between consecutive logs(Default:60)",
    )
    parser.add_argument(
        "-t",
        "--testname",
        type=str,
        dest="testname",
        required=True,
        help="Test name for which memory is logged",
    )
    args = parser.parse_args()

    # Declare all three parameters
    process_name = args.process_name
    interval = args.interval

    # Generating CSV file header
    with open(f"{process_name}-{args.testname}.csv", "a") as file:
        csv_writer_obj = csv.writer(file)
        csv_writer_obj.writerow([args.testname, "", "", ""])
        csv_writer_obj.writerow(
            ["Time stamp", "Process Name", "Process ID", "CPU Usage", "Memory Usage"]
        )

        # Taking memory output for a given
        # number of times
        stop_flag = "0"
        counter = 0
        while "0" in stop_flag:
            data = get_memory_and_cpu_consumption(process_name)

            # Logging information to csv file
            for line in data:
                info = line.split(" ")
                csv_writer_obj.writerow(
                    [" ".join(info[:2]), info[2], info[3], info[4], info[5]]
                )
                sleep(interval)
            counter += 1
            stop_flag = str(os.popen("cat /home/status").read())


if __name__ == "__main__":
    main()
