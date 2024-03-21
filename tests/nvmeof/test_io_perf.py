"""Test module to execute IO on different protocols and generate reports."""
import json
import os
import re
import tempfile

import plotly.graph_objects as go
import plotly.io as pio
import yaml

from ceph.ceph import Ceph
from ceph.nvmegw_cli.subsystem import Subsystem
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.cephadm import test_nvmeof, test_orch
from tests.nvmeof.test_ceph_nvmeof_gateway import (
    configure_subsystems,
    disconnect_initiator,
    initiators,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.io.fio_profiles import IO_Profiles
from utility.log import Log
from utility.utils import create_run_dir, generate_unique_id, run_fio

LOG = Log(__name__)

METRICS = [
    "iops",
    "bandwidth",
    "submission_latency_in_nanoseconds",
    "completion_latency_in_nanoseconds",
    "overall_latency_in_nanoseconds",
]

results = {}
csv_op = []

RBD_MAP = "rbd map {pool}/{image}"
RBD_UNMAP = "rbd unmap {device}"
ARTIFACTS_DIR = tempfile.TemporaryDirectory().name
cli_image = str()


def initialize():
    """Initialize the reports."""
    _results = {}
    _csv_op = [
        "name,protocol,io_type,image,iteration,read_iops,read_bandwidth,read_slat_avg_ns,read_clat_avg_ns,"
        "read_lat_avg_ns,write_iops,write_bandwidth,write_slat_avg_ns,write_clat_avg_ns,"
        "write_lat_avg_ns"
    ]
    return _results, _csv_op


def calculate_average(data):
    """Calculate average values for data sets(dict)."""
    dicts = [i for i in data]  # Combine the dictionaries into a tuple
    averages = {}
    dict1 = data[0]
    for key in dict1.keys():
        values = [d[key] for d in dicts]
        # Get the values for the current key from all dictionaries
        average = sum(values) / len(values)  # Calculate the average
        averages[key] = average

    LOG.info(f"Averages: {averages}")
    return averages


def combine_charts(charts, run_information=None):
    """Combine all charts under one html page."""
    # Create title section
    title = """
        <div>
            <h2>{io_type} IO Type Perf Comparison for Different Protocols</h2>
            <div>
                <pre>{run_information}</pre>
            </div>
        </div>
    """.format(
        io_type=run_information["IO_Type"].upper(),
        run_information=yaml.dump(run_information) if run_information else "No Data",
    )

    # Template for the combined charts
    template = """
        <div>
            <h2>{metric}</h2>
            <div>{chart_content}</div>
        </div>
    """

    def normalise_title(string):
        return re.sub(r"[-_]", " ", string)

    # Prepare Combined charts
    combined_html_content = title
    for metric, _file in charts.items():
        with open(_file, "r") as f:
            chart_html = f.read()
        combined_html_content += template.format(
            metric=normalise_title(metric).title(), chart_content=chart_html
        )
        # cleanup single metric html file
        os.system(f"rm -rf {_file}")

    return combined_html_content


def plot(io_type, name, metric, data, cfg):
    """Plot the chart."""
    # Data for different protocols and their corresponding IOPS values
    # Define custom colors for each bar
    _colors = ["#0000FF", "#00008B", "#ADD8E6", "#800080", "#FF00FF"]
    protocols = []
    values = []
    count = 0
    for proto, value in data.items():
        protocols.append(proto)
        values.append(value)
        count += 1
    colors = _colors[0:count]

    # Create the bar chart trace with custom colors
    bar_trace = go.Bar(x=protocols, y=values, marker=dict(color=colors))

    # Create text annotations for each bar
    annotations = [
        dict(x=x, y=y, text=str(y), showarrow=True, arrowhead=1, ax=0, ay=-30)
        for x, y in zip(protocols, values)
    ]

    # Create a legend
    legend_trace = go.Scatter(
        x=[],
        y=[],
        mode="markers",
        marker=dict(color=colors),
        legendgroup="Legend",
        showlegend=True,
        name="Protocols",
    )

    # Create a figure with the bar chart trace, annotations, and legend
    fig = go.Figure(
        data=[bar_trace, legend_trace], layout=dict(annotations=annotations)
    )

    # Set chart title and labels
    fig.update_layout(
        title=f"{metric} - {data}",
        xaxis_title="Protocols",
        yaxis_title=metric,
        width=1000,
        height=400,  # Adjust the height of the chart
    )

    # Save the figure as an HTML file
    html_file = f'{cfg["test_dir"]}/{io_type}-{name}-{metric}.html'
    pio.write_html(fig, file=html_file)
    LOG.info(
        f"Comparison chart for {name}, io_type:{io_type}, metric:{metric} - {html_file}"
    )
    return html_file


def build_charts(io_type, data, test_config, cfg):
    """Builds charts based on IO types.

    - Develop charts based on IOType(s) comparing LibRBD and NVMeoF protocols.
    - Store the charts in test run log path ( /tmp/<dir> | magna002 )

    {"io_type":  {rd: {iops:1,lat:2,bw:3}, wr: {iops:1,lat:2,bw:3}}}
    """
    protocols = []
    ios = []
    for proto, _data in data.items():
        protocols.append(proto)
        for io, _list in _data.items():
            if io not in ["write", "read"]:
                continue
            ios.append(io)
            __list = []
            for i in _list:
                __list.append(dict((k, v) for k, v in i.items() if k in METRICS))
            LOG.info(f"Calculating Averages for {proto}-{io} with data {_list}")
            data[proto][io] = calculate_average(__list)

    for io, metrics in data[protocols[0]].items():
        if io not in ["write", "read"]:
            continue
        charts = {}
        for metric in metrics.keys():
            if metric not in METRICS:
                continue
            charts.update(
                {
                    metric.upper(): plot(
                        io_type,
                        data[protocols[0]]["name"],
                        metric,
                        dict((proto, data[proto][io][metric]) for proto in protocols),
                        cfg,
                    )
                }
            )
        html_file = (
            f"{cfg['test_dir']}/"
            f"{io_type}-IO_Type-{io}-Operation-{data[protocols[0]]['name']}-Main-Report.html"
        )
        info = {
            "IO_Type": io_type,
            "IO_Operation": io,
            "protocols": [
                {proto: data[proto]["io_information"]} for proto in protocols
            ],
            "test_config": test_config,
        }
        with open(html_file, "w+") as fd:
            fd.write(combine_charts(charts, info))

        LOG.info(
            f"All IO metric charts for IO_type: {io_type}, IO_operation: {io} - {html_file}"
        )


def find_free_port(host):
    find_port = """
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((\'localhost\', {PORT_NUMBER}))
try:
    _, port = s.getsockname()
except:
    port = None
finally:
    s.close()
print(port)
"""
    for port in range(6000, 10000):
        out, _ = host.exec_command(
            cmd=f'python3 -c "{find_port.format(PORT_NUMBER=port)}"',
            sudo=True,
        )
        if not out or out == "None":
            continue
        return out.strip()


def parse_fio_output(node, op_file):
    """Parse FIO output response.

    collects the below job information,
    - job artifacts
    - Write and Read IOPS, BW, Latencies
    - normalize to standard units.

    Args:
        node: FIO node
        op_file: FIO result file
    Returns:
        parsed_op
    """
    _csv_op = []
    _file = node.remote_file(file_name=op_file, file_mode="r")
    data = json.loads(_file.read())
    _file.close()

    job = data["jobs"][0]
    # FIO_WRITE_BS_4k_IODepth8_LIBAIO-TIEA-0-_dev_rbd17-librbd
    name, _, iteration, protocol, image = job["jobname"].split("-")
    io_type = job["job options"]["rw"]
    io_information = data["global options"] | job["job options"]
    _csv_op += [name, protocol, io_type, image, iteration]

    if io_type not in results:
        results[io_type] = {}

    if protocol not in results[io_type]:
        results[io_type][protocol] = {
            "name": name,
            "protocol": protocol,
            "io_type": io_type,
            "io_information": io_information,
        }

    results[io_type][protocol]["num_of_iterations"] = iteration
    if job["job options"]["rw"] in ["read", "randread", "rw", "readwrite"]:
        read = job["read"]
        if "read" not in results[io_type][protocol]:
            results[io_type][protocol]["read"] = []

        results[io_type][protocol]["read"].append(
            {
                "iteration": iteration,
                "image": image,
                "iops": read["iops"],
                "bandwidth": read["bw_mean"],
                "submission_latency_in_nanoseconds": read["slat_ns"]["mean"],
                "completion_latency_in_nanoseconds": read["clat_ns"]["mean"],
                "overall_latency_in_nanoseconds": read["lat_ns"]["mean"],
            }
        )

        _csv_op += [
            read["iops"],
            read["bw_mean"],
            read["slat_ns"]["mean"],
            read["clat_ns"]["mean"],
            read["lat_ns"]["mean"],
        ]
    else:
        _csv_op += ["", "", "", "", ""]

    if job["job options"]["rw"] in ["write", "randwrite", "rw", "readwrite"]:
        write = job["write"]
        if "write" not in results[io_type][protocol]:
            results[io_type][protocol]["write"] = []

        results[io_type][protocol]["write"].append(
            {
                "iteration": iteration,
                "iops": write["iops"],
                "image": image,
                "bandwidth": write["bw_mean"],
                "submission_latency_in_nanoseconds": write["slat_ns"]["mean"],
                "completion_latency_in_nanoseconds": write["clat_ns"]["mean"],
                "overall_latency_in_nanoseconds": write["lat_ns"]["mean"],
            }
        )
        _csv_op += [
            write["iops"],
            write["bw_mean"],
            write["slat_ns"]["mean"],
            write["clat_ns"]["mean"],
            write["lat_ns"]["mean"],
        ]

    csv_op.append(",".join([op if isinstance(op, str) else str(op) for op in _csv_op]))


def librbd(ceph_cluster, **args):
    """Execute IO via LibRBD Protocol.

    - Ensure the libRBD client has all necessary packages and accessibility.(Pre-req)
    - Create a RBD Pool
    - Create image
    - map the image to client node
    - Run FIO on mapped device.

    Args:
        ceph_cluster: Ceph cluster object
        args: libRBD IO arguments.

    ::Example:

        librbd:
            image:
              size: 10G
              count: 1
            node: node7
            cleanup:
              - pool
    """
    # Create RBD Image
    pool = args["pool"]
    rbd = args["rbd"]
    client = get_node_by_id(ceph_cluster, args["node"])
    client.exec_command(cmd=f"mkdir -p {ARTIFACTS_DIR}", sudo=True)

    for count in range(args["iterations"]):
        name = generate_unique_id(4)
        images = {}
        try:
            for i in range(args["image"]["count"]):
                img_name = f"{name}-image{i}"
                rbd.create_image(pool, img_name, args["image"]["size"])
                dev, _ = client.exec_command(
                    cmd=RBD_MAP.format(pool=pool, image=img_name),
                    sudo=True,
                )
                images[img_name] = dev.strip()

            # Map the image, Run with profiles and Collect results
            io_overrides = args.get("io_overrides", {})
            for io_profile in args["io_profiles"]:
                with parallel() as p:
                    io_args = IO_Profiles[io_profile]
                    io_args.update(
                        {
                            "output_dir": ARTIFACTS_DIR,
                        }
                    )
                    io_args.update(io_overrides)
                    for _, dev in images.items():
                        test_name = f"{io_profile}-{name}-{count}-librbd-{dev.replace('/', '_')}"
                        io_args.update(
                            {
                                "test_name": test_name,
                                "device_name": dev,
                                "client_node": client,
                                "long_running": True,
                                "cmd_timeout": "notimeout",
                            }
                        )
                        p.spawn(run_fio, **io_args)
                    for op in p:
                        parse_fio_output(client, op)
        except Exception as err:
            raise Exception(err)
        finally:
            # cleanup images
            for img, dev in images.items():
                client.exec_command(cmd=RBD_UNMAP.format(device=dev), sudo=True)
                rbd.remove_image(pool, img)


def nvmeof(ceph_cluster, **args):
    """Execute IO via nvme Protocol.

    - Create RBD Pool
    - Create required number of images as per test
    - Deploy Ceph-Nvmeof Gateway and subsystems along with target namespaces
    - Initialize Ceph Initiators/Client
    - Connect to required subsystem using nvme cli
    - Run FIO on the targets as per the test
    - Move all artifacts to common place

    Args:
        ceph_cluster: Ceph Cluster object
        args: NVME IO arguments.


    ::Example:

        nvmeof:
              image:
                  size: 10G
                  count: 1
              gw_node: node6
              initiator_node: node7
              cleanup:
                  - subsystems
                  - initiators
    """
    # Create RBD Image
    pool = args["pool"]
    rbd = args["rbd"]

    # Configure NVMEoF Gateway
    gw_node = get_node_by_id(ceph_cluster, args["gw_node"])
    Subsystem.NVMEOF_CLI_IMAGE = cli_image
    gateway = Subsystem(gw_node, 5500)
    initiator = get_node_by_id(ceph_cluster, args["initiator_node"])
    initiator.exec_command(cmd=f"mkdir -p {ARTIFACTS_DIR}", sudo=True)

    # Install Gateway
    if args.get("install_gw"):
        cfg = {
            "config": {
                "command": "apply",
                "service": "nvmeof",
                "args": {
                    "placement": {"nodes": [args["gw_node"]]},
                },
                "pos_args": [pool],
            }
        }
        test_nvmeof.run(ceph_cluster, **cfg)

    for count in range(args["iterations"]):
        name = generate_unique_id(4)
        subsystem = {
            "nqn": f"nqn.2016-06.io.spdk:{generate_unique_id(4)}",
            "serial": generate_unique_id(2),
            "allow_host": "*",
            "listener_port": find_free_port(gw_node),
            "node": args["gw_node"],
        }
        initiator_cfg = {
            "subnqn": subsystem["nqn"],
            "listener_port": subsystem["listener_port"],
            "node": args["initiator_node"],
        }
        configure_subsystems(rbd, pool, gateway, subsystem)
        try:
            for i in range(args["image"]["count"]):
                rbd.create_image(pool, f"{name}-image{i}", args["image"]["size"])
                ns_args = {
                    "args": {
                        "subsystem": subsystem["nqn"],
                        "rbd-pool": pool,
                        "rbd-image": f"{name}-image{i}",
                    }
                }
                gateway.namespace.add(**ns_args)

            # Run with profiles and collect results
            for io_profile in args["io_profiles"]:
                initiator_cfg["io_args"] = IO_Profiles[io_profile]
                initiator_cfg["io_args"].update(
                    {
                        "test_name": f"{io_profile}-{name}-{count}-nvmeof",
                        "output_dir": ARTIFACTS_DIR,
                    }
                )
                # apply overrides
                initiator_cfg["io_args"].update(args.get("io_overrides", {}))

                [
                    parse_fio_output(
                        get_node_by_id(ceph_cluster, args["initiator_node"]), i
                    )
                    for i in initiators(ceph_cluster, gateway, initiator_cfg)
                ]

                # disconnect initiator
                disconnect_initiator(
                    ceph_cluster, args["initiator_node"], initiator_cfg
                )
        except Exception as err:
            raise Exception(err)
        finally:
            # disconnect and delete subsystems
            cleanup_cfg = {
                "gw_node": args["gw_node"],
                "cleanup": ["subsystems"],
                "subsystems": [subsystem],
            }

            teardown(ceph_cluster, rbd, cleanup_cfg)

            # cleanup images
            for i in range(args["image"]["count"]):
                rbd.remove_image(pool, f"{name}-image{i}")


IO_PROTO = {
    "librbd": librbd,
    "nvmeof": nvmeof,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Test module to run IO on LibRBD and NVMeoF protocol.

    - Configure the necessary resources to execute IO on LibRBD and nvmeof targets
    - Based on the number of iteration(s), the IO protocol will get executed.
    - LibRBD: creates image, map and runIO on the images.
    - SPDK: Configures Initiators and Run FIO on NVMe targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        test:
          name: Perf test with 10Gb with 4kb block size
          description: Test IO Write-Read with 4kb block size
          module: test_io_perf.py
          config:
              iterations: 1				  # number of iterations to find out average
              io_profiles:
                  - FIO_WRITE_BS_4k_IODepth8_LIBAIO
                  - FIO_READ_BS_4k_IODepth8_LIBAIO
              io_exec:
                  -   proto: librbd
                      image:
                          size: 10G
                          count: 1
                      node: node7
                  -   proto: nvmeof
                      image:
                          size: 10G
                          count: 1
                      gw_node: node6
                      initiator_node: node7
    """
    global results, csv_op, cli_image
    results, csv_op = initialize()

    config = kwargs["config"]
    LOG.info(f"Test IO Performance : {config}")
    rbd_pool = config.get("rbd_pool", "rbd_pool")
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": rbd_pool},
        }
    )
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            cli_image = value
            break

    try:
        io_profiles = config["io_profiles"]
        iterations = config.get("iterations", 1)
        for io in config["io_exec"]:
            io.update({"io_overrides": config.get("io_overrides", {})})
            IO_PROTO[io["proto"]](
                ceph_cluster=ceph_cluster,
                rbd=rbd_obj,
                pool=rbd_pool,
                iterations=iterations,
                io_profiles=io_profiles,
                **io,
            )

        # cleanup pool
        if not config.get("do_not_delete_pool"):
            rbd_obj.clean_up(pools=[rbd_pool])

        # Build artifacts, Create CSV and Json file
        run_cfg = kwargs["run_config"]
        test_dir = create_run_dir(
            run_cfg.get("run_id"),
            log_dir=f"{run_cfg['log_dir']}/{run_cfg['test_name']}",
        )
        run_cfg["test_dir"] = test_dir
        kwargs["config"][
            "artifacts"
        ] = f"Artifacts - {run_cfg['log_link'].rsplit('.', 1)[0]}"

        csv_file = f"{test_dir}/run.csv"
        json_file = f"{test_dir}/run.json"
        with open(csv_file, "w+") as _csv, open(json_file, "w+") as _json:
            _csv.write("\n".join(csv_op))
            LOG.info(f"CSV file located here: {csv_file}")
            _json.write(json.dumps(results, indent=2))
            LOG.info(f"Json file located here: {json_file}")

        # Plot charts
        test_config = {
            "io_exec": config["io_exec"],
            "iterations": config["iterations"],
        }
        for io_type, data in results.items():
            build_charts(io_type, data, test_config, run_cfg)
    except Exception as err:
        raise Exception(err)
    finally:
        # cleanup pool
        if not config.get("do_not_delete_pool"):
            rbd_obj.clean_up(pools=[rbd_pool])

        # delete NVMe Gateway
        if config.get("delete_gateway"):
            cfg = {
                "config": {
                    "command": "remove",
                    "service": "nvmeof",
                    "args": {"service_name": f"nvmeof.{rbd_pool}"},
                }
            }
            test_orch.run(ceph_cluster, **cfg)
    return 0
