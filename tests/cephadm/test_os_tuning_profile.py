import tempfile

import yaml

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class OsTuningProfileError(Exception):
    pass


def generate_tuned_profile_spec(installler_node, specs):
    """
    Generate tuned profile.
    Args:
        installler_node: installer node
        specs: tuned profile specs
    """

    log.info(f"Spec yaml file content:\n{specs}")
    # Create tuned profile spec yaml file
    temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")
    spec_file = installler_node.remote_file(
        sudo=True, file_name=temp_file.name, file_mode="wb"
    )
    spec = yaml.dump(specs, sort_keys=False, indent=2).encode("utf-8")
    spec_file.write(spec)
    spec_file.flush()
    return temp_file.name


def verify_tunables(hosts, settings):
    """
    Verify tune profile.
    Args:
        hosts: tuned profile hosts
        settings: tuned profile settings
    """
    timeout, interval = 10, 3
    for host in hosts:
        for setting, value in settings.items():
            cmd = f"sysctl {setting}"
            for w in WaitUntil(timeout=timeout, interval=interval):
                out, _ = host.exec_command(sudo=True, cmd=cmd)
                if str(value) == out.split("=")[1].strip():
                    break
            if w.expired:
                raise OsTuningProfileError("Fail to verify tuned value")


def run(ceph_cluster, **kw):
    """Verify tuning profile test cases.
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
        kw: test data
        e.g:
        test:
            name: create_tuning_profile
            desc: Verify tuning profile created successfully using cephadm
            polarion-id: CEPH-83575318
            module: test_os_tuning_profile.py
            config:
                command: apply
                specs:
                    profile_name: test-mon-host-profile
                    placement:
                        hosts:
                            - node1
                            - node2
                settings:
                    fs.file-max: 100000
                    vm.swappiness: 14
                apply_result: Saved tuned profile test-mon-host-profile
                ls_result: "profile_name: test-mon-host-profile"
    """
    installler_node = ceph_cluster.get_nodes(role="installer")[0]
    config = kw.get("config")
    mount = "/tmp"
    if config.get("command") in ("apply", "re-apply"):
        nodes = config.get("specs", {}).get("placement", {}).get("hosts")
        if "hostx" in nodes:
            hosts = ["NoHost1", "NoHost2"]
        else:
            hosts = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
        config["specs"]["placement"]["hosts"] = hosts
        spec_file = generate_tuned_profile_spec(installler_node, config.get("specs"))
        # Execute the command directly to get the actual output
        cmd = f"cephadm shell --mount {mount}:{mount} -- ceph orch tuned-profile apply -i {spec_file}"
        log.info(f"Executing command directly: {cmd}")

        try:
            # Capture both stdout and stderr
            out, err = installler_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
            combined_output = f"{out}\n{err}".strip()

            # Check if expected result is in any part of the output
            if config.get("result") in combined_output:
                log.info(
                    f"Found expected result '{config.get('result')}' in combined output"
                )
            else:
                raise OsTuningProfileError(
                    f"Expected '{config.get('result')}' not found in output: '{combined_output}'"
                )

        except Exception as e:
            log.error(f"Direct command execution failed: {str(e)}")
            raise OsTuningProfileError(f"Failed to execute command: {str(e)}")

    if config.get("action") == "verify":
        hosts = [
            ceph_cluster.get_nodes()[int(node[-1])] for node in config.get("hosts")
        ]
        verify_tunables(hosts, config.get("settings"))

    if config.get("command") == "ls":
        result = CephAdm(installler_node).ceph.orch.tuned_profile.list().split("\n")[0]
        if config.get("result") != result:
            raise OsTuningProfileError("Fail to list tuned profile")

    if config.get("command") == "modify":
        profile_name, setting, value = (
            config.get("profile_name"),
            config.get("settings"),
            str(config.get("value")),
        )
        result = CephAdm(installler_node).ceph.orch.tuned_profile.modify(
            profile_name, setting, value
        )
        if config.get("result") != result:
            raise OsTuningProfileError("Fail to modify tuned profile")

    if config.get("command") == "remove":
        profile_name = config.get("profile_name")
        result = CephAdm(installler_node).ceph.orch.tuned_profile.remove(profile_name)
        if config.get("result") != result:
            raise OsTuningProfileError("Fail to list tuned profile")
    return 0
