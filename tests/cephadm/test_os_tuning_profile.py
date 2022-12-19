import tempfile

import yaml

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
    mount = "/tmp:/tmp"
    if config.get("command") in ("apply", "re-apply"):
        nodes = config.get("specs", {}).get("placement", {}).get("hosts")
        hosts = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
        config["specs"]["placement"]["hosts"] = hosts
        spec_file = generate_tuned_profile_spec(installler_node, config.get("specs"))
        result = CephAdm(installler_node, mount).ceph.orch.tuned_profile.apply(
            spec_file
        )
        if config.get("apply_result") != result:
            raise OsTuningProfileError("Fail to apply tuned profile")
        result = CephAdm(installler_node).ceph.orch.tuned_profile.list().split("\n")[0]
        if config.get("ls_result") != result:
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
