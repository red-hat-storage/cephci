from unittest.mock import MagicMock, patch

import pytest

from compute.exceptions import NodeError
from compute.openshift import (
    DEFAULT_OCPVIRT_PROFILE,
    KUBEVIRT_CLUSTER_INSTANCETYPE_KIND,
    OCPVIRT_RESOURCE_CPU_BUMP,
    OCPVIRT_RESOURCE_MEMORY_BUMP_GI,
    _memory_gib,
    _ocpvirt_domain_resources,
    apply_ocpvirt_vm_profile,
    build_virtualmachine_cr,
    datavolume_names_from_vm,
    get_namespace_resource_max,
    load_ocpvirt_namespace_config,
    process_ocpvirt_custom_config,
    resolve_ocpvirt_credentials,
    resolve_ocpvirt_image_name,
    resolve_ocpvirt_instancetype,
    validate_ocpvirt_credentials,
    validate_ocpvirt_inventory,
    validate_ocpvirt_namespace_config,
    validate_ocpvirt_resources_against_limitrange,
    validate_precreated_volume_names,
)

AUTH_OSP_CRED = {
    "globals": {
        "ocpvirt-credentials": {
            "token": "secret",
            "private_key_path": "/home/jenkins/.ssh/id_ed25519",
        }
    }
}

OCP_CRED = {
    "server": "https://api.example:6443",
    "token": "secret",
    "namespace": "ceph-jenkins--runtime-int",
    "storage_class": "rh-restricted-nfs",
    "network": "bridge-504",
    "datasources": {
        "rhel9": "datasource://openshift-virtualization-os-images/rhel9",
        "rhel10": "datasource://openshift-virtualization-os-images/rhel10",
    },
}

CLUSTER_INSTANCE_TYPES = ["o1.large", "cx1.large", "cx1.2xlarge", "cx1.4xlarge"]


def _mock_custom_api():
    api = MagicMock()
    api.list_cluster_custom_object.return_value = {
        "items": [{"metadata": {"name": name}} for name in CLUSTER_INSTANCE_TYPES]
    }
    return api


def test_process_ocpvirt_custom_config_defaults():
    cfg = process_ocpvirt_custom_config(None)
    assert cfg == {
        "pvc_batch_size": 3,
        "vm_batch_size": 1,
        "ocpvirt_profile": DEFAULT_OCPVIRT_PROFILE,
        "ocpvirt_cpu": None,
        "ocpvirt_memory": None,
    }
    assert DEFAULT_OCPVIRT_PROFILE == "o1.large"


def test_process_ocpvirt_custom_config_overrides():
    cfg = process_ocpvirt_custom_config(
        [
            "pvc_batch_size=5",
            "vm_batch_size=2",
            "ocpvirt_namespace=rdu3_ceph_jenkins",
            "ocpvirt_profile=cx1.2xlarge",
            "ibm-build=True",
        ]
    )
    assert cfg == {
        "pvc_batch_size": 5,
        "vm_batch_size": 2,
        "ocpvirt_profile": "cx1.2xlarge",
        "ocpvirt_cpu": None,
        "ocpvirt_memory": None,
    }


def test_process_ocpvirt_custom_config_custom_resources():
    cfg = process_ocpvirt_custom_config(
        ["ocpvirt_cpu=4", "ocpvirt_memory=16Gi", "pvc_batch_size=2"]
    )
    assert cfg == {
        "pvc_batch_size": 2,
        "vm_batch_size": 1,
        "ocpvirt_profile": None,
        "ocpvirt_cpu": 4,
        "ocpvirt_memory": "16Gi",
    }


def test_process_ocpvirt_custom_config_hyphen_aliases():
    cfg = process_ocpvirt_custom_config(["ocpvirt-cpu=8", "ocpvirt-memory=32"])
    assert cfg["ocpvirt_cpu"] == 8
    assert cfg["ocpvirt_memory"] == "32Gi"
    assert cfg["ocpvirt_profile"] is None


def test_process_ocpvirt_custom_config_cpu_memory_require_both():
    with pytest.raises(NodeError, match="must both be set"):
        process_ocpvirt_custom_config(["ocpvirt_cpu=4"])
    with pytest.raises(NodeError, match="must both be set"):
        process_ocpvirt_custom_config(["ocpvirt_memory=16Gi"])


def test_process_ocpvirt_custom_config_profile_mutually_exclusive():
    with pytest.raises(NodeError, match="mutually exclusive"):
        process_ocpvirt_custom_config(
            ["ocpvirt_profile=o1.large", "ocpvirt_cpu=4", "ocpvirt_memory=16Gi"]
        )


@patch("compute.openshift.get_k8s_clients")
def test_apply_ocpvirt_vm_profile_uses_cluster_instancetype(mock_clients):
    mock_clients.return_value = (_mock_custom_api(), MagicMock())
    params = apply_ocpvirt_vm_profile(
        {"image-name": "datasource://rhel9"},
        OCP_CRED,
        ["ocpvirt_profile=cx1.2xlarge"],
    )
    assert params["instancetype"] == "cx1.2xlarge"
    assert params["image-name"] == "datasource://rhel9"
    assert "cpu" not in params
    assert "memory" not in params


@patch("compute.openshift.get_k8s_clients")
def test_apply_ocpvirt_vm_profile_defaults_to_o1_large(mock_clients):
    mock_clients.return_value = (_mock_custom_api(), MagicMock())
    params = apply_ocpvirt_vm_profile(
        {"image-name": "datasource://rhel9"},
        OCP_CRED,
        None,
    )
    assert params["instancetype"] == "o1.large"


def _mock_core_api_limitrange(cpu_max="20", memory_max="64Gi"):
    core = MagicMock()
    container = MagicMock()
    container.type = "Container"
    container.max = {"cpu": cpu_max, "memory": memory_max}
    pod = MagicMock()
    pod.type = "Pod"
    pod.max = {"cpu": cpu_max, "memory": memory_max}
    lr = MagicMock()
    lr.spec.limits = [pod, container]
    core.list_namespaced_limit_range.return_value = MagicMock(items=[lr])
    return core


@patch("compute.openshift.get_k8s_clients")
def test_apply_ocpvirt_vm_profile_uses_custom_resources(mock_clients):
    mock_clients.return_value = (MagicMock(), _mock_core_api_limitrange())
    params = apply_ocpvirt_vm_profile(
        {"image-name": "datasource://rhel9", "instancetype": "o1.large"},
        OCP_CRED,
        ["ocpvirt_cpu=4", "ocpvirt_memory=16Gi"],
    )
    assert params["cpu"] == 4
    assert params["memory"] == "16Gi"
    assert "instancetype" not in params


@patch("compute.openshift.get_k8s_clients")
def test_apply_ocpvirt_vm_profile_rejects_over_limitrange_max(mock_clients):
    mock_clients.return_value = (
        MagicMock(),
        _mock_core_api_limitrange(cpu_max="16", memory_max="64Gi"),
    )
    with pytest.raises(NodeError, match="exceeds LimitRange max"):
        apply_ocpvirt_vm_profile(
            {"image-name": "datasource://rhel9"},
            OCP_CRED,
            ["ocpvirt_cpu=16", "ocpvirt_memory=16Gi"],
        )


def test_validate_ocpvirt_resources_against_limitrange_ok():
    core = _mock_core_api_limitrange(cpu_max="20", memory_max="64Gi")
    validate_ocpvirt_resources_against_limitrange(core, "ns", 16, "16Gi")


def test_get_namespace_resource_max_picks_lower():
    core = MagicMock()
    container = MagicMock()
    container.type = "Container"
    container.max = {"cpu": "20", "memory": "64Gi"}
    pod = MagicMock()
    pod.type = "Pod"
    pod.max = {"cpu": "16", "memory": "32Gi"}
    lr = MagicMock()
    lr.spec.limits = [pod, container]
    core.list_namespaced_limit_range.return_value = MagicMock(items=[lr])
    assert get_namespace_resource_max(core, "ns") == {
        "cpu": "16",
        "memory": "32Gi",
    }


@patch(
    "compute.openshift.list_cluster_instancetypes", return_value=CLUSTER_INSTANCE_TYPES
)
def test_resolve_ocpvirt_instancetype_unknown(mock_list):
    with pytest.raises(NodeError, match="Unknown ocpvirt_profile"):
        resolve_ocpvirt_instancetype(MagicMock(), "missing")


def test_build_virtualmachine_cr_with_instancetype():
    vm = build_virtualmachine_cr(
        node_name="ceph-test-node",
        namespace="test-ns",
        image_name="https://example.com/disk.qcow2",
        storage_class="nfs",
        network="bridge-504",
        root_disk_size="80Gi",
        instancetype_name="cx1.2xlarge",
        precreated_volume_names=[],
    )
    assert vm["spec"]["instancetype"] == {
        "kind": KUBEVIRT_CLUSTER_INSTANCETYPE_KIND,
        "name": "cx1.2xlarge",
    }
    domain = vm["spec"]["template"]["spec"]["domain"]
    assert "cpu" not in domain
    assert "memory" not in domain
    assert "resources" not in domain


def test_build_virtualmachine_cr_with_custom_resources():
    vm = build_virtualmachine_cr(
        node_name="ceph-test-node",
        namespace="test-ns",
        image_name="https://example.com/disk.qcow2",
        storage_class="nfs",
        network="bridge-504",
        root_disk_size="80Gi",
        precreated_volume_names=[],
        cpu=4,
        memory="16Gi",
    )
    assert "instancetype" not in vm["spec"]
    domain = vm["spec"]["template"]["spec"]["domain"]
    assert domain["cpu"] == {"cores": 4}
    assert domain["memory"] == {"guest": "16Gi"}
    cpu_req = 4 + OCPVIRT_RESOURCE_CPU_BUMP
    mem_req = f"{16 + OCPVIRT_RESOURCE_MEMORY_BUMP_GI}Gi"
    assert domain["resources"] == {
        "requests": {"cpu": cpu_req, "memory": mem_req},
        "limits": {"cpu": cpu_req, "memory": mem_req},
    }


def test_ocpvirt_domain_resources_bump():
    assert _memory_gib("16Gi") == 16
    assert _memory_gib("2048Mi") == 2
    cpu_req = 8 + OCPVIRT_RESOURCE_CPU_BUMP
    mem_req = f"{32 + OCPVIRT_RESOURCE_MEMORY_BUMP_GI}Gi"
    assert _ocpvirt_domain_resources(8, "32Gi") == {
        "requests": {"cpu": cpu_req, "memory": mem_req},
        "limits": {"cpu": cpu_req, "memory": mem_req},
    }
    with pytest.raises(NodeError, match="Invalid ocpvirt_memory"):
        _memory_gib("not-a-quantity")
    with pytest.raises(NodeError, match="multiple of 1024"):
        _memory_gib("100Mi")


def test_build_virtualmachine_cr_requires_sizing():
    with pytest.raises(NodeError, match="Either instancetype_name or both cpu"):
        build_virtualmachine_cr(
            node_name="ceph-test-node",
            namespace="test-ns",
            image_name="https://example.com/disk.qcow2",
            storage_class="nfs",
            network="bridge-504",
            root_disk_size="80Gi",
            instancetype_name="",
            precreated_volume_names=[],
        )


def test_build_virtualmachine_cr_rejects_profile_and_custom():
    with pytest.raises(NodeError, match="mutually exclusive"):
        build_virtualmachine_cr(
            node_name="ceph-test-node",
            namespace="test-ns",
            image_name="https://example.com/disk.qcow2",
            storage_class="nfs",
            network="bridge-504",
            root_disk_size="80Gi",
            instancetype_name="o1.large",
            precreated_volume_names=[],
            cpu=4,
            memory="16Gi",
        )


def test_validate_ocpvirt_credentials_auth_only():
    cred = validate_ocpvirt_credentials(AUTH_OSP_CRED)
    assert cred["token"] == "secret"
    assert "server" not in cred


def test_validate_ocpvirt_namespace_config_requires_server():
    with pytest.raises(NodeError, match="server is required"):
        validate_ocpvirt_namespace_config({"namespace": "ns", "storage_class": "nfs"})


def test_resolve_ocpvirt_image_name_short_name():
    resolved = resolve_ocpvirt_image_name("datasource://rhel9", OCP_CRED)
    assert resolved == "datasource://openshift-virtualization-os-images/rhel9"


def test_load_ocpvirt_namespace_config():
    cfg = load_ocpvirt_namespace_config(["ocpvirt_namespace=rdu3_ceph_jenkins"])
    assert cfg["server"] == "https://10.5.229.33:6443"
    assert "instance_types" not in cfg


def test_resolve_ocpvirt_credentials():
    merged = resolve_ocpvirt_credentials(
        AUTH_OSP_CRED, ["ocpvirt_namespace=rdu3_ceph_jenkins"]
    )
    assert merged["token"] == "secret"
    assert merged["server"] == "https://10.5.229.33:6443"


def test_validate_ocpvirt_inventory_resolves_datasource():
    params = validate_ocpvirt_inventory(
        inventory={
            "instance": {
                "setup": "#cloud-config\n",
                "create": {"image-name": "datasource://rhel9"},
            }
        },
        ceph_cluster={"name": "ceph"},
        ocp_cred=OCP_CRED,
    )
    assert (
        params["image-name"] == "datasource://openshift-virtualization-os-images/rhel9"
    )
    assert "cpu" not in params
    assert "memory" not in params


def test_validate_precreated_volume_names_rejects_none():
    with pytest.raises(NodeError, match="precreated_volume_names is required"):
        validate_precreated_volume_names(None)


def test_build_virtualmachine_cr_requires_precreated_volume_names():
    with pytest.raises(NodeError, match="precreated_volume_names is required"):
        build_virtualmachine_cr(
            node_name="ceph-test-node",
            namespace="test-ns",
            image_name="https://example.com/disk.qcow2",
            storage_class="nfs",
            network="bridge-504",
            root_disk_size="80Gi",
            instancetype_name="o1.large",
        )


def test_datavolume_names_from_vm_includes_root_and_precreated():
    vm = build_virtualmachine_cr(
        node_name="ceph-test-node",
        namespace="test-ns",
        image_name="https://example.com/disk.qcow2",
        storage_class="nfs",
        network="bridge-504",
        root_disk_size="80Gi",
        instancetype_name="o1.large",
        precreated_volume_names=["ceph-test-node-vol-0", "ceph-test-node-vol-1"],
    )
    names = datavolume_names_from_vm(vm)
    assert "ceph-test-node-root" in names
    assert "ceph-test-node-vol-0" in names
    assert "ceph-test-node-vol-1" in names
