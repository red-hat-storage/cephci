"""OCP Virtualization (KubeVirt) provider implementation for CephVMNode."""

import re
from copy import deepcopy
from time import sleep
from typing import Any, Dict, List, Optional

from kubernetes import client, config

from ceph.parallel import parallel
from ceph.waiter import WaitUntil
from utility.log import Log

from .exceptions import NodeDeleteFailure, NodeError

LOG = Log(__name__)

KUBEVIRT_GROUP = "kubevirt.io"
KUBEVIRT_VERSION = "v1"
CDI_GROUP = "cdi.kubevirt.io"
CDI_VERSION = "v1beta1"

VM_POLL_INTERVAL = 10
VM_POLL_TIMEOUT = 1200
IP_POLL_TIMEOUT = 600


def _sanitize_k8s_name(name: str) -> str:
    """Return a DNS-1123 compatible resource name (lowercase, hyphens)."""
    sanitized = name.lower().replace("_", "-").replace(".", "-")
    sanitized = re.sub(r"[^a-z0-9-]", "-", sanitized)
    sanitized = re.sub(r"-+", "-", sanitized).strip("-")
    return sanitized[:63].rstrip("-") or "ceph-vm"


def _parse_cpu(cpu: Any) -> int:
    """Normalize cpu value from inventory to an integer core count."""
    if cpu is None:
        return 4
    if isinstance(cpu, int):
        return cpu
    text = str(cpu).strip()
    if text.endswith("m"):
        # millicores → round up to whole cores (minimum 1)
        try:
            return max(1, (int(text[:-1]) + 999) // 1000)
        except ValueError:
            return 4
    try:
        return max(1, int(float(text)))
    except ValueError:
        return 4


def _disk_size_str(size_gib: Any, default: str = "40Gi") -> str:
    """Return a Kubernetes quantity string for disk size in GiB."""
    if size_gib is None or size_gib == "":
        return default
    if isinstance(size_gib, str) and size_gib.endswith(("Gi", "G", "Mi", "M", "Ti")):
        return size_gib
    try:
        return f"{int(size_gib)}Gi"
    except (TypeError, ValueError):
        return default


def _image_source(image_name: str) -> Dict[str, Any]:
    """
    Build a CDI DataVolume source from an inventory image-name.

    - http(s)://... → HTTP source
    - docker://... or registry path → registry source
    """
    if not image_name:
        raise NodeError("image-name is required for OCP Virt provisioning")

    image = image_name.strip()
    if image.startswith(("http://", "https://")):
        return {"http": {"url": image}}
    if image.startswith("docker://"):
        return {"registry": {"url": image}}
    # Bare image reference (e.g. quay.io/containerdisks/rhel:9)
    return {"registry": {"url": f"docker://{image}"}}


def get_k8s_clients():
    """
    Return (CustomObjectsApi, CoreV1Api) using ~/.kube/config.

    Raises:
        NodeError: if kubeconfig cannot be loaded.
    """
    try:
        config.load_kube_config()
    except Exception as exc:
        raise NodeError(
            "Failed to load kubeconfig from ~/.kube/config. "
            "Ensure 'oc' / kubectl is configured before using --cloud ocpvirt."
        ) from exc

    return client.CustomObjectsApi(), client.CoreV1Api()


def build_datavolume_spec(
    name: str,
    storage_class: str,
    size: str,
    source: Optional[Dict[str, Any]] = None,
    access_modes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build a DataVolume template/spec body (used in dataVolumeTemplates)."""
    spec: Dict[str, Any] = {
        "storage": {
            "accessModes": access_modes or ["ReadWriteOnce"],
            "resources": {"requests": {"storage": size}},
            "storageClassName": storage_class,
        }
    }
    if source:
        spec["source"] = source
    else:
        spec["source"] = {"blank": {}}
    return {
        "metadata": {"name": name},
        "spec": spec,
    }


def build_virtualmachine_cr(
    node_name: str,
    namespace: str,
    image_name: str,
    storage_class: str,
    network: str,
    cpu: Any,
    memory: str,
    root_disk_size: str,
    cloud_data: str = "",
    size_of_disks: int = 0,
    no_of_volumes: int = 0,
    access_modes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build a simple KubeVirt VirtualMachine CR with embedded DataVolume templates.
    """
    vm_name = _sanitize_k8s_name(node_name)
    root_dv = f"{vm_name}-root"
    cores = _parse_cpu(cpu)
    root_size = _disk_size_str(root_disk_size)
    modes = access_modes or ["ReadWriteOnce"]
    mem = memory or "8Gi"

    disks = [
        {"name": "rootdisk", "disk": {"bus": "virtio"}},
        {"name": "cloudinitdisk", "disk": {"bus": "virtio"}},
    ]
    volumes: List[Dict[str, Any]] = [
        {"name": "rootdisk", "dataVolume": {"name": root_dv}},
        {
            "name": "cloudinitdisk",
            "cloudInitConfigDrive": {"userData": cloud_data or "#cloud-config\n"},
        },
    ]
    data_volume_templates = [
        build_datavolume_spec(
            name=root_dv,
            storage_class=storage_class,
            size=root_size,
            source=_image_source(image_name),
            access_modes=modes,
        )
    ]

    for idx in range(int(no_of_volumes or 0)):
        vol_name = f"{vm_name}-vol-{idx}"
        disk_name = f"disk-{idx}"
        disks.append({"name": disk_name, "disk": {"bus": "virtio"}})
        volumes.append({"name": disk_name, "dataVolume": {"name": vol_name}})
        data_volume_templates.append(
            build_datavolume_spec(
                name=vol_name,
                storage_class=storage_class,
                size=_disk_size_str(size_of_disks, default="15Gi"),
                source=None,
                access_modes=modes,
            )
        )

    if not network or network == "default":
        interfaces = [{"name": "default", "masquerade": {}}]
        networks = [{"name": "default", "pod": {}}]
    else:
        interfaces = [{"name": "default", "bridge": {}}]
        networks = [{"name": "default", "multus": {"networkName": network}}]

    return {
        "apiVersion": f"{KUBEVIRT_GROUP}/{KUBEVIRT_VERSION}",
        "kind": "VirtualMachine",
        "metadata": {
            "name": vm_name,
            "namespace": namespace,
            "labels": {
                "app": "cephci",
                "cephci/node-name": vm_name,
            },
        },
        "spec": {
            "running": True,
            "dataVolumeTemplates": data_volume_templates,
            "template": {
                "metadata": {
                    "labels": {
                        "kubevirt.io/domain": vm_name,
                        "app": "cephci",
                    }
                },
                "spec": {
                    "domain": {
                        "cpu": {"cores": cores},
                        "resources": {"requests": {"memory": mem}},
                        "devices": {
                            "disks": disks,
                            "interfaces": interfaces,
                            "rng": {},
                        },
                        "features": {"smm": {"enabled": True}},
                        "firmware": {"bootloader": {"efi": {}}},
                    },
                    "networks": networks,
                    "volumes": volumes,
                },
            },
        },
    }


def estimate_pvc_requirement(ceph_cluster: dict) -> int:
    """
    Estimate PersistentVolumeClaims needed for the cluster layout.

    Each node needs 1 root DataVolume PVC plus one PVC per extra volume.
    CDI import may temporarily create prime/scratch PVCs (~1 extra per DV),
    so the peak requirement is about 2x the final PVC count.
    """
    final = 0
    for idx in range(1, 100):
        node = ceph_cluster.get(f"node{idx}")
        if not node:
            break
        final += 1 + int(node.get("no-of-volumes") or 0)
    # Peak during CDI import (DV PVC + temporary prime/scratch)
    return final * 2


def _quota_remaining_pvcs(status: dict) -> Optional[int]:
    """Return remaining PVC slots from a quota status, or None if not limited."""
    hard = (status or {}).get("hard") or {}
    used = (status or {}).get("used") or {}
    if "persistentvolumeclaims" not in hard:
        return None
    try:
        hard_n = int(hard["persistentvolumeclaims"])
        used_n = int(used.get("persistentvolumeclaims") or 0)
    except (TypeError, ValueError):
        return None
    return hard_n - used_n


def ensure_namespace_quota(namespace: str, pvc_needed: int) -> None:
    """
    Fail fast when namespace ResourceQuota / AppliedClusterResourceQuota cannot
    satisfy the estimated PVC requirement.

    Raises:
        NodeError: if remaining PVC quota is below pvc_needed.
    """
    if pvc_needed <= 0:
        return

    custom_api, core_api = get_k8s_clients()
    remaining_values: List[tuple] = []

    try:
        for rq in core_api.list_namespaced_resource_quota(namespace).items:
            status = rq.status.to_dict() if rq.status else {}
            rem = _quota_remaining_pvcs(status)
            if rem is not None:
                remaining_values.append((f"ResourceQuota/{rq.metadata.name}", rem))
    except Exception as exc:
        LOG.warning(f"Unable to list ResourceQuota in {namespace}: {exc}")

    # OpenShift AppliedClusterResourceQuota (tenant PVC caps)
    try:
        resp = custom_api.list_namespaced_custom_object(
            group="quota.openshift.io",
            version="v1",
            namespace=namespace,
            plural="appliedclusterresourcequotas",
        )
        for item in resp.get("items") or []:
            name = (item.get("metadata") or {}).get("name", "unknown")
            total = (item.get("status") or {}).get("total") or {}
            rem = _quota_remaining_pvcs(total)
            if rem is not None:
                remaining_values.append((f"AppliedClusterResourceQuota/{name}", rem))
    except Exception as exc:
        LOG.debug(f"AppliedClusterResourceQuota not available or failed: {exc}")

    if not remaining_values:
        LOG.info(f"No PVC ResourceQuota found in {namespace}; skipping quota pre-check")
        return

    # Binding constraint is the tightest remaining allowance
    quota_name, remaining = min(remaining_values, key=lambda x: x[1])
    LOG.info(
        f"PVC quota check: need {pvc_needed}, remaining {remaining} ({quota_name})"
    )
    if remaining < pvc_needed:
        raise NodeError(
            f"Insufficient PVC quota in namespace {namespace}: "
            f"need {pvc_needed} PVCs (including CDI import headroom), "
            f"but only {remaining} remaining under {quota_name}. "
            f"Reduce node/volume count in --global-conf or free PVCs, then retry."
        )


def cleanup_ocpvirt_ceph_nodes(osp_cred, pattern, custom_config=None):
    """
    Delete VirtualMachines in the configured namespace whose name contains pattern.

    Args:
        osp_cred: Global credential file with globals["ocpvirt-credentials"].
        pattern: Substring to match against VM names.
        custom_config: Unused; accepted for API parity with other cleanups.
    """
    del custom_config  # API parity
    LOG.info(f"Destroying existing OCP Virt VMs matching pattern {pattern}")
    glbs = osp_cred.get("globals") if osp_cred else None
    if not glbs:
        raise NodeError("Missing 'globals' section in OCP Virt credentials file")
    ocp_cfg = glbs.get("ocpvirt-credentials")
    if not ocp_cfg:
        raise NodeError("Missing 'ocpvirt-credentials' section in globals")

    namespace = ocp_cfg.get("namespace")
    if not namespace:
        raise NodeError("Missing 'namespace' in ocpvirt-credentials")

    custom_api, _ = get_k8s_clients()
    try:
        resp = custom_api.list_namespaced_custom_object(
            group=KUBEVIRT_GROUP,
            version=KUBEVIRT_VERSION,
            namespace=namespace,
            plural="virtualmachines",
        )
    except Exception as exc:
        LOG.warning(f"Failed to list VirtualMachines in {namespace}: {exc}")
        return

    items = resp.get("items") or []
    matched = [
        vm
        for vm in items
        if pattern
        and pattern.lower() in (vm.get("metadata", {}).get("name") or "").lower()
    ]
    LOG.info(
        f"Found {len(matched)} VMs matching pattern '{pattern}' in namespace {namespace}"
    )

    counter = 0
    with parallel() as p:
        for vm in matched:
            sleep(counter * 2)
            name = vm["metadata"]["name"]
            node = CephVMNodeOCP(ocp_cred=ocp_cfg, node=vm)
            p.spawn(node.delete)
            LOG.info(f"Scheduled delete for VM {name}")
            counter += 1

    LOG.info(f"Done cleaning up OCP Virt nodes with pattern {pattern}")


class CephVMNodeOCP:
    """Represent the VM node required for cephci on OCP Virtualization."""

    def __init__(
        self,
        ocp_cred: dict,
        node: Optional[dict] = None,
        node_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the instance.

        Args:
            ocp_cred: ocpvirt-credentials dict (namespace, storage_class, ...).
            node: Optional existing VirtualMachine object (for cleanup).
            node_name: Optional VM name to look up in the namespace.
        """
        self._ocp_cred = ocp_cred
        self.namespace = ocp_cred["namespace"]
        self._subnet: str = ""
        self._roles: list = list()
        self._volumes: list = list()
        self.node: Optional[dict] = None
        self.root_login: bool = True
        self.osd_scenario = None
        self.location = None
        self.id = None

        self.custom_api, self.core_api = get_k8s_clients()

        if node:
            self.node = node
        elif node_name:
            self.node = self._get_vm(_sanitize_k8s_name(node_name))

    def __getstate__(self) -> dict:
        """Exclude non-picklable API clients."""
        state = self.__dict__.copy()
        state.pop("custom_api", None)
        state.pop("core_api", None)
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state and recreate API clients."""
        self.__dict__.update(state)
        self.custom_api, self.core_api = get_k8s_clients()

    @property
    def ip_address(self) -> str:
        """Return the primary IP address of the VMI."""
        return self._get_vmi_ip() or ""

    @property
    def hostname(self) -> str:
        """Return the VM name (used as hostname until SSH connect refreshes it)."""
        if not self.node:
            return ""
        return self.node.get("metadata", {}).get("name", "")

    @property
    def volumes(self) -> List:
        """Return list of extra (non-root) data volume names attached to the VM."""
        if self._volumes:
            return self._volumes
        if not self.node:
            return []
        templates = self.node.get("spec", {}).get("dataVolumeTemplates") or []
        root_name = f"{self.hostname}-root"
        self._volumes = [
            t.get("metadata", {}).get("name")
            for t in templates
            if t.get("metadata", {}).get("name")
            and t.get("metadata", {}).get("name") != root_name
        ]
        return self._volumes

    @property
    def subnet(self) -> str:
        """Return subnet CIDR if known; empty for pod network."""
        return self._subnet

    @property
    def shortname(self) -> str:
        """Return the short form of the hostname."""
        return self.hostname.split(".")[0] if self.hostname else ""

    @property
    def no_of_volumes(self) -> int:
        """Return the number of extra volumes attached to the VM."""
        return len(self.volumes)

    @property
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        self._roles = deepcopy(roles)

    @property
    def node_type(self) -> str:
        """Return the provider type."""
        return "ocpvirt"

    def create(
        self,
        node_name: str,
        image_name: str,
        cloud_data: str = "",
        size_of_disks: int = 0,
        no_of_volumes: int = 0,
        cpu: Optional[Any] = None,
        memory: Optional[str] = None,
        root_disk_size: Optional[str] = None,
        storage_class: Optional[str] = None,
        network: Optional[str] = None,
    ) -> None:
        """
        Create a VirtualMachine (and DataVolumes) on OCP Virtualization.

        Args:
            node_name: Desired VM name (sanitized for K8s).
            image_name: Container-disk / HTTP image URL from inventory.
            cloud_data: cloud-init userdata from inventory.
            size_of_disks: Size in GiB for each additional blank disk.
            no_of_volumes: Number of additional blank DataVolumes.
            cpu / memory / root_disk_size:
                VM sizing from inventory; defaults applied if omitted.
            storage_class / network:
                Optional overrides; defaults come from ocpvirt-credentials.
        """
        cred = self._ocp_cred
        storage_class = storage_class or cred.get("storage_class")
        network = network if network is not None else cred.get("network", "default")
        cpu = cpu if cpu is not None else "4"
        memory = memory or "8Gi"
        root_disk_size = root_disk_size or "40Gi"
        access_modes = cred.get("access_modes") or ["ReadWriteOnce"]
        if isinstance(access_modes, str):
            access_modes = [access_modes]

        if not storage_class:
            raise NodeError("storage_class is required in ocpvirt-credentials")

        vm_body = build_virtualmachine_cr(
            node_name=node_name,
            namespace=self.namespace,
            image_name=image_name,
            storage_class=storage_class,
            network=network,
            cpu=cpu,
            memory=memory,
            root_disk_size=root_disk_size,
            cloud_data=cloud_data,
            size_of_disks=size_of_disks,
            no_of_volumes=no_of_volumes,
            access_modes=access_modes,
        )
        vm_name = vm_body["metadata"]["name"]
        LOG.info(f"Creating VirtualMachine {vm_name} in namespace {self.namespace}")

        try:
            self.custom_api.create_namespaced_custom_object(
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=self.namespace,
                plural="virtualmachines",
                body=vm_body,
            )
            self._wait_until_vm_ready(vm_name)
            self._wait_until_ip_known(vm_name)
            self.node = self._get_vm(vm_name)
            if not self.node:
                raise NodeError(
                    f"Failed to fetch VirtualMachine {vm_name} after create"
                )
            LOG.info(f"Created VirtualMachine {vm_name} with IP {self.ip_address}")
        except NodeError:
            raise
        except Exception as exc:
            LOG.error(exc, exc_info=True)
            raise NodeError(f"Failed to create VM {vm_name}: {exc}") from exc

    def delete(self) -> None:
        """Delete the VirtualMachine (DataVolumes owned via templates are cleaned up)."""
        if not self.node:
            return

        vm_name = self.node.get("metadata", {}).get("name")
        if not vm_name:
            self.node = None
            return

        LOG.info(f"Deleting VirtualMachine {vm_name} in {self.namespace}")
        try:
            self.custom_api.delete_namespaced_custom_object(
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=self.namespace,
                plural="virtualmachines",
                name=vm_name,
                body={},
            )
        except Exception as exc:
            # 404 is fine (already gone)
            status = getattr(exc, "status", None)
            if status == 404:
                LOG.info(f"VirtualMachine {vm_name} already deleted")
            else:
                LOG.warning(f"delete VirtualMachine failed: {exc}")
                raise NodeDeleteFailure(f"Failed to delete {vm_name}: {exc}") from exc

        self._wait_until_vm_deleted(vm_name)
        self.node = None
        LOG.info(f"Successfully removed {vm_name}")

    def shutdown(self, wait: bool = False) -> None:
        """Stop the VirtualMachine by setting spec.running=False."""
        if not self.node:
            return
        vm_name = self.hostname
        LOG.info(f"Shutting down VirtualMachine {vm_name}")
        self._patch_running(vm_name, False)
        if wait:
            self._wait_until_vmi_phase(vm_name, "Succeeded", allow_missing=True)

    def power_on(self) -> None:
        """Start the VirtualMachine by setting spec.running=True."""
        if not self.node:
            return
        vm_name = self.hostname
        LOG.info(f"Powering on VirtualMachine {vm_name}")
        self._patch_running(vm_name, True)
        self._wait_until_vm_ready(vm_name)
        self.node = self._get_vm(vm_name)

    def get_private_ip(self) -> str:
        """Return the private IP address of the VM (alias for ip_address)."""
        return self.ip_address

    # --- private helpers ---

    def _patch_running(self, vm_name: str, running: bool) -> None:
        body = {"spec": {"running": running}}
        self.custom_api.patch_namespaced_custom_object(
            group=KUBEVIRT_GROUP,
            version=KUBEVIRT_VERSION,
            namespace=self.namespace,
            plural="virtualmachines",
            name=vm_name,
            body=body,
        )

    def _get_vm(self, vm_name: str) -> Optional[dict]:
        try:
            return self.custom_api.get_namespaced_custom_object(
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=self.namespace,
                plural="virtualmachines",
                name=vm_name,
            )
        except Exception as exc:
            status = getattr(exc, "status", None)
            if status == 404:
                return None
            LOG.warning(f"get VirtualMachine {vm_name} failed: {exc}")
            return None

    def _get_vmi(self, vm_name: str) -> Optional[dict]:
        try:
            return self.custom_api.get_namespaced_custom_object(
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=self.namespace,
                plural="virtualmachineinstances",
                name=vm_name,
            )
        except Exception as exc:
            status = getattr(exc, "status", None)
            if status == 404:
                return None
            LOG.warning(f"get VMI {vm_name} failed: {exc}")
            return None

    def _get_vmi_ip(self) -> Optional[str]:
        if not self.node:
            return None
        vm_name = self.node.get("metadata", {}).get("name")
        if not vm_name:
            return None
        vmi = self._get_vmi(vm_name)
        if not vmi:
            return None
        interfaces = (vmi.get("status") or {}).get("interfaces") or []
        for iface in interfaces:
            ip = iface.get("ipAddress") or iface.get("ipAddresses", [None])[0]
            if ip:
                return ip
        # Fallback: some clusters put IPs under status.interfaces[].ipAddresses
        for iface in interfaces:
            ips = iface.get("ipAddresses") or []
            if ips:
                return ips[0]
        return None

    def _wait_until_vm_ready(
        self, vm_name: str, timeout: int = VM_POLL_TIMEOUT
    ) -> None:
        """Wait until VM printableStatus/Ready condition is True."""
        for w in WaitUntil(timeout=timeout, interval=VM_POLL_INTERVAL):
            vm = self._get_vm(vm_name)
            if not vm:
                continue
            status = vm.get("status") or {}
            printable = status.get("printableStatus", "")
            ready = False
            for cond in status.get("conditions") or []:
                if cond.get("type") == "Ready" and cond.get("status") == "True":
                    ready = True
                    break
            if ready or printable == "Running":
                LOG.info(f"VirtualMachine {vm_name} is ready ({printable})")
                return
            if printable in ("ErrorUnschedulable", "ErrImagePull", "ImagePullBackOff"):
                raise NodeError(f"VirtualMachine {vm_name} failed: {printable}")
        if w.expired:
            raise NodeError(f"VirtualMachine {vm_name} not ready within {timeout}s")

    def _wait_until_ip_known(
        self, vm_name: str, timeout: int = IP_POLL_TIMEOUT
    ) -> None:
        """Poll VMI until an IP address is assigned."""
        for w in WaitUntil(timeout=timeout, interval=VM_POLL_INTERVAL):
            # Refresh node so ip_address can read VMI
            self.node = self._get_vm(vm_name) or self.node
            ip = self._get_vmi_ip()
            if ip:
                LOG.info(f"VirtualMachine {vm_name} has IP {ip}")
                return
        if w.expired:
            raise NodeError(f"VirtualMachine {vm_name} has no IP within {timeout}s")

    def _wait_until_vm_deleted(
        self, vm_name: str, timeout: int = VM_POLL_TIMEOUT
    ) -> None:
        for w in WaitUntil(timeout=timeout, interval=VM_POLL_INTERVAL):
            if self._get_vm(vm_name) is None:
                return
        if w.expired:
            raise NodeDeleteFailure(
                f"VirtualMachine {vm_name} still present after delete"
            )

    def _wait_until_vmi_phase(
        self,
        vm_name: str,
        phase: str,
        timeout: int = VM_POLL_TIMEOUT,
        allow_missing: bool = False,
    ) -> None:
        for w in WaitUntil(timeout=timeout, interval=VM_POLL_INTERVAL):
            vmi = self._get_vmi(vm_name)
            if vmi is None:
                if allow_missing:
                    return
                continue
            current = (vmi.get("status") or {}).get("phase")
            if current == phase:
                return
        if w.expired:
            raise NodeError(
                f"VMI {vm_name} did not reach phase {phase} within {timeout}s"
            )
